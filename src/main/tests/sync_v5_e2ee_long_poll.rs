#![cfg(test)]

//! A long poll on an e2ee-only sliding sync connection waits for news
//! (MEO-200).
//!
//! Element X keeps a second connection, `encryption`, that asks only for the
//! to-device and e2ee extensions. The e2ee extension reports the device's
//! unused fallback key types on every pass, and those counted as content. So
//! a message, receipt or typing notice in any of the user's rooms woke the
//! poll, and it answered at once with nothing but the fallback key types. On
//! a busy account that was a sync every second or two instead of one every
//! 30 s. The poll must now keep waiting through such activity, still answer
//! at once for real news (a to-device message), and still answer promptly
//! after this device shared a room key, which matrix-rust-sdk backs up only
//! when an e2ee answer arrives.

#[expect(
	dead_code,
	reason = "the shared client harness exposes helpers used by sibling integration tests"
)]
mod client;

use std::{
	fs::remove_dir_all,
	net::TcpListener,
	time::{Duration, Instant},
};

use futures::future::join;
use serde_json::{Value, json};
use tokio::time::sleep;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Err, Result, implement, ruma::RoomId};
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

const READER_TOKEN: &str = "sync-v5-e2ee-long-poll-reader-token-00";
const OTHER_TOKEN: &str = "sync-v5-e2ee-long-poll-other-token-000";

/// The poll every round asks for, in milliseconds.
const POLL: u64 = 5_000;

/// How far short of its poll a round that waited may answer.
const SLACK: Duration = Duration::from_millis(200);

/// How long a round with news may take. Far below `POLL`, so a round that
/// sat out its poll fails.
const NEWS_DEADLINE: Duration = Duration::from_secs(2);

/// How long into a round the activity happens, so it wakes a waiting poll
/// rather than landing before its first pass.
const ACTIVITY_DELAY: Duration = Duration::from_millis(500);

#[test]
fn e2ee_long_poll_waits_through_unrelated_activity() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = Args::test_database_path("sync-v5-e2ee-long-poll");
	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option(format!("database_path={db_path:?}"))
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("allow_local_presence=false")
		.with_option("allow_outgoing_presence=false");

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let exercise = async {
			let outcome = exercise(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};

		let (run, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run.and(outcome)
	});

	drop(runtime);
	remove_dir_all(&db_path).ok();

	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let reader_id = register(services, "e2eereader", READER_TOKEN).await?;
	let other_id = register(services, "e2eeother", OTHER_TOKEN).await?;
	let reader = Client { services, base, token: READER_TOKEN };
	let other = Client { services, base, token: OTHER_TOKEN };

	let reader_device = reader.device_id().await?;
	let other_device = other.device_id().await?;

	// A room both are in: what happens there wakes the reader's watchers.
	let room_id = reader
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;
	other
		.post(&format!("rooms/{room_id}/join"), &json!({}))
		.await?;

	let opening = reader.sync_encryption(None).await?;
	let mut pos = field(&opening, "pos")?.to_owned();

	// 1. The trigger seen on Element X: a message (and typing) in one of the
	// reader's rooms during the poll.
	let activity = async {
		sleep(ACTIVITY_DELAY).await;
		other.send_message(&room_id, "woken-1").await?;
		other
			.put(
				&format!("rooms/{room_id}/typing/{other_id}"),
				&json!({ "typing": true, "timeout": 10_000 }),
			)
			.await?;
		other.send_message(&room_id, "woken-2").await
	};

	let (round, activity) = join(reader.timed_poll(&pos), activity).await;
	activity?;
	let (response, waited) = round?;
	if waited + SLACK < Duration::from_millis(POLL) {
		return Err!(
			"activity in a room ended the e2ee long poll after {waited:?} with {response}"
		);
	}

	pos = field(&response, "pos")?.to_owned();

	// 2. Activity elsewhere on the server between two polls, so the poll's
	// first pass runs (upstream tuwunel #658).
	other
		.create_room(&json!({ "preset": "private_chat" }))
		.await?;

	let (response, waited) = reader.timed_poll(&pos).await?;
	if waited + SLACK < Duration::from_millis(POLL) {
		return Err!("activity elsewhere ended the e2ee long poll after {waited:?}: {response}");
	}

	if !response["extensions"]["e2ee"]["device_unused_fallback_key_types"].is_array() {
		return Err!("a poll that waited no longer carries the fallback key types: {response}");
	}

	pos = field(&response, "pos")?.to_owned();

	// 3. Real news still answers at once: a to-device message for the reader.
	let news = async {
		sleep(ACTIVITY_DELAY).await;
		other
			.send_to_device(&reader_id, &reader_device, "news")
			.await
	};

	let (round, news) = join(reader.timed_poll(&pos), news).await;
	news?;
	let (response, waited) = round?;
	if waited > NEWS_DEADLINE {
		return Err!("a to-device message waited for the poll ({waited:?})");
	}

	if !response["extensions"]["to_device"]["events"].is_array() {
		return Err!("the round with a to-device message did not carry it: {response}");
	}

	pos = field(&response, "pos")?.to_owned();

	// 4. The reader's device shares a room key and sends: matrix-rust-sdk backs
	// the new key up on the next e2ee answer, so it must come promptly.
	let own = async {
		sleep(ACTIVITY_DELAY).await;
		reader
			.send_to_device(&other_id, &other_device, "room-key")
			.await?;
		reader.send_message(&room_id, "own").await
	};

	let (round, own) = join(reader.timed_poll(&pos), own).await;
	own?;
	let (response, waited) = round?;
	if waited > NEWS_DEADLINE {
		return Err!("the reader's own key share waited for the poll ({waited:?})");
	}

	if !response["extensions"]["e2ee"]["device_unused_fallback_key_types"].is_array() {
		return Err!("the answer after a key share carries no e2ee extension: {response}");
	}

	Ok(())
}

/// Element X's `encryption` connection: to-device and e2ee, nothing else.
#[implement(Client, params = "<'_>")]
async fn sync_encryption(&self, resume: Option<(&str, u64)>) -> Result<Value> {
	let body = json!({
		"conn_id": "encryption",
		"extensions": {
			"to_device": { "enabled": true },
			"e2ee": { "enabled": true },
		},
	});

	let query =
		resume.map_or_else(String::new, |(pos, timeout)| format!("?pos={pos}&timeout={timeout}"));

	let url = format!(
		"{}/_matrix/client/unstable/org.matrix.simplified_msc3575/sync{query}",
		self.base
	);

	self.services
		.client
		.clients
		.default
		.post(url)
		.bearer_auth(self.token)
		.json(&body)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}

/// One long poll from `pos`, with how long it took.
#[implement(Client, params = "<'_>")]
async fn timed_poll(&self, pos: &str) -> Result<(Value, Duration)> {
	let started = Instant::now();
	let response = self.sync_encryption(Some((pos, POLL))).await?;

	Ok((response, started.elapsed()))
}

#[implement(Client, params = "<'_>")]
async fn device_id(&self) -> Result<String> {
	let whoami = self
		.services
		.client
		.clients
		.default
		.get(self.url("account/whoami"))
		.bearer_auth(self.token)
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;

	Ok(field(&whoami, "device_id")?.to_owned())
}

#[implement(Client, params = "<'_>")]
async fn send_message(&self, room_id: &RoomId, txn_id: &str) -> Result<Value> {
	self.put(
		&format!("rooms/{room_id}/send/m.room.message/{txn_id}"),
		&json!({ "msgtype": "m.text", "body": txn_id }),
	)
	.await
}

#[implement(Client, params = "<'_>")]
async fn send_to_device(
	&self,
	user_id: impl std::fmt::Display,
	device_id: &str,
	txn_id: &str,
) -> Result<Value> {
	self.put(
		&format!("sendToDevice/m.room.encrypted/{txn_id}"),
		&json!({ "messages": { user_id.to_string(): { device_id: { "algorithm": "m.olm.v1.curve25519-aes-sha2", "ciphertext": {} } } } }),
	)
	.await
}

#[implement(Client, params = "<'_>")]
async fn put(&self, path: &str, body: &Value) -> Result<Value> {
	self.services
		.client
		.clients
		.default
		.put(self.url(path))
		.bearer_auth(self.token)
		.json(body)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}
