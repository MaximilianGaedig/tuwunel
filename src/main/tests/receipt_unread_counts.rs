#![cfg(test)]

//! A read receipt leaves a room's unread counts at what is still unread after
//! it, and a receipt behind the user's current one changes nothing.

use std::{fs::remove_dir_all, net::TcpListener, path::PathBuf, time::Duration};

use futures::{StreamExt, future::join};
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err, implement,
	ruma::{EventId, OwnedEventId, RoomId, UserId},
	utils::{BoolExt, ReadyExt},
};
use tuwunel_service::Services;

use self::client::{Client, field, poll_until, register, wait_until_ready};

mod client;

const COUNT_DEADLINE: Duration = Duration::from_secs(5);
const READER_TOKEN: &str = "receipt-unread-counts-reader-token";
const SENDER_TOKEN: &str = "receipt-unread-counts-sender-token";

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn receipt_unread_counts() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = DatabasePath(Args::test_database_path("receipt-unread-counts"));

	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option(format!("database_path={:?}", db_path.0))
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true");

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

		let (run_result, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run_result?;

		outcome
	});

	drop(runtime);

	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let reader_id = register(services, "unreadreader", READER_TOKEN).await?;

	register(services, "unreadsender", SENDER_TOKEN).await?;

	let reader = Client { services, base, token: READER_TOKEN };
	let sender = Client { services, base, token: SENDER_TOKEN };

	// Two members, so every message notifies through the one-to-one rule; the
	// last one also mentions the reader and highlights.
	let room = sender
		.create_room(&json!({ "preset": "private_chat", "invite": [reader_id] }))
		.await?;

	reader.join(&room).await?;

	let first = sender
		.message(&room, "first", "one", None)
		.await?;
	let second = sender
		.message(&room, "second", "two", None)
		.await?;
	let third = sender
		.message(&room, "third", "three", Some(&reader_id))
		.await?;

	counts_reach(services, &reader_id, &room, (3, 1), "after the three messages").await?;

	reader.receipt(&room, "m.read", &first).await?;
	expect_counts(services, &reader_id, &room, (2, 1), "a receipt on the first message").await?;

	reader.receipt(&room, "m.read", &first).await?;
	expect_counts(services, &reader_id, &room, (2, 1), "the same receipt again").await?;

	reader
		.receipt(&room, "m.read.private", &second)
		.await?;
	expect_counts(services, &reader_id, &room, (1, 1), "a private receipt on the second").await?;

	// The public receipt advances to where the private one already is: the
	// counts stay what is unread after that point.
	reader.receipt(&room, "m.read", &second).await?;
	expect_counts(services, &reader_id, &room, (1, 1), "a public receipt catching up").await?;

	reader.receipt(&room, "m.read", &third).await?;
	expect_counts(services, &reader_id, &room, (0, 0), "a receipt on the last message").await?;

	// A receipt behind the current one is accepted but ignored.
	let fourth = sender
		.message(&room, "fourth", "four", None)
		.await?;
	counts_reach(services, &reader_id, &room, (1, 0), "after a fourth message").await?;

	reader.receipt(&room, "m.read", &first).await?;
	expect_counts(services, &reader_id, &room, (1, 0), "a receipt behind the current one")
		.await?;

	let stored: Vec<String> = services
		.read_receipt
		.readreceipts_since(&room, 0, None)
		.ready_filter_map(|(user_id, _, event)| {
			(user_id == &*reader_id).then(|| event.json().get().to_owned())
		})
		.collect()
		.await;

	if stored.len() != 1 || !stored[0].contains(third.as_str()) {
		return Err!("the stored receipt is not the one on {third}: {stored:?}");
	}

	reader.receipt(&room, "m.read", &fourth).await?;
	expect_counts(services, &reader_id, &room, (0, 0), "a receipt on the fourth message").await
}

async fn counts(services: &Services, user_id: &UserId, room_id: &RoomId) -> (u64, u64) {
	join(
		services
			.pusher
			.notification_count(user_id, room_id),
		services.pusher.highlight_count(user_id, room_id),
	)
	.await
}

async fn expect_counts(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	expected: (u64, u64),
	what: &str,
) -> Result {
	let actual = counts(services, user_id, room_id).await;
	if actual != expected {
		return Err!(
			"{what}: unread (notifications, highlights) {actual:?}, wanted {expected:?}"
		);
	}

	Ok(())
}

async fn counts_reach(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	expected: (u64, u64),
	what: &str,
) -> Result {
	poll_until(COUNT_DEADLINE, async || counts(services, user_id, room_id).await == expected)
		.await
		.into_option()
		.ok_or_else(|| err!("{what}: counts never reached {expected:?}"))
}

#[implement(Client, params = "<'_>")]
async fn join(&self, room_id: &RoomId) -> Result {
	self.post(&format!("rooms/{room_id}/join"), &json!({}))
		.await
		.map(drop)
}

#[implement(Client, params = "<'_>")]
async fn message(
	&self,
	room_id: &RoomId,
	txn: &str,
	body: &str,
	mention: Option<&UserId>,
) -> Result<OwnedEventId> {
	let mentions: Vec<_> = mention.into_iter().collect();
	let content = json!({
		"msgtype": "m.text",
		"body": body,
		"m.mentions": { "user_ids": mentions },
	});

	let response: Value = self
		.services
		.client
		.clients
		.default
		.put(self.url(&format!("rooms/{room_id}/send/m.room.message/{txn}")))
		.bearer_auth(self.token)
		.json(&content)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	Ok(field(&response, "event_id")?.try_into()?)
}

#[implement(Client, params = "<'_>")]
async fn receipt(&self, room_id: &RoomId, kind: &str, event_id: &EventId) -> Result {
	self.post(&format!("rooms/{room_id}/receipt/{kind}/{event_id}"), &json!({}))
		.await
		.map(drop)
}
