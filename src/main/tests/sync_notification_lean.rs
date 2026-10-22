#![cfg(test)]

//! A notification client's sliding sync connection is sent only the global
//! account data a notification needs (`sync_notification_lean`).
//!
//! Element X's notification extension opens a new `notifications`
//! connection for every push and asks for the account data extension. All
//! of a user's global account data came back each time, most of the bytes of
//! every push, while the notification client reads only the push rules,
//! `m.direct` and the ignored users. That connection must now carry just
//! those, still with the subscribed room's own account data, and any other
//! connection must still be sent everything.

#[expect(
	dead_code,
	reason = "the shared client harness exposes helpers used by sibling integration tests"
)]
mod client;

use std::{collections::BTreeSet, fs::remove_dir_all, net::TcpListener};

use futures::future::join;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Err, Result, implement, ruma::RoomId};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

const TOKEN: &str = "sync-notification-lean-access-token-00";

/// What the notification client reads.
const NEEDED: [&str; 3] = ["m.push_rules", "m.direct", "m.ignored_user_list"];

/// Global account data a notification never reads.
const UNNEEDED: [&str; 2] = ["im.vector.setting.breadcrumbs", "org.example.meo195.large"];

/// Room account data of the room the push is for.
const ROOM_TYPE: &str = "org.example.meo195.room";

#[test]
fn notification_connection_gets_only_what_a_notification_reads() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = Args::test_database_path("sync-notification-lean");
	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option(format!("database_path={db_path:?}"))
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("allow_local_presence=false")
		.with_option("allow_outgoing_presence=false")
		.with_option("sync_notification_lean=true");

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

	let user_id = register(services, "nselean", TOKEN).await?;
	let alice = Client { services, base, token: TOKEN };
	let room_id = alice
		.create_room(&json!({ "preset": "private_chat" }))
		.await?;

	let global = |kind: &str| format!("user/{user_id}/account_data/{kind}");
	alice
		.put(&global("m.direct"), &json!({ "@bob:example.org": [room_id] }))
		.await?;
	alice
		.put(
			&global("m.ignored_user_list"),
			&json!({ "ignored_users": { "@spam:example.org": {} } }),
		)
		.await?;
	alice
		.put(&global(UNNEEDED[0]), &json!({ "recent_rooms": [room_id] }))
		.await?;
	alice
		.put(&global(UNNEEDED[1]), &json!({ "padding": "x".repeat(4096) }))
		.await?;
	alice
		.put(
			&format!("user/{user_id}/rooms/{room_id}/account_data/{ROOM_TYPE}"),
			&json!({ "kept": true }),
		)
		.await?;

	let main = alice.sync_v5("main", &room_id).await?;
	let main_types = global_types(&main);
	for kind in NEEDED.iter().chain(&UNNEEDED) {
		if !main_types.contains(*kind) {
			return Err!("a new main connection was not sent {kind}: {main_types:?}");
		}
	}

	let notification = alice.sync_v5("notifications", &room_id).await?;
	let served = global_types(&notification);
	let expected: BTreeSet<String> = NEEDED.iter().map(ToString::to_string).collect();
	if served != expected {
		return Err!("the notification connection was sent {served:?}, not {expected:?}");
	}

	if !room_types(&notification, &room_id).contains(ROOM_TYPE) {
		return Err!("the notification connection lost the subscribed room's account data");
	}

	Ok(())
}

fn global_types(response: &Value) -> BTreeSet<String> {
	types(&response["extensions"]["account_data"]["global"])
}

fn room_types(response: &Value, room_id: &RoomId) -> BTreeSet<String> {
	types(&response["extensions"]["account_data"]["rooms"][room_id.as_str()])
}

fn types(events: &Value) -> BTreeSet<String> {
	events
		.as_array()
		.into_iter()
		.flatten()
		.filter_map(|event| event["type"].as_str())
		.map(ToOwned::to_owned)
		.collect()
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

/// A new sliding sync connection shaped like Element X's notification sync:
/// a subscription to the pushed room and the account data extension.
#[implement(Client, params = "<'_>")]
async fn sync_v5(&self, conn_id: &str, room_id: &RoomId) -> Result<Value> {
	let url = format!(
		"{}/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?timeout=0",
		self.base
	);

	let body = json!({
		"conn_id": conn_id,
		"lists": {},
		"room_subscriptions": {
			room_id.as_str(): {
				"required_state": [["m.room.name", ""], ["m.room.power_levels", ""]],
				"timeline_limit": 16,
			},
		},
		"extensions": { "account_data": { "enabled": true } },
	});

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
