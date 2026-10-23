#![cfg(test)]

//! A knocker, like an invitee, sees the room listed but none of its events
//! (upstream 5aedf8395). Upstream boots through a `fixture` module the fork's
//! harness does not have yet, so the boot is spelled out here as in the
//! fork's other integration tests; the exercise is upstream's.

#[expect(
	dead_code,
	reason = "the shared client harness exposes helpers used by sibling integration tests"
)]
mod client;

use std::{fs::remove_dir_all, net::TcpListener};

use futures::future::join;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::Result;
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

const OWNER_TOKEN: &str = "sync-v5-knock-timeline-owner-access-token";
const KNOCKER_TOKEN: &str = "sync-v5-knock-timeline-knocker-access-token";

#[test]
fn knocked_room_carries_no_timeline() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = Args::test_database_path("sync-v5-knock-timeline");
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
	register(services, "knockowner", OWNER_TOKEN).await?;
	register(services, "knocker", KNOCKER_TOKEN).await?;

	let owner = Client { services, base, token: OWNER_TOKEN };
	let knocker = Client { services, base, token: KNOCKER_TOKEN };
	let knock = json!({ "type": "m.room.join_rules", "content": { "join_rule": "knock" } });
	let room_id = owner
		.create_room(&json!({ "preset": "private_chat", "initial_state": [knock] }))
		.await?;

	knocker
		.post(&format!("knock/{room_id}"), &json!({}))
		.await?;

	let url = format!("{base}/_matrix/client/unstable/org.matrix.simplified_msc3575/sync");
	let lists = json!({ "lists": { "all": { "ranges": [[0, 9]], "timeline_limit": 10 } } });
	let response: Value = services
		.client
		.clients
		.default
		.post(url)
		.bearer_auth(KNOCKER_TOKEN)
		.json(&lists)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	let room = &response["rooms"][room_id.as_str()];
	let timeline = room["timeline"].as_array();

	assert_eq!(room["membership"], "knock", "knocked room missing from the list: {response}");
	assert!(timeline.is_none_or(Vec::is_empty), "knocker reads the timeline: {room}");

	Ok(())
}
