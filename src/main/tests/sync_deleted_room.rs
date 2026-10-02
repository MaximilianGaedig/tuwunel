#![cfg(test)]

//! A client learns that a room was deleted under it.
//!
//! Deleting a room makes its local users leave without an event in the room,
//! and then purges it. A sync that runs between the two - which a waiting long
//! poll does, woken by the leave - has a left room to report with no leave
//! event in it, or a room whose state is already gone. A client knows it left
//! a room from its own membership event and from nothing else, and a left
//! room is told once: so each of these left the room in the client's list
//! until its cache was cleared.
//!
//! Each stage of a delete is stopped at here, and the sync from before it must
//! carry the user's own leave.

mod client;

use std::{fs::remove_dir_all, net::TcpListener};

use futures::future::join;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, implement,
	ruma::{RoomId, UserId},
};
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

const ALICE: &str = "sync-deleted-room-access-token-alice";

/// The state fields a sync may answer with, and the timeline.
const EVENT_LISTS: [&str; 4] = ["state", "state_after", "org.matrix.msc4222.state_after", "timeline"];

#[test]
fn a_deleted_room_is_reported_as_left_at_every_stage_of_the_delete() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = Args::test_database_path("sync-deleted-room");

	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option(format!("database_path={db_path:?}"))
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("allow_local_presence=false")
		.with_option("allow_outgoing_presence=false")
		.with_option("client_sync_timeout_min=200");

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

	let alice_id = register(services, "deletedalice", ALICE).await?;
	let alice = Client { services, base, token: ALICE };
	let public = json!({ "preset": "public_chat" });

	let evicted = alice.create_room(&public).await?;
	let stateless = alice.create_room(&public).await?;
	let purged = alice.create_room(&public).await?;
	let kept = alice.create_room(&public).await?;

	let opening = alice.sync(None, false).await?;
	let since = field(&opening, "next_batch")?.to_owned();

	// Its users are out, and nothing of it is purged yet: the room is all
	// there, and holds no event saying that Alice left.
	{
		let state_lock = services.state.mutex.lock(&evicted).await;
		services
			.delete
			.shutdown_room(&evicted, &state_lock)
			.await;
	}

	// Part-way through the purge: its state is gone, the room is not.
	{
		let state_lock = services.state.mutex.lock(&stateless).await;
		services
			.delete
			.shutdown_room(&stateless, &state_lock)
			.await;

		services
			.db
			.get("roomid_shortstatehash")?
			.remove(&*stateless);
	}

	// The whole delete.
	{
		let state_lock = services.state.mutex.lock(&purged).await;
		services
			.delete
			.delete_room(&purged, false, state_lock)
			.await?;
	}

	for state_after in [false, true] {
		let response = alice.sync(Some(&since), state_after).await?;

		for (room, stage) in [
			(&evicted, "its users evicted and nothing purged"),
			(&stateless, "its state purged"),
			(&purged, "all of it purged"),
		] {
			if !tells_own_leave(&response, room, &alice_id) {
				return Err(err!(
					"A room with {stage} was not reported to its user as left \
					 (state_after={state_after}): {}",
					response["rooms"]["leave"][room.as_str()]
				));
			}
		}

		if !response["rooms"]["leave"][kept.as_str()].is_null() {
			return Err(err!("A room nobody deleted was reported as left"));
		}
	}

	Ok(())
}

/// Whether the sync says the room is left and carries the user's own leave.
fn tells_own_leave(response: &Value, room: &RoomId, user: &UserId) -> bool {
	let left = &response["rooms"]["leave"][room.as_str()];

	EVENT_LISTS
		.iter()
		.filter_map(|list| left[*list]["events"].as_array())
		.flatten()
		.any(|event| {
			event["type"] == "m.room.member"
				&& event["state_key"] == user.as_str()
				&& event["content"]["membership"] == "leave"
		})
}

/// One sync, optionally resuming from a token, in either state format.
#[implement(Client, params = "<'_>")]
async fn sync(&self, since: Option<&str>, state_after: bool) -> Result<Value> {
	let since = since.map(|since| ("since", since));
	let state_after = state_after.then_some(("org.matrix.msc4222.use_state_after", "true"));

	self.services
		.client
		.clients
		.default
		.get(self.url("sync"))
		.bearer_auth(self.token)
		.query(&[("timeout", "0")])
		.query(since.as_slice())
		.query(state_after.as_slice())
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}
