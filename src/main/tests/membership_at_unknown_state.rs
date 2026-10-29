#![cfg(test)]

//! MSC4115: an event this server holds no state snapshot for (sent before
//! the server joined the room) has no known membership, so it must not be
//! reported as `leave`. Clients read `leave` as "sent while you were not a
//! member" and show "You don't have access to this message" (MEO-206).

use std::{fs::remove_dir_all, path::PathBuf};

use serde_json::json;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result,
	matrix::PduEvent,
	ruma::{events::room::member::MembershipState, user_id},
};
use tuwunel_service::Services;

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) {
		remove_dir_all(&self.0).ok();
	}
}

#[test]
fn membership_at_event_without_state_is_unknown() -> Result {
	let db_path = DatabasePath(Args::test_database_path("membership-at-unknown-state"));

	let mut args = Args::default_test(&["fresh", "cleanup"]);

	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", db_path.0));

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let outcome = exercise(&services).await;
		let shutdown = server.server.shutdown();

		drop(services);

		let run = async_run(&server).await;
		let stop = async_stop(&server).await;

		outcome.and(shutdown).and(run).and(stop)
	});

	drop(runtime);

	result
}

async fn exercise(services: &Services) -> Result {
	let reader = user_id!("@reader:localhost");

	// Never stored, so the server has no state snapshot for it, like a
	// message fetched from before this server joined the room.
	let message = pdu(json!({
		"type": "m.room.encrypted",
		"event_id": "$sent-before-join:remote.test",
		"sender": "@sender:remote.test",
		"content": { "algorithm": "m.megolm.v1.aes-sha2" },
	}))?;

	let membership = services
		.state_accessor
		.user_membership_at_pdu(reader, &message)
		.await;

	assert_eq!(membership, None, "unknown state must not be reported as a membership");

	// The reader's own member event still carries its membership.
	let invite = pdu(json!({
		"type": "m.room.member",
		"event_id": "$invite:remote.test",
		"sender": "@sender:remote.test",
		"state_key": reader,
		"content": { "membership": "invite" },
	}))?;

	let membership = services
		.state_accessor
		.user_membership_at_pdu(reader, &invite)
		.await;

	assert_eq!(membership, Some(MembershipState::Invite));

	Ok(())
}

fn pdu(fields: serde_json::Value) -> Result<PduEvent> {
	let mut value = json!({
		"room_id": "!membership-unknown:remote.test",
		"prev_events": [],
		"auth_events": [],
		"origin_server_ts": 1,
		"depth": 1,
		"hashes": { "sha256": "" },
	});

	value
		.as_object_mut()
		.expect("object")
		.extend(fields.as_object().expect("object").clone());

	// From text: event content is held as raw JSON, which a `Value` cannot fill.
	Ok(serde_json::from_str(&value.to_string())?)
}
