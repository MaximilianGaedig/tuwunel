//! Lets a bridge import a chat's history: messages behind a room's existing
//! ones, or a run stamped with the times they were originally sent, under the
//! event IDs the bridge chose for them. Without this a bridge can only send
//! messages as new ones, so a room that already exists can never be given its
//! older history.
//!
//! The shape follows the batch-sending endpoint bridges already speak
//! (`com.beeper.backfill`), advertised as `com.beeper.batch_sending`.
//!
//! Ours adds membership (`im.mxg.batch_send_members`): an `m.room.member` event
//! with a `state_key` naming one of the bridge's own users, which goes in as
//! history only (see `timeline::batch`).

use axum::extract::State;
use ruma::{
	MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId, OwnedUserId, UserId,
	api::{auth_scheme::AccessToken, request, response},
	events::TimelineEventType,
	metadata,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;
use tuwunel_core::{Err, Result};
use tuwunel_service::rooms::timeline::{BatchEvent, BatchOptions};

use crate::Ruma;

metadata! {
	method: POST,
	rate_limited: false,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/com.beeper.backfill/rooms/{room_id}/batch_send",
	}
}

/// More than a bridge's page of history (it sends 100 at a time) but bounded.
const EVENTS_MAX: usize = 1000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IncomingEvent {
	pub event_id: OwnedEventId,
	pub sender: OwnedUserId,
	#[serde(rename = "type")]
	pub kind: TimelineEventType,
	pub origin_server_ts: MilliSecondsSinceUnixEpoch,
	pub content: Box<RawJsonValue>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub state_key: Option<String>,
}

#[request]
pub struct Request {
	#[ruma_api(path)]
	pub room_id: OwnedRoomId,

	#[serde(default)]
	pub forward_if_no_messages: bool,

	#[serde(default)]
	pub forward: bool,

	#[serde(default)]
	pub send_notification: bool,

	pub mark_read_by: Option<OwnedUserId>,

	pub events: Vec<IncomingEvent>,
}

#[response]
pub struct Response {
	pub event_ids: Vec<OwnedEventId>,
}

/// # `POST /_matrix/client/unstable/com.beeper.backfill/rooms/{roomId}/batch_send`
pub(crate) async fn batch_send_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let Some(appservice) = body.appservice_info.as_ref() else {
		return Err!(Request(Forbidden("Only an application service can import history.")));
	};

	if body.events.len() > EVENTS_MAX {
		return Err!(Request(TooLarge("Too many events in one batch.")));
	}

	if !services.metadata.exists(&body.room_id).await {
		return Err!(Request(NotFound("Unknown room.")));
	}

	// The bridge speaks for its own users and its bot, and for people of this server who are in the
	// room: a bridge already acts as its owner through double puppeting, and the owner's own
	// messages are part of the history it is importing.
	for event in &body.events {
		if let Some(state_key) = &event.state_key {
			// Historical membership of the bridge's own users, nothing else: other state in
			// the past would say what a room was like without the room having been so.
			let own_user = UserId::parse(state_key.as_str())
				.is_ok_and(|user_id| appservice.is_user_match(&user_id));

			if event.kind != TimelineEventType::RoomMember || !own_user {
				return Err!(Request(InvalidParam(
					"Only the membership of the application service's own users can be imported."
				)));
			}
		}

		if appservice.is_user_match(&event.sender) {
			continue;
		}

		let member = services.globals.user_is_local(&event.sender)
			&& (services
				.state_cache
				.is_joined(&event.sender, &body.room_id)
				.await || services
				.state_cache
				.once_joined(&event.sender, &body.room_id)
				.await);

		if !member {
			return Err!(Request(Forbidden(
				"The application service may not import messages from {}.",
				event.sender
			)));
		}
	}

	let events = body
		.events
		.iter()
		.map(|event| BatchEvent {
			event_id: event.event_id.clone(),
			sender: event.sender.clone(),
			kind: event.kind.clone(),
			origin_server_ts: event.origin_server_ts,
			content: event.content.clone(),
			state_key: event.state_key.clone(),
		})
		.collect();

	let opts = BatchOptions {
		forward: body.forward,
		forward_if_no_messages: body.forward_if_no_messages,
		notify: body.send_notification,
		mark_read_by: body.mark_read_by.clone(),
	};

	let state_lock = services.state.mutex.lock(&body.room_id).await;
	let event_ids = services
		.timeline
		.insert_batch(&body.room_id, events, &opts, &state_lock)
		.await?;

	Ok(Response { event_ids })
}
