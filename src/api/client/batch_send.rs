//! Lets a bridge import a chat's history: messages behind a room's existing
//! ones, or a run stamped with the times they were originally sent, under the
//! event IDs the bridge chose for them. Without this a bridge can only send
//! messages as new ones, so a room that already exists can never be given its
//! older history.
//!
//! The shape follows the batch-sending endpoint bridges already speak
//! (`com.beeper.backfill`), advertised as `com.beeper.batch_sending`.

use axum::extract::State;
use ruma::{
	MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId, OwnedUserId,
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

	// The bridge speaks for its own users and its bot, not for anyone else.
	if let Some(event) = body
		.events
		.iter()
		.find(|event| !appservice.is_user_match(&event.sender))
	{
		return Err!(Request(Forbidden(
			"The application service does not control {}.",
			event.sender
		)));
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
