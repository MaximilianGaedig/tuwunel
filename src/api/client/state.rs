use axum::{
	extract::{FromRequest, State},
	response::IntoResponse,
};
use futures::{FutureExt, TryStreamExt};
use ruma::{
	OwnedEventId, RoomId, UserId,
	api::client::state::{
		get_state_event_for_key::{self, v3::StateEventFormat},
		get_state_events, send_state_event,
	},
	events::{AnyStateEventContent, StateEventType},
	serde::Raw,
};
use serde_json::json;
use tuwunel_core::{
	Err, Result, err,
	matrix::{Event, pdu::PduBuilder},
	utils::stream::TryBroadbandExt,
};
use tuwunel_service::Services;

use crate::{Ruma, RumaResponse, client::with_membership};

/// Dispatch handler for empty-key state events (normal only; delayed events
/// require a state key).
pub(crate) async fn send_state_event_for_empty_key_dispatch(
	State(services): State<crate::State>,
	req: axum::extract::Request,
) -> Result<axum::response::Response> {
	let body = Ruma::<send_state_event::v3::Request>::from_request(req, &services)
		.await
		.map_err(|e| err!(Request(BadJson("Invalid state event request: {e}"))))?;
	let timestamp = if body.appservice_info.is_some() {
		body.timestamp
	} else {
		None
	};
	let event_id = send_state_event_for_key_helper(
		&services,
		body.sender_user(),
		&body.room_id,
		&body.event_type,
		&body.body.body,
		&body.state_key,
		timestamp,
	)
	.await?;
	Ok(axum::Json(serde_json::json!({ "event_id": event_id })).into_response())
}

/// # `GET /_matrix/client/v3/rooms/{roomid}/state`
///
/// Get all state events for a room.
///
/// - If not joined: Only works if current room history visibility is world
///   readable
pub(crate) async fn get_state_events_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_events::v3::Request>,
) -> Result<get_state_events::v3::Response> {
	let sender_user = body.sender_user();

	if !services
		.state_accessor
		.user_can_see_state_events(sender_user, &body.room_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to view the room state.")));
	}

	let encrypted = services
		.state_accessor
		.is_encrypted_room(&body.room_id)
		.await;

	let room_state = services
		.state_accessor
		.room_state_full_pdus(&body.room_id)
		.map_ok(Event::into_pdu)
		.broad_and_then(async |pdu| {
			Ok(with_membership(&services, pdu, sender_user, encrypted).await)
		})
		.map_ok(Event::into_format)
		.try_collect()
		.await?;

	Ok(get_state_events::v3::Response { room_state })
}

/// # `GET /_matrix/client/v3/rooms/{roomid}/state/{eventType}/{stateKey}`
///
/// Get single state event of a room with the specified state key.
/// The optional query parameter `?format=event|content` allows returning the
/// full room state event or just the state event's content (default behaviour)
///
/// - If not joined: Only works if current room history visibility is world
///   readable
pub(crate) async fn get_state_events_for_key_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_event_for_key::v3::Request>,
) -> Result<get_state_event_for_key::v3::Response> {
	let sender_user = body.sender_user();

	if !services
		.state_accessor
		.user_can_see_state_events(sender_user, &body.room_id)
		.await
	{
		return Err!(Request(NotFound(debug_warn!(
			"You don't have permission to view the room state."
		))));
	}

	let event = services
		.state_accessor
		.room_state_get(&body.room_id, &body.event_type, &body.state_key)
		.await
		.map_err(|e| {
			err!(Request(NotFound(debug_warn!(
				room_id = ?body.room_id,
				event_type = ?body.event_type,
				"Failed to get state event: {e}.",
			))))
		})?;

	let event_or_content = match body.format {
		| StateEventFormat::Event => json!({
			"content": event.content(),
			"event_id": event.event_id(),
			"origin_server_ts": event.origin_server_ts(),
			"room_id": event.room_id(),
			"sender": event.sender(),
			"state_key": event.state_key(),
			"type": event.kind(),
			"unsigned": event.unsigned(),
		}),

		| _ => event.get_content_as_value(),
	};

	let event_or_content =
		serde_json::value::to_raw_value(&event_or_content).expect("serializable JSON value");

	Ok(get_state_event_for_key::v3::Response::new(event_or_content))
}

/// # `GET /_matrix/client/v3/rooms/{roomid}/state/{eventType}`
///
/// Get single state event of a room.
/// The optional query parameter `?format=event|content` allows returning the
/// full room state event or just the state event's content (default behaviour)
///
/// - If not joined: Only works if current room history visibility is world
///   readable
pub(crate) async fn get_state_events_for_empty_key_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_event_for_key::v3::Request>,
) -> Result<RumaResponse<get_state_event_for_key::v3::Response>> {
	get_state_events_for_key_route(State(services), body)
		.await
		.map(RumaResponse)
}

pub(crate) async fn send_state_event_for_key_helper(
	services: &Services,
	sender: &UserId,
	room_id: &RoomId,
	event_type: &StateEventType,
	json: &Raw<AnyStateEventContent>,
	state_key: &str,
	timestamp: Option<ruma::MilliSecondsSinceUnixEpoch>,
) -> Result<OwnedEventId> {
	tuwunel_service::rooms::event_policy::allowed_to_send_state_event(
		services, room_id, event_type, state_key, json,
	)
	.await?;
	let state_lock = services.state.mutex.lock(room_id).await;
	let event_id = services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: event_type.to_string().into(),
				content: serde_json::from_str(json.json().get())?,
				state_key: Some(state_key.into()),
				timestamp,
				..Default::default()
			},
			sender,
			room_id,
			&state_lock,
		)
		.boxed()
		.await?;

	Ok(event_id)
}
