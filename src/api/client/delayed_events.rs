use axum::{
	Json,
	extract::{FromRequest, State},
	response::{IntoResponse, Response},
};
use ruma::{
	DeviceId, OwnedUserId, TransactionId, UserId,
	api::client::delayed_events::{
		delayed_message_event, delayed_state_event, update_delayed_event,
	},
};
use serde_json::json;
use tuwunel_core::{Err, Result, err};

use crate::Ruma;

/// Parse query string to check if this is a delayed event request.
/// Looks for `org.matrix.msc4140.delay` parameter.
fn has_delay_param(query: &str) -> bool {
	for (key, _) in url::form_urlencoded::parse(query.as_bytes()) {
		// Parameter names are case-insensitive per HTML spec; clients may
		// URL-encode them, which form_urlencoded::parse already handles.
		if key.eq_ignore_ascii_case("org.matrix.msc4140.delay") {
			return true;
		}
	}
	false
}

/// Handles both normal and delayed message events on the same path.
pub(crate) async fn send_message_event_dispatch(
	State(services): State<crate::State>,
	req: axum::extract::Request,
) -> Result<Response> {
	let query = req.uri().query().unwrap_or("");
	let is_delayed = has_delay_param(query);

	if is_delayed {
		let body = Ruma::<delayed_message_event::unstable::Request>::from_request(req, &services)
			.await?;
		let delay_id = handle_delayed_message_event(&services, &body).await?;
		Ok(Json(json!({ "delay_id": delay_id })).into_response())
	} else {
		let body =
			Ruma::<ruma::api::client::message::send_message_event::v3::Request>::from_request(
				req, &services,
			)
			.await?;
		let event_id = crate::client::send::send_message_event_helper(&services, &body).await?;
		Ok(Json(json!({ "event_id": event_id })).into_response())
	}
}

/// Handles both normal and delayed state events on the same path.
pub(crate) async fn send_state_event_for_key_dispatch(
	State(services): State<crate::State>,
	req: axum::extract::Request,
) -> Result<Response> {
	let query = req.uri().query().unwrap_or("");
	let is_delayed = has_delay_param(query);

	if is_delayed {
		let body =
			Ruma::<delayed_state_event::unstable::Request>::from_request(req, &services).await?;
		let delay_id = handle_delayed_state_event(&services, &body).await?;
		Ok(Json(json!({ "delay_id": delay_id })).into_response())
	} else {
		let body = Ruma::<ruma::api::client::state::send_state_event::v3::Request>::from_request(
			req, &services,
		)
		.await?;
		let timestamp = if body.appservice_info.is_some() {
			body.timestamp
		} else {
			None
		};
		let event_id = crate::client::state::send_state_event_for_key_helper(
			&services,
			body.sender_user(),
			&body.room_id,
			&body.event_type,
			&body.body.body,
			&body.state_key,
			timestamp,
		)
		.await?;
		Ok(Json(json!({ "event_id": event_id })).into_response())
	}
}

async fn handle_delayed_message_event(
	services: &crate::State,
	body: &Ruma<delayed_message_event::unstable::Request>,
) -> Result<String> {
	let sender_user = body.sender_user();
	let sender_device = body.sender_device.as_deref();

	// Check transaction id idempotency
	if let Some(existing_delay_id) =
		check_existing_txnid(services, sender_user, sender_device, &body.txn_id).await
	{
		// Verify the stored delay_id still exists; if it was finalized
		// and cleaned up, treat as a fresh request.
		if services
			.delayed_events
			.get(&existing_delay_id)
			.await?
			.is_some()
		{
			return Ok(existing_delay_id);
		}
	}

	let delay_ms = match body.delay_parameters {
		| ruma::api::client::delayed_events::DelayParameters::Timeout { timeout } =>
			u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
	};

	if delay_ms == 0 {
		return Err!(Request(InvalidParam("Delay must be greater than 0")));
	}

	let delay_id = generate_delay_id();

	let content = serde_json::from_str(body.body.body.json().get())
		.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

	services
		.delayed_events
		.add(
			&delay_id,
			&body.room_id,
			sender_user,
			body.sender_device.as_ref(),
			&body.event_type.to_string(),
			None,
			Some(body.txn_id.to_string()),
			content,
			delay_ms,
		)
		.await?;

	// Store transaction id mapping for idempotency
	services.transaction_ids.add_txnid(
		sender_user,
		sender_device,
		&body.txn_id,
		delay_id.as_bytes(),
	);

	Ok(delay_id)
}

async fn check_existing_txnid(
	services: &crate::State,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	txn_id: &TransactionId,
) -> Option<String> {
	let Ok(response) = services
		.transaction_ids
		.existing_txnid(sender_user, sender_device, txn_id)
		.await
	else {
		return None;
	};

	if response.is_empty() {
		return None;
	}

	String::from_utf8(response.to_vec()).ok()
}

async fn handle_delayed_state_event(
	services: &crate::State,
	body: &Ruma<delayed_state_event::unstable::Request>,
) -> Result<String> {
	let sender_user = body.sender_user();

	// NOTE: We intentionally do NOT validate the state event here.
	// MSC4140 requires validation at send time, not schedule time,
	// because room state may change between scheduling and sending.

	let delay_ms = match body.delay_parameters {
		| ruma::api::client::delayed_events::DelayParameters::Timeout { timeout } =>
			u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
	};

	if delay_ms == 0 {
		return Err!(Request(InvalidParam("Delay must be greater than 0")));
	}

	let delay_id = generate_delay_id();

	let content = serde_json::from_str(body.body.body.json().get())
		.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

	services
		.delayed_events
		.add(
			&delay_id,
			&body.room_id,
			sender_user,
			body.sender_device.as_ref(),
			&body.event_type.to_string(),
			Some(body.state_key.clone()),
			None,
			content,
			delay_ms,
		)
		.await?;

	Ok(delay_id)
}

/// # `POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}`
///
/// Update a delayed event (restart, send immediately, or cancel).
/// Per MSC4140, this is idempotent: cancel/send on already-finalized events
/// returns success if the outcome matches the action.
pub(crate) async fn update_delayed_event_route(
	State(services): State<crate::State>,
	body: Ruma<update_delayed_event::unstable::Request>,
) -> Result<update_delayed_event::unstable::Response> {
	let sender_user = body.sender_user();

	match body.action {
		| update_delayed_event::unstable::UpdateAction::Cancel => {
			services
				.delayed_events
				.cancel(&body.delay_id, sender_user)
				.await?;
		},
		| update_delayed_event::unstable::UpdateAction::Send => {
			services
				.delayed_events
				.send_now(&body.delay_id, sender_user)
				.await?;
		},
		| update_delayed_event::unstable::UpdateAction::Restart => {
			services
				.delayed_events
				.restart(&body.delay_id, sender_user)
				.await?;
		},
		| _ => return Err!(Request(InvalidParam("Unknown update action"))),
	}

	Ok(update_delayed_event::unstable::Response::new())
}

/// # `POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}/cancel`
///
/// Dedicated cancel endpoint. Body is ignored.
pub(crate) async fn cancel_delayed_event_route(
	State(services): State<crate::State>,
	axum::extract::Path(delay_id): axum::extract::Path<String>,
	req: axum::extract::Request,
) -> Result<Json<serde_json::Value>> {
	let token = extract_token_from_request(&req)
		.ok_or_else(|| err!(Request(MissingToken("Missing access token"))))?;
	let sender_user = find_user_from_token(&services, &token).await?;

	services
		.delayed_events
		.cancel(&delay_id, &sender_user)
		.await?;
	Ok(Json(serde_json::json!({})))
}

/// # `POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}/restart`
///
/// Dedicated restart endpoint. Body is ignored.
pub(crate) async fn restart_delayed_event_route(
	State(services): State<crate::State>,
	axum::extract::Path(delay_id): axum::extract::Path<String>,
	req: axum::extract::Request,
) -> Result<Json<serde_json::Value>> {
	let token = extract_token_from_request(&req)
		.ok_or_else(|| err!(Request(MissingToken("Missing access token"))))?;
	let sender_user = find_user_from_token(&services, &token).await?;

	services
		.delayed_events
		.restart(&delay_id, &sender_user)
		.await?;
	Ok(Json(serde_json::json!({})))
}

/// # `POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}/send`
///
/// Dedicated send-now endpoint. Body is ignored.
pub(crate) async fn send_delayed_event_route(
	State(services): State<crate::State>,
	axum::extract::Path(delay_id): axum::extract::Path<String>,
	req: axum::extract::Request,
) -> Result<Json<serde_json::Value>> {
	let token = extract_token_from_request(&req)
		.ok_or_else(|| err!(Request(MissingToken("Missing access token"))))?;
	let sender_user = find_user_from_token(&services, &token).await?;

	services
		.delayed_events
		.send_now(&delay_id, &sender_user)
		.await?;
	Ok(Json(serde_json::json!({})))
}

/// Extract the access token from either the Authorization header or the
/// `access_token` query parameter, matching the standard Matrix auth flow.
fn extract_token_from_request(req: &axum::extract::Request) -> Option<String> {
	// Check Authorization header first
	if let Some(auth_header) = req.headers().get(http::header::AUTHORIZATION)
		&& let Ok(auth_str) = auth_header.to_str()
		&& let Some((scheme, token)) = auth_str.split_once(' ')
		&& scheme.eq_ignore_ascii_case("Bearer")
	{
		return Some(token.to_owned());
	}

	// Fall back to query parameter
	req.uri().query().and_then(|q| {
		url::form_urlencoded::parse(q.as_bytes())
			.find(|(k, _)| k == "access_token")
			.map(|(_, v)| v.to_string())
	})
}

/// # `GET /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}`
///
/// Get a single delayed event by ID.
pub(crate) async fn get_delayed_event_route(
	State(services): State<crate::State>,
	axum::extract::Path(delay_id): axum::extract::Path<String>,
	req: axum::extract::Request,
) -> Result<Json<serde_json::Value>> {
	let token = extract_token_from_request(&req)
		.ok_or_else(|| err!(Request(MissingToken("Missing access token"))))?;
	let sender_user = find_user_from_token(&services, &token).await?;

	let event = services
		.delayed_events
		.get(&delay_id)
		.await?
		.ok_or_else(|| err!(Request(NotFound("Delayed event not found"))))?;

	if event.sender_user != sender_user {
		return Err!(Request(NotFound("Delayed event not found")));
	}

	Ok(Json(event_to_json(event)))
}

/// # `GET /_matrix/client/unstable/org.matrix.msc4140/delayed_events`
///
/// List delayed events for the authenticated user.
pub(crate) async fn list_delayed_events_route(
	State(services): State<crate::State>,
	req: axum::extract::Request,
) -> Result<Json<serde_json::Value>> {
	let token = extract_token_from_request(&req)
		.ok_or_else(|| err!(Request(MissingToken("Missing access token"))))?;
	let sender_user = find_user_from_token(&services, &token).await?;

	let events = services
		.delayed_events
		.get_by_sender(&sender_user)
		.await?;

	Ok(Json(serde_json::json!({
		"delayed_events": events.into_iter().map(event_to_json).collect::<Vec<_>>(),
	})))
}

async fn find_user_from_token(services: &crate::State, token: &str) -> Result<OwnedUserId> {
	let Ok((user_id, _, expires_at)) = services.users.find_from_token(token).await else {
		return Err!(Request(Forbidden("Invalid access token")));
	};

	if expires_at.is_some_and(|t| t < std::time::SystemTime::now()) {
		return Err!(Request(Forbidden("Access token has expired")));
	}

	Ok(user_id)
}

fn event_to_json(
	event: tuwunel_service::rooms::delayed_events::DelayedEvent,
) -> serde_json::Value {
	let status = match event.status {
		| tuwunel_service::rooms::delayed_events::DelayedEventStatus::Scheduled => "scheduled",
		| tuwunel_service::rooms::delayed_events::DelayedEventStatus::Finalized => "finalized",
	};

	let mut obj = serde_json::json!({
		"delay_id": event.delay_id,
		"room_id": event.room_id,
		"type": event.event_type,
		"delay": event.delay_ms,
		"send_ts": event.timeout,
		"running_since": event.running_since,
		"content": event.content,
		"status": status,
	});

	if let Some(state_key) = event.state_key {
		obj["state_key"] = serde_json::Value::String(state_key);
	}

	if let Some(error) = event.error {
		obj["error"] = error;
	}

	if let Some(event_id) = event.event_id {
		obj["event_id"] = serde_json::Value::String(event_id.as_str().to_owned());
	}

	if let Some(finalised_ts) = event.finalised_ts {
		obj["finalised_ts"] = serde_json::Value::Number(serde_json::Number::from(finalised_ts));
	}

	obj
}

/// Generates a cryptographically secure random delay_id.
/// Per MSC4140, delay_id MUST be globally unique and SHOULD be
/// cryptographically secure (infeasible to predict).
fn generate_delay_id() -> String {
	use base64::{
		Engine,
		alphabet::URL_SAFE,
		engine::{GeneralPurpose, general_purpose::NO_PAD},
	};
	let mut binary: [u8; 16] = [0; 16];
	rand::fill(&mut binary);

	let mut encoded: [u8; 22] = [0; 22];
	GeneralPurpose::new(&URL_SAFE, NO_PAD)
		.encode_slice(binary, &mut encoded)
		.expect("Failed to encode binary to base64");

	std::str::from_utf8(&encoded)
		.expect("Failed to convert base64 bytes to valid utf8")
		.to_owned()
}
