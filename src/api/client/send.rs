use std::collections::BTreeMap;

use ruma::{
	DeviceId, TransactionId, UserId,
	api::client::message::send_message_event,
	events::{MessageLikeEventType, room::redaction::RoomRedactionEventContent},
};
use serde_json::from_str;
use tuwunel_core::{
	Err, Result, err,
	matrix::pdu::PduBuilder,
	utils::{self},
};
use tuwunel_service::Services;

use crate::Ruma;

/// # `PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}`
///
/// Send a message event into the room.
///
/// - Is a NOOP if the txn id was already used before and returns the same event
///   id again
/// - The only requirement for the content is that it has to be valid json
/// - Tries to send the event into the room, auth rules will determine if it is
///   allowed
pub(crate) async fn send_message_event_helper(
	services: &crate::State,
	body: &Ruma<send_message_event::v3::Request>,
) -> Result<ruma::OwnedEventId> {
	let sender_user = body.sender_user();
	let sender_device = body.sender_device.as_deref();
	let appservice_info = body.appservice_info.as_ref();

	tuwunel_service::rooms::event_policy::validate_message_event_policy(
		services,
		sender_user,
		&body.event_type,
		&body.body.body,
		&body.room_id,
	)
	.await?;

	let state_lock = services.state.mutex.lock(&body.room_id).await;

	if let Some(existing_txnid) =
		check_existing_txnid(services, sender_user, sender_device, &body.txn_id).await
	{
		return match existing_txnid {
			| Ok(response) => Ok(response.event_id),
			| Err(e) => Err(e),
		};
	}

	let mut unsigned = BTreeMap::new();
	unsigned.insert("transaction_id".to_owned(), body.txn_id.to_string().into());

	let content = from_str(body.body.body.json().get())
		.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

	// MSC4169: clients sending m.room.redaction via /send put `redacts` in
	// `content`. Pre-v11 auth rules read it from the top level; lift it so
	// `redacts_id(...)` resolves regardless of room version. Mirrors the
	// /redact handler.
	let redacts = body
		.event_type
		.eq(&MessageLikeEventType::RoomRedaction)
		.then(|| {
			body.body
				.body
				.deserialize_as_unchecked::<RoomRedactionEventContent>()
				.ok()
		})
		.flatten()
		.and_then(|content| content.redacts);

	let event_id = services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: body.event_type.clone().into(),
				content,
				unsigned: Some(unsigned),
				timestamp: appservice_info.and(body.timestamp),
				redacts,
				..Default::default()
			},
			sender_user,
			&body.room_id,
			&state_lock,
		)
		.await?;

	services.transaction_ids.add_txnid(
		sender_user,
		sender_device,
		&body.txn_id,
		event_id.as_bytes(),
	);

	drop(state_lock);

	Ok(event_id)
}

/// Check if this is a new transaction id. Returns Some when the transaction id
/// exists and the send must then be terminated by returning the contained
/// result.
async fn check_existing_txnid(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	txn_id: &TransactionId,
) -> Option<Result<send_message_event::v3::Response>> {
	let Ok(response) = services
		.transaction_ids
		.existing_txnid(sender_user, sender_device, txn_id)
		.await
	else {
		return None;
	};

	// The client might have sent a txnid of the /sendToDevice endpoint
	// This txnid has no response associated with it
	if response.is_empty() {
		return Some(Err!(Request(InvalidParam(
			"Tried to use txn_id already used for an incompatible endpoint."
		))));
	}

	let Ok(Ok(event_id)) = utils::string_from_bytes(&response).map(TryInto::try_into) else {
		return Some(Err!(Database("Invalid event_id in txn_id data: {response:?}.")));
	};

	Some(Ok(send_message_event::v3::Response { event_id }))
}
