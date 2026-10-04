use std::collections::BTreeMap;

use axum::extract::State;
use ruma::{
	MilliSecondsSinceUnixEpoch,
	api::client::read_marker::set_read_marker,
	events::{
		RoomAccountDataEventType,
		fully_read::{FullyReadEvent, FullyReadEventContent},
		receipt::{Receipt, ReceiptEvent, ReceiptEventContent, ReceiptThread, ReceiptType},
	},
};
use tuwunel_core::Result;
use tuwunel_service::presence::Ping;

use super::{read_receipt_ts, reset_and_refresh_badge, set_private_marker};
use crate::{ClientIp, Ruma};

/// # `POST /_matrix/client/r0/rooms/{roomId}/read_markers`
///
/// Sets different types of read markers.
///
/// - Updates fully-read account data event to `fully_read`
/// - If `read_receipt` is set: Update private marker and public read receipt
///   EDU
pub(crate) async fn set_read_marker_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<set_read_marker::v3::Request>,
) -> Result<set_read_marker::v3::Response> {
	let sender_user = body.sender_user();

	if let Some(event) = &body.fully_read {
		let fully_read_event = FullyReadEvent {
			content: FullyReadEventContent { event_id: event.clone() },
		};

		services
			.account_data
			.update(
				Some(&body.room_id),
				sender_user,
				RoomAccountDataEventType::FullyRead,
				&serde_json::to_value(fully_read_event)?,
			)
			.await
			.ok();
	}

	let private_advanced = match &body.private_read_receipt {
		| None => false,
		| Some(event) =>
			set_private_marker(
				&services,
				&body.room_id,
				sender_user,
				event,
				&ReceiptThread::Unthreaded,
			)
			.await?,
	};

	let public_advanced = match &body.read_receipt {
		| None => false,
		| Some(event) => {
			let ts = read_receipt_ts(
				body.appservice_info.is_some(),
				body.json_body.as_ref(),
				MilliSecondsSinceUnixEpoch::now(),
			);

			let receipt_content = BTreeMap::from_iter([(
				event.to_owned(),
				BTreeMap::from_iter([(
					ReceiptType::Read,
					BTreeMap::from_iter([(sender_user.to_owned(), Receipt {
						ts: Some(ts),
						thread: ReceiptThread::Unthreaded,
					})]),
				)]),
			)]);

			let advanced = services
				.read_receipt
				.readreceipt_update(sender_user, &body.room_id, &ReceiptEvent {
					content: ReceiptEventContent(receipt_content),
					room_id: body.room_id.clone(),
				})
				.await;

			let ping = Ping {
				device_id: body.sender_device.as_deref(),
				client_ip: Some(client),
				appservice: body.appservice_info.as_ref(),
				..Default::default()
			};

			services
				.presence
				.maybe_ping_presence(sender_user, ping)
				.await
				.ok();

			advanced
		},
	};

	// Route through the dispatcher so per-thread counts are also cleared;
	// `/read_markers` predates MSC3771 and carries no thread field.
	if private_advanced || public_advanced {
		let read: Vec<_> = [
			body.private_read_receipt
				.as_deref()
				.filter(|_| private_advanced),
			body.read_receipt
				.as_deref()
				.filter(|_| public_advanced),
		]
		.into_iter()
		.flatten()
		.collect();

		reset_and_refresh_badge(
			&services,
			sender_user,
			&body.room_id,
			None,
			&ReceiptThread::Unthreaded,
			&read,
		)
		.await;
	}

	Ok(set_read_marker::v3::Response {})
}
