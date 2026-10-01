//! The media manifest of a room export: every file in the room, with what a
//! client needs to decide whether and how to fetch it.
//!
//! Read from the media index (`rooms::media_index`), so listing the files of a
//! chat costs one event read per file and nothing per message. No bytes are
//! sent: a client downloads the files it wants through the media endpoints.
//!
//! Much of a bridged chat's media is not on this server at all - the bridge
//! fetches a file from its network when someone opens it, and networks ration
//! that. The manifest says per file whether it is stored here, and listing a
//! file never fetches it, so an export costs a bridge nothing until the client
//! chooses to download.
//!
//! Encrypted rooms are not indexed, because the server cannot read them, so
//! their manifest is empty and says why.

use std::pin::pin;

use axum::extract::State;
use futures::StreamExt;
use ruma::{
	OwnedRoomId, UInt,
	api::{auth_scheme::AccessToken, request, response},
	metadata,
};
use tuwunel_core::{
	Err, Result, err,
	matrix::{Event, pdu::PduCount},
	utils::math::usize_from_ruma_bounded,
};
use tuwunel_service::rooms::export::{
	FILE_KINDS, ManifestRow, format_cursor, manifest_row, parse_cursor,
};

use crate::Ruma;

const LIMIT_DEFAULT: usize = 1000;
const LIMIT_MAX: usize = 5000;

metadata! {
	method: GET,
	rate_limited: true,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/im.mxg.export/rooms/{room_id}/media",
	}
}

#[request]
pub struct Request {
	/// The room to list the files of.
	#[ruma_api(path)]
	pub room_id: OwnedRoomId,

	/// Continues after the `end` of a previous response.
	#[ruma_api(query)]
	pub from: Option<String>,

	/// How many files to return.
	#[ruma_api(query)]
	pub limit: Option<UInt>,
}

#[response]
pub struct Response {
	/// The files: pictures and videos, then files, music and voice messages, each newest first.
	pub chunk: Vec<ManifestRow>,

	/// Pass as `from` for the next page; absent once every file has been listed.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub end: Option<String>,

	/// Whether the room is encrypted. The server cannot tell which of such a room's events are
	/// files, so its manifest is empty, and a client has to find the files in the events itself.
	pub encrypted: bool,
}

/// # `GET /_matrix/client/unstable/im.mxg.export/rooms/{roomId}/media`
pub(crate) async fn get_room_export_media_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();
	let room_id = &body.room_id;

	let (first_kind, mut resume): (usize, Option<PduCount>) = match body.from.as_deref() {
		| None => (0, None),
		| Some(token) => {
			let (kind, count) = parse_cursor(token)
				.ok_or_else(|| err!(Request(InvalidParam("Invalid `from` token."))))?;
			(kind, Some(count))
		},
	};

	// At least one: a page of none could not say where the next one starts.
	let limit = body
		.limit
		.map_or(LIMIT_DEFAULT, |limit| usize_from_ruma_bounded(limit, LIMIT_DEFAULT, LIMIT_MAX))
		.max(1);

	if !services
		.state_accessor
		.user_can_see_room(sender_user, room_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to view this room.")));
	}

	let shortroomid = services.short.get_shortroomid(room_id).await?;
	let server_name = services.globals.server_name().as_str();
	let encrypted = services
		.state_accessor
		.is_encrypted_room(room_id)
		.await;

	let mut chunk: Vec<ManifestRow> = Vec::new();
	let mut end: Option<String> = None;
	'kinds: for kind in FILE_KINDS.into_iter().skip(first_kind) {
		// Only the kind the last page stopped in continues part-way; the kinds after it start at
		// their newest.
		let resumed = resume.take();
		let mut at = resumed.unwrap_or_else(PduCount::max);
		let mut ids = pin!(
			services
				.media_index
				.media_ids(shortroomid, kind, Some(at))
		);

		while let Some(pdu_id) = ids.next().await {
			let count = pdu_id.pdu_count();
			// The index is read from the cursor's own row on, and that row was the last page's.
			if resumed == Some(count) {
				continue;
			}

			// Stopping only once a further row is in hand is what lets a full last page say it
			// is the last.
			if chunk.len() >= limit {
				end = Some(format_cursor(kind, at));
				break 'kinds;
			}
			at = count;

			let Ok(pdu) = services.timeline.get_pdu_from_id(&pdu_id).await else {
				continue;
			};

			if pdu.is_redacted()
				|| !services
					.state_accessor
					.user_can_see_event(sender_user, room_id, pdu.event_id())
					.await
			{
				continue;
			}

			let row = manifest_row(
				pdu.event_id().as_str(),
				pdu.sender().as_str(),
				u64::from(pdu.origin_server_ts().get()),
				kind,
				&pdu.get_content_as_value(),
				server_name,
			);
			if let Some(row) = row {
				chunk.push(row);
			}
		}
	}

	Ok(Response { chunk, end, encrypted })
}
