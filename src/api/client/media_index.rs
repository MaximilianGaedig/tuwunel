//! Lists a room's media from the index (`rooms::media_index`), so a client's
//! Media, Files, Links, Music and Voice tabs open at once instead of paging
//! back through the room's history and sorting it out themselves.
//!
//! Encrypted rooms are not indexed, because the server cannot read them; a
//! client still has to read those itself.

use axum::extract::State;
use futures::StreamExt;
use ruma::{
	OwnedRoomId, UInt,
	api::{auth_scheme::AccessToken, request, response},
	events::AnyTimelineEvent,
	metadata,
	serde::Raw,
};
use tuwunel_core::{
	Err, Result, err,
	matrix::{Event, pdu::PduCount},
	utils::math::usize_from_ruma_bounded,
};
use tuwunel_service::rooms::media_index::MediaKind;

use crate::Ruma;

const LIMIT_DEFAULT: usize = 50;
const LIMIT_MAX: usize = 500;

metadata! {
	method: GET,
	rate_limited: true,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/im.mxg.media_index/rooms/{room_id}/media",
	}
}

#[request]
pub struct Request {
	/// The room to list media of.
	#[ruma_api(path)]
	pub room_id: OwnedRoomId,

	/// Which list: `media`, `files`, `links`, `music` or `voice`.
	#[ruma_api(query)]
	pub kind: String,

	/// How many to return, newest first.
	#[ruma_api(query)]
	pub limit: Option<UInt>,

	/// Continues after the `end` of a previous response.
	#[ruma_api(query)]
	pub from: Option<String>,
}

#[response]
pub struct Response {
	/// The matching events, newest first.
	pub chunk: Vec<Raw<AnyTimelineEvent>>,

	/// Pass as `from` for the next page; absent once the list is exhausted.
	pub end: Option<String>,
}

/// # `GET /_matrix/client/unstable/im.mxg.media_index/rooms/{roomId}/media`
pub(crate) async fn get_room_media_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();
	let room_id = &body.room_id;

	let Some(kind) = MediaKind::parse(&body.kind) else {
		return Err!(Request(InvalidParam("Unknown media kind.")));
	};

	if !services
		.state_accessor
		.user_can_see_room(sender_user, room_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to view this room.")));
	}

	let from: Option<PduCount> = body
		.from
		.as_deref()
		.map(str::parse)
		.transpose()
		.map_err(|_| err!(Request(InvalidParam("Invalid `from` token."))))?;

	let limit = body
		.limit
		.map_or(LIMIT_DEFAULT, |limit| usize_from_ruma_bounded(limit, LIMIT_DEFAULT, LIMIT_MAX));

	let shortroomid = services.short.get_shortroomid(room_id).await?;

	let events: Vec<_> = services
		.media_index
		.media_ids(shortroomid, kind, from)
		.filter_map(async |pdu_id| {
			let pdu = services
				.timeline
				.get_pdu_from_id(&pdu_id)
				.await
				.ok()?;

			(!pdu.is_redacted()
				&& services
					.state_accessor
					.user_can_see_event(sender_user, room_id, pdu.event_id())
					.await)
				.then_some((pdu_id.pdu_count(), pdu))
		})
		.take(limit)
		.collect()
		.await;

	// Where the next page continues, or nothing once this one wasn't full.
	let end = (events.len() == limit)
		.then(|| events.last().map(|(count, _)| count.to_string()))
		.flatten();

	Ok(Response {
		chunk: events
			.into_iter()
			.map(|(_, pdu)| pdu.into_format())
			.collect(),
		end,
	})
}
