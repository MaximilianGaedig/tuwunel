//! What a room's media says, in words: stored by whoever read it, searchable by
//! everybody afterwards.
//!
//! A device that has a picture on screen can read the words off it, and a
//! machine with time to spare can work through a room's whole history; either
//! way the answer is sent here once, indexed as if it were the message's own
//! body, and found by ordinary search from any device. `missing` says what has
//! not been read yet, so neither of them does the same work twice.
//!
//! Only somebody who can see the event may say what it says, and only for a room
//! the server can read - an encrypted room's media is not the server's to index.

use axum::extract::State;
use ruma::{
	OwnedEventId, OwnedRoomId, UInt,
	api::{auth_scheme::AccessToken, request, response},
	metadata,
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Result, err, matrix::pdu::PduCount, utils::math::usize_from_ruma_bounded};
use tuwunel_service::rooms::{
	media_index::MediaKind,
	media_text::{TEXT_MAX, TextKind},
};

use crate::Ruma;

const MISSING_DEFAULT: usize = 100;
const MISSING_MAX: usize = 1000;

pub mod put {
	use super::*;

	metadata! {
		method: POST,
		rate_limited: true,
		authentication: AccessToken,
		history: {
			unstable => "/_matrix/client/unstable/im.mxg.media_text/rooms/{room_id}/{event_id}",
		}
	}

	#[request]
	pub struct Request {
		#[ruma_api(path)]
		pub room_id: OwnedRoomId,

		/// The event whose media was read.
		#[ruma_api(path)]
		pub event_id: OwnedEventId,

		/// Which reading this is: `ocr`, `transcript` or `description`.
		pub kind: String,

		/// What it said. Empty means it said nothing, which is worth recording -
		/// it is how a picture with no words in it stops being read again.
		pub text: String,
	}

	#[response]
	pub struct Response {}
}

pub mod get {
	use super::*;

	metadata! {
		method: GET,
		rate_limited: true,
		authentication: AccessToken,
		history: {
			unstable => "/_matrix/client/unstable/im.mxg.media_text/rooms/{room_id}/{event_id}",
		}
	}

	#[request]
	pub struct Request {
		#[ruma_api(path)]
		pub room_id: OwnedRoomId,

		#[ruma_api(path)]
		pub event_id: OwnedEventId,
	}

	#[response]
	pub struct Response {
		/// What is known about this event's media, by where it came from.
		pub texts: Vec<Text>,
	}
}

pub mod missing {
	use super::*;

	metadata! {
		method: GET,
		rate_limited: true,
		authentication: AccessToken,
		history: {
			unstable => "/_matrix/client/unstable/im.mxg.media_text/rooms/{room_id}/missing",
		}
	}

	#[request]
	pub struct Request {
		#[ruma_api(path)]
		pub room_id: OwnedRoomId,

		/// Which of the room's media to look through: `media`, `voice`, `music`, `files`.
		#[ruma_api(query)]
		pub media: String,

		/// Which reading is missing: `ocr`, `transcript` or `description`.
		#[ruma_api(query)]
		pub kind: String,

		#[ruma_api(query)]
		pub limit: Option<UInt>,

		/// Continues after the `end` of a previous answer.
		#[ruma_api(query)]
		pub from: Option<String>,
	}

	#[response]
	pub struct Response {
		/// The events that have not been read, newest first.
		pub event_ids: Vec<OwnedEventId>,

		/// Pass as `from` to go on; absent once there are no more.
		pub end: Option<String>,
	}
}

/// One reading of one event's media.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Text {
	/// `ocr`, `transcript` or `description`.
	pub kind: String,
	pub text: String,
}

/// # `POST /_matrix/client/unstable/im.mxg.media_text/rooms/{roomId}/{eventId}`
pub(crate) async fn put_media_text_route(
	State(services): State<crate::State>,
	body: Ruma<put::Request>,
) -> Result<put::Response> {
	let sender_user = body.sender_user();
	let Some(kind) = TextKind::parse(&body.kind) else {
		return Err!(Request(InvalidParam("Unknown kind of text.")));
	};
	if body.text.len() > TEXT_MAX {
		return Err!(Request(TooLarge("That is more text than media can say.")));
	}

	let (shortroomid, pdu_id) = readable(&services, sender_user, &body.room_id, &body.event_id).await?;

	services
		.media_text
		.put(shortroomid, &pdu_id, kind, &body.text, sender_user)
		.await?;

	Ok(put::Response {})
}

/// # `GET /_matrix/client/unstable/im.mxg.media_text/rooms/{roomId}/{eventId}`
pub(crate) async fn get_media_text_route(
	State(services): State<crate::State>,
	body: Ruma<get::Request>,
) -> Result<get::Response> {
	let sender_user = body.sender_user();
	let (shortroomid, pdu_id) = readable(&services, sender_user, &body.room_id, &body.event_id).await?;

	let texts = services
		.media_text
		.get(shortroomid, &pdu_id)
		.await
		.into_iter()
		.map(|(kind, text)| Text { kind: kind.name().to_owned(), text })
		.collect();

	Ok(get::Response { texts })
}

/// # `GET /_matrix/client/unstable/im.mxg.media_text/rooms/{roomId}/missing`
pub(crate) async fn get_missing_media_text_route(
	State(services): State<crate::State>,
	body: Ruma<missing::Request>,
) -> Result<missing::Response> {
	let sender_user = body.sender_user();
	let Some(media) = MediaKind::parse(&body.media) else {
		return Err!(Request(InvalidParam("Unknown media kind.")));
	};
	let Some(kind) = TextKind::parse(&body.kind) else {
		return Err!(Request(InvalidParam("Unknown kind of text.")));
	};

	if !services
		.state_accessor
		.user_can_see_room(sender_user, &body.room_id)
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
		.map_or(MISSING_DEFAULT, |limit| {
			usize_from_ruma_bounded(limit, MISSING_DEFAULT, MISSING_MAX)
		});

	let shortroomid = services
		.short
		.get_shortroomid(&body.room_id)
		.await?;

	let pdu_ids = services
		.media_text
		.missing(shortroomid, media, kind, from, limit)
		.await;

	// Where to go on from, which is the oldest one looked at and not the oldest one
	// returned: everything in between has been read already and must not be walked
	// again on the next page.
	let end = (pdu_ids.len() == limit)
		.then(|| pdu_ids.last().map(|id| id.pdu_count().to_string()))
		.flatten();

	let mut event_ids = Vec::with_capacity(pdu_ids.len());
	for pdu_id in &pdu_ids {
		if let Ok(pdu) = services.timeline.get_pdu_from_id(pdu_id).await {
			event_ids.push(tuwunel_core::matrix::Event::event_id(&pdu).to_owned());
		}
	}

	Ok(missing::Response { event_ids, end })
}

/// The event's place in the index, once it is settled that this user may read it.
async fn readable(
	services: &crate::State,
	sender_user: &ruma::UserId,
	room_id: &ruma::RoomId,
	event_id: &ruma::EventId,
) -> Result<(tuwunel_service::rooms::short::ShortRoomId, tuwunel_core::matrix::pdu::RawPduId)> {
	if !services
		.state_accessor
		.user_can_see_event(sender_user, room_id, event_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to see that event.")));
	}

	services
		.media_text
		.pdu_id_of(room_id, event_id)
		.await
}
