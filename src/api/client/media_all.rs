//! Every chat's media in one list, newest first: the shared-media view of a
//! whole account rather than of one room, which is how a phone shows "all my
//! pictures" without the user remembering which chat a thing was sent in.
//!
//! Built from the per-room index (`rooms::media_index`), whose rows carry each
//! item's time, so the merge reads index keys only - no event is fetched until
//! the page has been decided. One page therefore costs one small read per joined
//! room plus `limit` event fetches, whatever the size of the history behind it.
//!
//! Encrypted rooms are absent for the reason they are absent from the per-room
//! index: the server cannot see what their messages are.

use std::pin::pin;

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
	matrix::{Event, pdu::RawPduId},
	utils::math::usize_from_ruma_bounded,
};
use tuwunel_service::rooms::media_index::MediaKind;

use crate::Ruma;

const LIMIT_DEFAULT: usize = 50;
const LIMIT_MAX: usize = 200;

metadata! {
	method: GET,
	rate_limited: true,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/im.mxg.media_index/media",
	}
}

#[request]
pub struct Request {
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
	/// The matching events, newest first, from every chat at once.
	pub chunk: Vec<Raw<AnyTimelineEvent>>,

	/// Which room each event of `chunk` came from, in the same order: a client
	/// showing a mixed list needs it to open the right chat.
	pub rooms: Vec<OwnedRoomId>,

	/// Pass as `from` for the next page; absent once the list is exhausted.
	pub end: Option<String>,
}

/// Where a page stopped: the time and the event, because two chats can hold
/// items sent in the same millisecond and a time alone would skip one of them.
struct Cursor {
	ts: u64,
	pdu_id: RawPduId,
}

impl Cursor {
	fn parse(token: &str) -> Option<Self> {
		let (ts, id) = token.split_once('_')?;
		Some(Self {
			ts: ts.parse().ok()?,
			pdu_id: RawPduId::from(&*hex_to_bytes(id)?),
		})
	}

	fn token(&self) -> String { format!("{}_{}", self.ts, bytes_to_hex(self.pdu_id.as_ref())) }

	/// Whether an item belongs to a later page than this cursor: strictly older,
	/// or the same moment in an event this page already showed.
	fn precedes(&self, ts: u64, pdu_id: &RawPduId) -> bool {
		ts < self.ts || (ts == self.ts && pdu_id.as_ref() < self.pdu_id.as_ref())
	}
}

fn bytes_to_hex(bytes: &[u8]) -> String {
	bytes
		.iter()
		.map(|byte| format!("{byte:02x}"))
		.collect()
}

fn hex_to_bytes(text: &str) -> Option<Vec<u8>> {
	(text.len() % 2 == 0)
		.then(|| {
			(0..text.len())
				.step_by(2)
				.map(|at| u8::from_str_radix(text.get(at..at + 2)?, 16).ok())
				.collect::<Option<Vec<u8>>>()
		})
		.flatten()
}

/// # `GET /_matrix/client/unstable/im.mxg.media_index/media`
pub(crate) async fn get_all_media_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();

	let Some(kind) = MediaKind::parse(&body.kind) else {
		return Err!(Request(InvalidParam("Unknown media kind.")));
	};

	let limit = body
		.limit
		.map_or(LIMIT_DEFAULT, |limit| usize_from_ruma_bounded(limit, LIMIT_DEFAULT, LIMIT_MAX));

	let from = body
		.from
		.as_deref()
		.map(|token| Cursor::parse(token).ok_or_else(|| err!(Request(InvalidParam("Invalid `from` token.")))))
		.transpose()?;

	let rooms: Vec<OwnedRoomId> = services
		.state_cache
		.rooms_joined(sender_user)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	// One room at a time, taking only as many as could possibly make this page:
	// a room's own list is already newest-first, so anything past `limit` of it
	// cannot outrank what is already held.
	let mut candidates: Vec<(u64, RawPduId, OwnedRoomId)> = Vec::new();
	for room_id in rooms {
		let Ok(shortroomid) = services
			.short
			.get_shortroomid(&room_id)
			.await
		else {
			continue;
		};

		let mut taken = 0_usize;
		// Dated as they are read: a row written before the index stored times is
		// placed by its event's own time rather than left out of the list.
		let mut entries = pin!(services.media_index.dated_entries(shortroomid, kind, None));
		while let Some((pdu_id, ts)) = entries.next().await {
			if from
				.as_ref()
				.is_some_and(|cursor| !cursor.precedes(ts, &pdu_id))
			{
				continue;
			}
			candidates.push((ts, pdu_id, room_id.clone()));
			taken = taken.saturating_add(1);
			if taken >= limit {
				break;
			}
		}
	}

	// Newest first across the lot, then the visible ones until the page is full.
	candidates.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.as_ref().cmp(a.1.as_ref())));

	let mut chunk = Vec::with_capacity(limit);
	let mut rooms_of = Vec::with_capacity(limit);
	let mut last: Option<Cursor> = None;

	for (ts, pdu_id, room_id) in candidates {
		if chunk.len() >= limit {
			break;
		}
		let Ok(pdu) = services
			.timeline
			.get_pdu_from_id(&pdu_id)
			.await
		else {
			continue;
		};
		if pdu.is_redacted()
			|| !services
				.state_accessor
				.user_can_see_event(sender_user, &room_id, pdu.event_id())
				.await
		{
			continue;
		}
		last = Some(Cursor { ts, pdu_id });
		chunk.push(pdu.into_format());
		rooms_of.push(room_id);
	}

	// A full page may have more behind it; a short one has reached the end.
	let end = (chunk.len() == limit)
		.then(|| last.map(|cursor| cursor.token()))
		.flatten();

	Ok(Response { chunk, rooms: rooms_of, end })
}
