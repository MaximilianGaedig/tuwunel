//! Exports a room's events: everything the user may see, in order, in pages
//! large enough that a chat of a million messages is a few hundred requests
//! instead of the tens of thousands `/messages` would take.
//!
//! The events are the ones `/messages` returns, in the format it returns them
//! in, so whatever a client already does with a page of history it can do with
//! a page of an export. What is left out is what `/messages` works out per
//! event for a timeline on screen - bundled reactions and edits - because an
//! export holds the reactions and edits themselves.
//!
//! The room's files are listed by `room_export_media`. Exporting every room
//! needs nothing more from the server: a client goes through its joined rooms.

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
	matrix::{Event, pdu::PduCount},
	utils::{math::usize_from_ruma_bounded, stream::TryIgnore},
};
use tuwunel_service::rooms::export::{Order, Selection};

use super::message::event_filters;
use crate::Ruma;

const LIMIT_DEFAULT: usize = 1000;
const LIMIT_MAX: usize = 5000;

/// The most events one request reads, kept or not. A narrow selection in a large room - one
/// month of a million messages - would otherwise read the whole room before answering; this
/// way it answers with what it found so far and where to continue.
const SCAN_MAX: usize = 20_000;

metadata! {
	method: GET,
	rate_limited: true,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/im.mxg.export/rooms/{room_id}/events",
	}
}

#[request]
pub struct Request {
	/// The room to export.
	#[ruma_api(path)]
	pub room_id: OwnedRoomId,

	/// Continues after the `end` of a previous response.
	#[ruma_api(query)]
	pub from: Option<String>,

	/// How many events to return.
	#[ruma_api(query)]
	pub limit: Option<UInt>,

	/// `f` for oldest first, which is the default, or `b` for newest first.
	#[ruma_api(query)]
	pub dir: Option<String>,

	/// Only events sent at or after this time (milliseconds since the epoch).
	#[ruma_api(query)]
	pub since_ts: Option<UInt>,

	/// Only events sent before this time.
	#[ruma_api(query)]
	pub until_ts: Option<UInt>,

	/// Only events of these types, separated by commas.
	#[ruma_api(query)]
	pub types: Option<String>,
}

#[response]
pub struct Response {
	/// The events, in the order asked for. A redacted event is here as it is stored, without its
	/// content: whether an export shows that something was removed is the client's to decide.
	pub chunk: Vec<Raw<AnyTimelineEvent>>,

	/// Pass as `from` for the next page; absent once the room is exhausted. A page may hold fewer
	/// events than `limit`, or none, and still have an `end`: only its absence means the end.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub end: Option<String>,
}

/// # `GET /_matrix/client/unstable/im.mxg.export/rooms/{roomId}/events`
pub(crate) async fn get_room_export_events_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();
	let room_id = &body.room_id;

	let Some(order) = Order::parse(body.dir.as_deref()) else {
		return Err!(Request(InvalidParam("`dir` is `f` or `b`.")));
	};

	let since_ts: Option<u64> = body.since_ts.map(Into::into);
	let until_ts: Option<u64> = body.until_ts.map(Into::into);
	let selection = Selection::parse(since_ts, until_ts, body.types.as_deref())
		.map_err(|reason| err!(Request(InvalidParam("{reason}"))))?;

	let from: Option<PduCount> = body
		.from
		.as_deref()
		.map(str::parse)
		.transpose()
		.map_err(|_| err!(Request(InvalidParam("Invalid `from` token."))))?;

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

	// Read as stored, in the room's own order. Unlike `/messages`, reading back never asks other
	// servers for history this one lacks: an export is of what is here.
	let pdus = match order {
		| Order::OldestFirst => services
			.timeline
			.pdus(Some(sender_user), room_id, from)
			.ignore_err()
			.left_stream(),
		| Order::NewestFirst => services
			.timeline
			.pdus_rev(Some(sender_user), room_id, from)
			.ignore_err()
			.right_stream(),
	};
	let mut pdus = pin!(pdus);

	let mut chunk: Vec<Raw<AnyTimelineEvent>> = Vec::new();
	let mut scanned: usize = 0;
	let mut last: Option<PduCount> = None;
	let mut more = false;
	while let Some((count, pdu)) = pdus.next().await {
		// Stopping only once a further event is in hand is what lets a full last page say it is
		// the last, instead of sending the client back for an empty one.
		if chunk.len() >= limit || scanned >= SCAN_MAX {
			more = true;
			break;
		}
		scanned = scanned.saturating_add(1);
		last = Some(count);

		let ts = u64::from(pdu.origin_server_ts().get());
		if !selection.admits(pdu.kind().to_string().as_str(), ts) {
			continue;
		}

		// The checks `/messages` makes: nothing the user may not see, nothing from someone they ignore.
		let Some((_, pdu)) = event_filters(&services, sender_user, (count, pdu), false).await
		else {
			continue;
		};

		chunk.push(pdu.into_format());
	}

	let end = more
		.then(|| last.as_ref().map(ToString::to_string))
		.flatten();

	Ok(Response { chunk, end })
}
