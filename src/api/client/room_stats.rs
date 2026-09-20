//! A room's message counts, for the room's details: how many messages there
//! are, how many of each kind, and how many each sender wrote.

use std::collections::BTreeMap;

use axum::extract::State;
use ruma::{
	OwnedRoomId, UInt,
	api::{auth_scheme::AccessToken, request, response},
	metadata,
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Result, utils::math::usize_from_ruma_bounded};

use crate::Ruma;

const SENDERS_DEFAULT: usize = 100;
const SENDERS_MAX: usize = 5000;

metadata! {
	method: GET,
	rate_limited: true,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/im.mxg.stats/rooms/{room_id}",
	}
}

#[request]
pub struct Request {
	#[ruma_api(path)]
	pub room_id: OwnedRoomId,

	/// How many senders to list, the most active first.
	#[ruma_api(query)]
	pub senders: Option<UInt>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SenderStats {
	pub user_id: String,
	pub total: u64,
	pub by_kind: BTreeMap<String, u64>,
}

#[response]
pub struct Response {
	/// All counted messages.
	pub total: u64,

	/// Messages by kind: text, image, video, audio, voice, file, sticker.
	pub by_kind: BTreeMap<String, u64>,

	/// The most active senders, and how many of each kind they sent.
	pub senders: Vec<SenderStats>,

	/// How many different people wrote.
	pub sender_count: u64,

	/// When the first and the latest counted message were sent (milliseconds).
	pub first_ts: Option<u64>,
	pub last_ts: Option<u64>,

	/// False until the counters have covered the room's whole history.
	pub complete: bool,
}

/// # `GET /_matrix/client/unstable/im.mxg.stats/rooms/{roomId}`
pub(crate) async fn get_room_stats_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();
	let room_id = &body.room_id;

	if !services
		.state_accessor
		.user_can_see_room(sender_user, room_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to view this room.")));
	}

	let limit = body.senders.map_or(SENDERS_DEFAULT, |limit| {
		usize_from_ruma_bounded(limit, SENDERS_DEFAULT, SENDERS_MAX)
	});

	let shortroomid = services.short.get_shortroomid(room_id).await?;
	let stats = services.room_stats.stats(shortroomid).await;

	let mut by_kind: BTreeMap<String, u64> = BTreeMap::new();
	let mut by_sender: BTreeMap<String, SenderStats> = BTreeMap::new();
	let mut total: u64 = 0;

	for entry in stats.counts {
		total = total.saturating_add(entry.count);
		let kind = entry.class.name().to_owned();
		*by_kind.entry(kind.clone()).or_default() += entry.count;

		let sender = by_sender
			.entry(entry.sender.clone())
			.or_insert_with(|| SenderStats {
				user_id: entry.sender,
				total: 0,
				by_kind: BTreeMap::new(),
			});
		sender.total = sender.total.saturating_add(entry.count);
		*sender.by_kind.entry(kind).or_default() += entry.count;
	}

	let sender_count = by_sender.len() as u64;
	let mut senders: Vec<_> = by_sender.into_values().collect();
	senders.sort_by(|a, b| b.total.cmp(&a.total).then_with(|| a.user_id.cmp(&b.user_id)));
	senders.truncate(limit);

	Ok(Response {
		total,
		by_kind,
		senders,
		sender_count,
		first_ts: stats.first_ts,
		last_ts: stats.last_ts,
		complete: stats.complete,
	})
}
