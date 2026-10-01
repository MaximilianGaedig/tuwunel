//! A person's activity log (`activity_log`): when they were around, as the
//! rows themselves or folded into the hours of their week.
//!
//! Anyone who shares a room with the person may read it, which is who sees
//! their presence. Rooms are named only where the asker is in them too.

use std::collections::HashMap;

use axum::extract::State;
use futures::StreamExt;
use ruma::{
	Int, OwnedRoomId, OwnedUserId, UInt,
	api::{auth_scheme::AccessToken, request, response},
	metadata,
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{
	Err, Result,
	utils::{math::usize_from_ruma_bounded, millis_since_unix_epoch},
};
use tuwunel_service::activity_log::Week;

use crate::Ruma;

const LIMIT_DEFAULT: usize = 500;
const LIMIT_MAX: usize = 10_000;

metadata! {
	method: GET,
	rate_limited: true,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/im.mxg.activity/users/{user_id}",
	}
}

#[request]
pub struct Request {
	/// Whose log.
	#[ruma_api(path)]
	pub user_id: OwnedUserId,

	/// The times to read between, in milliseconds since the epoch; everything by default.
	#[ruma_api(query)]
	pub from_ts: Option<UInt>,
	#[ruma_api(query)]
	pub to_ts: Option<UInt>,

	/// How many rows to return, newest first.
	#[ruma_api(query)]
	pub limit: Option<UInt>,

	/// Returns the hours of their week instead of the rows.
	#[ruma_api(query)]
	pub week: Option<bool>,

	/// For `week`: the asker's distance from UTC in minutes, so the hours are theirs.
	#[ruma_api(query)]
	pub utc_offset_minutes: Option<Int>,
}

#[response]
pub struct Response {
	/// The rows, newest first. Empty when `week` was asked for.
	pub chunk: Vec<Row>,

	/// Pass as `to_ts` for the next page; absent once the log is exhausted.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub end: Option<u64>,

	/// When they are usually around, when `week` was asked for.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub week: Option<WeekGrid>,
}

/// One sign of the person being there.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Row {
	/// When, in milliseconds since the epoch.
	pub ts: u64,

	/// `online`, `unavailable`, `offline`, `sent`, `typing`, `read` or `reaction`.
	pub kind: String,

	/// Where, for what happened in a room the asker is in too.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub room_id: Option<OwnedRoomId>,

	/// For a presence change: when the server held them as last active.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub last_active_ts: Option<u64>,
}

/// The hours of a person's week. Rows are weekdays from Monday, columns the
/// hours of the day.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WeekGrid {
	/// On how many different days they were around in each hour.
	pub seen: Vec<Vec<u32>>,

	/// How long they were online in each hour, in milliseconds, over all those days.
	pub online_ms: Vec<Vec<u64>>,

	/// How many of each weekday the log covers: what `seen` is out of.
	pub days: Vec<u32>,

	/// The first and the last row read, and how many there were.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub first_ts: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub last_ts: Option<u64>,
	pub entries: u64,
}

impl From<Week> for WeekGrid {
	fn from(week: Week) -> Self {
		Self {
			seen: week.seen.iter().map(|day| day.to_vec()).collect(),
			online_ms: week
				.online_ms
				.iter()
				.map(|day| day.to_vec())
				.collect(),
			days: week.days.to_vec(),
			first_ts: week.first_ts,
			last_ts: week.last_ts,
			entries: week.entries,
		}
	}
}

/// # `GET /_matrix/client/unstable/im.mxg.activity/users/{userId}`
pub(crate) async fn get_user_activity_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();
	let user_id = &body.user_id;

	if sender_user != user_id
		&& !services
			.state_cache
			.user_sees_user(sender_user, user_id)
			.await
	{
		return Err!(Request(Forbidden("You don't share a room with this user.")));
	}

	let from_ts: u64 = body.from_ts.map_or(0, Into::into);
	let to_ts: u64 = body
		.to_ts
		.map_or_else(millis_since_unix_epoch, Into::into);

	if body.week.unwrap_or(false) {
		let offset_ms = body
			.utc_offset_minutes
			.map_or(0, i64::from)
			.saturating_mul(60_000);
		let week = services
			.activity_log
			.week(user_id, from_ts, to_ts, offset_ms)
			.await;

		return Ok(Response {
			chunk: Vec::new(),
			end: None,
			week: Some(week.into()),
		});
	}

	let limit = body
		.limit
		.map_or(LIMIT_DEFAULT, |limit| usize_from_ruma_bounded(limit, LIMIT_DEFAULT, LIMIT_MAX));

	let entries: Vec<_> = services
		.activity_log
		.entries_rev(user_id, from_ts, to_ts)
		.take(limit)
		.collect()
		.await;

	// Each room is looked up once: its id, and whether the asker may be told it.
	let mut rooms: HashMap<u64, Option<OwnedRoomId>> = HashMap::new();
	let mut chunk = Vec::with_capacity(entries.len());
	for entry in &entries {
		let mut row = Row {
			ts: entry.ts,
			kind: entry.kind.name().to_owned(),
			room_id: None,
			last_active_ts: None,
		};
		if entry.kind.is_presence() {
			row.last_active_ts = Some(entry.value);
		} else {
			if !rooms.contains_key(&entry.value) {
				let room_id = services
					.short
					.get_roomid_from_short(entry.value)
					.await
					.ok();
				let mut shown = None;
				if let Some(room_id) = room_id
					&& services
						.state_cache
						.is_joined(sender_user, &room_id)
						.await
				{
					shown = Some(room_id);
				}
				rooms.insert(entry.value, shown);
			}
			row.room_id = rooms.get(&entry.value).cloned().flatten();
		}
		chunk.push(row);
	}

	// Where the next page continues, or nothing once this one wasn't full.
	let end = (entries.len() == limit)
		.then(|| entries.last().map(|entry| entry.ts.saturating_sub(1)))
		.flatten();

	Ok(Response { chunk, end, week: None })
}
