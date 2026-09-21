//! Lists a room's media from the index (`rooms::media_index`), so a client's
//! Media, Files, Links, Music and Voice tabs open at once instead of paging
//! back through the room's history and sorting it out themselves.
//!
//! Encrypted rooms are not indexed, because the server cannot read them; a
//! client still has to read those itself.

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

	/// Starts at the newest item sent at or before this time (milliseconds since the epoch), so a
	/// client can open the list at a date instead of paging back to it.
	#[ruma_api(query)]
	pub before_ts: Option<UInt>,

	/// Returns how many items there are per calendar month instead of the items themselves, so a
	/// scrubber can be drawn and labelled without loading anything.
	#[ruma_api(query)]
	pub months: Option<bool>,
}

#[response]
pub struct Response {
	/// The matching events, newest first. Empty when `months` was asked for.
	pub chunk: Vec<Raw<AnyTimelineEvent>>,

	/// Pass as `from` for the next page; absent once the list is exhausted.
	pub end: Option<String>,

	/// How many items each month holds, newest month first, when `months` was asked for.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub months: Vec<MonthCount>,
}

/// One month of a room's media of one kind.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct MonthCount {
	/// The month, as `YYYY-MM` in UTC.
	pub month: String,

	/// How many items it holds.
	pub count: usize,

	/// The time to pass as `before_ts` to open the list at this month.
	pub before_ts: u64,
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

	if body.months.unwrap_or(false) {
		return month_counts(&services, shortroomid, kind).await;
	}

	let before_ts: Option<u64> = body.before_ts.map(Into::into);

	let events: Vec<_> = services
		.media_index
		.media_entries(shortroomid, kind, from)
		// Seeking by date reads the index alone: the rows carry their own time, so skipping to a month
		// costs no event fetches at all. A row written before the index stored times has none, and is
		// kept rather than skipped, so an unrebuilt index still lists everything.
		.skip_while(move |(_, ts)| {
			let skip = before_ts.is_some_and(|before| ts.is_some_and(|ts| ts > before));
			async move { skip }
		})
		.map(|(pdu_id, _)| pdu_id)
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
		months: Vec::new(),
	})
}

/// How many items the room holds per calendar month, newest first.
///
/// Read from the index alone - the rows carry their own time - so a scrubber can be sized and labelled
/// without opening a single event. Rows from before the index stored times are left out of the counts
/// rather than guessed at; `rebuild_room` fills them in.
async fn month_counts(
	services: &crate::State,
	shortroomid: tuwunel_service::rooms::short::ShortRoomId,
	kind: MediaKind,
) -> Result<Response> {
	let mut months: Vec<MonthCount> = Vec::new();
	let mut entries = pin!(services.media_index.media_entries(shortroomid, kind, None));
	while let Some((_, ts)) = entries.next().await {
		let Some(ts) = ts else {
			continue;
		};
		let month = month_of(ts);
		match months.last_mut() {
			| Some(last) if last.month == month => last.count = last.count.saturating_add(1),
			| _ => months.push(MonthCount { month, count: 1, before_ts: ts }),
		}
	}

	Ok(Response { chunk: Vec::new(), end: None, months })
}

/// `YYYY-MM` in UTC for a time in milliseconds, which is how a client labels the months.
fn month_of(ts: u64) -> String {
	let days = i64::try_from(ts / 86_400_000).unwrap_or(0);
	// Days since 1970-01-01 to a civil date, Howard Hinnant's `civil_from_days`.
	let z = days.saturating_add(719_468);
	let era = z.div_euclid(146_097);
	let doe = z.rem_euclid(146_097);
	let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
	let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
	let mp = (5 * doy + 2) / 153;
	let month = if mp < 10 { mp + 3 } else { mp - 9 };
	let year = era * 400 + yoe + i64::from(month <= 2);
	format!("{year:04}-{month:02}")
}

#[cfg(test)]
mod tests {
	use super::month_of;

	/// Date maths is easy to get subtly wrong, and a scrubber labelled a month out is worse than none.
	#[test]
	fn months_are_utc_calendar_months() {
		assert_eq!(month_of(0), "1970-01");
		// 2026-09-21T06:00:00Z, the day this was written.
		assert_eq!(month_of(1_789_970_400_000), "2026-09");
		// The last millisecond of a month and the first of the next must not share one.
		assert_eq!(month_of(1_788_220_799_999), "2026-08");
		assert_eq!(month_of(1_788_220_800_000), "2026-09");
		// A leap day, where a year's length matters.
		assert_eq!(month_of(1_709_164_800_000), "2024-02");
		// The turn of a century, where the 400-year rule matters.
		assert_eq!(month_of(946_684_800_000), "2000-01");
	}
}
