mod read_markers;
mod receipt;

use futures::future::try_join;
use ruma::{
	CanonicalJsonValue, EventId, MilliSecondsSinceUnixEpoch, RoomId, UInt, UserId,
	events::receipt::ReceiptThread,
};
use tuwunel_core::{Err, PduCount, PduId, Result, debug, err, utils::result::LogErr};
use tuwunel_service::{Services, rooms::read_receipt::PrivateRead};

pub(crate) use self::{read_markers::set_read_marker_route, receipt::create_receipt_route};

/// Where a bridge says when a receipt it replays was earned on its network.
const BRIDGED_READ_EXTRA: &str = "com.beeper.read.extra";

/// When a public read receipt was earned.
///
/// For a person's own client that is now. A bridge passes on what happened on
/// its network, sometimes long after: syncing a chat replays the other side's
/// read state, and the bridge says in `com.beeper.read.extra.ts` when they
/// read. Only an appservice is believed, and only about the past.
fn read_receipt_ts(
	appservice: bool,
	json_body: Option<&CanonicalJsonValue>,
	now: MilliSecondsSinceUnixEpoch,
) -> MilliSecondsSinceUnixEpoch {
	if !appservice {
		return now;
	}

	bridged_read_ts(json_body)
		.filter(|ts| *ts > UInt::MIN && *ts <= now.0)
		.map_or(now, MilliSecondsSinceUnixEpoch)
}

fn bridged_read_ts(json_body: Option<&CanonicalJsonValue>) -> Option<UInt> {
	let CanonicalJsonValue::Object(body) = json_body? else {
		return None;
	};
	let CanonicalJsonValue::Object(extra) = body.get(BRIDGED_READ_EXTRA)? else {
		return None;
	};
	let CanonicalJsonValue::Integer(ts) = extra.get("ts")? else {
		return None;
	};

	UInt::try_from(i64::from(*ts)).ok()
}

/// Resolves `event` to its timeline position and stores the private read
/// marker for `thread` there.
///
/// Returns whether the marker advanced. A backfilled event carries no forward
/// position, so it is skipped like a non-advancing write rather than failing
/// the request.
async fn set_private_marker(
	services: &Services,
	room_id: &RoomId,
	user_id: &UserId,
	event: &EventId,
	thread: &ReceiptThread,
) -> Result<bool> {
	let (pdu_id, shortroomid) =
		try_join(services.timeline.get_pdu_id(event), services.short.get_shortroomid(room_id))
			.await
			.map_err(|_| err!(Request(NotFound("Event not found."))))?;

	let pdu_id = PduId::from(pdu_id);

	if pdu_id.shortroomid != shortroomid {
		return Err!(Request(NotFound("Event not found.")));
	}

	let PduCount::Normal(count) = pdu_id.count else {
		debug!(%user_id, %room_id, %event, "Skipping private read marker at a backfilled event");
		return Ok(false);
	};

	let advanced = services
		.read_receipt
		.private_read_set(PrivateRead {
			room_id,
			user_id,
			count,
			ts: MilliSecondsSinceUnixEpoch::now(),
			thread,
			announce: true,
		})
		.await;

	Ok(advanced)
}

/// Clears the receipt's notification counts and refreshes the push badge.
///
/// The refresh follows every advance because the gateway can hold a stale
/// badge while the stored count is already zero; only a delivery reconciles
/// it.
async fn reset_and_refresh_badge(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	acknowledged: Option<&EventId>,
	thread: &ReceiptThread,
) {
	services
		.pusher
		.reset_notification_counts_for_thread(user_id, room_id, acknowledged, thread)
		.await;

	services
		.sending
		.refresh_push_badge(user_id)
		.await
		.log_err()
		.ok();
}

#[cfg(test)]
mod tests {
	use ruma::{CanonicalJsonValue, MilliSecondsSinceUnixEpoch, UInt};
	use serde_json::json;

	use super::read_receipt_ts;

	fn body(value: serde_json::Value) -> CanonicalJsonValue {
		CanonicalJsonValue::try_from(value).expect("canonical JSON")
	}

	#[test]
	fn a_bridge_says_when_a_replayed_receipt_was_earned() {
		let now = MilliSecondsSinceUnixEpoch(UInt::new(1_790_000_000_000).expect("in range"));
		let then = MilliSecondsSinceUnixEpoch(UInt::new(1_780_000_000_000).expect("in range"));
		let replayed = body(json!({
			"m.read": "$event",
			"com.beeper.read.extra": { "ts": 1_780_000_000_000_u64 },
		}));

		assert_eq!(read_receipt_ts(true, Some(&replayed), now), then);
		assert_eq!(
			read_receipt_ts(false, Some(&replayed), now),
			now,
			"a person's own client reads now, whatever it says"
		);
	}

	#[test]
	fn a_receipt_without_a_believable_time_was_earned_now() {
		let now = MilliSecondsSinceUnixEpoch(UInt::new(1_790_000_000_000).expect("in range"));

		for value in [
			json!({ "m.read": "$event" }),
			json!({ "com.beeper.read.extra": {} }),
			json!({ "com.beeper.read.extra": { "ts": "yesterday" } }),
			json!({ "com.beeper.read.extra": { "ts": 0 } }),
			json!({ "com.beeper.read.extra": { "ts": -5 } }),
			json!({ "com.beeper.read.extra": { "ts": 1_790_000_000_001_u64 } }),
			json!({ "com.beeper.read.extra": 7 }),
			json!([]),
		] {
			assert_eq!(read_receipt_ts(true, Some(&body(value.clone())), now), now, "{value}");
		}

		assert_eq!(read_receipt_ts(true, None, now), now);
	}
}
