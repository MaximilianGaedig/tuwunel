//! When something was last said in a room: what sliding sync orders the room
//! list by and sends as each room's bump_stamp.
//!
//! It is a time - when the newest message was sent - not where that message sits
//! in the stream. A bridge appending a chat's old history puts years-old messages
//! at the end of the stream, and by position the chat jumped above everything said
//! today; a time is also what clients can set against the timestamps they sort by.
//! Only messages count (and being invited): not reactions, renames, receipts or a
//! bridge's bookkeeping, and not the user's own join, which a bridge makes for a
//! portal to a chat that may have been silent for years.

use std::{
	collections::HashMap,
	sync::{LazyLock, Mutex},
};

use futures::{StreamExt, TryStreamExt, future::ready, pin_mut};
use ruma::{
	OwnedRoomId, OwnedUserId, RoomId, UInt, UserId,
	events::TimelineEventType::{
		self, Beacon, CallInvite, PollStart, RoomEncrypted, RoomMessage, Sticker,
	},
};
use tuwunel_core::{
	Result, is_equal_to,
	matrix::{
		Event,
		pdu::{PduCount, PduEvent},
	},
	utils::TryReadyExt,
};
use tuwunel_service::Services;

/// How many of a room's latest events are looked through for its newest message.
/// Enough for the reactions, edits and statuses that follow messages; a bridge's
/// backfill appended after a newer message is older than it, and loses to it here.
const SCAN: usize = 50;

/// The newest send time of a message among the room's latest events after `since`.
pub(super) async fn newest_said(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	until: PduCount,
	since: PduCount,
) -> Result<Option<UInt>> {
	services
		.timeline
		.pdus_rev(Some(sender_user), room_id, Some(until.saturating_add(1)))
		.ready_try_take_while(|&(pdu_count, _)| Ok(pdu_count > since))
		.take(SCAN)
		.try_fold(Said::default(), |said, (_, pdu)| ready(Ok(said.with(&pdu, sender_user))))
		.await
		.map(|said| said.newest)
}

/// What `newest_said` has found so far, going back from the newest event.
#[derive(Clone, Copy, Debug, Default)]
struct Said {
	newest: Option<UInt>,
	/// One of the user's own membership events has been passed.
	own_membership_seen: bool,
}

impl Said {
	/// Being invited is news only while it is still the user's membership: a
	/// bridge invites the user to a portal and joins them to it at once, and that
	/// invite dated the chat by when the portal was made rather than by its last
	/// message.
	fn with(self, pdu: &PduEvent, sender_user: &UserId) -> Self {
		let own_membership = *pdu.event_type() == TimelineEventType::RoomMember
			&& pdu
				.state_key()
				.is_some_and(is_equal_to!(sender_user.as_str()));

		let answered = own_membership && self.own_membership_seen;
		let counts = is_bumpable_pdu(pdu, sender_user) && !answered;
		let newest = match (counts, self.newest) {
			| (false, newest) => newest,
			| (true, None) => Some(pdu.origin_server_ts().get()),
			| (true, Some(newest)) => Some(newest.max(pdu.origin_server_ts().get())),
		};

		Self {
			newest,
			own_membership_seen: self.own_membership_seen || own_membership,
		}
	}
}

/// The room's newest message up to `until`, with its position, for a room list's preview: the event
/// `newest_said` dates the room by, among the room's latest events. Not an invite, which a room list
/// shows by itself.
pub(super) async fn newest_said_event(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	until: PduCount,
) -> Result<Option<(PduCount, PduEvent)>> {
	services
		.timeline
		.pdus_rev(Some(sender_user), room_id, Some(until.saturating_add(1)))
		.take(SCAN)
		.ready_try_filter_map(|(count, pdu)| {
			Ok(is_preview_pdu(&pdu, sender_user).then_some((count, pdu)))
		})
		.try_fold(None, |newest: Option<(PduCount, PduEvent)>, (count, pdu)| {
			let newer = newest
				.as_ref()
				.is_none_or(|(_, seen)| pdu.origin_server_ts() > seen.origin_server_ts());
			ready(Ok(if newer { Some((count, pdu)) } else { newest }))
		})
		.await
}

/// Whether an event can be a room's preview: a message, as for the list's order, but not a membership.
pub(super) fn is_preview_pdu(pdu: &PduEvent, sender_user: &UserId) -> bool {
	*pdu.event_type() != TimelineEventType::RoomMember && is_bumpable_pdu(pdu, sender_user)
}

/// Per user and room: the stream position it was worked out at, and the time.
type Known = HashMap<(OwnedUserId, OwnedRoomId), (PduCount, u64)>;
static KNOWN: LazyLock<Mutex<Known>> = LazyLock::new(Default::default);

/// When something was last said in the room, for ordering the list: worked out
/// again only when the room has moved on from `last` (its newest event), and the
/// newest event's own time when none of the latest is a message.
pub(super) async fn last_said(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	last: PduCount,
) -> u64 {
	let key = (sender_user.to_owned(), room_id.to_owned());
	if let Some(&(at, ts)) = KNOWN.lock().expect("locked").get(&key) {
		if at == last {
			return ts;
		}
	}

	let said = newest_said(services, sender_user, room_id, last, PduCount::Normal(0))
		.await
		.ok()
		.flatten();
	let ts = match said {
		| Some(ts) => ts.into(),
		| None => {
			let newest = services.timeline.pdus_rev(
				Some(sender_user),
				room_id,
				Some(last.saturating_add(1)),
			);
			pin_mut!(newest);
			newest
				.try_next()
				.await
				.ok()
				.flatten()
				.map_or(0, |(_, pdu)| pdu.origin_server_ts().get().into())
		},
	};

	KNOWN
		.lock()
		.expect("locked")
		.insert(key, (last, ts));
	ts
}

/// MUST be sorted by `TimelineEventType::event_type_str()` for `binary_search`.
static DEFAULT_BUMP_TYPES: [TimelineEventType; 6] = [
	CallInvite,    // m.call.invite
	PollStart,     // m.poll.start
	RoomEncrypted, // m.room.encrypted
	RoomMessage,   // m.room.message
	Sticker,       // m.sticker
	Beacon,        // org.matrix.msc3672.beacon
];

fn is_bumpable_pdu(pdu: &PduEvent, sender_user: &UserId) -> bool {
	if pdu.is_redacted() {
		return false;
	}

	// Being invited is news; joining is not: a bridge joins the user to a portal for a chat that
	// may not have been written in for years, and it is the chat's messages that say when it was.
	if *pdu.event_type() == TimelineEventType::RoomMember {
		return pdu
			.state_key()
			.is_some_and(is_equal_to!(sender_user.as_str()))
			&& pdu
				.get_content_as_value()
				.get("membership")
				.and_then(|membership| membership.as_str())
				== Some("invite");
	}

	DEFAULT_BUMP_TYPES
		.binary_search(pdu.event_type())
		.is_ok()
}

#[cfg_attr(debug_assertions, tuwunel_core::ctor(unsafe))]
fn _is_sorted() {
	debug_assert!(
		DEFAULT_BUMP_TYPES.is_sorted(),
		"DEFAULT_BUMP_TYPES must be sorted by the developer"
	);
}

#[cfg(test)]
mod tests {
	use ruma::{event_id, events::TimelineEventType, room_id, serde::Raw, uint, user_id};
	use serde_json::{json, value::to_raw_value};
	use tuwunel_core::matrix::{StateKey, pdu::PduEvent};

	use super::{DEFAULT_BUMP_TYPES, Said, is_bumpable_pdu, is_preview_pdu};

	fn pdu(kind: TimelineEventType, state_key: Option<StateKey>, redacted: bool) -> PduEvent {
		pdu_with(kind, state_key, redacted, json!({}))
	}

	fn pdu_with(
		kind: TimelineEventType,
		state_key: Option<StateKey>,
		redacted: bool,
		content: serde_json::Value,
	) -> PduEvent {
		let unsigned = redacted.then(|| {
			to_raw_value(&json!({ "redacted_because": {} }))
				.expect("valid unsigned")
				.into()
		});

		PduEvent {
			kind,
			content: Raw::from_json(to_raw_value(&content).expect("valid content")),
			event_id: event_id!("$event:example.com").to_owned(),
			room_id: room_id!("!room:example.com").to_owned(),
			sender: user_id!("@alice:example.com").to_owned(),
			state_key,
			redacts: None,
			prev_events: Default::default(),
			auth_events: Default::default(),
			origin_server_ts: uint!(1),
			depth: uint!(1),
			hashes: Default::default(),
			origin: None,
			unsigned,
		}
	}

	#[test]
	fn default_bump_types_are_sorted() {
		assert!(DEFAULT_BUMP_TYPES.is_sorted());
	}

	#[test]
	fn default_bump_types_bump() {
		let sender = user_id!("@alice:example.com");

		for kind in DEFAULT_BUMP_TYPES.iter().cloned() {
			assert!(is_bumpable_pdu(&pdu(kind, None, false), sender));
		}
	}

	#[test]
	fn non_bump_type_does_not_bump() {
		let sender = user_id!("@alice:example.com");
		let pdu = pdu(TimelineEventType::RoomName, Some("".into()), false);

		assert!(!is_bumpable_pdu(&pdu, sender));
	}

	#[test]
	fn own_invite_bumps() {
		let sender = user_id!("@alice:example.com");
		let pdu = pdu_with(
			TimelineEventType::RoomMember,
			Some(sender.as_str().into()),
			false,
			json!({ "membership": "invite" }),
		);

		assert!(is_bumpable_pdu(&pdu, sender));
	}

	// A bridge joining the user to a portal for an old chat is not news.
	#[test]
	fn messages_are_previews_and_reactions_and_invites_are_not() {
		let sender = user_id!("@alice:example.com");

		assert!(is_preview_pdu(&pdu(TimelineEventType::RoomMessage, None, false), sender));
		assert!(!is_preview_pdu(&pdu(TimelineEventType::Reaction, None, false), sender));
		assert!(!is_preview_pdu(&pdu(TimelineEventType::RoomMessage, None, true), sender));

		// An invite orders the list but is not a message to show beside it.
		let invite = pdu_with(
			TimelineEventType::RoomMember,
			Some(sender.as_str().into()),
			false,
			json!({ "membership": "invite" }),
		);
		assert!(is_bumpable_pdu(&invite, sender));
		assert!(!is_preview_pdu(&invite, sender));
	}

	#[test]
	fn own_join_does_not_bump() {
		let sender = user_id!("@alice:example.com");
		let pdu = pdu_with(
			TimelineEventType::RoomMember,
			Some(sender.as_str().into()),
			false,
			json!({ "membership": "join" }),
		);

		assert!(!is_bumpable_pdu(&pdu, sender));
	}

	#[test]
	fn other_membership_does_not_bump() {
		let sender = user_id!("@alice:example.com");
		let pdu = pdu(TimelineEventType::RoomMember, Some("@bob:example.com".into()), false);

		assert!(!is_bumpable_pdu(&pdu, sender));
	}

	#[test]
	fn redacted_pdu_does_not_bump() {
		let sender = user_id!("@alice:example.com");
		let pdu = pdu(TimelineEventType::RoomMessage, None, true);

		assert!(!is_bumpable_pdu(&pdu, sender));
	}

	fn at(mut pdu: PduEvent, ts: u64) -> PduEvent {
		pdu.origin_server_ts = ts.try_into().expect("fits");
		pdu
	}

	fn newest_said(newest_first: &[PduEvent]) -> Option<u64> {
		let sender = user_id!("@alice:example.com");

		newest_first
			.iter()
			.fold(Said::default(), |said, pdu| said.with(pdu, sender))
			.newest
			.map(Into::into)
	}

	fn own_membership(membership: &str, ts: u64) -> PduEvent {
		at(
			pdu_with(
				TimelineEventType::RoomMember,
				Some("@alice:example.com".into()),
				false,
				json!({ "membership": membership }),
			),
			ts,
		)
	}

	// A bridge makes a portal for an old chat: it invites and joins the user, then
	// imports the chat's history, all dated by when it was said.
	#[test]
	fn an_invite_already_joined_does_not_date_the_room() {
		let newest_first = [
			at(pdu(TimelineEventType::from("im.mxg.settings"), Some("".into()), false), 300),
			at(pdu(TimelineEventType::RoomMessage, None, false), 100),
			own_membership("join", 210),
			own_membership("invite", 200),
		];

		assert_eq!(newest_said(&newest_first), Some(100));
	}

	#[test]
	fn a_pending_invite_dates_the_room() {
		let newest_first = [
			own_membership("invite", 200),
			at(pdu(TimelineEventType::RoomMessage, None, false), 100),
		];

		assert_eq!(newest_said(&newest_first), Some(200));
	}
}
