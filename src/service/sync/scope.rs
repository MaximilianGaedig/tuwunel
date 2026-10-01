//! How much of the joined rooms a `/sync` filter leaves to report.
//!
//! A bridge bot that syncs only for to-device messages sends a filter that
//! excludes every room event. Assembling the rooms for it anyway is the whole
//! cost of its sync loop, so a filter that provably lets no room event through
//! is recognised before the rooms are walked.

use ruma::api::client::filter::{FilterDefinition, RoomEventFilter};

/// The event type that excludes every type when it appears in `not_types`.
///
/// The filter API lets `*` stand for any sequence of characters, and a lone
/// `*` is how a client says "nothing of this kind". Wildcards inside a type
/// are not interpreted here: only the lone one is certain to match everything.
const EVERY_TYPE: &str = "*";

/// What a filter leaves of the joined rooms.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoomScope {
	/// The filter may let something through; rooms are reported.
	Everything,

	/// No event of any joined room can pass, but the rooms still decide whose
	/// device lists changed.
	DeviceLists,

	/// No room can pass at all.
	Nothing,
}

impl RoomScope {
	/// Reads the scope off a filter, choosing the wider one when in doubt.
	///
	/// `Nothing` takes an empty `room.rooms`: a room has to be on that list to
	/// be reported, and none is.
	///
	/// `DeviceLists` takes all four of `room.timeline`, `room.state`,
	/// `room.ephemeral` and `room.account_data` rejecting every event, each by
	/// its own [`rejects_everything`].
	///
	/// A `limit` of zero is not counted. It empties a timeline of events, and
	/// the room still reports that it has a gap.
	#[must_use]
	pub fn of(filter: &FilterDefinition) -> Self {
		let room = &filter.room;

		if room.rooms.as_ref().is_some_and(Vec::is_empty) {
			return Self::Nothing;
		}

		let sections = [&room.timeline, &room.state, &room.ephemeral, &room.account_data];

		if sections.into_iter().all(rejects_everything) {
			return Self::DeviceLists;
		}

		Self::Everything
	}

	/// Whether joined rooms appear in the response.
	#[must_use]
	pub fn reports_rooms(self) -> bool { matches!(self, Self::Everything) }
}

/// Whether no event at all can pass one section of a room filter.
///
/// That is so when `not_types` holds the lone wildcard, or when one of the
/// lists an event has to be on (`types`, `senders`, `rooms`) is present and
/// empty. Anything else might match some event and counts as passing: a
/// `not_senders` or `not_rooms` list names particular senders and rooms, and
/// says nothing about the rest.
#[must_use]
pub fn rejects_everything(filter: &RoomEventFilter) -> bool {
	let every_type_excluded = filter
		.not_types
		.iter()
		.any(|not_type| not_type == EVERY_TYPE);

	every_type_excluded
		|| filter.types.as_ref().is_some_and(Vec::is_empty)
		|| filter.senders.as_ref().is_some_and(Vec::is_empty)
		|| filter.rooms.as_ref().is_some_and(Vec::is_empty)
}

#[cfg(test)]
mod tests {
	use ruma::api::client::filter::FilterDefinition;
	use serde_json::{Value, json};

	use super::RoomScope;

	fn scope(filter: Value) -> RoomScope {
		let filter: FilterDefinition =
			serde_json::from_value(filter).expect("a filter definition");

		RoomScope::of(&filter)
	}

	fn sections(section: &Value) -> Value {
		json!({
			"room": {
				"timeline": section,
				"state": section,
				"ephemeral": section,
				"account_data": section,
			},
		})
	}

	#[test]
	fn no_filter_reports_everything() {
		assert_eq!(scope(json!({})), RoomScope::Everything);
		assert_eq!(RoomScope::of(&FilterDefinition::default()), RoomScope::Everything);
	}

	#[test]
	fn the_bridge_bots_filter_leaves_only_device_lists() {
		// What mautrix's crypto syncer sends (bridgev2/matrix/crypto.go).
		let filter = json!({
			"presence": { "not_types": ["*"] },
			"account_data": { "not_types": ["*"] },
			"room": {
				"include_leave": false,
				"ephemeral": { "not_types": ["*"] },
				"account_data": { "not_types": ["*"] },
				"state": { "not_types": ["*"] },
				"timeline": { "not_types": ["*"] },
			},
		});

		assert_eq!(scope(filter), RoomScope::DeviceLists);
	}

	#[test]
	fn an_empty_list_of_allowed_types_rejects_everything() {
		assert_eq!(scope(sections(&json!({ "types": [] }))), RoomScope::DeviceLists);
	}

	#[test]
	fn an_empty_list_of_allowed_senders_or_rooms_rejects_everything() {
		assert_eq!(scope(sections(&json!({ "senders": [] }))), RoomScope::DeviceLists);
		assert_eq!(scope(sections(&json!({ "rooms": [] }))), RoomScope::DeviceLists);
	}

	#[test]
	fn the_sections_may_reject_everything_each_in_its_own_way() {
		let filter = json!({
			"room": {
				"timeline": { "not_types": ["m.room.message", "*"] },
				"state": { "types": [] },
				"ephemeral": { "senders": [] },
				"account_data": { "rooms": [] },
			},
		});

		assert_eq!(scope(filter), RoomScope::DeviceLists);
	}

	#[test]
	fn one_open_section_keeps_the_rooms() {
		for open in ["timeline", "state", "ephemeral", "account_data"] {
			let mut filter = sections(&json!({ "not_types": ["*"] }));
			filter["room"]
				.as_object_mut()
				.expect("the room filter")
				.remove(open);

			assert_eq!(scope(filter), RoomScope::Everything, "{open} was left open");
		}
	}

	#[test]
	fn a_limit_of_zero_does_not_reject_everything() {
		assert_eq!(scope(sections(&json!({ "limit": 0 }))), RoomScope::Everything);
	}

	#[test]
	fn excluding_particular_things_does_not_reject_everything() {
		let particular = [
			json!({ "not_types": ["m.room.message"] }),
			json!({ "not_types": ["m.room.*"] }),
			json!({ "not_senders": ["@someone:example.org"] }),
			json!({ "not_rooms": ["!room:example.org"] }),
			json!({ "types": ["m.room.message"] }),
			json!({ "types": ["*"] }),
		];

		for section in &particular {
			assert_eq!(scope(sections(section)), RoomScope::Everything, "{section}");
		}
	}

	#[test]
	fn an_empty_list_of_rooms_leaves_nothing() {
		assert_eq!(scope(json!({ "room": { "rooms": [] } })), RoomScope::Nothing);
	}

	#[test]
	fn naming_rooms_to_include_or_exclude_keeps_the_rooms() {
		let included = json!({ "room": { "rooms": ["!room:example.org"] } });
		let excluded = json!({ "room": { "not_rooms": ["!room:example.org"] } });

		assert_eq!(scope(included), RoomScope::Everything);
		assert_eq!(scope(excluded), RoomScope::Everything);
	}

	#[test]
	fn only_everything_reports_rooms() {
		assert!(RoomScope::Everything.reports_rooms());
		assert!(!RoomScope::DeviceLists.reports_rooms());
		assert!(!RoomScope::Nothing.reports_rooms());
	}
}
