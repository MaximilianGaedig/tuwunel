//! Hands a user's chat settings to the bridges that carry the chat.
//!
//! A bridge acts on some of the account data its users set: a room tagged as a favourite or archived, a
//! room marked unread or muted, and people put on the ignore list. Appservices otherwise never see account
//! data, so each change of those types is also sent to the appservices it concerns, as an ephemeral event
//! carrying the sender, the room and the previous content.

use std::collections::BTreeSet;

use ruma::{RoomId, UserId};
use serde_json::{Map, Value, json};
use tuwunel_core::{implement, utils::result::LogErr};

use crate::sending::EduBuf;

/// Room account data that bridges act on.
const ROOM_TYPES: &[&str] = &["m.tag", "m.marked_unread", "com.famedly.marked_unread", "com.beeper.mute"];

/// The global account data listing the people a user ignores.
const IGNORED_USER_LIST: &str = "m.ignored_user_list";

/// The global push rules, where Matrix clients keep which rooms are muted.
const PUSH_RULES: &str = "m.push_rules";

/// What bridges read a room's mute from.
const MUTE: &str = "com.beeper.mute";

/// Whether a change of this account data goes to appservices.
pub(super) fn is_bridged(room_id: Option<&RoomId>, event_type: &str) -> bool {
	match room_id {
		| Some(_) => ROOM_TYPES.contains(&event_type),
		| None => event_type == IGNORED_USER_LIST || event_type == PUSH_RULES,
	}
}

/// The ephemeral event an appservice receives for an account data change.
pub(super) fn edu(
	room_id: Option<&RoomId>,
	user_id: &UserId,
	event_type: &str,
	content: &Value,
	prev_content: Option<&Value>,
) -> Value {
	let mut edu = json!({
		"type": event_type,
		"sender": user_id,
		"content": content,
	});
	if let Some(room_id) = room_id {
		edu["room_id"] = json!(room_id);
	}
	if let Some(prev_content) = prev_content {
		edu["unsigned"] = json!({ "prev_content": prev_content });
	}
	edu
}

/// The people an `m.ignored_user_list` content ignores.
pub(super) fn ignored_users(content: Option<&Value>) -> Vec<String> {
	content
		.and_then(|content| content.get("ignored_users"))
		.and_then(Value::as_object)
		.map(Map::keys)
		.into_iter()
		.flatten()
		.cloned()
		.collect()
}

/// The rooms push rules mute: an enabled override or room rule named after the room that notifies
/// nothing, which is how Matrix clients mute a room.
pub(super) fn muted_rooms(content: Option<&Value>) -> BTreeSet<String> {
	let Some(global) = content.and_then(|content| content.get("global")) else {
		return BTreeSet::new();
	};
	["override", "room"]
		.into_iter()
		.filter_map(|kind| global.get(kind).and_then(Value::as_array))
		.flatten()
		.filter(|rule| rule.get("enabled").and_then(Value::as_bool) != Some(false))
		.filter(|rule| {
			rule.get("actions")
				.and_then(Value::as_array)
				.is_some_and(|actions| actions.iter().all(|action| action == "dont_notify"))
		})
		.filter_map(|rule| rule.get("rule_id").and_then(Value::as_str))
		.filter(|rule_id| rule_id.starts_with('!'))
		.map(ToOwned::to_owned)
		.collect()
}

/// Sends an account data change to the appservices it concerns: for room data, those interested in the
/// room; for the ignore list, those whose namespace holds someone added to or removed from it.
#[implement(super::Service)]
pub(super) async fn notify_appservices(
	&self,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	event_type: &str,
	data: &Value,
	prev_data: Option<&Value>,
) {
	if !is_bridged(room_id, event_type) {
		return;
	}
	let Some(content) = data.get("content") else {
		return;
	};
	let prev_content = prev_data.and_then(|prev| prev.get("content"));
	if prev_content == Some(content) {
		return;
	}
	if event_type == PUSH_RULES {
		self.notify_mute_changes(user_id, content, prev_content).await;
		return;
	}
	let edu = edu(room_id, user_id, event_type, content, prev_content);

	if let Some(room_id) = room_id {
		self.services
			.sending
			.send_edu_room_appservices(room_id, |buf| Ok(serde_json::to_writer(buf, &edu)?))
			.await
			.log_err()
			.ok();
		return;
	}

	let mut changed = ignored_users(Some(content));
	changed.extend(ignored_users(prev_content));
	let appservices = self.services.appservice.read().await;
	for appservice in appservices.values() {
		if !appservice.registration.receive_ephemeral
			|| !changed
				.iter()
				.any(|user| appservice.users.is_match(user))
		{
			continue;
		}
		let mut buf = EduBuf::new();
		if serde_json::to_writer(&mut buf, &edu).is_ok() {
			self.services
				.sending
				.send_edu_appservice(appservice.registration.id.clone(), buf)
				.log_err()
				.ok();
		}
	}
}

/// Hands a room's mute, when a push rule change mutes or unmutes it, to the room's appservices as the
/// mute account data bridges read.
#[implement(super::Service)]
async fn notify_mute_changes(&self, user_id: &UserId, content: &Value, prev_content: Option<&Value>) {
	let now = muted_rooms(Some(content));
	let before = muted_rooms(prev_content);
	for room in now.symmetric_difference(&before) {
		let Ok(room_id) = <&RoomId>::try_from(room.as_str()) else {
			continue;
		};
		let muted_until = if now.contains(room) { -1 } else { 0 };
		let edu = edu(Some(room_id), user_id, MUTE, &json!({ "muted_until": muted_until }), None);
		self.services
			.sending
			.send_edu_room_appservices(room_id, |buf| Ok(serde_json::to_writer(buf, &edu)?))
			.await
			.log_err()
			.ok();
	}
}

#[cfg(test)]
mod tests {
	use ruma::{room_id, user_id};
	use serde_json::json;

	use super::{edu, ignored_users, is_bridged, muted_rooms};

	#[test]
	fn bridged_types() {
		let room = room_id!("!r:example.org");
		assert!(is_bridged(Some(room), "m.tag"));
		assert!(is_bridged(Some(room), "com.beeper.mute"));
		assert!(is_bridged(None, "m.ignored_user_list"));
		assert!(!is_bridged(None, "m.tag"), "tags are room data");
		assert!(!is_bridged(Some(room), "m.fully_read"));
		assert!(!is_bridged(None, "m.push_rules"), "the user's other account data stays private");
	}

	#[test]
	fn edu_shape() {
		let room = room_id!("!r:example.org");
		let user = user_id!("@u:example.org");
		let content = json!({"tags": {"m.favourite": {}}});
		let prev = json!({"tags": {}});
		assert_eq!(
			edu(Some(room), user, "m.tag", &content, Some(&prev)),
			json!({
				"type": "m.tag",
				"sender": "@u:example.org",
				"room_id": "!r:example.org",
				"content": {"tags": {"m.favourite": {}}},
				"unsigned": {"prev_content": {"tags": {}}},
			})
		);
		let global = edu(None, user, "m.ignored_user_list", &json!({}), None);
		assert!(global.get("room_id").is_none() && global.get("unsigned").is_none());
	}

	#[test]
	fn ignored_user_ids() {
		let content = json!({"ignored_users": {"@a:x": {}, "@b:x": {}}});
		let mut users = ignored_users(Some(&content));
		users.sort();
		assert_eq!(users, ["@a:x", "@b:x"]);
		assert!(ignored_users(None).is_empty());
		assert!(ignored_users(Some(&json!({}))).is_empty());
	}

	#[test]
	fn muted_rooms_from_push_rules() {
		let rules = json!({"global": {
			"override": [
				{"rule_id": "!muted:x", "enabled": true, "actions": []},
				{"rule_id": ".m.rule.master", "enabled": false, "actions": []},
				{"rule_id": "!disabled:x", "enabled": false, "actions": []},
			],
			"room": [
				{"rule_id": "!quiet:x", "enabled": true, "actions": ["dont_notify"]},
				{"rule_id": "!loud:x", "enabled": true, "actions": ["notify"]},
			],
		}});
		let muted: Vec<_> = muted_rooms(Some(&rules)).into_iter().collect();
		assert_eq!(muted, ["!muted:x", "!quiet:x"]);
		assert!(muted_rooms(None).is_empty());
	}
}
