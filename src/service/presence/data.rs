use std::sync::Arc;

use futures::Stream;
use ruma::{UInt, UserId, events::presence::PresenceEvent, presence::PresenceState};
use tuwunel_core::{
	Err, Result, debug_warn, implement, utils,
	utils::{ReadyExt, result::NotFound, stream::TryIgnore},
};
use tuwunel_database::{Deserialized, Json, Map};

use crate::presence::Presence;

pub(crate) struct Data {
	presenceid_presence: Arc<Map>,
	userid_presenceid: Arc<Map>,
	services: Arc<crate::services::OnceServices>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			presenceid_presence: db["presenceid_presence"].clone(),
			userid_presenceid: db["userid_presenceid"].clone(),
			services: args.services.clone(),
		}
	}

	pub(super) async fn get_presence_raw(&self, user_id: &UserId) -> Result<(u64, Presence)> {
		let count = self
			.userid_presenceid
			.get(user_id)
			.await
			.deserialized::<u64>()?;

		let key = presenceid_key(count, user_id);
		let bytes = self.presenceid_presence.get(&key).await?;
		let presence = Presence::from_json_bytes(&bytes)?;

		Ok((count, presence))
	}

	pub(super) async fn set_presence(
		&self,
		user_id: &UserId,
		presence_state: &PresenceState,
		currently_active: Option<bool>,
		last_active_ago: Option<UInt>,
		status_msg: Option<String>,
	) -> Result<Option<u64>> {
		let last_presence = self.get_presence(user_id).await;
		let state_changed = match last_presence {
			| Err(_) => true,
			| Ok(ref presence) => presence.1.content.presence != *presence_state,
		};

		let status_msg_changed = match last_presence {
			| Err(_) => true,
			| Ok(ref last_presence) => {
				let old_msg = last_presence
					.1
					.content
					.status_msg
					.clone()
					.unwrap_or_default();

				let new_msg = status_msg.clone().unwrap_or_default();

				new_msg != old_msg
			},
		};

		let now = utils::millis_since_unix_epoch();
		let last_last_active_ts = match last_presence {
			| Err(_) => 0,
			| Ok((_, ref presence)) => now.saturating_sub(
				presence
					.content
					.last_active_ago
					.unwrap_or_default()
					.into(),
			),
		};

		let last_active_ts = match last_active_ago {
			| None => now,
			| Some(last_active_ago) => now.saturating_sub(last_active_ago.into()),
		};

		// TODO: tighten for state flicker?
		if !status_msg_changed && !state_changed && last_active_ts < last_last_active_ts {
			debug_warn!(
				"presence spam {user_id:?} last_active_ts:{last_active_ts:?} < \
				 {last_last_active_ts:?}",
			);
			return Ok(None);
		}

		let status_msg = if status_msg.as_ref().is_some_and(String::is_empty) {
			None
		} else {
			status_msg
		};

		let presence = Presence {
			state: presence_state.to_owned(),
			currently_active: currently_active.unwrap_or(false),
			last_active_ts,
			status_msg,
		};

		let count = self.services.globals.next_count();
		let key = presenceid_key(*count, user_id);

		self.userid_presenceid.raw_put(user_id, *count);
		self.presenceid_presence
			.raw_put(key, Json(presence));

		if let Ok((last_count, _)) = last_presence {
			let key = presenceid_key(last_count, user_id);
			self.presenceid_presence.remove(&key);
		}

		// This row replaces the last one; the log is what keeps when they came and went.
		if state_changed {
			self.services
				.activity_log
				.log_presence(user_id, presence_state, last_active_ts);
		}

		Ok(Some(*count))
	}

	/// Moves the time a person was last active forward to when their network
	/// saw them.
	///
	/// A bridge learns "last seen at 14:05" about someone who is not around.
	/// Presence has no word for that other than how long ago they were active,
	/// so that is where it goes: clients then show it as they show anyone's
	/// last activity. The state stays what it was, offline for someone presence
	/// never heard of, and nothing is written to the activity log, which has
	/// its own row for this.
	pub(super) async fn note_seen(&self, user_id: &UserId, ts: u64) -> Option<u64> {
		let last = self.get_presence_raw(user_id).await.ok();
		if !seen_moves_last_active(last.as_ref().map(|(_, presence)| presence), ts) {
			return None;
		}

		let (state, status_msg) = match &last {
			| Some((_, presence)) => (presence.state.clone(), presence.status_msg.clone()),
			| None => (PresenceState::Offline, None),
		};

		let presence = Presence {
			state,
			currently_active: false,
			last_active_ts: ts,
			status_msg,
		};

		let count = self.services.globals.next_count();
		let key = presenceid_key(*count, user_id);

		self.userid_presenceid.raw_put(user_id, *count);
		self.presenceid_presence
			.raw_put(key, Json(presence));

		if let Some((last_count, _)) = last {
			let key = presenceid_key(last_count, user_id);
			self.presenceid_presence.remove(&key);
		}

		Some(*count)
	}

	#[inline]
	pub(super) async fn remove_presence(&self, user_id: &UserId) {
		let Ok(count) = self
			.userid_presenceid
			.get(user_id)
			.await
			.deserialized::<u64>()
		else {
			return;
		};

		let key = presenceid_key(count, user_id);
		self.presenceid_presence.remove(&key);
		self.userid_presenceid.remove(user_id);
	}

	/// The rows written after `since`, up to and including `to`, oldest first.
	///
	/// Every sync asks this, nearly always for the last few rows, and the table holds one row for
	/// everyone who was ever seen. The key starts with the count, so the rows wanted are the end
	/// of the table: seek to them and stop after `to`, rather than read every row to find them.
	#[inline]
	pub(super) fn presence_since(
		&self,
		since: u64,
		to: Option<u64>,
	) -> impl Stream<Item = (&UserId, u64, &[u8])> + Send + '_ {
		self.presenceid_presence
			.raw_stream_from(&presenceid_first_after(since))
			.ignore_err()
			.ready_take_while(move |(key, _): &(&[u8], &[u8])| !presenceid_is_past(key, to))
			.ready_filter_map(move |(key, presence)| {
				let (count, user_id) = presenceid_parse(key).ok()?;
				(count > since && to.is_none_or(|to| count <= to))
					.then_some((user_id, count, presence))
			})
	}
}

#[implement(Data)]
#[inline]
pub(super) async fn get_presence(&self, user_id: &UserId) -> Result<(u64, PresenceEvent)> {
	let count = self
		.userid_presenceid
		.get(user_id)
		.await
		.deserialized::<u64>()?;

	let event = self.get_presence_event(count, user_id).await?;

	Ok((count, event))
}

#[implement(Data)]
#[inline]
pub(super) async fn get_presence_optional(
	&self,
	user_id: &UserId,
) -> Result<Option<(u64, PresenceEvent)>> {
	let Some(count) = self
		.userid_presenceid
		.get(user_id)
		.await
		.optional()?
	else {
		return Ok(None);
	};

	let count = count.deserialized::<u64>()?;
	let event = self.get_presence_event(count, user_id).await?;

	Ok(Some((count, event)))
}

#[implement(Data)]
#[tracing::instrument(level = "trace", skip(self))]
async fn get_presence_event(&self, count: u64, user_id: &UserId) -> Result<PresenceEvent> {
	let key = presenceid_key(count, user_id);
	let bytes = self.presenceid_presence.get(&key).await?;

	self.services
		.presence
		.from_json_bytes_to_event(&bytes, user_id)
		.await
}

#[inline]
fn presenceid_key(count: u64, user_id: &UserId) -> Vec<u8> {
	let cap = size_of::<u64>().saturating_add(user_id.as_bytes().len());
	let mut key = Vec::with_capacity(cap);
	key.extend_from_slice(&count.to_be_bytes());
	key.extend_from_slice(user_id.as_bytes());
	key
}

/// Where to start reading for the rows written after `since`: the smallest key a row with the
/// next count can have. Counts are stored big-endian, so their order as bytes is their order as
/// numbers, and no row with a count up to `since` sorts at or after this.
///
/// At the largest count there is no next one; the seek then lands on rows with that count, which
/// are not after it, and the caller's own comparison leaves them out.
#[inline]
fn presenceid_first_after(since: u64) -> [u8; size_of::<u64>()] {
	since.saturating_add(1).to_be_bytes()
}

/// Whether a key's count is beyond `to`, which ends a read in key order: every later key is
/// beyond it too. A key too short to hold a count ends nothing; it is skipped when parsed.
#[inline]
fn presenceid_is_past(key: &[u8], to: Option<u64>) -> bool {
	let Some(to) = to else {
		return false;
	};

	key.first_chunk::<{ size_of::<u64>() }>()
		.is_some_and(|count| u64::from_be_bytes(*count) > to)
}

#[inline]
fn presenceid_parse(key: &[u8]) -> Result<(u64, &UserId)> {
	let Some((count, user_id)) = key.split_at_checked(size_of::<u64>()) else {
		return Err!(Database("Presence key is too short to hold a count"));
	};
	let user_id = user_id_from_bytes(user_id)?;
	let count = utils::u64_from_u8(count);

	Ok((count, user_id))
}

/// Parses a `UserId` from bytes.
fn user_id_from_bytes(bytes: &[u8]) -> Result<&UserId> {
	let str: &str = utils::str_from_bytes(bytes)?;
	let user_id: &UserId = str.try_into()?;

	Ok(user_id)
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use futures::StreamExt;
	use ruma::{OwnedUserId, UserId, presence::PresenceState, user_id};
	use tuwunel_core::{Result, config::Figment, utils::stream::TryIgnore};

	use super::{
		Data, presenceid_first_after, presenceid_is_past, presenceid_key, presenceid_parse,
	};
	use crate::test_utils::fixture;

	/// Counts on both sides of every place where a byte carries into the next, which is where an
	/// order of bytes and an order of numbers come apart if the key is built the wrong way round.
	const COUNTS: [u64; 12] = [
		0,
		1,
		2,
		255,
		256,
		257,
		65_535,
		65_536,
		4_294_967_295,
		4_294_967_296,
		u64::MAX - 1,
		u64::MAX,
	];

	// The table is read in the order of its keys, as bytes. A map ordered the same way stands
	// in for it here, and the answer is checked against reading every row, which is what this
	// used to do. A seek key that starts too late, or a stop that comes too early, loses rows.
	#[test]
	fn seeking_finds_exactly_the_rows_that_reading_everything_did() {
		let users = [user_id!("@a:example.com"), user_id!("@zebra:example.com")];
		let table: BTreeMap<Vec<u8>, u64> = COUNTS
			.iter()
			.zip(users.iter().cycle())
			.map(|(count, user)| (presenceid_key(*count, user), *count))
			.collect();

		let bounds = COUNTS.iter().copied().map(Some).chain([None]);
		for (since, to) in COUNTS
			.iter()
			.flat_map(|since| bounds.clone().map(move |to| (*since, to)))
		{
			let wanted = |count: &u64| *count > since && to.is_none_or(|to| *count <= to);
			let by_reading_everything: Vec<u64> =
				table.values().copied().filter(wanted).collect();
			let by_seeking: Vec<u64> = table
				.range(presenceid_first_after(since).to_vec()..)
				.take_while(|(key, _)| !presenceid_is_past(key, to))
				.map(|(_, count)| *count)
				.filter(wanted)
				.collect();

			assert_eq!(by_seeking, by_reading_everything, "since {since}, to {to:?}");
		}
	}

	// A key that cannot be a presence row is passed over: no reason to stop reading, or to panic.
	#[test]
	fn a_key_too_short_for_a_count_is_passed_over() {
		assert!(!presenceid_is_past(b"short", Some(0)));
		assert!(!presenceid_is_past(b"", None));
		assert!(presenceid_parse(b"short").is_err());
	}

	async fn read(db: &Data, since: u64, to: Option<u64>) -> Vec<(OwnedUserId, u64)> {
		db.presence_since(since, to)
			.map(|(user_id, count, _)| (user_id.to_owned(), count))
			.collect()
			.await
	}

	/// Every row of the table, read from its start: what `presence_since` used to filter.
	async fn everything(db: &Data) -> Vec<(OwnedUserId, u64)> {
		db.presenceid_presence
			.raw_stream()
			.ignore_err()
			.map(|(key, _)| {
				let (count, user_id) = presenceid_parse(key).expect("a presence key");
				(user_id.to_owned(), count)
			})
			.collect()
			.await
	}

	// The same through the real table: rows written the way presence writes them, and each answer
	// checked against a read of the whole table. This is what would notice the database ordering
	// keys differently from the map above, or the seek being given something it reads otherwise.
	#[tokio::test]
	async fn presence_since_is_the_rows_after_since_up_to_to() -> Result {
		let Some(fixture) = fixture(Figment::new()).await? else {
			return Ok(());
		};

		let db = &fixture.services.presence.db;
		let users: [&UserId; 5] = [
			user_id!("@zoe:localhost"),
			user_id!("@adam:localhost"),
			user_id!("@mia:localhost"),
			user_id!("@bo:localhost"),
			user_id!("@yusuf:localhost"),
		];
		for user in users {
			db.set_presence(user, &PresenceState::Online, Some(true), None, None)
				.await?;
		}

		let all = everything(db).await;
		let written: Vec<&UserId> = all.iter().map(|(user, _)| &**user).collect();
		assert_eq!(written, users, "one row each, in the order they were written");

		for (position, (_, count)) in all.iter().enumerate() {
			let after = position.saturating_add(1);
			assert_eq!(read(db, *count, None).await, all[after..], "after {count}");
			assert_eq!(read(db, 0, Some(*count)).await, all[..after], "up to {count}");
			assert_eq!(
				read(db, all[0].1, Some(*count)).await,
				all[1..after.max(1)],
				"after the first, up to {count}"
			);
			assert!(read(db, *count, Some(*count)).await.is_empty(), "an empty range at {count}");
		}

		assert_eq!(read(db, 0, None).await, all);
		assert!(read(db, u64::MAX, None).await.is_empty());

		// A change moves a row to the end, which is what a sync waiting at the old end is for.
		let newest = all.last().map_or(0, |(_, count)| *count);
		db.set_presence(users[0], &PresenceState::Unavailable, Some(false), None, None)
			.await?;
		let moved = read(db, newest, None).await;
		assert_eq!(moved.len(), 1, "{moved:?}");
		assert_eq!(&*moved[0].0, users[0]);
		assert_eq!(everything(db).await.len(), users.len(), "the old row is gone");

		Ok(())
	}
}

/// Whether a network's "last seen" is news to presence: the person is not
/// around right now, and the time is later than the one held.
fn seen_moves_last_active(held: Option<&Presence>, ts: u64) -> bool {
	match held {
		| None => true,
		| Some(presence) =>
			presence.state != PresenceState::Online
				&& !presence.currently_active
				&& ts > presence.last_active_ts,
	}
}

#[cfg(test)]
mod tests {
	use ruma::presence::PresenceState;

	use super::seen_moves_last_active;
	use crate::presence::Presence;

	fn held(state: PresenceState, currently_active: bool, last_active_ts: u64) -> Presence {
		Presence {
			state,
			currently_active,
			last_active_ts,
			status_msg: None,
		}
	}

	#[test]
	fn a_network_s_last_seen_moves_last_active_only_forward_and_only_for_the_absent() {
		assert!(seen_moves_last_active(None, 1000), "someone presence never heard of");

		let offline = held(PresenceState::Offline, false, 1000);
		assert!(seen_moves_last_active(Some(&offline), 1001));
		assert!(!seen_moves_last_active(Some(&offline), 1000), "the time already held");
		assert!(!seen_moves_last_active(Some(&offline), 999), "an older sighting");

		let away = held(PresenceState::Unavailable, false, 1000);
		assert!(seen_moves_last_active(Some(&away), 2000));

		let online = held(PresenceState::Online, true, 1000);
		assert!(!seen_moves_last_active(Some(&online), 2000), "someone who is here now");
		let active = held(PresenceState::Unavailable, true, 1000);
		assert!(!seen_moves_last_active(Some(&active), 2000));
	}
}
