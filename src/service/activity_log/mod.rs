//! A log of when each person was around, so a client can show when someone
//! usually is ("weekdays 9-12, evenings") instead of only where they are now.
//!
//! The server sees every sign of a person being there and, but for messages,
//! keeps none of it: presence is overwritten by the next change, typing is
//! gone when it stops, and a read receipt is replaced by the next one. Each of
//! those is written here as it happens, one small row per sign:
//!
//! - a presence change (online, unavailable, offline),
//! - an event they sent (a message, a reaction, a call, an edit, …),
//! - starting to type,
//! - a read receipt.
//!
//! A row's key is `user_id | 0xFF | time (ms, big-endian) | kind`, so one
//! person's log is one ordered scan between two times. The value is eight
//! bytes: the room's short id for the kinds that happen in a room, and for a
//! presence change the time the server holds as when they were last active.
//!
//! What they sent is also read out of existing history once, so the log starts
//! with every year the rooms hold rather than with the day it was deployed.
//! Past presence, typing and receipts were never stored and cannot be.

use std::{
	collections::HashMap,
	pin::pin,
	sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use ruma::{OwnedUserId, RoomId, UserId, events::TimelineEventType, presence::PresenceState};
use tuwunel_core::{
	Result, error, implement, info,
	matrix::Event,
	utils::{ReadyExt, millis_since_unix_epoch, stream::TryIgnore},
};
use tuwunel_database::{Deserialized, Map};

use crate::rooms::short::ShortRoomId;

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
	/// When each person's typing was last written, so one burst of keystrokes is one row.
	typed: Mutex<HashMap<OwnedUserId, u64>>,
}

struct Data {
	log: Arc<Map>,
	global: Arc<Map>,
}

/// Which history has been read into the log. Raise it when [`kind_of_event`]
/// counts events differently, and existing history is read again on the next
/// start.
const HISTORY_VERSION: u64 = 1;
const HISTORY_VERSION_KEY: &[u8] = b"activity_log_history_version";

/// A burst of typing is written once per this long.
const TYPING_EVERY_MS: u64 = 30_000;

const SEP: u8 = 0xFF;

/// What a row records. The byte is part of the key, so the values are fixed
/// once written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Kind {
	Online = 1,
	Unavailable = 2,
	Offline = 3,
	/// An event they sent that isn't one of the kinds below.
	Sent = 4,
	Typing = 5,
	Read = 6,
	Reaction = 7,
	/// A network said they were last active then, without them doing anything
	/// the server saw: what a bridge is told when someone has come and gone.
	Seen = 8,
}

impl Kind {
	#[must_use]
	pub const fn from_byte(byte: u8) -> Option<Self> {
		match byte {
			| 1 => Some(Self::Online),
			| 2 => Some(Self::Unavailable),
			| 3 => Some(Self::Offline),
			| 4 => Some(Self::Sent),
			| 5 => Some(Self::Typing),
			| 6 => Some(Self::Read),
			| 7 => Some(Self::Reaction),
			| 8 => Some(Self::Seen),
			| _ => None,
		}
	}

	#[must_use]
	pub const fn name(self) -> &'static str {
		match self {
			| Self::Online => "online",
			| Self::Unavailable => "unavailable",
			| Self::Offline => "offline",
			| Self::Sent => "sent",
			| Self::Typing => "typing",
			| Self::Read => "read",
			| Self::Reaction => "reaction",
			| Self::Seen => "seen",
		}
	}

	/// Whether this is a presence change rather than something done in a room.
	#[must_use]
	pub const fn is_presence(self) -> bool {
		matches!(self, Self::Online | Self::Unavailable | Self::Offline)
	}

	fn of_presence(state: &PresenceState) -> Option<Self> {
		match state {
			| PresenceState::Online => Some(Self::Online),
			| PresenceState::Unavailable => Some(Self::Unavailable),
			| PresenceState::Offline => Some(Self::Offline),
			| _ => None,
		}
	}
}

/// One row of a person's log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Entry {
	/// When, in milliseconds since the epoch.
	pub ts: u64,
	pub kind: Kind,
	/// The room's short id for what happened in a room; for a presence change,
	/// when the server holds them as last active.
	pub value: u64,
}

/// When a person is usually around, by weekday and hour of their week.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Week {
	/// `seen[weekday][hour]`: on how many different days they were around in
	/// that hour. Monday is 0.
	pub seen: [[u32; 24]; 7],
	/// `online_ms[weekday][hour]`: how long they were online in that hour, over
	/// all those days.
	pub online_ms: [[u64; 24]; 7],
	/// `days[weekday]`: how many of each weekday the log covers, to divide by.
	pub days: [u32; 7],
	/// The first and last row read.
	pub first_ts: Option<u64>,
	pub last_ts: Option<u64>,
	/// How many rows were read.
	pub entries: u64,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				log: args.db["useractivity"].clone(),
				global: args.db["global"].clone(),
			},
			services: args.services.clone(),
			typed: Mutex::new(HashMap::new()),
		}))
	}

	/// Reads what people sent out of existing history, once, in the
	/// background. A stop part-way leaves the version unrecorded, so the next
	/// start does it again; rows are keyed by what they record, so reading a
	/// room twice writes nothing twice.
	async fn worker(self: Arc<Self>) -> Result {
		let stored = self
			.db
			.global
			.get(HISTORY_VERSION_KEY)
			.await
			.deserialized::<u64>()
			.ok();
		if stored.is_some_and(|version| version >= HISTORY_VERSION) {
			return Ok(());
		}

		info!(?stored, current = HISTORY_VERSION, "Reading existing history into the activity log");
		match self.log_history().await {
			| Ok(logged) => {
				self.db
					.global
					.raw_put(HISTORY_VERSION_KEY, HISTORY_VERSION);
				info!("Logged the activity of {logged} existing events.");
			},
			| Err(e) if !self.services.server.is_running() => info!("Activity log stopped: {e}"),
			| Err(e) => error!("Reading history into the activity log failed: {e}"),
		}

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Records a presence change. Called only when the state is a different one
/// than before: a bridge repeats "online" every few minutes to keep it alive,
/// and that is not a change.
#[implement(Service)]
pub fn log_presence(&self, user_id: &UserId, state: &PresenceState, last_active_ts: u64) {
	if let Some(kind) = Kind::of_presence(state) {
		self.put(user_id, millis_since_unix_epoch(), kind, last_active_ts);
	}
}

/// Records an event accepted into a room's timeline, if sending it is a sign
/// of its sender being there.
#[implement(Service)]
pub fn log_pdu<E: Event>(&self, shortroomid: ShortRoomId, pdu: &E) {
	if let Some(kind) = kind_of_event(pdu.kind(), pdu.state_key().is_some()) {
		self.put(pdu.sender(), u64::from(pdu.origin_server_ts().0), kind, shortroomid);
	}
}

/// Records when a network says someone was last active. The time is theirs,
/// not now: a bridge learns it afterwards. One in the future is the network's
/// clock or a mistake, and is left out.
#[implement(Service)]
pub fn log_seen(&self, user_id: &UserId, ts: u64) -> bool {
	if !seen_is_past(ts, millis_since_unix_epoch()) {
		return false;
	}
	self.put(user_id, ts, Kind::Seen, 0);

	true
}

/// Records someone starting to type, once per burst.
#[implement(Service)]
pub async fn log_typing(&self, user_id: &UserId, room_id: &RoomId) {
	let now = millis_since_unix_epoch();
	{
		let mut typed = self.typed.lock().expect("locked");
		if !typing_is_new(typed.get(user_id).copied(), now) {
			return;
		}
		typed.insert(user_id.to_owned(), now);
	}

	if let Ok(shortroomid) = self.services.short.get_shortroomid(room_id).await {
		self.put(user_id, now, Kind::Typing, shortroomid);
	}
}

/// Records a read receipt.
#[implement(Service)]
pub async fn log_read(&self, user_id: &UserId, room_id: &RoomId) {
	if let Ok(shortroomid) = self.services.short.get_shortroomid(room_id).await {
		self.put(user_id, millis_since_unix_epoch(), Kind::Read, shortroomid);
	}
}

#[implement(Service)]
fn put(&self, user_id: &UserId, ts: u64, kind: Kind, value: u64) {
	self.db
		.log
		.insert(&make_key(user_id, ts, kind), value.to_be_bytes());
}

/// A person's log between two times (milliseconds, both included), oldest
/// first.
#[implement(Service)]
pub fn entries<'a>(
	&'a self,
	user_id: &'a UserId,
	from_ts: u64,
	to_ts: u64,
) -> impl Stream<Item = Entry> + Send + 'a {
	let prefix = make_prefix(user_id);
	let start = make_key_from(&prefix, from_ts, 0);
	let prefix_len = prefix.len();

	self.db
		.log
		.raw_stream_from(&start)
		.ignore_err()
		.ready_take_while(move |(key, _): &(&[u8], &[u8])| key.starts_with(&prefix))
		.ready_filter_map(move |(key, val)| read_entry(key.get(prefix_len..)?, val))
		.ready_take_while(move |entry| entry.ts <= to_ts)
}

/// A person's log between two times, newest first.
#[implement(Service)]
pub fn entries_rev<'a>(
	&'a self,
	user_id: &'a UserId,
	from_ts: u64,
	to_ts: u64,
) -> impl Stream<Item = Entry> + Send + 'a {
	let prefix = make_prefix(user_id);
	let end = make_key_from(&prefix, to_ts, u8::MAX);
	let prefix_len = prefix.len();

	self.db
		.log
		.rev_raw_stream_from(&end)
		.ignore_err()
		.ready_take_while(move |(key, _): &(&[u8], &[u8])| key.starts_with(&prefix))
		.ready_filter_map(move |(key, val)| read_entry(key.get(prefix_len..)?, val))
		.ready_take_while(move |entry| entry.ts >= from_ts)
}

/// When a person is usually around, from their log between two times.
/// `offset_ms` is the asker's distance from UTC, so the hours are theirs.
#[implement(Service)]
pub async fn week(&self, user_id: &UserId, from_ts: u64, to_ts: u64, offset_ms: i64) -> Week {
	let mut fold = WeekFold::new(offset_ms);
	let mut entries = pin!(self.entries(user_id, from_ts, to_ts));
	while let Some(entry) = entries.next().await {
		fold.add(entry);
	}

	fold.finish(to_ts.min(millis_since_unix_epoch()))
}

/// Reads every room's existing events into the log.
#[implement(Service)]
async fn log_history(&self) -> Result<usize> {
	let rooms: Vec<_> = self
		.services
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let mut logged: usize = 0;
	for room_id in rooms {
		self.services.server.check_running()?;
		let Ok(shortroomid) = self.services.short.get_shortroomid(&room_id).await else {
			continue;
		};

		let mut pdus = pin!(
			self.services
				.timeline
				.pdus(None, &room_id, None)
				.ignore_err()
		);
		while let Some((_, pdu)) = pdus.next().await {
			if kind_of_event(pdu.kind(), pdu.state_key().is_some()).is_some() {
				self.log_pdu(shortroomid, &pdu);
				logged = logged.saturating_add(1);
			}
		}
	}

	Ok(logged)
}

/// Whether sending this event shows its sender was there, and as what.
///
/// State events are left out: a bridge sets its ghosts' names, avatars and
/// memberships whenever the network tells it to, and none of that is the
/// person doing anything.
fn kind_of_event(event_type: &TimelineEventType, is_state: bool) -> Option<Kind> {
	if is_state {
		return None;
	}

	match event_type {
		| TimelineEventType::Reaction => Some(Kind::Reaction),
		| _ => Some(Kind::Sent),
	}
}

/// Whether a last-active time can be one: not after now, give or take a clock.
const fn seen_is_past(ts: u64, now: u64) -> bool { ts > 0 && ts <= now.saturating_add(60_000) }

/// Whether typing at `now` starts a new burst, given when the last was written.
const fn typing_is_new(last: Option<u64>, now: u64) -> bool {
	match last {
		| Some(last) => now.saturating_sub(last) >= TYPING_EVERY_MS,
		| None => true,
	}
}

fn make_prefix(user_id: &UserId) -> Vec<u8> {
	let mut key = Vec::with_capacity(user_id.as_bytes().len().saturating_add(10));
	key.extend_from_slice(user_id.as_bytes());
	key.push(SEP);
	key
}

fn make_key_from(prefix: &[u8], ts: u64, kind: u8) -> Vec<u8> {
	let mut key = prefix.to_vec();
	key.extend_from_slice(&ts.to_be_bytes());
	key.push(kind);
	key
}

fn make_key(user_id: &UserId, ts: u64, kind: Kind) -> Vec<u8> {
	make_key_from(&make_prefix(user_id), ts, kind as u8)
}

/// A row from the part of its key after the person, and its value.
fn read_entry(rest: &[u8], val: &[u8]) -> Option<Entry> {
	let ts = u64::from_be_bytes(rest.get(..8)?.try_into().ok()?);
	let kind = Kind::from_byte(*rest.get(8)?)?;
	let value = val
		.try_into()
		.ok()
		.map(u64::from_be_bytes)
		.unwrap_or_default();

	Some(Entry { ts, kind, value })
}

const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;

/// A stretch online that never got its end (a bridge that died, a server that
/// stopped) is not counted past this.
const ONLINE_MAX_MS: u64 = 12 * HOUR_MS;

/// Folds a person's log, oldest first, into their week.
struct WeekFold {
	week: Week,
	offset_ms: i64,
	/// Since when they have been online, if they are.
	online_since: Option<u64>,
	/// The last day each hour of the week was counted on, so a day counts once.
	last_day: [[Option<u64>; 24]; 7],
}

impl WeekFold {
	fn new(offset_ms: i64) -> Self {
		Self {
			week: Week::default(),
			offset_ms,
			online_since: None,
			last_day: [[None; 24]; 7],
		}
	}

	fn add(&mut self, entry: Entry) {
		self.week.first_ts.get_or_insert(entry.ts);
		self.week.last_ts = Some(entry.ts);
		self.week.entries = self.week.entries.saturating_add(1);

		match entry.kind {
			| Kind::Online =>
				if self.online_since.is_none() {
					self.online_since = Some(entry.ts);
				},
			| Kind::Unavailable | Kind::Offline => self.close(entry.ts),
			| Kind::Sent | Kind::Typing | Kind::Read | Kind::Reaction | Kind::Seen => {
				self.seen_at(entry.ts);
			},
		}
	}

	fn finish(mut self, now: u64) -> Week {
		self.close(now);
		if let (Some(first), Some(last)) = (self.week.first_ts, self.week.last_ts) {
			let first_day = self.local(first) / DAY_MS;
			let last_day = self.local(last.max(now)) / DAY_MS;
			for day in first_day..=last_day.min(first_day.saturating_add(366 * 30)) {
				let weekday = weekday_of(day);
				self.week.days[weekday] = self.week.days[weekday].saturating_add(1);
			}
		}

		self.week
	}

	/// Ends the stretch online, if one is open, and counts its hours.
	fn close(&mut self, at: u64) {
		let Some(since) = self.online_since.take() else {
			return;
		};
		let until = at.min(since.saturating_add(ONLINE_MAX_MS));
		let mut from = since;
		while from < until {
			let local = self.local(from);
			let hour_end = from.saturating_add(HOUR_MS - local % HOUR_MS);
			let to = hour_end.min(until);
			let (weekday, hour) = self.seen_at(from);
			self.week.online_ms[weekday][hour] =
				self.week.online_ms[weekday][hour].saturating_add(to.saturating_sub(from));
			from = to;
		}
		// Online for no time at all is still having been there.
		if until <= since {
			self.seen_at(since);
		}
	}

	/// Counts the day of `ts` for its hour of the week, once.
	fn seen_at(&mut self, ts: u64) -> (usize, usize) {
		let local = self.local(ts);
		let day = local / DAY_MS;
		let weekday = weekday_of(day);
		let hour = usize::try_from(local % DAY_MS / HOUR_MS).unwrap_or(0);
		if self.last_day[weekday][hour] != Some(day) {
			self.last_day[weekday][hour] = Some(day);
			self.week.seen[weekday][hour] = self.week.seen[weekday][hour].saturating_add(1);
		}

		(weekday, hour)
	}

	fn local(&self, ts: u64) -> u64 { ts.saturating_add_signed(self.offset_ms) }
}

/// The weekday of a day counted from the epoch, Monday being 0. The epoch was
/// a Thursday.
fn weekday_of(day: u64) -> usize { usize::try_from((day.saturating_add(3)) % 7).unwrap_or(0) }

#[cfg(test)]
mod tests {
	use ruma::events::TimelineEventType;

	use super::{
		DAY_MS, Entry, HOUR_MS, Kind, WeekFold, kind_of_event, seen_is_past, typing_is_new,
		weekday_of,
	};

	// Monday 2024-01-01 00:00 UTC.
	const MONDAY: u64 = 1_704_067_200_000;

	fn fold(entries: &[(u64, Kind)], offset_ms: i64, now: u64) -> super::Week {
		let mut fold = WeekFold::new(offset_ms);
		for &(ts, kind) in entries {
			fold.add(Entry { ts, kind, value: 0 });
		}
		fold.finish(now)
	}

	#[test]
	fn the_epoch_was_a_thursday() {
		assert_eq!(weekday_of(0), 3);
		assert_eq!(weekday_of(MONDAY / DAY_MS), 0);
	}

	// Presence was overwritten by its next change, so "online 9:30 to 11:10 on Monday" was gone by
	// Tuesday. From the log it is an hour of the week they were around in, and for how long.
	#[test]
	fn a_stretch_online_counts_each_hour_it_touches() {
		let on = MONDAY + 9 * HOUR_MS + HOUR_MS / 2;
		let off = MONDAY + 11 * HOUR_MS + HOUR_MS / 6;
		let week = fold(&[(on, Kind::Online), (off, Kind::Offline)], 0, off + DAY_MS);

		assert_eq!(week.seen[0][9], 1);
		assert_eq!(week.seen[0][10], 1);
		assert_eq!(week.seen[0][11], 1);
		assert_eq!(week.seen[0][12], 0);
		assert_eq!(week.online_ms[0][9], HOUR_MS / 2);
		assert_eq!(week.online_ms[0][10], HOUR_MS);
		assert_eq!(week.online_ms[0][11], HOUR_MS / 6);
	}

	#[test]
	fn the_same_hour_on_the_same_day_counts_once_and_on_another_day_again() {
		let at = MONDAY + 20 * HOUR_MS;
		let week = fold(
			&[
				(at, Kind::Sent),
				(at + 60_000, Kind::Typing),
				(at + 120_000, Kind::Read),
				(at + 7 * DAY_MS, Kind::Reaction),
			],
			0,
			at + 8 * DAY_MS,
		);

		assert_eq!(week.seen[0][20], 2);
		// Two Mondays and two Tuesdays in the nine days covered, one of every other day.
		assert_eq!(week.days, [2, 2, 1, 1, 1, 1, 1]);
		assert_eq!(week.entries, 4);
	}

	#[test]
	fn hours_are_the_askers() {
		// 23:30 UTC on Monday is 01:30 on Tuesday two hours east.
		let at = MONDAY + 23 * HOUR_MS + HOUR_MS / 2;
		let week = fold(&[(at, Kind::Sent)], 2 * 3_600_000, at);

		assert_eq!(week.seen[1][1], 1);
		assert_eq!(week.seen[0][23], 0);
	}

	// A bridge that dies while someone is online never says they left.
	#[test]
	fn a_stretch_online_without_an_end_is_cut_off() {
		let on = MONDAY + 8 * HOUR_MS;
		let week = fold(&[(on, Kind::Online)], 0, on + 5 * DAY_MS);
		let total: u64 = week.online_ms.iter().flatten().sum();

		assert_eq!(total, 12 * HOUR_MS);
		assert_eq!(week.seen[0][19], 1);
		assert_eq!(week.seen[0][20], 0);
	}

	// A bridge names its ghosts and moves them in and out of rooms; none of that is the person.
	#[test]
	fn what_a_bridge_sets_for_someone_is_not_them_being_there() {
		assert_eq!(kind_of_event(&TimelineEventType::RoomMember, true), None);
		assert_eq!(kind_of_event(&TimelineEventType::RoomMessage, false), Some(Kind::Sent));
		assert_eq!(kind_of_event(&TimelineEventType::RoomEncrypted, false), Some(Kind::Sent));
		assert_eq!(kind_of_event(&TimelineEventType::Reaction, false), Some(Kind::Reaction));
	}

	#[test]
	fn a_burst_of_typing_is_one_row() {
		assert!(typing_is_new(None, 1_000));
		assert!(!typing_is_new(Some(1_000), 5_000));
		assert!(typing_is_new(Some(1_000), 40_000));
	}

	// Messenger says when someone was last active, to the second, and the bridge could only pass
	// on "offline": the hour they were there in went unrecorded.
	#[test]
	fn a_last_active_time_counts_the_hour_it_names() {
		let at = MONDAY + 14 * HOUR_MS + 5;
		let week = fold(&[(at, Kind::Seen)], 0, at + HOUR_MS);

		assert_eq!(week.seen[0][14], 1);
		assert!(seen_is_past(at, at + HOUR_MS));
		assert!(!seen_is_past(at + DAY_MS, at), "a time that hasn't come");
		assert!(!seen_is_past(0, at), "no time at all");
	}

	#[test]
	fn every_kind_reads_back_as_itself() {
		for kind in [
			Kind::Online,
			Kind::Unavailable,
			Kind::Offline,
			Kind::Sent,
			Kind::Typing,
			Kind::Read,
			Kind::Reaction,
			Kind::Seen,
		] {
			assert_eq!(Kind::from_byte(kind as u8), Some(kind));
		}
	}

	// The manager runs only the workers of the services it lists.
	#[test]
	fn the_worker_is_started() {
		let services = include_str!("../services.rs");
		assert!(services.contains("cast!(self.activity_log)"));
	}
}
