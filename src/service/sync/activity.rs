//! A record, kept in memory, of which rooms were written to and when.
//!
//! An incremental `/sync` has to say, for every joined room, whether anything
//! changed since the token it was given. Asking the database costs several
//! seeks per room, and a user in thousands of rooms pays that for every room
//! on every round, although almost none of them changed. The database already
//! tells its maps' subscribers about every write, so the same notification
//! is kept here as a stamp per room, and a round only loads the rooms whose
//! stamp is newer than its token.
//!
//! The record starts empty with the process, so it can only answer for tokens
//! issued after it began. For an older token every room is loaded as before.

use std::{
	collections::HashMap,
	sync::{
		Arc, OnceLock, RwLock, Weak,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};
use tokio::sync::watch;
use tuwunel_core::matrix::ShortRoomId;
use tuwunel_database::{Database, SEP};

use crate::services::OnceServices;

/// Maps whose keys begin with the room: receipts, the private read marker,
/// room account data and tags, the notification read cursors, and the
/// device-list changes fanned out to a room.
const ROOM_FIRST: &[&str] = &[
	"readreceiptid_readreceipt",
	"roomuserid_lastprivatereadupdate",
	"roomuserdataid_accountdata",
	"roomuserid_lastnotificationread",
	"keychangeid_userid",
];

/// Maps whose keys name the user first and the room second: the unread
/// counts.
const ROOM_SECOND: &[&str] = &["userroomid_notificationcount", "userroomid_highlightcount"];

/// The timeline, whose keys begin with the room's short id.
const TIMELINE: &str = "pduid_pdu";

/// Where a room gets its short id.
const SHORT_IDS: &str = "roomid_shortroomid";

/// The map among those above whose writes can change a device list: a key
/// change fanned out to a room. The other such write is an event in the
/// timeline, which may be somebody joining or leaving.
const DEVICE_LISTS: &[&str] = &["keychangeid_userid"];

/// What is known about the writes to one room.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stamp {
	/// No row written to the room since the record began carries a count
	/// above this.
	pub mark: u64,

	/// Differs after every write to the room, including one that leaves the
	/// mark where it was.
	pub seq: [u64; 2],
}

pub struct Activity {
	/// Set to load every room on every round, as if there were no record.
	disabled: AtomicBool,

	/// The count the record began at.
	began: OnceLock<u64>,

	seq: AtomicU64,
	by_id: RwLock<HashMap<Vec<u8>, (u64, u64)>>,
	by_short: RwLock<HashMap<ShortRoomId, (u64, u64)>>,
	short_ids: RwLock<ShortIds>,
	quiet: RwLock<HashMap<OwnedUserId, HashMap<OwnedRoomId, [u64; 2]>>>,
	device_lists: watch::Sender<u64>,
}

impl Default for Activity {
	fn default() -> Self {
		Self {
			disabled: AtomicBool::new(false),
			began: OnceLock::new(),
			seq: AtomicU64::new(0),
			by_id: RwLock::default(),
			by_short: RwLock::default(),
			short_ids: RwLock::default(),
			quiet: RwLock::default(),
			device_lists: watch::Sender::new(0),
		}
	}
}

#[derive(Default)]
struct ShortIds {
	/// Counts the changes to the map the ids are read from, so a value read
	/// before a change is not remembered after it.
	generation: u64,
	ids: HashMap<OwnedRoomId, ShortRoomId>,
}

impl Activity {
	/// Starts the record at `frontier`, unless it has started already.
	///
	/// `frontier` is the highest count handed out so far. Writes seen before
	/// this could not be stamped, and all of them carry a count at or below
	/// it, which is why the record answers only for tokens from it onwards.
	pub fn begin(&self, frontier: u64) -> u64 { *self.began.get_or_init(|| frontier) }

	/// Whether the record knows of every write a round resuming from `since`
	/// would report.
	pub fn covers(&self, since: u64) -> bool {
		!self.disabled.load(Ordering::Relaxed)
			&& self
				.began
				.get()
				.is_some_and(|began| since >= *began)
	}

	pub fn set_disabled(&self, disabled: bool) { self.disabled.store(disabled, Ordering::Relaxed); }

	/// Stamps the room a key names by its id.
	///
	/// `frontier` is the highest count handed out at the time of the write. It
	/// is at least the count the row carries, so a round whose token is at or
	/// past it has nothing to learn from the row.
	pub fn note_room(&self, room_id: &[u8], frontier: u64) {
		let seq = self.next_seq();
		let mut rooms = self.by_id.write().expect("locked");
		match rooms.get_mut(room_id) {
			| Some(stamp) => *stamp = (stamp.0.max(frontier), seq),
			| None => {
				rooms.insert(room_id.to_vec(), (frontier, seq));
			},
		}
	}

	/// Stamps the room a timeline key names by its short id.
	pub fn note_short(&self, short_id: ShortRoomId, frontier: u64) {
		let seq = self.next_seq();
		let mut rooms = self.by_short.write().expect("locked");
		let stamp = rooms.entry(short_id).or_default();
		*stamp = (stamp.0.max(frontier), seq);
	}

	/// What is known about the writes to a room, under both of its names.
	pub fn stamp(&self, room_id: &RoomId, short_id: ShortRoomId) -> Stamp {
		let by_id = self
			.by_id
			.read()
			.expect("locked")
			.get(room_id.as_bytes())
			.copied()
			.unwrap_or_default();

		let by_short = self
			.by_short
			.read()
			.expect("locked")
			.get(&short_id)
			.copied()
			.unwrap_or_default();

		Stamp {
			mark: by_id.0.max(by_short.0),
			seq: [by_id.1, by_short.1],
		}
	}

	/// Whether a room was found, at this very stamp, to have nothing it would
	/// report to this user without a write.
	pub fn is_quiet(&self, user_id: &UserId, room_id: &RoomId, stamp: Stamp) -> bool {
		self.quiet
			.read()
			.expect("locked")
			.get(user_id)
			.and_then(|rooms| rooms.get(room_id))
			.is_some_and(|seq| *seq == stamp.seq)
	}

	/// Remembers that finding. `stamp` must have been read before the room was
	/// examined, so that a write landing meanwhile voids it.
	pub fn set_quiet(&self, user_id: &UserId, room_id: &RoomId, stamp: Stamp) {
		self.quiet
			.write()
			.expect("locked")
			.entry(user_id.to_owned())
			.or_default()
			.insert(room_id.to_owned(), stamp.seq);
	}

	/// The short id last read for a room, and the generation to hand back
	/// when remembering one that had to be read.
	pub fn short_id(&self, room_id: &RoomId) -> (Option<ShortRoomId>, u64) {
		let short_ids = self.short_ids.read().expect("locked");

		(short_ids.ids.get(room_id).copied(), short_ids.generation)
	}

	/// Remembers a room's short id, unless the map it was read from changed
	/// since `generation` was taken.
	pub fn set_short_id(&self, room_id: &RoomId, short_id: ShortRoomId, generation: u64) {
		let mut short_ids = self.short_ids.write().expect("locked");
		if short_ids.generation == generation {
			short_ids
				.ids
				.insert(room_id.to_owned(), short_id);
		}
	}

	/// Forgets the short id of the room a key of the short id map names.
	pub fn forget_short_id(&self, key: &[u8]) {
		let mut short_ids = self.short_ids.write().expect("locked");
		short_ids.generation = short_ids.generation.wrapping_add(1);
		let room_id = str::from_utf8(key)
			.ok()
			.and_then(|room_id| <&RoomId>::try_from(room_id).ok());

		match room_id {
			| Some(room_id) => {
				short_ids.ids.remove(room_id);
			},
			// A key that cannot be read names an unknown room: forget them all.
			| None => short_ids.ids.clear(),
		}
	}

	/// Subscribes to the writes that can change a device list in any room.
	pub fn device_lists(&self) -> watch::Receiver<u64> { self.device_lists.subscribe() }

	fn note_device_lists(&self) {
		self.device_lists
			.send_modify(|writes| *writes = writes.wrapping_add(1));
	}

	fn next_seq(&self) -> u64 {
		self.seq
			.fetch_add(1, Ordering::Relaxed)
			.wrapping_add(1)
	}
}

/// Has the database tell the record about every write a joined room's part of
/// a sync response is read from.
///
/// These are all of them: a response is assembled from rows of these maps and
/// from the typing state, which lives in memory and is asked directly.
pub(super) fn observe(
	db: &Arc<Database>,
	services: &Arc<OnceServices>,
	activity: &Arc<Activity>,
) {
	// In both, the room is stamped before anyone is woken, so whoever wakes
	// finds the stamp.
	for (names, index) in [(ROOM_FIRST, 0_usize), (ROOM_SECOND, 1_usize)] {
		for &name in names {
			let at_write = noted(services, activity);
			let device_lists = DEVICE_LISTS.contains(&name);
			db[name].observe(Box::new(move |key: &[u8]| {
				let Some((activity, frontier)) = at_write() else {
					return;
				};

				if let Some(room_id) = room_at(key, index) {
					activity.note_room(room_id, frontier);
				}

				if device_lists {
					activity.note_device_lists();
				}
			}));
		}
	}

	let at_write = noted(services, activity);
	db[TIMELINE].observe(Box::new(move |key: &[u8]| {
		let Some((activity, frontier)) = at_write() else {
			return;
		};

		if let Some(short_id) = short_at(key) {
			activity.note_short(short_id, frontier);
		}

		activity.note_device_lists();
	}));

	let forgetful = Arc::downgrade(activity);
	db[SHORT_IDS].observe(Box::new(move |key: &[u8]| {
		if let Some(activity) = forgetful.upgrade() {
			activity.forget_short_id(key);
		}
	}));
}

/// What an observer needs at each write: the record, and the highest count
/// handed out so far.
///
/// Neither is held strongly. The maps outlive a set of services when those are
/// rebuilt over an open database, and an observer left behind must not keep
/// them alive. Before the services are complete there is no count to read and
/// the write goes unstamped, which is safe because the record has not begun.
fn noted(
	services: &Arc<OnceServices>,
	activity: &Arc<Activity>,
) -> impl Fn() -> Option<(Arc<Activity>, u64)> + Send + Sync + use<> {
	let services = Arc::downgrade(services);
	let activity = Arc::downgrade(activity);

	move || {
		let activity = activity.upgrade()?;
		let frontier = frontier(&services)?;

		activity.begin(frontier);

		Some((activity, frontier))
	}
}

fn frontier(services: &Weak<OnceServices>) -> Option<u64> {
	let services = services.upgrade()?;
	let services = services.try_get()?;

	Some(services.globals.pending_count().end)
}

/// The room id in the `index`th element of a key, if one is there.
///
/// Elements are separated by a byte no id contains. Some of these maps also
/// hold rows keyed by a user where others have the room, and those are not
/// rooms.
fn room_at(key: &[u8], index: usize) -> Option<&[u8]> {
	key.split(|byte| *byte == SEP)
		.nth(index)
		.filter(|id| id.first() == Some(&b'!'))
}

/// The short room id a timeline key begins with.
fn short_at(key: &[u8]) -> Option<ShortRoomId> {
	key.first_chunk::<8>()
		.copied()
		.map(ShortRoomId::from_be_bytes)
}

#[cfg(test)]
mod tests {
	use ruma::{room_id, user_id};

	use super::{Activity, Stamp, room_at, short_at};

	#[test]
	fn nothing_is_covered_before_the_record_begins() {
		let activity = Activity::default();

		assert!(!activity.covers(0));
		assert!(!activity.covers(u64::MAX));
	}

	#[test]
	fn only_tokens_from_the_beginning_onwards_are_covered() {
		let activity = Activity::default();

		assert_eq!(activity.begin(100), 100);
		assert_eq!(activity.begin(200), 100, "the record begins once");

		assert!(!activity.covers(99), "a write before the record began is unknown to it");
		assert!(activity.covers(100));
		assert!(activity.covers(101));
	}

	#[test]
	fn a_disabled_record_covers_nothing() {
		let activity = Activity::default();
		activity.begin(100);

		activity.set_disabled(true);
		assert!(!activity.covers(100));

		activity.set_disabled(false);
		assert!(activity.covers(100));
	}

	#[test]
	fn an_unwritten_room_has_the_empty_stamp() {
		let activity = Activity::default();
		let room = room_id!("!quiet:example.org");

		assert_eq!(activity.stamp(room, 7), Stamp::default());
	}

	#[test]
	fn a_write_by_room_id_marks_only_that_room() {
		let activity = Activity::default();
		let written = room_id!("!written:example.org");
		let other = room_id!("!other:example.org");

		activity.note_room(written.as_bytes(), 150);

		assert_eq!(activity.stamp(written, 1).mark, 150);
		assert_eq!(activity.stamp(other, 2), Stamp::default());
	}

	#[test]
	fn a_write_by_short_id_marks_only_that_room() {
		let activity = Activity::default();
		let room = room_id!("!room:example.org");

		activity.note_short(1, 150);

		assert_eq!(activity.stamp(room, 1).mark, 150);
		assert_eq!(activity.stamp(room, 2), Stamp::default());
	}

	#[test]
	fn the_mark_is_the_newest_write_under_either_name() {
		let activity = Activity::default();
		let room = room_id!("!room:example.org");

		activity.note_short(1, 150);
		activity.note_room(room.as_bytes(), 120);
		assert_eq!(activity.stamp(room, 1).mark, 150);

		activity.note_room(room.as_bytes(), 180);
		assert_eq!(activity.stamp(room, 1).mark, 180);

		// A late notification of an older write does not take the mark back.
		activity.note_short(1, 110);
		assert_eq!(activity.stamp(room, 1).mark, 180);
	}

	#[test]
	fn every_write_changes_the_stamp_even_at_the_same_mark() {
		let activity = Activity::default();
		let room = room_id!("!room:example.org");

		activity.note_room(room.as_bytes(), 150);
		let first = activity.stamp(room, 1);

		activity.note_room(room.as_bytes(), 150);
		let second = activity.stamp(room, 1);

		activity.note_short(1, 150);
		let third = activity.stamp(room, 1);

		assert_eq!(first.mark, second.mark);
		assert_ne!(first.seq, second.seq);
		assert_ne!(second.seq, third.seq);
	}

	#[test]
	fn a_quiet_finding_holds_until_the_next_write() {
		let activity = Activity::default();
		let user = user_id!("@user:example.org");
		let room = room_id!("!room:example.org");

		let stamp = activity.stamp(room, 1);
		assert!(!activity.is_quiet(user, room, stamp), "nothing was found yet");

		activity.set_quiet(user, room, stamp);
		assert!(activity.is_quiet(user, room, activity.stamp(room, 1)));

		activity.note_short(1, 150);
		assert!(!activity.is_quiet(user, room, activity.stamp(room, 1)));
	}

	#[test]
	fn a_quiet_finding_is_one_users_about_one_room() {
		let activity = Activity::default();
		let user = user_id!("@user:example.org");
		let other_user = user_id!("@other:example.org");
		let room = room_id!("!room:example.org");
		let other_room = room_id!("!other:example.org");

		let stamp = activity.stamp(room, 1);
		activity.set_quiet(user, room, stamp);

		assert!(!activity.is_quiet(other_user, room, stamp));
		assert!(!activity.is_quiet(user, other_room, activity.stamp(other_room, 2)));
	}

	#[test]
	fn a_finding_made_across_a_write_is_void() {
		let activity = Activity::default();
		let user = user_id!("@user:example.org");
		let room = room_id!("!room:example.org");

		// The stamp is read, then the room is written while it is examined.
		let stamp = activity.stamp(room, 1);
		activity.note_room(room.as_bytes(), 150);
		activity.set_quiet(user, room, stamp);

		assert!(!activity.is_quiet(user, room, activity.stamp(room, 1)));
	}

	#[test]
	fn a_short_id_is_remembered_until_the_map_changes() {
		let activity = Activity::default();
		let room = room_id!("!room:example.org");
		let other = room_id!("!other:example.org");

		let (short_id, generation) = activity.short_id(room);
		assert_eq!(short_id, None);

		activity.set_short_id(room, 7, generation);
		activity.set_short_id(other, 8, generation);
		assert_eq!(activity.short_id(room).0, Some(7));

		activity.forget_short_id(room.as_bytes());
		assert_eq!(activity.short_id(room).0, None);
		assert_eq!(activity.short_id(other).0, Some(8), "another room keeps its id");
	}

	#[test]
	fn a_short_id_read_before_a_change_is_not_remembered_after_it() {
		let activity = Activity::default();
		let room = room_id!("!room:example.org");

		let (_, generation) = activity.short_id(room);
		activity.forget_short_id(room.as_bytes());
		activity.set_short_id(room, 7, generation);

		assert_eq!(activity.short_id(room).0, None);
	}

	#[test]
	fn an_unreadable_short_id_key_forgets_every_room() {
		let activity = Activity::default();
		let room = room_id!("!room:example.org");

		let (_, generation) = activity.short_id(room);
		activity.set_short_id(room, 7, generation);
		activity.forget_short_id(b"\xFF\xFE");

		assert_eq!(activity.short_id(room).0, None);
	}

	#[test]
	fn device_list_writes_wake_a_subscriber() {
		let activity = Activity::default();
		let mut subscriber = activity.device_lists();

		assert!(!subscriber.has_changed().unwrap(), "nothing was written yet");

		activity.note_device_lists();
		assert!(subscriber.has_changed().unwrap());

		subscriber.borrow_and_update();
		assert!(!subscriber.has_changed().unwrap());
	}

	#[test]
	fn the_room_is_read_from_the_element_it_is_in() {
		let room_first = b"!room:example.org\xFF@user:example.org";
		let room_second = b"@user:example.org\xFF!room:example.org\xFF$thread";

		assert_eq!(room_at(room_first, 0), Some(b"!room:example.org".as_slice()));
		assert_eq!(room_at(room_second, 1), Some(b"!room:example.org".as_slice()));
		assert_eq!(room_at(b"!room:example.org", 0), Some(b"!room:example.org".as_slice()));
	}

	#[test]
	fn a_key_naming_no_room_there_marks_nothing() {
		// Global account data has no room, and a user's own key changes are
		// keyed by the user.
		assert_eq!(room_at(b"\xFF@user:example.org\xFFcount", 0), None);
		assert_eq!(room_at(b"@user:example.org\xFFcount", 0), None);
		assert_eq!(room_at(b"@user:example.org", 1), None);
		assert_eq!(room_at(b"", 0), None);
	}

	#[test]
	fn the_short_id_is_the_first_eight_bytes_of_a_timeline_key() {
		let mut key = 7_u64.to_be_bytes().to_vec();
		key.extend_from_slice(&900_u64.to_be_bytes());

		assert_eq!(short_at(&key), Some(7));
		assert_eq!(short_at(&key[..4]), None, "a key too short names no room");
	}
}
