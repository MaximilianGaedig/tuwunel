use std::{collections::BTreeMap, fmt::Debug};

use futures::{StreamExt, future::join3, stream::select};
use ruma::{
	EventId, OwnedEventId, RoomId, UserId,
	events::receipt::ReceiptThread,
	push::{Action, HighlightTweakValue, Tweak},
};
use serde::Serialize;
use tuwunel_core::{
	Result, debug, implement,
	matrix::pdu::{PduCount, PduId, RawPduId},
	trace,
	utils::{
		stream::{BroadbandExt, ReadyExt, TryIgnore},
		u64_from_u8,
	},
};
use tuwunel_database::{
	Deserialized, Ignore, IgnoreAll, Interfix, KeyBuf, deserialize_from_slice as deserialize_key,
};

use super::Notified;

/// Per-thread unread counts: `(notification, highlight)` keyed by thread root.
type ThreadCounts = BTreeMap<OwnedEventId, (u64, u64)>;

/// Per-thread last-read counts keyed by thread root. Used by sync v3 to
/// gate emission of `unread_thread_notifications` to threads whose read
/// cursor advanced within the sync window.
type ThreadLastReads = BTreeMap<OwnedEventId, u64>;

/// Reset the room's main-timeline notification counts.
///
/// The last-read stamp gates sync output; callers dispatch the badge refresh
/// after every reset.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn reset_notification_counts(&self, user_id: &UserId, room_id: &RoomId) {
	let count = self.services.globals.next_count();

	let userroom_id = (user_id, room_id);

	self.reset_notification_count(room_id, user_id, userroom_id)
		.await;

	self.db
		.userroomid_highlightcount
		.put(userroom_id, 0_u64);

	let roomuser_id = (room_id, user_id);
	self.db
		.roomuserid_lastnotificationread
		.put(roomuser_id, *count);

	let removed = self.clear_suppressed_room(user_id, room_id);
	if removed > 0 {
		trace!(?user_id, ?room_id, removed, "Cleared suppressed push events after read");
	}
}

#[implement(super::Service)]
async fn reset_notification_count<K>(&self, room_id: &RoomId, user_id: &UserId, key: K)
where
	K: Serialize + Debug + Send + Sync,
{
	// The increment path is a read-modify-write under this lock; an unlocked
	// zero could land inside it and be overwritten by the stale sum.
	let _lock = self
		.notification_increment_mutex
		.lock(&(room_id.to_owned(), user_id.to_owned()))
		.await;

	self.db
		.userroomid_notificationcount
		.put(key, 0_u64);
}

/// Reset counts for a single thread within a room.
///
/// The last-read stamp gates sync output.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn reset_thread_notification_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread_root: &EventId,
) {
	let count = self.services.globals.next_count();

	let userroom_thread = (user_id, room_id, thread_root);

	self.reset_notification_count(room_id, user_id, userroom_thread)
		.await;

	self.db
		.userroomid_highlightcount
		.put(userroom_thread, 0_u64);

	let roomuser_thread = (room_id, user_id, thread_root);
	self.db
		.roomuserid_lastnotificationread
		.put(roomuser_thread, *count);
}

/// Clear all per-thread notification state for this user and room.
///
/// The `Interfix` prefix excludes the main row. The notification-count sweep
/// runs under the increment mutex so a concurrent read-modify-write cannot
/// resurrect a cleared row.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn clear_all_thread_notification_counts(&self, user_id: &UserId, room_id: &RoomId) {
	let userroom_prefix = (user_id, room_id, Interfix);
	let roomuser_prefix = (room_id, user_id, Interfix);

	let highlights = self
		.db
		.userroomid_highlightcount
		.del_prefix(&userroom_prefix);

	let last_reads = self
		.db
		.roomuserid_lastnotificationread
		.del_prefix(&roomuser_prefix);

	let notifications = async {
		let _lock = self
			.notification_increment_mutex
			.lock(&(room_id.to_owned(), user_id.to_owned()))
			.await;

		self.db
			.userroomid_notificationcount
			.del_prefix(&userroom_prefix)
			.await;
	};

	join3(notifications, highlights, last_reads).await;
}

/// Dispatcher: route a receipt's `ReceiptThread` to the matching reset path.
///
/// `Unthreaded` settles all room and thread counts; `Main` only the
/// main-timeline counts; `Thread(id)` just that thread unless the
/// acknowledged event is the thread root. `None` denotes a non-receipt reset.
///
/// `read_at` is how far the user has read. The counts become what is still
/// unread after it, recounted from the notifications stored per event, and
/// never rise above what they were. `None` means everything is read, as when
/// the user sends a message.
#[implement(super::Service)]
pub async fn reset_notification_counts_for_thread(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	acknowledged: Option<&EventId>,
	thread: &ReceiptThread,
	read_at: Option<PduCount>,
) {
	if matches!(thread, ReceiptThread::Thread(root) if acknowledged == Some(root)) {
		return;
	}

	let left = match read_at {
		| Some(read_at) => self.unread_after(user_id, room_id, read_at).await,
		| None => Some(Unread::default()),
	};

	let Some(left) = left.filter(|left| !left.is_empty()) else {
		self.clear_notification_counts_for_thread(user_id, room_id, thread)
			.await;
		return;
	};

	match thread {
		| ReceiptThread::Main =>
			self.settle_main_counts(user_id, room_id, left.main())
				.await,
		| ReceiptThread::Thread(root) =>
			self.settle_thread_counts(user_id, room_id, root, left.thread(root))
				.await,
		| _ => {
			self.settle_main_counts(user_id, room_id, left.main())
				.await;

			let threads = self
				.thread_notification_counts(user_id, room_id)
				.await;

			for root in threads.keys() {
				self.settle_thread_counts(user_id, room_id, root, left.thread(root))
					.await;
			}
		},
	}
}

/// The reset every read did before counts were recounted: all to zero.
#[implement(super::Service)]
async fn clear_notification_counts_for_thread(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread: &ReceiptThread,
) {
	match thread {
		| ReceiptThread::Main =>
			self.reset_notification_counts(user_id, room_id)
				.await,
		| ReceiptThread::Thread(root) =>
			self.reset_thread_notification_counts(user_id, room_id, root)
				.await,
		| _ => {
			self.reset_notification_counts(user_id, room_id)
				.await;

			self.clear_all_thread_notification_counts(user_id, room_id)
				.await;
		},
	}
}

/// Lowers the main-timeline counts to `left`, or clears them when nothing
/// is left.
#[implement(super::Service)]
async fn settle_main_counts(&self, user_id: &UserId, room_id: &RoomId, left: (u64, u64)) {
	if left == (0, 0) {
		self.reset_notification_counts(user_id, room_id)
			.await;
		return;
	}

	let count = self.services.globals.next_count();
	let userroom_id = (user_id, room_id);

	self.lower_counts(room_id, user_id, userroom_id, left)
		.await;

	self.db
		.roomuserid_lastnotificationread
		.put((room_id, user_id), *count);

	self.clear_suppressed_room(user_id, room_id);
}

/// Lowers one thread's counts to `left`, or clears them when nothing is
/// left.
#[implement(super::Service)]
async fn settle_thread_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	root: &EventId,
	left: (u64, u64),
) {
	if left == (0, 0) {
		self.reset_thread_notification_counts(user_id, room_id, root)
			.await;
		return;
	}

	let count = self.services.globals.next_count();

	self.lower_counts(room_id, user_id, (user_id, room_id, root), left)
		.await;

	self.db
		.roomuserid_lastnotificationread
		.put((room_id, user_id, root), *count);
}

/// Sets the `(notification, highlight)` counts at `key` to `left` where that
/// is lower than what is stored.
///
/// Both locks of the increment paths are held: a read-modify-write there
/// must not land between the read and the write here.
#[implement(super::Service)]
async fn lower_counts<K>(&self, room_id: &RoomId, user_id: &UserId, key: K, left: (u64, u64))
where
	K: Serialize + Debug + Send + Sync,
{
	let lock_key = (room_id.to_owned(), user_id.to_owned());
	let _notification = self
		.notification_increment_mutex
		.lock(&lock_key)
		.await;
	let _highlight = self
		.highlight_increment_mutex
		.lock(&lock_key)
		.await;

	for (map, left) in [
		(&self.db.userroomid_notificationcount, left.0),
		(&self.db.userroomid_highlightcount, left.1),
	] {
		let current: u64 = map.qry(&key).await.deserialized().unwrap_or(0);

		map.put(&key, current.min(left));
	}
}

/// The notifications still unread in a room once the user has read up to
/// `read_at`, by thread.
///
/// Read from the notifications stored per event at append time, which only
/// exist for events with a forward position. `None` when the scan would be
/// too long to answer a receipt with; the caller then clears the counts.
#[implement(super::Service)]
async fn unread_after(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	read_at: PduCount,
) -> Option<Unread> {
	let shortroomid = self
		.services
		.short
		.get_shortroomid(room_id)
		.await
		.ok()?;

	let start = match read_at {
		| PduCount::Normal(count) => count.saturating_add(1),
		| PduCount::Backfilled(_) => 0,
	};

	// Rows of other rooms are kept as `None` so the limit counts every row
	// read, not only the matching ones.
	let scanned: Vec<Option<(u64, Notified)>> = self
		.db
		.useridcount_notification
		.stream_from(&(user_id, start))
		.ignore_err()
		.ready_take_while(|((user, _), _): &((&UserId, u64), Notified)| *user == user_id)
		.map(|((_, count), notified)| {
			(notified.sroomid == shortroomid).then_some((count, notified))
		})
		.take(RECOUNT_SCAN_LIMIT.saturating_add(1))
		.collect()
		.await;

	if scanned.len() > RECOUNT_SCAN_LIMIT {
		debug!(%user_id, %room_id, "Too many notifications after the read to recount");
		return None;
	}

	let mut unread = Unread::default();
	for (count, notified) in scanned.into_iter().flatten() {
		let notify = notified.actions.iter().any(Action::should_notify);

		let highlight = notified.actions.iter().any(|action| {
			matches!(action, Action::SetTweak(Tweak::Highlight(HighlightTweakValue::Yes)))
		});

		let pdu_id: RawPduId = PduId {
			shortroomid,
			count: PduCount::Normal(count),
		}
		.into();
		let Ok(pdu) = self
			.services
			.timeline
			.get_pdu_from_id(&pdu_id)
			.await
		else {
			continue;
		};

		let root = self.services.threads.get_thread_id(&pdu).await;
		unread.add(root, notify, highlight);
	}

	Some(unread)
}

/// Most stored notifications a recount reads before it gives up.
const RECOUNT_SCAN_LIMIT: usize = 20_000;

/// Unread `(notification, highlight)` counts by thread root; `None` is the
/// main timeline.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Unread(BTreeMap<Option<OwnedEventId>, (u64, u64)>);

impl Unread {
	pub(super) fn add(&mut self, root: Option<OwnedEventId>, notify: bool, highlight: bool) {
		let entry = self.0.entry(root).or_default();
		entry.0 = entry.0.saturating_add(notify.into());
		entry.1 = entry.1.saturating_add(highlight.into());
	}

	pub(super) fn is_empty(&self) -> bool { self.0.values().all(|counts| *counts == (0, 0)) }

	pub(super) fn main(&self) -> (u64, u64) { self.0.get(&None).copied().unwrap_or_default() }

	pub(super) fn thread(&self, root: &EventId) -> (u64, u64) {
		self.0
			.get(&Some(root.to_owned()))
			.copied()
			.unwrap_or_default()
	}
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn notification_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_notificationcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

/// Return the user's account-wide unread notification count.
///
/// Joined main and thread rows contribute to a saturating total.
#[implement(super::Service)]
#[tracing::instrument(level = "trace", skip(self), ret)]
pub async fn global_notification_count(&self, user_id: &UserId) -> u64 {
	self.db
		.userroomid_notificationcount
		.stream_prefix_raw(&(user_id, Interfix))
		.ignore_err()
		.ready_filter_map(|(key, count)| {
			let count = u64_from_u8(count);

			(count > 0).then(|| (KeyBuf::from(key), count))
		})
		.broad_filter_map(async |(key, count)| {
			let (_, room_id, _): (Ignore, &RoomId, IgnoreAll) =
				deserialize_key(&key).expect("notification count key");

			self.services
				.state_cache
				.is_joined(user_id, room_id)
				.await
				.then_some(count)
		})
		.ready_fold(0_u64, u64::saturating_add)
		.await
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn highlight_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_highlightcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

/// Per-thread `(notification, highlight)` counts for one room and user.
/// `Interfix` excludes the legacy 2-tuple main row from the scan; only
/// 3-tuple `(user, room, root)` rows match.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn thread_notification_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> ThreadCounts {
	let prefix = (user_id, room_id, Interfix);
	let notifications = self
		.db
		.userroomid_notificationcount
		.stream_prefix(&prefix)
		.ignore_err()
		.map(notification_kv);

	let highlights = self
		.db
		.userroomid_highlightcount
		.stream_prefix(&prefix)
		.ignore_err()
		.map(highlight_kv);

	select(notifications, highlights)
		.ready_fold(ThreadCounts::default(), merge_thread_count)
		.await
}

fn notification_kv(
	(key, notifications): ((&UserId, &RoomId, OwnedEventId), u64),
) -> (OwnedEventId, (u64, u64)) {
	(key.2, (notifications, 0))
}

fn highlight_kv(
	(key, highlights): ((&UserId, &RoomId, OwnedEventId), u64),
) -> (OwnedEventId, (u64, u64)) {
	(key.2, (0, highlights))
}

fn merge_thread_count(
	mut counts: ThreadCounts,
	(root, (notifications, highlights)): (OwnedEventId, (u64, u64)),
) -> ThreadCounts {
	let entry = counts.entry(root).or_default();
	entry.0 = entry.0.saturating_add(notifications);
	entry.1 = entry.1.saturating_add(highlights);
	counts
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn last_notification_read(&self, user_id: &UserId, room_id: &RoomId) -> Result<u64> {
	let key = (room_id, user_id);
	self.db
		.roomuserid_lastnotificationread
		.qry(&key)
		.await
		.deserialized()
}

/// Per-thread last-read counts for one room and user. `Interfix` keeps the
/// scan to 3-tuple `(room, user, root)` rows; the legacy 2-tuple main row
/// is excluded by construction and lives behind `last_notification_read`.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn thread_last_notification_reads(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> ThreadLastReads {
	let prefix = (room_id, user_id, Interfix);
	self.db
		.roomuserid_lastnotificationread
		.stream_prefix(&prefix)
		.ignore_err()
		.map(|((_, _, root), count): ((Ignore, Ignore, OwnedEventId), u64)| (root, count))
		.collect()
		.await
}

#[implement(super::Service)]
pub async fn delete_room_notification_read(&self, room_id: &RoomId) -> Result {
	let key = (room_id, Interfix);
	self.db
		.roomuserid_lastnotificationread
		.keys_prefix_raw(&key)
		.ignore_err()
		.ready_for_each(|key| {
			trace!("Removing key: {key:?}");
			self.db
				.roomuserid_lastnotificationread
				.remove(key);
		})
		.await;

	Ok(())
}
