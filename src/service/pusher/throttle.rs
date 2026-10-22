//! Push delivery shaping: which event notices go out as a full push and which
//! only as a counts-only (badge) update.
//!
//! A full push to an `event_id_only` pusher with `mutable-content` makes the
//! device start its notification extension, which fetches the event over the
//! network. A counts-only notice carries no event and only sets the app badge.
//! Two optional policies trade banners for fewer extension starts:
//!
//! - `push_burst_window_s`: per user, pushkey and room, the first pushable
//!   event goes out in full; later ones within the window of the last full push
//!   only update the badge.
//! - `push_quiet_badge_only`: a notification whose actions carry no sound tweak
//!   only updates the badge.
//!
//! Highlights (mentions) and call invites or rings are exempt from both, and a
//! full push sent for them restarts the window. Both policies are off by
//! default.

use std::{
	collections::HashMap,
	sync::{Mutex, MutexGuard, PoisonError},
	time::{Duration, Instant},
};

use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId, events::TimelineEventType, push::Action};
use tuwunel_core::implement;

/// Event types that ring or start a call. Their notices always go out in full.
const CALL_EVENT_TYPES: &[&str] = &[
	"m.call.invite",
	"m.call.notify",
	"org.matrix.msc4075.call.notify",
	"m.rtc.notification",
	"org.matrix.msc4075.rtc.notification",
];

/// Entries beyond this many trigger a sweep of the expired ones.
const SWEEP_THRESHOLD: usize = 1024;

type Key = (OwnedUserId, String, OwnedRoomId);

/// When each (user, pushkey, room) last had a full push sent.
#[derive(Default)]
pub(super) struct BurstWindows {
	inner: Mutex<HashMap<Key, Instant>>,
}

/// How one event notice is delivered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Delivery {
	/// The event notice in full.
	Full(Option<Instant>),
	/// Counts only: a later event in a burst within the window.
	BurstBadge,
	/// Counts only: a notification without a sound.
	QuietBadge,
}

impl Delivery {
	pub(super) fn as_str(self) -> &'static str {
		match self {
			| Self::Full(_) => "full",
			| Self::BurstBadge => "burst_badge",
			| Self::QuietBadge => "quiet_badge",
		}
	}
}

impl BurstWindows {
	fn lock(&self) -> MutexGuard<'_, HashMap<Key, Instant>> {
		self.inner
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}
}

/// Whether an event type rings or starts a call.
pub(super) fn is_call_event(kind: &TimelineEventType) -> bool {
	let kind = kind.to_string();

	CALL_EVENT_TYPES.contains(&kind.as_str())
}

/// Decide how a notifying event is delivered to one pusher.
///
/// A `Full` decision for a throttled room reserves the window at the returned
/// instant; the caller releases it with [`release_burst`] when the delivery
/// fails, so a retry of the same event is not mistaken for a later one.
#[implement(super::Service)]
pub(super) fn plan_delivery(
	&self,
	user_id: &UserId,
	pushkey: &str,
	room_id: &RoomId,
	kind: &TimelineEventType,
	actions: &[Action],
) -> Delivery {
	let config = &self.services.config;
	let highlight = actions.iter().any(Action::is_highlight);
	let exempt = highlight || is_call_event(kind);

	if !exempt
		&& config.push_quiet_badge_only
		&& !actions
			.iter()
			.any(|action| action.sound().is_some())
	{
		return Delivery::QuietBadge;
	}

	let window = Duration::from_secs(config.push_burst_window_s);
	if window.is_zero() {
		return Delivery::Full(None);
	}

	let now = Instant::now();
	let key = (user_id.to_owned(), pushkey.to_owned(), room_id.to_owned());
	let mut windows = self.push_bursts.lock();

	if !exempt
		&& windows
			.get(&key)
			.is_some_and(|last| now.saturating_duration_since(*last) < window)
	{
		return Delivery::BurstBadge;
	}

	windows.insert(key, now);

	if windows.len() > SWEEP_THRESHOLD {
		windows.retain(|_, last| now.saturating_duration_since(*last) < window);
	}

	Delivery::Full(Some(now))
}

/// Give back a window reserved by a full push that was not delivered.
#[implement(super::Service)]
pub(super) fn release_burst(
	&self,
	user_id: &UserId,
	pushkey: &str,
	room_id: &RoomId,
	reserved: Instant,
) {
	let key = (user_id.to_owned(), pushkey.to_owned(), room_id.to_owned());
	let mut windows = self.push_bursts.lock();

	if windows.get(&key) == Some(&reserved) {
		windows.remove(&key);
	}
}

/// Forget every window of one pusher.
#[implement(super::Service)]
pub(super) fn forget_bursts(&self, user_id: &UserId, pushkey: &str) {
	self.push_bursts
		.lock()
		.retain(|(user, key, _), _| user != user_id || key != pushkey);
}
