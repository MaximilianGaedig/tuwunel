//! Rooms a user is active in from some device right now.
//!
//! A user who has just sent an event or a read receipt into a room from one
//! device is reading that room there. A full push of the next message to
//! another device (a phone) would only repeat what they are already looking
//! at, and on Element X for iOS it starts the notification extension, which
//! wakes the radio to fetch the event. With `push_active_elsewhere_s` set,
//! such a push goes out as a counts-only (badge) update instead.
//!
//! The record is in-memory only: after a restart the next push in every room
//! is full again, which is the safe direction. Activity without a device (an
//! appservice acting for the user, such as a bridge's double puppet relaying
//! what the user did in the bridged network's own app) counts as elsewhere
//! for every pusher.

use std::{
	collections::HashMap,
	sync::{Mutex, MutexGuard, PoisonError},
};

use ruma::{DeviceId, OwnedDeviceId, OwnedRoomId, OwnedUserId, RoomId, UserId};
use tuwunel_core::{implement, utils::millis_since_unix_epoch};

/// Entries beyond this many trigger a sweep of the expired ones.
const SWEEP_THRESHOLD: usize = 4096;

type Key = (OwnedUserId, OwnedRoomId);
type Devices = Vec<(Option<OwnedDeviceId>, u64)>;

/// Per user and room, when each device (or no device) was last active there,
/// in milliseconds since the Unix epoch.
#[derive(Default)]
pub(super) struct ActiveRooms {
	inner: Mutex<HashMap<Key, Devices>>,
}

impl ActiveRooms {
	fn lock(&self) -> MutexGuard<'_, HashMap<Key, Devices>> {
		self.inner
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}
}

/// The window in milliseconds, or `None` when the policy is off.
#[implement(super::Service)]
fn active_window_ms(&self) -> Option<u64> {
	let window = self.services.config.push_active_elsewhere_s;

	(window > 0).then(|| window.saturating_mul(1000))
}

/// Record that `user_id` acted in `room_id` from `device_id` at `ts_ms`
/// (milliseconds since the Unix epoch; `None` for now).
///
/// Called for events the user sends and read receipts or markers they set.
/// An activity older than the window, such as a bridge replaying a receipt
/// earned long ago, is not recorded. Nothing is recorded while
/// `push_active_elsewhere_s` is 0.
#[implement(super::Service)]
pub fn note_room_activity(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	device_id: Option<&DeviceId>,
	ts_ms: Option<u64>,
) {
	let Some(window) = self.active_window_ms() else {
		return;
	};

	if !self.services.globals.user_is_local(user_id) {
		return;
	}

	let now = millis_since_unix_epoch();
	let ts = ts_ms.unwrap_or(now).min(now);
	if now.saturating_sub(ts) >= window {
		return;
	}

	let mut rooms = self.active_rooms.lock();
	let devices = rooms
		.entry((user_id.to_owned(), room_id.to_owned()))
		.or_default();

	match devices
		.iter_mut()
		.find(|(device, _)| device.as_deref() == device_id)
	{
		| Some((_, last)) => *last = (*last).max(ts),
		| None => devices.push((device_id.map(ToOwned::to_owned), ts)),
	}

	if rooms.len() > SWEEP_THRESHOLD {
		rooms.retain(|_, devices| {
			devices.retain(|(_, last)| now.saturating_sub(*last) < window);
			!devices.is_empty()
		});
	}
}

/// Whether anything was recorded for the user in the room within the window;
/// a cheap pre-check before looking up the pusher's device.
#[implement(super::Service)]
pub(super) fn any_room_activity(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	let Some(window) = self.active_window_ms() else {
		return false;
	};

	let now = millis_since_unix_epoch();
	let key = (user_id.to_owned(), room_id.to_owned());

	self.active_rooms
		.lock()
		.get(&key)
		.is_some_and(|devices| {
			devices
				.iter()
				.any(|(_, last)| now.saturating_sub(*last) < window)
		})
}

/// Whether the user was active in the room within the window from a device
/// other than `pusher_device`.
#[implement(super::Service)]
pub(super) fn active_elsewhere(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	pusher_device: &DeviceId,
) -> bool {
	let Some(window) = self.active_window_ms() else {
		return false;
	};

	let now = millis_since_unix_epoch();
	let key = (user_id.to_owned(), room_id.to_owned());

	self.active_rooms
		.lock()
		.get(&key)
		.is_some_and(|devices| {
			devices.iter().any(|(device, last)| {
				device.as_deref() != Some(pusher_device) && now.saturating_sub(*last) < window
			})
		})
}
