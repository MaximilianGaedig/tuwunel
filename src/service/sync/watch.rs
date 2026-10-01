use std::collections::HashSet;

use futures::{
	FutureExt, Stream, StreamExt, future::BoxFuture, pin_mut, stream::FuturesUnordered,
};
use ruma::{DeviceId, OwnedRoomId, RoomId, UserId};
use tuwunel_core::{implement, trace};
use tuwunel_database::{Interfix, Separator, serialize_key};

/// Register all sync watchers for the given user, device, and rooms eagerly,
/// then return a future that resolves when any watcher fires.
///
/// The outer `await` completes once registration is done. Callers must drive
/// it before sampling state, so a write between the registration window and
/// the long-poll await cannot be missed; that race had `MustSyncUntil` calls
/// hang for the full timeout when fast-path federation invites landed during
/// the gap.
///
/// Two-phase by design: outer await registers, inner future awaits a hit.
#[implement(super::Service)]
#[tracing::instrument(skip(self, rooms), level = "debug")]
#[expect(clippy::async_yields_async)]
pub async fn watch<'a, Rooms>(
	&'a self,
	user_id: &'a UserId,
	device_id: Option<&'a DeviceId>,
	rooms: Rooms,
) -> impl Future<Output = ()> + Send + 'a
where
	Rooms: Stream<Item = &'a RoomId> + Send + 'a,
{
	let userid_prefix =
		serialize_key((user_id, Interfix)).expect("failed to serialize watch prefix");

	let mut futures: FuturesUnordered<BoxFuture<'a, ()>> = [
		self.db
			.userroomid_joined
			.watch_raw_prefix(&userid_prefix)
			.boxed(),
		self.db
			.userroomid_invitestate
			.watch_raw_prefix(&userid_prefix)
			.boxed(),
		self.db
			.userroomid_leftstate
			.watch_raw_prefix(&userid_prefix)
			.boxed(),
		self.db
			.userroomid_knockedstate
			.watch_raw_prefix(&userid_prefix)
			.boxed(),
		self.db
			.userroomid_notificationcount
			.watch_raw_prefix(&userid_prefix)
			.boxed(),
		self.db
			.userroomid_highlightcount
			.watch_raw_prefix(&userid_prefix)
			.boxed(),
		self.db
			.roomusertype_roomuserdataid
			.watch_prefix((Separator, user_id, Interfix))
			.boxed(),
		// More key changes (used when user is not joined to any rooms)
		self.db
			.keychangeid_userid
			.watch_raw_prefix(&userid_prefix)
			.boxed(),
		// Profile changes (used when user is not joined to any rooms)
		self.db
			.profilechangeid_userid
			.watch_raw_prefix(&userid_prefix)
			.boxed(),
		// One time keys
		self.db
			.userid_lastonetimekeyupdate
			.watch_raw_prefix(user_id)
			.boxed(),
		// User account data
		self.db
			.roomuserdataid_accountdata
			.watch_prefix((Option::<&RoomId>::None, user_id, Interfix))
			.boxed(),
	]
	.into_iter()
	.collect();

	if let Some(device_id) = device_id {
		// Return when *any* user changed their key
		// TODO: only send for user they share a room with
		futures.push(
			self.db
				.todeviceid_events
				.watch_prefix((user_id, device_id, Interfix))
				.boxed(),
		);
	}

	// Typing: one receiver for all the rooms, where each room used to hold its
	// own and every update was copied to each of them. It subscribes ahead of
	// the rooms so that it is registered before this fn returns and before any
	// room is; `wait_for_update` would defer until poll.
	let mut typing_rx = self
		.services
		.typing
		.typing_update_sender
		.subscribe();

	let mut typing_rooms: HashSet<OwnedRoomId> = HashSet::new();

	// Drive the rooms stream during phase 1 so per-room watchers register
	// before this fn returns. Stream items are not retained across cursor
	// advances; the rocksdb slice contract forbids stashing them.
	pin_mut!(rooms);
	while let Some(room_id) = rooms.next().await {
		let Ok(short_roomid) = self.services.short.get_shortroomid(room_id).await else {
			continue;
		};

		// Notification clearance
		futures.push(
			self.db
				.roomuserid_lastnotificationread
				.watch_prefix((room_id, user_id))
				.boxed(),
		);
		// Key changes
		futures.push(
			self.db
				.keychangeid_userid
				.watch_prefix((room_id, Interfix))
				.boxed(),
		);
		futures.push(
			self.db
				.profilechangeid_userid
				.watch_prefix((room_id, Interfix))
				.boxed(),
		);
		// Room account data
		futures.push(
			self.db
				.roomusertype_roomuserdataid
				.watch_prefix((room_id, user_id))
				.boxed(),
		);
		// PDUs
		futures.push(
			self.db
				.pduid_pdu
				.watch_prefix(short_roomid)
				.boxed(),
		);
		// EDUs
		futures.push(
			self.db
				.readreceiptid_readreceipt
				.watch_prefix((room_id, Interfix))
				.boxed(),
		);
		typing_rooms.insert(room_id.to_owned());
	}

	// Without rooms there is no typing to wait for, and a receiver that fell
	// behind would wake the caller for nothing.
	if !typing_rooms.is_empty() {
		futures.push(
			async move {
				// A receiver that fell behind ends the wait as well: it cannot know
				// what it missed.
				while let Ok(next) = typing_rx.recv().await {
					if typing_rooms.contains(&next) {
						break;
					}
				}
			}
			.boxed(),
		);
	}

	// Server shutdown
	futures.push(self.services.server.until_shutdown().boxed());

	async move {
		if !self.services.server.is_running() {
			return;
		}

		trace!(futures = futures.len(), "watch started");
		futures.next().await;
		trace!(futures = futures.len(), "watch finished");
	}
}
