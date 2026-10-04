//! Inserts a batch of messages a bridge imported from another network, keeping
//! the event IDs the bridge chose for them.
//!
//! A bridge that imports a chat's history needs to put messages *behind* the
//! ones a room already has (`backward`), or to append a run of messages
//! stamped with their original times (`forward`), and it records the event IDs
//! it derived for them beforehand, so an edit, a reaction or a redaction from
//! the other network can find its target later. Neither can go through the
//! ordinary send path, which mints its own IDs and stamps the present. Events
//! here are trusted (the caller is an appservice) and are not authorised
//! against room state or signed.
//!
//! A batch put behind a room's messages may also carry membership: a person of
//! the other network invited and joining just before the first message of
//! theirs that was imported, so the history says when they came and clients
//! can name them there. That membership is history only: the events after it
//! get it in their state, the room's current state is left as it is, and a
//! later batch reaching further back moves it to the earlier message.

use std::iter::once;

use futures::StreamExt;
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedUserId, RoomId,
	events::{TimelineEventType, receipt::ReceiptThread},
	uint,
};
use serde_json::value::RawValue as RawJsonValue;
use tuwunel_core::{
	Result, err, implement,
	matrix::{
		event::Event,
		pdu::{EventHash, PduCount, PduEvent, PduId, PrevEvents, RawPduId},
	},
	utils::{result::LogErr, to_canonical_object},
	validated,
};

use super::{ExtractBody, RoomMutexGuard, pdus::bias_count};
use crate::rooms::{read_receipt::PrivateRead, state_accessor::plain_text_topic};

/// One message of a batch, as the bridge described it.
pub struct BatchEvent {
	pub event_id: OwnedEventId,
	pub sender: OwnedUserId,
	pub kind: TimelineEventType,
	pub origin_server_ts: MilliSecondsSinceUnixEpoch,
	pub content: Box<RawJsonValue>,
	/// Set for membership the batch carries; see the module documentation.
	pub state_key: Option<String>,
}

/// A batch event the room does not have yet, or historical membership the room
/// has at a later point than this batch puts it.
struct Fresh {
	event: BatchEvent,
	/// The stored copy this one takes the place of: where it is and when it says
	/// it was sent.
	replaces: Option<(RawPduId, u64)>,
}

/// What to do with a batch beyond inserting it.
pub struct BatchOptions {
	/// Append after the newest event rather than before the oldest.
	pub forward: bool,
	/// Append as well if the room has no messages yet (its first import).
	pub forward_if_no_messages: bool,
	/// Whether appended messages should notify the room's members.
	pub notify: bool,
	/// Mark the room read for this user up to the batch's last message.
	pub mark_read_by: Option<OwnedUserId>,
}

/// Inserts `events` and returns the IDs of all of them, in the order given,
/// including any the room already had (a retried batch is a no-op).
#[implement(super::Service)]
#[tracing::instrument(name = "batch", level = "debug", skip_all, fields(events = events.len()))]
pub async fn insert_batch(
	&self,
	room_id: &RoomId,
	mut events: Vec<BatchEvent>,
	opts: &BatchOptions,
	state_lock: &RoomMutexGuard,
) -> Result<Vec<OwnedEventId>> {
	let ids: Vec<OwnedEventId> = events
		.iter()
		.map(|event| event.event_id.clone())
		.collect();

	let mut fresh = Vec::with_capacity(events.len());
	for event in events.drain(..) {
		if self
			.non_outlier_pdu_exists(&event.event_id)
			.await
			.is_err()
		{
			fresh.push(Fresh { event, replaces: None });
		} else if let Some(replaces) = self.moved_earlier(room_id, &event).await {
			fresh.push(Fresh { event, replaces: Some(replaces) });
		}
	}
	fresh.sort_by_key(|fresh| fresh.event.origin_server_ts);

	if fresh.is_empty() {
		return Ok(ids);
	}

	let forward = opts.forward
		|| (opts.forward_if_no_messages && !self.room_has_messages(room_id).await);

	if forward {
		// Appended events are the room's live end, and every client takes state
		// there for the room's current state. Historical membership must not
		// make anyone a member now, so it only goes in behind the messages.
		let events: Vec<_> = fresh
			.into_iter()
			.map(|fresh| fresh.event)
			.filter(|event| event.state_key.is_none())
			.collect();

		if !events.is_empty() {
			self.append_batch(room_id, events, opts, state_lock)
				.await?;
		}
	} else {
		self.prepend_batch(room_id, fresh).await?;
	}

	Ok(ids)
}

#[implement(super::Service)]
async fn room_has_messages(&self, room_id: &RoomId) -> bool {
	let pdus = self.pdus(None, room_id, None);
	futures::pin_mut!(pdus);

	while let Some(Ok((_, pdu))) = pdus.next().await {
		if matches!(
			*pdu.event_type(),
			TimelineEventType::RoomMessage
				| TimelineEventType::Sticker
				| TimelineEventType::RoomEncrypted
		) {
			return true;
		}
	}

	false
}

/// Where the room already has `event`, when it is historical membership this
/// batch puts earlier: a person's join belongs before the first message of
/// theirs, and a batch reaching further back finds an earlier one.
#[implement(super::Service)]
async fn moved_earlier(&self, room_id: &RoomId, event: &BatchEvent) -> Option<(RawPduId, u64)> {
	event.state_key.as_ref()?;

	let pdu_id = self.get_pdu_id(&event.event_id).await.ok()?;
	if !matches!(pdu_id.pdu_count(), PduCount::Backfilled(_)) {
		return None;
	}

	let stored = self.get_pdu_from_id(&pdu_id).await.ok()?;
	let ts = u64::from(stored.origin_server_ts);

	(stored.room_id() == room_id && ts > u64::from(event.origin_server_ts.get()))
		.then_some((pdu_id, ts))
}

/// The event as stored, hung off `prev_events`.
fn make_pdu(
	room_id: &RoomId,
	event: &BatchEvent,
	prev_events: PrevEvents,
	depth: u64,
	origin: &ruma::ServerName,
) -> Result<(PduEvent, CanonicalJsonObject)> {
	let pdu = PduEvent {
		event_id: event.event_id.clone(),
		room_id: room_id.to_owned(),
		sender: event.sender.clone(),
		origin: Some(origin.to_owned()),
		content: event.content.clone().into(),
		origin_server_ts: event.origin_server_ts.get(),
		kind: event.kind.clone(),
		state_key: event.state_key.clone().map(Into::into),
		depth: depth.try_into().unwrap_or_else(|_| uint!(1)),
		redacts: None,
		unsigned: None,
		hashes: EventHash::default(),
		prev_events,
		auth_events: Default::default(),
	};

	let mut json = to_canonical_object(&pdu)
		.map_err(|e| err!(Request(BadJson("Cannot store batch event: {e}"))))?;
	json.insert("event_id".into(), CanonicalJsonValue::String(event.event_id.to_string()));

	Ok((pdu, json))
}

/// Puts the events behind everything the room already has, oldest first in
/// reading order: the newest is stored first so each older one sorts before it.
///
/// Each event gets the state the room had where it now sits: the state at the
/// room's oldest event, plus the membership this batch put before it.
#[implement(super::Service)]
async fn prepend_batch(&self, room_id: &RoomId, events: Vec<Fresh>) -> Result {
	let shortroomid = self
		.services
		.short
		.get_shortroomid(room_id)
		.await
		.map_err(|_| err!(Request(NotFound("Unknown room."))))?;

	// The state the room had when its history began, so history visibility
	// applies to imported events as it does to the events beside them.
	let oldest = self.first_item_in_room(room_id).await;
	let shortstatehash = match &oldest {
		| Ok((_, pdu)) => self
			.services
			.state
			.pdu_shortstatehash(pdu.event_id())
			.await
			.ok(),
		| Err(_) => None,
	};

	let origin = self.services.globals.server_name();
	let mut state = shortstatehash;
	let mut stored = Vec::with_capacity(events.len());
	for fresh in &events {
		let (pdu, json) = make_pdu(room_id, &fresh.event, PrevEvents::new(), 1, origin)?;
		let state_before = state;
		if let Some(state_key) = &pdu.state_key {
			state = Some(
				self.services
					.state
					.state_with_event(state, &pdu, state_key)
					.await?,
			);
		}

		stored.push((fresh, pdu, json, state_before));
	}

	for (fresh, pdu, json, shortstatehash) in stored.into_iter().rev() {
		if let Some(shortstatehash) = shortstatehash {
			self.services
				.state
				.set_event_shortstatehash(&pdu.event_id, shortstatehash)
				.await;
		}

		if let Some((old_pdu_id, old_ts)) = &fresh.replaces {
			self.forget_backfilled(room_id, old_pdu_id, *old_ts);
		}

		let insert_lock = self.mutex_insert.lock(room_id).await;
		let count: i64 = (*self.services.globals.next_count()).try_into()?;
		let pdu_id: RawPduId = PduId {
			shortroomid,
			count: PduCount::Backfilled(validated!(0 - count)),
		}
		.into();

		self.prepend_backfill_pdu(
			&pdu_id,
			room_id,
			&pdu.event_id,
			u64::from(pdu.origin_server_ts),
			&json,
		);
		drop(insert_lock);

		// Membership is not a message: nothing to find, count or list.
		if pdu.state_key.is_some() {
			continue;
		}

		self.services
			.media_index
			.index_pdu(shortroomid, &pdu_id, &pdu);

		self.services
			.room_stats
			.count_pdu(shortroomid, &pdu);

		self.services
			.activity_log
			.log_pdu(shortroomid, &pdu);

		self.index_text(shortroomid, &pdu_id, &pdu);
	}

	Ok(())
}

/// Takes a historical event out of the place it had, before it is stored again
/// at an earlier one.
#[implement(super::Service)]
fn forget_backfilled(&self, room_id: &RoomId, pdu_id: &RawPduId, origin_server_ts: u64) {
	let mut txn = self.db.db.txn();

	txn.del_raw(&self.db.pduid_pdu, pdu_id);

	let count_key = bias_count(pdu_id.count());
	txn.del(&self.db.roomid_tscount_pducount, (room_id, origin_server_ts, count_key));

	txn.execute();
}

/// Appends the events after everything the room already has, each after the
/// one before, with the times they were originally sent.
#[implement(super::Service)]
async fn append_batch(
	&self,
	room_id: &RoomId,
	events: Vec<BatchEvent>,
	opts: &BatchOptions,
	state_lock: &RoomMutexGuard,
) -> Result {
	let shortroomid = self
		.services
		.short
		.get_shortroomid(room_id)
		.await
		.map_err(|_| err!(Request(NotFound("Unknown room."))))?;

	let shortstatehash = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.await
		.ok();

	let origin = self.services.globals.server_name();
	let mut last: Option<(RawPduId, MilliSecondsSinceUnixEpoch, u64)> = None;

	for event in &events {
		let prev_events: PrevEvents = self
			.services
			.state
			.get_forward_extremities(room_id)
			.take(20)
			.map(ToOwned::to_owned)
			.collect()
			.await;

		let mut depth = 0_u64;
		for prev in &prev_events {
			if let Ok(prev) = self.get_pdu(prev).await {
				depth = depth.max(u64::from(prev.depth));
			}
		}

		let (pdu, json) = make_pdu(room_id, event, prev_events, depth.saturating_add(1), origin)?;

		if let Some(shortstatehash) = shortstatehash {
			self.services
				.state
				.set_event_shortstatehash(&pdu.event_id, shortstatehash)
				.await;
		}

		self.services
			.pdu_metadata
			.mark_as_referenced(room_id, pdu.prev_events.iter().map(AsRef::as_ref));
		self.services
			.state
			.set_forward_extremities(room_id, once(pdu.event_id.as_ref()), state_lock)
			.await;

		let insert_lock = self.mutex_insert.lock(room_id).await;
		let next_count = self.services.globals.next_count();
		let count = PduCount::Normal(*next_count);
		let pdu_id: RawPduId = PduId { shortroomid, count }.into();
		self.append_pdu_json(&pdu_id, &pdu, &json);
		drop(insert_lock);

		self.append_pdu_effects(pdu_id, &pdu, shortroomid, count, state_lock)
			.await?;

		if opts.notify {
			self.services
				.pusher
				.append_pdu(pdu_id, &pdu)
				.await
				.log_err()
				.ok();
		}

		last = Some((pdu_id, event.origin_server_ts, *next_count));
		drop(next_count);
	}

	if let (Some(user), Some((_, ts, count))) = (&opts.mark_read_by, last) {
		self.services
			.read_receipt
			.private_read_set(PrivateRead {
				room_id,
				user_id: user,
				count,
				ts,
				thread: &ReceiptThread::Unthreaded,
				announce: false,
			})
			.await;
	}

	Ok(())
}

/// Makes a message or topic findable by text search.
#[implement(super::Service)]
fn index_text(&self, shortroomid: crate::rooms::short::ShortRoomId, pdu_id: &RawPduId, pdu: &PduEvent) {
	match *pdu.event_type() {
		| TimelineEventType::RoomMessage => {
			if let Ok(ExtractBody { body: Some(body) }) = pdu.get_content() {
				self.services
					.search
					.index_pdu(shortroomid, pdu_id, &body);
			}
		},
		| TimelineEventType::RoomTopic =>
			if let Some(topic) = pdu.get_content().ok().and_then(plain_text_topic) {
				self.services
					.search
					.index_pdu(shortroomid, pdu_id, &topic);
			},
		| _ => {},
	}
}

#[cfg(test)]
mod tests {
	use futures::StreamExt;
	use ruma::{
		MilliSecondsSinceUnixEpoch, OwnedEventId, RoomId, UInt, UserId,
		events::{StateEventType, TimelineEventType},
		owned_room_id,
	};
	use serde_json::{json, value::to_raw_value};
	use tuwunel_core::{Result, config::Figment, matrix::Event};

	use super::{BatchEvent, BatchOptions};
	use crate::{Services, test_utils::fixture};

	const BOT: &str = "@bot:localhost";
	const ADA: &str = "@ada:localhost";
	const BEN: &str = "@ben:localhost";

	fn batch_event(
		id: &str,
		sender: &str,
		ts: u64,
		state_key: Option<&str>,
		content: &serde_json::Value,
	) -> BatchEvent {
		BatchEvent {
			event_id: id.try_into().expect("event id"),
			sender: sender.try_into().expect("user id"),
			kind: if state_key.is_some() {
				TimelineEventType::RoomMember
			} else {
				TimelineEventType::RoomMessage
			},
			origin_server_ts: MilliSecondsSinceUnixEpoch(UInt::new(ts).expect("ts")),
			content: to_raw_value(content).expect("content"),
			state_key: state_key.map(ToOwned::to_owned),
		}
	}

	fn message(id: &str, sender: &str, ts: u64) -> BatchEvent {
		batch_event(id, sender, ts, None, &json!({ "msgtype": "m.text", "body": id }))
	}

	/// Ben invited by the bridge bot and joining, as a bridge puts it before his first message.
	fn ben_arrives(ts: u64) -> [BatchEvent; 2] {
		[
			batch_event("$ben-invite", BOT, ts, Some(BEN), &json!({ "membership": "invite" })),
			batch_event(
				"$ben-join",
				BEN,
				ts.saturating_add(1),
				Some(BEN),
				&json!({ "membership": "join" }),
			),
		]
	}

	fn options(forward: bool) -> BatchOptions {
		BatchOptions {
			forward,
			forward_if_no_messages: false,
			notify: false,
			mark_read_by: None,
		}
	}

	/// The room's timeline in reading order, as `(event id, origin_server_ts)`.
	async fn timeline(services: &Services, room_id: &RoomId) -> Vec<(OwnedEventId, u64)> {
		services
			.timeline
			.pdus(None, room_id, None)
			.filter_map(async |item| item.ok())
			.map(|(_, pdu)| (pdu.event_id().to_owned(), u64::from(pdu.origin_server_ts)))
			.collect()
			.await
	}

	/// Who `user` is in the state the server keeps for the event, if anyone.
	async fn member_at(services: &Services, event_id: &str, user: &str) -> Option<String> {
		let event_id: OwnedEventId = event_id.try_into().ok()?;
		let shortstatehash = services
			.state
			.pdu_shortstatehash(&event_id)
			.await
			.ok()?;

		services
			.state_accessor
			.state_get(shortstatehash, &StateEventType::RoomMember, user)
			.await
			.ok()
			.map(|pdu| pdu.event_id().to_string())
	}

	// MEO-146: a person's invite and join go into the imported history just before their first
	// message there, dated with it, and give the messages after them their membership. The room's
	// current state is not changed, a batch reaching further back moves them to the earlier
	// message, and a batch appended at the live end leaves them out.
	#[tokio::test]
	async fn imported_membership_is_history_before_the_first_message() -> Result {
		let Some(fixture) = fixture(Figment::new()).await? else {
			return Ok(());
		};

		let services = &fixture.services;
		let room = owned_room_id!("!imported:localhost");
		let ben: &UserId = BEN.try_into().expect("user id");
		services
			.short
			.get_or_create_shortroomid(&room)
			.await;

		let lock = services.state.mutex.lock(&room).await;

		let [invite, join] = ben_arrives(1998);
		let newer =
			vec![message("$ada-1", ADA, 1000), invite, join, message("$ben-1", BEN, 2000)];
		services
			.timeline
			.insert_batch(&room, newer, &options(false), &lock)
			.await?;

		let ids = |timeline: &[(OwnedEventId, u64)]| -> Vec<String> {
			timeline
				.iter()
				.map(|(id, _)| id.to_string())
				.collect()
		};

		assert_eq!(ids(&timeline(services, &room).await), [
			"$ada-1",
			"$ben-invite",
			"$ben-join",
			"$ben-1"
		]);
		assert_eq!(
			member_at(services, "$ben-1", BEN)
				.await
				.as_deref(),
			Some("$ben-join"),
			"Ben's message does not have his membership in its state"
		);
		assert_eq!(
			member_at(services, "$ada-1", BEN).await,
			None,
			"Ada's earlier message has it"
		);
		assert!(
			services
				.state
				.get_room_shortstatehash(&room)
				.await
				.is_err(),
			"the room's current state changed"
		);
		assert!(!services.state_cache.is_joined(ben, &room).await, "Ben became a member now");

		// An older batch with an earlier message of Ben's: his membership moves there.
		let [invite, join] = ben_arrives(498);
		let older = vec![invite, join, message("$ben-0", BEN, 500)];
		services
			.timeline
			.insert_batch(&room, older, &options(false), &lock)
			.await?;

		let moved = timeline(services, &room).await;
		assert_eq!(ids(&moved), ["$ben-invite", "$ben-join", "$ben-0", "$ada-1", "$ben-1"]);
		assert_eq!(moved[1].1, 499, "the join does not have the date of the earlier message");
		assert_eq!(
			member_at(services, "$ben-0", BEN)
				.await
				.as_deref(),
			Some("$ben-join")
		);

		// At the live end, membership would be taken for the room's current state: left out.
		let appended = vec![
			batch_event(
				"$ben-invite-2",
				BOT,
				2998,
				Some(BEN),
				&json!({ "membership": "invite" }),
			),
			batch_event("$ben-join-2", BEN, 2999, Some(BEN), &json!({ "membership": "join" })),
			message("$ben-2", BEN, 3000),
		];
		services
			.timeline
			.insert_batch(&room, appended, &options(true), &lock)
			.await?;

		let live = ids(&timeline(services, &room).await);
		assert!(live.contains(&"$ben-2".to_owned()));
		assert!(!live.contains(&"$ben-join-2".to_owned()), "membership was appended live");

		Ok(())
	}
}
