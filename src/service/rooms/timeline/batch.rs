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

use super::{ExtractBody, RoomMutexGuard};
use crate::rooms::{read_receipt::PrivateRead, state_accessor::plain_text_topic};

/// One message of a batch, as the bridge described it.
pub struct BatchEvent {
	pub event_id: OwnedEventId,
	pub sender: OwnedUserId,
	pub kind: TimelineEventType,
	pub origin_server_ts: MilliSecondsSinceUnixEpoch,
	pub content: Box<RawJsonValue>,
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
			fresh.push(event);
		}
	}
	fresh.sort_by_key(|event| event.origin_server_ts);

	if fresh.is_empty() {
		return Ok(ids);
	}

	let forward = opts.forward
		|| (opts.forward_if_no_messages && !self.room_has_messages(room_id).await);

	if forward {
		self.append_batch(room_id, fresh, opts, state_lock)
			.await?;
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

/// The event as stored: a message with no state key, hung off `prev_events`.
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
		state_key: None,
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
#[implement(super::Service)]
async fn prepend_batch(&self, room_id: &RoomId, events: Vec<BatchEvent>) -> Result {
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
	for event in events.iter().rev() {
		let (pdu, json) = make_pdu(room_id, event, PrevEvents::new(), 1, origin)?;

		if let Some(shortstatehash) = shortstatehash {
			self.services
				.state
				.set_event_shortstatehash(&pdu.event_id, shortstatehash)
				.await;
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
