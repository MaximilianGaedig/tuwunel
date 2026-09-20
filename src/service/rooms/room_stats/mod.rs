//! How many messages a room has, and who sent them.
//!
//! Chat details show this ("12,340 messages; 6,210 from Alice, 5,900 from Bob;
//! 480 pictures…"). Counting it from the timeline each time would read the
//! whole room, so each accepted message adds to a counter instead, and a
//! redaction or purge takes it off again. Counters are keyed
//! `shortroomid | 1 | class | sender` (a value is a big-endian count), plus the
//! room's earliest and latest message times under `shortroomid | 2` and `| 3`.
//! Encrypted rooms count as one class: the server cannot tell what they hold.

use std::{collections::BTreeMap, pin::pin, sync::Arc};

use async_trait::async_trait;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use futures::StreamExt;
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, RoomId, UserId, events::TimelineEventType,
};
use serde::Deserialize;
use tuwunel_core::{
	Result, implement,
	matrix::Event,
	utils::{ReadyExt, stream::TryIgnore},
};
use tuwunel_database::Map;

use crate::rooms::short::ShortRoomId;

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
	/// Counting never happens on the path that stores a message: the change is queued and a worker
	/// applies queued changes in batches.
	queue: UnboundedSender<Update>,
	inbox: std::sync::Mutex<Option<UnboundedReceiver<Update>>>,
}

struct Update {
	shortroomid: ShortRoomId,
	class: Class,
	sender: String,
	delta: i64,
	ts: Option<u64>,
}

/// The most changes one pass applies, so one busy moment can't hold the worker for long.
const BATCH_MAX: usize = 2000;

struct Data {
	roomstats: Arc<Map>,
}

/// What kind of message it was. The byte is part of the key, so the values are
/// fixed once written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Class {
	Text = 1,
	Image = 2,
	Video = 3,
	Audio = 4,
	Voice = 5,
	File = 6,
	Sticker = 7,
	Encrypted = 8,
}

impl Class {
	#[must_use]
	pub const fn name(self) -> &'static str {
		match self {
			| Self::Text => "text",
			| Self::Image => "image",
			| Self::Video => "video",
			| Self::Audio => "audio",
			| Self::Voice => "voice",
			| Self::File => "file",
			| Self::Sticker => "sticker",
			| Self::Encrypted => "encrypted",
		}
	}

	fn from_byte(byte: u8) -> Option<Self> {
		Some(match byte {
			| 1 => Self::Text,
			| 2 => Self::Image,
			| 3 => Self::Video,
			| 4 => Self::Audio,
			| 5 => Self::Voice,
			| 6 => Self::File,
			| 7 => Self::Sticker,
			| 8 => Self::Encrypted,
			| _ => return None,
		})
	}
}

/// One sender's messages in one class.
pub struct SenderCount {
	pub sender: String,
	pub class: Class,
	pub count: u64,
}

pub struct Stats {
	pub counts: Vec<SenderCount>,
	pub first_ts: Option<u64>,
	pub last_ts: Option<u64>,
	/// Whether the counters cover the room's whole history (they were rebuilt
	/// once; after that every message adds to them).
	pub complete: bool,
}

#[derive(Deserialize)]
struct MessageContent {
	msgtype: Option<String>,
	#[serde(rename = "org.matrix.msc3245.voice")]
	msc3245_voice: Option<serde_json::Value>,
	#[serde(rename = "org.matrix.msc2516.voice")]
	msc2516_voice: Option<serde_json::Value>,
}

const COUNTS: u8 = 1;
const FIRST: u8 = 2;
const LAST: u8 = 3;
/// Under this key, once the counters were rebuilt from the whole history.
const READY_KEY: &[u8] = &[0xFF; 8];

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let (queue, inbox) = unbounded_channel();
		Ok(Arc::new(Self {
			db: Data { roomstats: args.db["roomstats"].clone() },
			services: args.services.clone(),
			queue,
			inbox: std::sync::Mutex::new(Some(inbox)),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		let Some(mut inbox) = self.inbox.lock().expect("locked").take() else {
			return Ok(());
		};

		loop {
			let mut batch = Vec::new();
			tokio::select! {
				received = inbox.recv() => match received {
					| Some(update) => batch.push(update),
					| None => return Ok(()),
				},
				() = self.services.server.until_shutdown() => {
					while let Ok(update) = inbox.try_recv() {
						batch.push(update);
					}
					self.apply(batch);
					return Ok(());
				},
			}

			while batch.len() < BATCH_MAX
				&& let Ok(update) = inbox.try_recv()
			{
				batch.push(update);
			}

			// Reads and writes here are blocking database calls: keep them off the async threads.
			let this = self.clone();
			let _ = tokio::task::spawn_blocking(move || this.apply(batch)).await;
		}
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Counts an accepted event, if it is a message.
#[implement(Service)]
pub fn count_pdu<E: Event>(&self, shortroomid: ShortRoomId, pdu: &E) {
	let Some(class) = class_of(pdu.kind(), pdu.get_content::<MessageContent>().ok().as_ref()) else {
		return;
	};

	let ts = u64::from(pdu.origin_server_ts().get());
	self.change(shortroomid, class, pdu.sender(), 1, Some(ts));
}

/// Takes a purged message off its counter.
#[implement(Service)]
pub fn uncount_pdu<E: Event>(&self, shortroomid: ShortRoomId, pdu: &E) {
	if let Some(class) = class_of(pdu.kind(), pdu.get_content::<MessageContent>().ok().as_ref()) {
		self.change(shortroomid, class, pdu.sender(), -1, None);
	}
}

/// Takes a redacted message off its counter. `pdu` is the event as it was
/// stored, before its content was removed.
#[implement(Service)]
pub fn uncount_json(&self, shortroomid: ShortRoomId, pdu: &CanonicalJsonObject) {
	let (Some(CanonicalJsonValue::String(kind)), Some(CanonicalJsonValue::String(sender))) =
		(pdu.get("type"), pdu.get("sender"))
	else {
		return;
	};

	let content = pdu
		.get("content")
		.and_then(|content| serde_json::to_value(content).ok())
		.and_then(|content| serde_json::from_value::<MessageContent>(content).ok());

	let kind = TimelineEventType::from(kind.as_str());
	let Some(class) = class_of(&kind, content.as_ref()) else {
		return;
	};
	if let Ok(sender) = <&UserId>::try_from(sender.as_str()) {
		self.change(shortroomid, class, sender, -1, None);
	}
}

#[implement(Service)]
fn change(&self, shortroomid: ShortRoomId, class: Class, sender: &UserId, delta: i64, ts: Option<u64>) {
	// The receiver only goes away at shutdown, when there is nothing left to count for.
	let _ = self.queue.send(Update {
		shortroomid,
		class,
		sender: sender.to_string(),
		delta,
		ts,
	});
}

/// Applies queued changes: every counter is read and written once for the whole batch.
#[implement(Service)]
fn apply(&self, batch: Vec<Update>) {
	let mut deltas: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
	let mut spans: BTreeMap<ShortRoomId, (u64, u64)> = BTreeMap::new();

	for update in batch {
		let key = count_key(update.shortroomid, update.class, &update.sender);
		*deltas.entry(key).or_default() += update.delta;

		if let Some(ts) = update.ts {
			let span = spans.entry(update.shortroomid).or_insert((ts, ts));
			span.0 = span.0.min(ts);
			span.1 = span.1.max(ts);
		}
	}

	for (key, delta) in deltas {
		let current = self
			.db
			.roomstats
			.get_blocking(&key)
			.ok()
			.and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
			.map_or(0, u64::from_be_bytes);
		self.db
			.roomstats
			.insert(&key, current.saturating_add_signed(delta).to_be_bytes());
	}

	for (shortroomid, (first, last)) in spans {
		self.widen(shortroomid, FIRST, first, u64::min);
		self.widen(shortroomid, LAST, last, u64::max);
	}
}

#[implement(Service)]
fn widen(&self, shortroomid: ShortRoomId, which: u8, ts: u64, pick: fn(u64, u64) -> u64) {
	let mut key = shortroomid.to_be_bytes().to_vec();
	key.push(which);
	let current = self
		.db
		.roomstats
		.get_blocking(&key)
		.ok()
		.and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
		.map(u64::from_be_bytes);
	let next = current.map_or(ts, |current| pick(current, ts));
	if current != Some(next) {
		self.db.roomstats.insert(&key, next.to_be_bytes());
	}
}

/// A room's counters.
#[implement(Service)]
pub async fn stats(&self, shortroomid: ShortRoomId) -> Stats {
	let mut prefix = shortroomid.to_be_bytes().to_vec();
	prefix.push(COUNTS);
	let head = prefix.len();

	let mut counts = Vec::new();
	let mut entries = pin!(
		self.db
			.roomstats
			.raw_stream_from(&prefix)
			.ignore_err()
			.ready_take_while(|(key, _)| key.starts_with(&prefix))
	);
	while let Some((key, value)) = entries.next().await {
		let (Some(class), Ok(sender), Ok(count)) = (
			key.get(head).copied().and_then(Class::from_byte),
			std::str::from_utf8(key.get(head.saturating_add(1)..).unwrap_or_default()),
			<[u8; 8]>::try_from(value),
		) else {
			continue;
		};
		let count = u64::from_be_bytes(count);
		if count > 0 {
			counts.push(SenderCount { sender: sender.to_owned(), class, count });
		}
	}

	let read = |which: u8| {
		let mut key = shortroomid.to_be_bytes().to_vec();
		key.push(which);
		self.db
			.roomstats
			.get_blocking(&key)
			.ok()
			.and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
			.map(u64::from_be_bytes)
	};

	Stats {
		counts,
		first_ts: read(FIRST),
		last_ts: read(LAST),
		complete: self.db.roomstats.get_blocking(READY_KEY).is_ok(),
	}
}

/// Counts every room's existing messages, for history that predates the counters.
#[implement(Service)]
pub async fn rebuild(&self) -> Result<usize> {
	self.db.roomstats.clear().await;

	let rooms: Vec<_> = self
		.services
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let mut counted: usize = 0;
	for room_id in rooms {
		counted = counted.saturating_add(self.rebuild_room(&room_id).await?);
	}

	self.db.roomstats.insert(READY_KEY, [1_u8]);
	Ok(counted)
}

#[implement(Service)]
async fn rebuild_room(&self, room_id: &RoomId) -> Result<usize> {
	let Ok(shortroomid) = self.services.short.get_shortroomid(room_id).await else {
		return Ok(0);
	};

	let mut counted: usize = 0;
	let mut pdus = pin!(self.services.timeline.pdus(None, room_id, None).ignore_err());
	while let Some((_, pdu)) = pdus.next().await {
		if class_of(pdu.kind(), pdu.get_content::<MessageContent>().ok().as_ref()).is_some() {
			self.count_pdu(shortroomid, &pdu);
			counted = counted.saturating_add(1);
		}
	}

	Ok(counted)
}

fn class_of(kind: &TimelineEventType, content: Option<&MessageContent>) -> Option<Class> {
	match kind {
		| TimelineEventType::Sticker => Some(Class::Sticker),
		| TimelineEventType::RoomEncrypted => Some(Class::Encrypted),
		| TimelineEventType::RoomMessage => {
			let content = content?;
			let is_voice = content.msc3245_voice.is_some() || content.msc2516_voice.is_some();
			Some(match content.msgtype.as_deref() {
				| Some("m.image") => Class::Image,
				| Some("m.video") => Class::Video,
				| Some("m.audio") if is_voice => Class::Voice,
				| Some("m.audio") => Class::Audio,
				| Some("m.file") => Class::File,
				| _ => Class::Text,
			})
		},
		| _ => None,
	}
}

fn count_key(shortroomid: ShortRoomId, class: Class, sender: &str) -> Vec<u8> {
	let mut key = shortroomid.to_be_bytes().to_vec();
	key.push(COUNTS);
	key.push(class as u8);
	key.extend_from_slice(sender.as_bytes());
	key
}
