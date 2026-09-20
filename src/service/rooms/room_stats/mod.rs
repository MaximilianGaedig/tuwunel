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
	/// When the message was sent (milliseconds): which month and which hour of the week it counts in.
	at: u64,
	bytes: Bytes,
}

/// What a message takes up: its own text and JSON, and the file it points at, stored here or served on
/// demand from the bridge's network (not stored at all until someone opens it).
#[derive(Clone, Copy, Debug, Default)]
pub struct Bytes {
	pub event: u64,
	pub media_stored: u64,
	pub media_on_demand: u64,
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
	/// Messages per calendar month, `(year * 100 + month, count)`, oldest first.
	pub months: Vec<(u32, u64)>,
	/// Messages per hour of the week in UTC, Monday 00:00 first (168 entries).
	pub hours: Vec<u64>,
	pub bytes: Bytes,
	pub counts: Vec<SenderCount>,
	pub first_ts: Option<u64>,
	pub last_ts: Option<u64>,
	/// Whether the counters cover the room's whole history (they were rebuilt
	/// once; after that every message adds to them).
	pub complete: bool,
}

#[derive(Deserialize)]
struct MessageContent {
	url: Option<String>,
	file: Option<FileContent>,
	info: Option<MediaInfo>,
	msgtype: Option<String>,
	#[serde(rename = "org.matrix.msc3245.voice")]
	msc3245_voice: Option<serde_json::Value>,
	#[serde(rename = "org.matrix.msc2516.voice")]
	msc2516_voice: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct FileContent {
	url: Option<String>,
}

#[derive(Deserialize)]
struct MediaInfo {
	size: Option<u64>,
}

/// Counters `shortroomid | 5 | year*100+month` (big-endian u32) and `shortroomid | 6 | hour of week` (u16).
const MONTHS: u8 = 5;
const HOURS: u8 = 6;

/// Counters `shortroomid | 4 | which`: bytes of events, of stored media, of on-demand media.
const BYTES: u8 = 4;

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
	let content = pdu.get_content::<MessageContent>().ok();
	let mut bytes = self.bytes_of(pdu.content().get().len(), content.as_ref());
	bytes.event = bytes.event.saturating_add(EVENT_OVERHEAD);
	self.change(shortroomid, class, pdu.sender(), 1, Some(ts), ts, bytes);
}

/// Takes a purged message off its counter.
#[implement(Service)]
pub fn uncount_pdu<E: Event>(&self, shortroomid: ShortRoomId, pdu: &E) {
	let content = pdu.get_content::<MessageContent>().ok();
	if let Some(class) = class_of(pdu.kind(), content.as_ref()) {
		let mut bytes = self.bytes_of(pdu.content().get().len(), content.as_ref());
		bytes.event = bytes.event.saturating_add(EVENT_OVERHEAD);
		self.change(shortroomid, class, pdu.sender(), -1, None, u64::from(pdu.origin_server_ts().get()), bytes);
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

	let at = match pdu.get("origin_server_ts") {
		| Some(CanonicalJsonValue::Integer(ts)) => u64::try_from(i64::from(*ts)).unwrap_or(0),
		| _ => 0,
	};
	let content_value = pdu
		.get("content")
		.and_then(|content| serde_json::to_value(content).ok());
	let event_len = content_value.as_ref().map_or(0, |v| v.to_string().len());
	let content = content_value.and_then(|content| serde_json::from_value::<MessageContent>(content).ok());

	let kind = TimelineEventType::from(kind.as_str());
	let Some(class) = class_of(&kind, content.as_ref()) else {
		return;
	};
	if let Ok(sender) = <&UserId>::try_from(sender.as_str()) {
		let mut bytes = self.bytes_of(event_len, content.as_ref());
		bytes.event = bytes.event.saturating_add(EVENT_OVERHEAD);
		self.change(shortroomid, class, sender, -1, None, at, bytes);
	}
}

#[implement(Service)]
fn change(
	&self,
	shortroomid: ShortRoomId,
	class: Class,
	sender: &UserId,
	delta: i64,
	ts: Option<u64>,
	at: u64,
	bytes: Bytes,
) {
	// The receiver only goes away at shutdown, when there is nothing left to count for.
	let _ = self.queue.send(Update {
		shortroomid,
		class,
		sender: sender.to_string(),
		delta,
		ts,
		at,
		bytes,
	});
}

/// A rough size for each event beyond its content: ids, hashes, signatures and keys.
const EVENT_OVERHEAD: u64 = 500;

/// Sizes a message: its content, and the file it points at (stored here, or not stored: an mxc URL
/// whose server isn't this one is served on demand by a bridge).
#[implement(Service)]
fn bytes_of(&self, content_len: usize, content: Option<&MessageContent>) -> Bytes {
	let mut bytes = Bytes { event: content_len as u64, ..Bytes::default() };
	let Some(content) = content else {
		return bytes;
	};

	let size = content.info.as_ref().and_then(|info| info.size).unwrap_or(0);
	let url = content
		.url
		.as_deref()
		.or_else(|| content.file.as_ref().and_then(|f| f.url.as_deref()));
	let Some(url) = url else {
		return bytes;
	};

	let ours = url
		.strip_prefix("mxc://")
		.and_then(|rest| rest.split('/').next())
		.is_some_and(|server| server == self.services.globals.server_name().as_str());
	if ours {
		bytes.media_stored = size;
	} else {
		bytes.media_on_demand = size;
	}

	bytes
}

/// Applies queued changes: every counter is read and written once for the whole batch.
#[implement(Service)]
fn apply(&self, batch: Vec<Update>) {
	let mut deltas: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
	let mut spans: BTreeMap<ShortRoomId, (u64, u64)> = BTreeMap::new();
	let mut bytes: BTreeMap<(ShortRoomId, u8), i64> = BTreeMap::new();
	let mut buckets: BTreeMap<(ShortRoomId, u8, u32), i64> = BTreeMap::new();

	for update in batch {
		if update.at > 0 {
			let (month, hour_of_week) = calendar(update.at);
			*buckets.entry((update.shortroomid, MONTHS, month)).or_default() += update.delta;
			*buckets.entry((update.shortroomid, HOURS, hour_of_week)).or_default() += update.delta;
		}
		for (which, amount) in [
			(1_u8, update.bytes.event),
			(2, update.bytes.media_stored),
			(3, update.bytes.media_on_demand),
		] {
			let amount = i64::try_from(amount).unwrap_or(i64::MAX);
			*bytes.entry((update.shortroomid, which)).or_default() += update.delta.saturating_mul(amount);
		}

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

	for ((shortroomid, kind, bucket), delta) in buckets {
		let mut key = shortroomid.to_be_bytes().to_vec();
		key.push(kind);
		key.extend_from_slice(&bucket.to_be_bytes());
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

	for ((shortroomid, which), delta) in bytes {
		let mut key = shortroomid.to_be_bytes().to_vec();
		key.push(BYTES);
		key.push(which);
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

	let read_bytes = |which: u8| {
		let mut key = shortroomid.to_be_bytes().to_vec();
		key.push(BYTES);
		key.push(which);
		self.db
			.roomstats
			.get_blocking(&key)
			.ok()
			.and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
			.map_or(0, u64::from_be_bytes)
	};

	let mut months = Vec::new();
	let mut hours = vec![0_u64; 168];
	for (kind, out_months) in [(MONTHS, true), (HOURS, false)] {
		let mut kprefix = shortroomid.to_be_bytes().to_vec();
		kprefix.push(kind);
		let head = kprefix.len();
		let mut entries = pin!(
			self.db
				.roomstats
				.raw_stream_from(&kprefix)
				.ignore_err()
				.ready_take_while(|(key, _)| key.starts_with(&kprefix))
		);
		while let Some((key, value)) = entries.next().await {
			let (Ok(bucket), Ok(count)) = (
				<[u8; 4]>::try_from(key.get(head..head.saturating_add(4)).unwrap_or_default()),
				<[u8; 8]>::try_from(value),
			) else {
				continue;
			};
			let (bucket, count) = (u32::from_be_bytes(bucket), u64::from_be_bytes(count));
			if out_months {
				if count > 0 {
					months.push((bucket, count));
				}
			} else if let Some(slot) = hours.get_mut(bucket as usize) {
				*slot = count;
			}
		}
	}

	Stats {
		months,
		hours,
		bytes: Bytes {
			event: read_bytes(1),
			media_stored: read_bytes(2),
			media_on_demand: read_bytes(3),
		},
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

/// The calendar month (`year * 100 + month`) and the hour of the week (Monday 00:00 is 0) of a time in
/// milliseconds since the epoch, in UTC.
fn calendar(ms: u64) -> (u32, u32) {
	let secs = ms / 1000;
	let days = i64::try_from(secs / 86_400).unwrap_or(0);
	let hour = u32::try_from((secs % 86_400) / 3600).unwrap_or(0);
	// 1970-01-01 was a Thursday; Monday is 0.
	let weekday = u32::try_from((days + 3).rem_euclid(7)).unwrap_or(0);

	// Civil date from days since the epoch (Howard Hinnant's algorithm).
	let z = days + 719_468;
	let era = z.div_euclid(146_097);
	let doe = z.rem_euclid(146_097);
	let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
	let mut year = yoe + era * 400;
	let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
	let mp = (5 * doy + 2) / 153;
	let month = if mp < 10 { mp + 3 } else { mp - 9 };
	if month <= 2 {
		year += 1;
	}

	(u32::try_from(year * 100 + month).unwrap_or(0), weekday * 24 + hour)
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

#[cfg(test)]
mod tests {
	use super::calendar;

	#[test]
	fn calendar_month_and_hour_of_week() {
		// Thursday 1970-01-01 00:00 UTC: month 197001, Monday-based hour 3 * 24.
		assert_eq!(calendar(0), (197_001, 72));
		// Friday 2024-03-15 13:45:00 UTC.
		assert_eq!(calendar(1_710_510_300_000), (202_403, 4 * 24 + 13));
		// Tuesday 2000-02-29 23:59:59 UTC (a leap day).
		assert_eq!(calendar(951_868_799_000), (200_002, 24 + 23));
	}
}
