//! An index of a room's media, so a client can list a room's pictures, files,
//! links, music or voice messages without reading its whole history.
//!
//! Clients show these as tabs beside a room ("Media", "Files", "Links", …).
//! Without an index they have to page back through every message and sort it
//! out themselves, which takes many requests and, for rooms whose messages have
//! no URL to filter on (links) or cannot be read by the server (encrypted
//! rooms), means reading everything.
//!
//! Each indexed event is one key, `shortroomid | kind | pduid`, so listing a
//! kind newest-first is one ordered scan, and paging continues from the last
//! count seen. Encrypted rooms are not indexed: the server cannot see what
//! their messages are.

use std::{pin::pin, sync::Arc};

use futures::{Stream, StreamExt};
use ruma::{RoomId, events::TimelineEventType};
use serde::Deserialize;
use tuwunel_core::{
	Result, implement,
	matrix::{
		Event,
		pdu::{PduCount, PduId, RawPduId},
	},
	utils::{ReadyExt, stream::TryIgnore},
};
use tuwunel_database::{Map, Txn};

use crate::rooms::short::ShortRoomId;

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
}

struct Data {
	mediaids: Arc<Map>,
}

/// The kinds a client lists separately. The byte is part of the key, so the
/// values are fixed once written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum MediaKind {
	/// Pictures, videos and stickers.
	Media = 1,
	/// Anything sent as a file.
	Files = 2,
	/// Messages whose text contains a link.
	Links = 3,
	/// Audio that isn't a voice message.
	Music = 4,
	/// Voice messages.
	Voice = 5,
}

impl MediaKind {
	#[must_use]
	pub fn parse(name: &str) -> Option<Self> {
		match name {
			| "media" => Some(Self::Media),
			| "files" => Some(Self::Files),
			| "links" => Some(Self::Links),
			| "music" => Some(Self::Music),
			| "voice" => Some(Self::Voice),
			| _ => None,
		}
	}

	#[must_use]
	pub const fn name(self) -> &'static str {
		match self {
			| Self::Media => "media",
			| Self::Files => "files",
			| Self::Links => "links",
			| Self::Music => "music",
			| Self::Voice => "voice",
		}
	}
}

#[derive(Deserialize)]
struct MessageContent {
	msgtype: Option<String>,
	body: Option<String>,
	url: Option<String>,
	file: Option<serde_json::Value>,
	#[serde(rename = "org.matrix.msc3245.voice")]
	msc3245_voice: Option<serde_json::Value>,
	#[serde(rename = "org.matrix.msc2516.voice")]
	msc2516_voice: Option<serde_json::Value>,
}

const KEY_LEN: usize = size_of::<ShortRoomId>() + 1 + size_of::<RawPduId>();

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data { mediaids: args.db["roommediaids"].clone() },
			services: args.services.clone(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Records which kinds an event belongs to, if any. Called as events are
/// accepted into a room's timeline, and again when rebuilding the index.
#[implement(Service)]
pub fn index_pdu<E: Event>(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId, pdu: &E) {
	let kinds = kinds_of(pdu);
	if kinds.is_empty() {
		return;
	}

	let ts = origin_ts(pdu);
	let items = kinds
		.into_iter()
		.map(|kind| (make_key(shortroomid, kind, pdu_id), ts));

	Txn::insert(&self.db.mediaids, items).execute();
}

/// The event's own time, stored beside its id so the index can be read by date without opening a
/// single event: a client's media scrubber needs a count per month and a place to start from, and
/// both would otherwise cost one fetch per item.
fn origin_ts<E: Event>(pdu: &E) -> [u8; 8] {
	u64::from(pdu.origin_server_ts().0).to_be_bytes()
}

/// Removes an event from the index (it was redacted or purged). Its content may
/// already be gone, so every kind's key is removed.
#[implement(Service)]
pub fn deindex_pdu(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId) {
	for kind in [
		MediaKind::Media,
		MediaKind::Files,
		MediaKind::Links,
		MediaKind::Music,
		MediaKind::Voice,
	] {
		self.db
			.mediaids
			.remove(&make_key(shortroomid, kind, pdu_id));
	}
}

/// A room's media of one kind, newest first, with each event's time where the index knows it.
///
/// The time is missing only for rows written before it was stored; [`dated_entries`](Self::dated_entries)
/// fills them in as they are read, and `rebuild_room` all at once.
#[implement(Service)]
pub fn media_entries<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	kind: MediaKind,
	until: Option<PduCount>,
) -> impl Stream<Item = (RawPduId, Option<u64>)> + Send + 'a {
	let until_id: RawPduId =
		PduId { shortroomid, count: until.unwrap_or_else(PduCount::max) }.into();
	let end = make_key(shortroomid, kind, &until_id);
	let prefix = make_prefix(shortroomid, kind);

	self.db
		.mediaids
		.rev_raw_stream_from(&end)
		.ignore_err()
		.ready_take_while(move |(key, _): &(&[u8], &[u8])| key.starts_with(&prefix))
		.map(|(key, val)| (RawPduId::from(&key[prefix_len()..]), read_ts(val)))
}

/// [`media_entries`](Self::media_entries), with every row's time: one written before times were
/// stored gets it from its event, and keeps it, so each such row is opened once and never again.
///
/// Without this a room indexed before the times existed counted only its newest media by month - a
/// scrubber over years of history read "September 2026" from end to end - and a seek to a date
/// stopped at the first undated row, whatever its date really was. Rows whose event can no longer
/// be read are dropped from the stream: there is nothing to list for them either.
#[implement(Service)]
pub fn dated_entries<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	kind: MediaKind,
	until: Option<PduCount>,
) -> impl Stream<Item = (RawPduId, u64)> + Send + 'a {
	self.media_entries(shortroomid, kind, until)
		.filter_map(move |(pdu_id, ts)| async move {
			if let Some(ts) = ts {
				return Some((pdu_id, ts));
			}
			let pdu = self
				.services
				.timeline
				.get_pdu_from_id(&pdu_id)
				.await
				.ok()?;
			let ts = origin_ts(&pdu);
			self.db
				.mediaids
				.insert(&make_key(shortroomid, kind, &pdu_id), ts);
			Some((pdu_id, u64::from_be_bytes(ts)))
		})
}

/// A room's media of one kind, newest first. `until` continues a previous page.
#[implement(Service)]
pub fn media_ids<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	kind: MediaKind,
	until: Option<PduCount>,
) -> impl Stream<Item = RawPduId> + Send + 'a {
	self.media_entries(shortroomid, kind, until)
		.map(|(pdu_id, _)| pdu_id)
}

fn read_ts(val: &[u8]) -> Option<u64> {
	val.try_into().ok().map(u64::from_be_bytes)
}

/// Indexes every room's existing messages: for history that predates the index.
#[implement(Service)]
pub async fn rebuild(&self) -> Result<usize> {
	self.db.mediaids.clear().await;

	let rooms: Vec<_> = self
		.services
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let mut indexed: usize = 0;
	for room_id in rooms {
		indexed = indexed.saturating_add(self.rebuild_room(&room_id).await?);
	}

	Ok(indexed)
}

/// Indexes one room's existing messages.
#[implement(Service)]
pub async fn rebuild_room(&self, room_id: &RoomId) -> Result<usize> {
	let Ok(shortroomid) = self
		.services
		.short
		.get_shortroomid(room_id)
		.await
	else {
		return Ok(0);
	};

	let mut indexed: usize = 0;
	let mut pdus = pin!(
		self.services
			.timeline
			.pdus(None, room_id, None)
			.ignore_err()
	);

	while let Some((count, pdu)) = pdus.next().await {
		let pdu_id: RawPduId = PduId { shortroomid, count }.into();
		let kinds = kinds_of(&pdu);
		if kinds.is_empty() {
			continue;
		}
		indexed = indexed.saturating_add(1);
		let ts = origin_ts(&pdu);
		let items = kinds
			.into_iter()
			.map(|kind| (make_key(shortroomid, kind, &pdu_id), ts));
		Txn::insert(&self.db.mediaids, items).execute();
	}

	Ok(indexed)
}

/// Which kinds an event belongs to: a message may be both (a link in a file's
/// caption), and anything the server cannot read belongs to none.
fn kinds_of<E: Event>(pdu: &E) -> Vec<MediaKind> {
	let mut kinds = Vec::new();
	match pdu.kind() {
		| TimelineEventType::Sticker => {
			kinds.push(MediaKind::Media);
			return kinds;
		},
		| TimelineEventType::RoomMessage => {},
		| _ => return kinds,
	}

	let Ok(content) = pdu.get_content::<MessageContent>() else {
		return kinds;
	};
	let has_file = content.url.is_some() || content.file.is_some();
	let is_voice = content.msc3245_voice.is_some() || content.msc2516_voice.is_some();

	match content.msgtype.as_deref() {
		| Some("m.image" | "m.video") if has_file => kinds.push(MediaKind::Media),
		| Some("m.file") if has_file => kinds.push(MediaKind::Files),
		| Some("m.audio") if has_file =>
			kinds.push(if is_voice { MediaKind::Voice } else { MediaKind::Music }),
		| _ => {},
	}

	if let Some(body) = content.body.as_deref()
		&& kinds.is_empty()
		&& contains_link(body)
	{
		kinds.push(MediaKind::Links);
	}

	kinds
}

/// Whether the text has a link in it, the way a client's "Links" tab counts one.
fn contains_link(body: &str) -> bool {
	body.split_whitespace()
		.any(|word| word.starts_with("http://") || word.starts_with("https://"))
}

fn make_key(shortroomid: ShortRoomId, kind: MediaKind, pdu_id: &RawPduId) -> Vec<u8> {
	let mut key = make_prefix(shortroomid, kind);
	key.extend_from_slice(pdu_id.as_ref());
	key
}

fn make_prefix(shortroomid: ShortRoomId, kind: MediaKind) -> Vec<u8> {
	let mut key = Vec::with_capacity(KEY_LEN);
	key.extend_from_slice(&shortroomid.to_be_bytes());
	key.push(kind as u8);
	key
}

const fn prefix_len() -> usize { size_of::<ShortRoomId>().saturating_add(1) }
