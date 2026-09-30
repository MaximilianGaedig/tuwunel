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
	Result, error, implement, info,
	matrix::{
		Event,
		pdu::{PduCount, PduId, RawPduId},
	},
	utils::{ReadyExt, stream::TryIgnore},
};
use tuwunel_database::{Deserialized, Map, Txn};

use crate::rooms::short::ShortRoomId;

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
}

struct Data {
	mediaids: Arc<Map>,
	global: Arc<Map>,
}

/// Which rules the index was built by. Raise it whenever [`kinds_for`] sorts
/// any event differently: history indexed by the old rules is then indexed
/// again on the next start, without anyone having to ask.
///
/// 1. media, files, links, music, voice.
/// 2. calls.
const INDEX_VERSION: u64 = 2;
const INDEX_VERSION_KEY: &[u8] = b"media_index_version";

/// Whether history indexed by the rules `stored` names has to be indexed
/// again. An index without a version predates versions, so it is redone too.
const fn needs_rebuild(stored: Option<u64>) -> bool {
	match stored {
		| Some(version) => version < INDEX_VERSION,
		| None => true,
	}
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
	/// Calls starting: a legacy call's invite, a MatrixRTC call's ring, or a bridge's line about a call
	/// on its network - so a client's call history is one request instead of every room's timeline.
	Calls = 6,
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
			| "calls" => Some(Self::Calls),
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
			| Self::Calls => "calls",
		}
	}
}

/// A bridge's line about a call on its network (`com.beeper.action_message` of type `call`).
#[derive(Deserialize)]
struct ActionMessage {
	#[serde(rename = "type")]
	kind: Option<String>,
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
	#[serde(rename = "com.beeper.action_message")]
	action_message: Option<ActionMessage>,
}

/// The event types that start a MatrixRTC call's ringing, stable and unstable.
const RTC_NOTIFICATIONS: [&str; 4] = [
	"m.rtc.notification",
	"org.matrix.msc4075.rtc.notification",
	"m.call.notify",
	"org.matrix.msc4075.call.notify",
];

const KEY_LEN: usize = size_of::<ShortRoomId>() + 1 + size_of::<RawPduId>();

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				mediaids: args.db["roommediaids"].clone(),
				global: args.db["global"].clone(),
			},
			services: args.services.clone(),
		}))
	}

	/// Brings history up to the current rules, once, in the background: the
	/// server serves meanwhile, and a stop part-way leaves the version
	/// unrecorded so the next start does it again.
	async fn worker(self: Arc<Self>) -> Result {
		let stored = self
			.db
			.global
			.get(INDEX_VERSION_KEY)
			.await
			.deserialized::<u64>()
			.ok();
		if !needs_rebuild(stored) {
			return Ok(());
		}

		info!(?stored, current = INDEX_VERSION, "Indexing existing history for the media index");
		// Added over what is there rather than cleared first: the rooms' tabs stay full while it runs.
		match self.index_history().await {
			| Ok(indexed) => {
				self.db.global.raw_put(INDEX_VERSION_KEY, INDEX_VERSION);
				info!("Indexed the media of {indexed} messages.");
			},
			| Err(e) if !self.services.server.is_running() => info!("Media indexing stopped: {e}"),
			| Err(e) => error!("Rebuilding the media index failed: {e}"),
		}

		Ok(())
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
		MediaKind::Calls,
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
	let indexed = self.index_history().await?;
	self.db.global.raw_put(INDEX_VERSION_KEY, INDEX_VERSION);

	Ok(indexed)
}

/// Indexes every room's existing messages over whatever the index holds.
#[implement(Service)]
async fn index_history(&self) -> Result<usize> {
	let rooms: Vec<_> = self
		.services
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let mut indexed: usize = 0;
	for room_id in rooms {
		self.services.server.check_running()?;
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
	let content = (*pdu.kind() == TimelineEventType::RoomMessage)
		.then(|| pdu.get_content::<MessageContent>().ok())
		.flatten();
	kinds_for(pdu.kind(), content.as_ref())
}

/// [`kinds_of`] from the event's type and, for a message, its content.
fn kinds_for(event_type: &TimelineEventType, content: Option<&MessageContent>) -> Vec<MediaKind> {
	let mut kinds = Vec::new();
	match event_type {
		| TimelineEventType::Sticker => {
			kinds.push(MediaKind::Media);
			return kinds;
		},
		| TimelineEventType::CallInvite => {
			kinds.push(MediaKind::Calls);
			return kinds;
		},
		| TimelineEventType::RoomMessage => {},
		| other => {
			if RTC_NOTIFICATIONS.contains(&other.to_string().as_str()) {
				kinds.push(MediaKind::Calls);
			}
			return kinds;
		},
	}

	let Some(content) = content else {
		return kinds;
	};
	if content
		.action_message
		.as_ref()
		.is_some_and(|action| action.kind.as_deref() == Some("call"))
	{
		kinds.push(MediaKind::Calls);
		return kinds;
	}
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

#[cfg(test)]
mod tests {
	use ruma::events::TimelineEventType;

	use super::{ActionMessage, INDEX_VERSION, MediaKind, MessageContent, kinds_for, needs_rebuild};

	fn message(msgtype: Option<&str>, body: Option<&str>, action: Option<&str>) -> MessageContent {
		MessageContent {
			msgtype: msgtype.map(ToOwned::to_owned),
			body: body.map(ToOwned::to_owned),
			url: None,
			file: None,
			msc3245_voice: None,
			msc2516_voice: None,
			action_message: action.map(|kind| ActionMessage { kind: Some(kind.to_owned()) }),
		}
	}

	#[test]
	fn calls_are_indexed_in_every_shape_they_take() {
		// A legacy 1:1 call, a MatrixRTC call's ring (stable and unstable), and a bridge's line.
		assert_eq!(kinds_for(&TimelineEventType::CallInvite, None), [MediaKind::Calls]);
		for rtc in ["m.rtc.notification", "org.matrix.msc4075.rtc.notification"] {
			assert_eq!(kinds_for(&TimelineEventType::from(rtc), None), [MediaKind::Calls], "{rtc}");
		}
		let line = message(Some("m.notice"), Some("Missed call"), Some("call"));
		assert_eq!(kinds_for(&TimelineEventType::RoomMessage, Some(&line)), [MediaKind::Calls]);
	}

	#[test]
	fn other_events_are_not_calls() {
		let text = message(Some("m.text"), Some("call me at https://example.org"), None);
		assert_eq!(kinds_for(&TimelineEventType::RoomMessage, Some(&text)), [MediaKind::Links]);
		assert!(kinds_for(&TimelineEventType::CallHangup, None).is_empty());
		let other_action = message(Some("m.notice"), Some("x"), Some("something_else"));
		assert!(kinds_for(&TimelineEventType::RoomMessage, Some(&other_action)).is_empty());
	}

	// Calls were added to the rules after the live server had years of history indexed: that history
	// had no calls in it until someone thought to type an admin command.
	#[test]
	fn history_indexed_by_older_rules_is_indexed_again() {
		assert!(needs_rebuild(None), "an index from before versions were recorded");
		assert!(needs_rebuild(Some(1)), "an index from before calls");
		assert!(!needs_rebuild(Some(INDEX_VERSION)));
	}

	// The rules as INDEX_VERSION names them. If this fails, the rules changed: update the table and
	// raise INDEX_VERSION, or existing history keeps being sorted by the old ones.
	#[test]
	fn the_rules_are_the_ones_the_version_names() {
		assert_eq!(INDEX_VERSION, 2);
		let link = message(Some("m.text"), Some("see https://example.org"), None);
		let call = message(Some("m.notice"), Some("Voice call"), Some("call"));
		assert_eq!(kinds_for(&TimelineEventType::Sticker, None), [MediaKind::Media]);
		assert_eq!(kinds_for(&TimelineEventType::CallInvite, None), [MediaKind::Calls]);
		assert_eq!(kinds_for(&TimelineEventType::RoomMessage, Some(&link)), [MediaKind::Links]);
		assert_eq!(kinds_for(&TimelineEventType::RoomMessage, Some(&call)), [MediaKind::Calls]);
	}
}
