//! What media says, in words: the text read out of a picture, what was said in
//! a voice message, and what a picture shows.
//!
//! None of that is in the event, so none of it can be searched for - a receipt,
//! a screenshot of an address, a voice message agreeing to a date are all
//! invisible to search, which is most of what people go looking for. Reading
//! them is work a device can do (it has the picture on screen anyway) but the
//! answer belongs to everyone: read once on a phone, findable afterwards from a
//! laptop, and never read twice.
//!
//! So the text is kept here, against the event it came out of, and fed to the
//! same search index as a message's body - which is what makes it findable with
//! no change to search at all. A room's own history can be read in bulk by a
//! machine with time to spare, using `missing` to find what has not been read.
//!
//! Encrypted rooms are not here: the server cannot see what their media is, so
//! nothing can be stored against it that the server could index.

use std::sync::Arc;

use futures::{Stream, StreamExt};
use ruma::{MilliSecondsSinceUnixEpoch, UInt, UserId};
use tuwunel_core::{
	Result, err, implement,
	matrix::{
		Event,
		pdu::{PduCount, PduId, RawPduId},
	},
	utils::{ReadyExt, stream::TryIgnore},
};
use tuwunel_database::Map;

use crate::rooms::short::ShortRoomId;

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
}

struct Data {
	mediatext: Arc<Map>,
}

/// Where a piece of text came from. The byte is part of the key, so the values
/// are fixed once written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TextKind {
	/// Words that were in the picture, read off it.
	Ocr = 1,
	/// What was said in a voice message or a video.
	Transcript = 2,
	/// What a picture shows, in words, for a picture with no words in it.
	Description = 3,
}

impl TextKind {
	#[must_use]
	pub fn parse(name: &str) -> Option<Self> {
		match name {
			| "ocr" => Some(Self::Ocr),
			| "transcript" => Some(Self::Transcript),
			| "description" => Some(Self::Description),
			| _ => None,
		}
	}

	#[must_use]
	pub const fn name(self) -> &'static str {
		match self {
			| Self::Ocr => "ocr",
			| Self::Transcript => "transcript",
			| Self::Description => "description",
		}
	}
}

pub const KINDS: [TextKind; 3] = [TextKind::Ocr, TextKind::Transcript, TextKind::Description];

/// Long enough for what is written on a poster or said in a long voice message,
/// short enough that nobody can fill the database through this door.
pub const TEXT_MAX: usize = 64 * 1024;

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data { mediatext: args.db["roommediatext"].clone() },
			services: args.services.clone(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Records what an event's media says, and makes it searchable.
///
/// Storing it twice is how a better reading replaces a worse one - a phone's
/// quick pass, then a machine's careful one - so the old text is taken out of
/// the search index first, or its words would go on matching this event forever.
#[implement(Service)]
pub async fn put(
	&self,
	shortroomid: ShortRoomId,
	pdu_id: &RawPduId,
	kind: TextKind,
	text: &str,
	by: &UserId,
) -> Result {
	let key = make_key(shortroomid, kind, pdu_id);
	if let Ok(previous) = self.db.mediatext.get(&key).await {
		let previous = String::from_utf8_lossy(&previous).into_owned();
		self.services
			.search
			.deindex_pdu(shortroomid, pdu_id, &previous);
	}

	self.db.mediatext.insert(&key, text.as_bytes());
	let sent = self.sent(pdu_id).await;
	self.services
		.search
		.index_pdu(shortroomid, pdu_id, sent, text);

	tuwunel_core::debug!(
		"Indexed {} of {pdu_id:?} in room {shortroomid} ({} chars) for {by}",
		kind.name(),
		text.len(),
	);

	Ok(())
}

/// What is known about an event's media, if anything.
#[implement(Service)]
pub async fn get(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId) -> Vec<(TextKind, String)> {
	let mut found = Vec::new();
	for kind in KINDS {
		if let Ok(text) = self
			.db
			.mediatext
			.get(&make_key(shortroomid, kind, pdu_id))
			.await
		{
			found.push((kind, String::from_utf8_lossy(&text).into_owned()));
		}
	}
	found
}

/// Whether an event's media has already been read this way, which is what keeps
/// a device and a bulk run from doing the same work twice.
#[implement(Service)]
pub async fn has(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId, kind: TextKind) -> bool {
	self.db
		.mediatext
		.get(&make_key(shortroomid, kind, pdu_id))
		.await
		.is_ok()
}

/// Forgets what an event's media said: it was redacted or purged, so its words
/// must stop matching it.
#[implement(Service)]
pub async fn deindex_pdu(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId) {
	for kind in KINDS {
		let key = make_key(shortroomid, kind, pdu_id);
		if let Ok(text) = self.db.mediatext.get(&key).await {
			let text = String::from_utf8_lossy(&text).into_owned();
			self.services
				.search
				.deindex_pdu(shortroomid, pdu_id, &text);
			self.db.mediatext.remove(&key);
		}
	}
}

/// Every piece of text a room's media has, oldest first, for rebuilding the
/// search index: the index can be thrown away and refilled from the events,
/// which say nothing about what their media said, so it is refilled from here.
#[implement(Service)]
pub fn texts<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	kind: TextKind,
) -> impl Stream<Item = (RawPduId, String)> + Send + 'a {
	let prefix = make_prefix(shortroomid, kind);
	self.db
		.mediatext
		.raw_stream_from(&prefix)
		.ignore_err()
		.ready_take_while(move |(key, _): &(&[u8], &[u8])| key.starts_with(&prefix))
		.map(|(key, val)| {
			(
				RawPduId::from(&key[prefix_len()..]),
				String::from_utf8_lossy(val).into_owned(),
			)
		})
}

/// Puts a room's media text back into the search index, after the index was
/// rebuilt from the events - which know nothing of it.
#[implement(Service)]
pub async fn reindex_room(&self, shortroomid: ShortRoomId) -> usize {
	let mut indexed: usize = 0;
	for kind in KINDS {
		let mut texts = std::pin::pin!(self.texts(shortroomid, kind));
		while let Some((pdu_id, text)) = texts.next().await {
			let sent = self.sent(&pdu_id).await;
			self.services
				.search
				.index_pdu(shortroomid, &pdu_id, sent, &text);
			indexed = indexed.saturating_add(1);
		}
	}
	indexed
}

/// Turns an event's id into the id the index is keyed by.
#[implement(Service)]
pub async fn pdu_id_of(
	&self,
	room_id: &ruma::RoomId,
	event_id: &ruma::EventId,
) -> Result<(ShortRoomId, RawPduId)> {
	let shortroomid = self
		.services
		.short
		.get_shortroomid(room_id)
		.await?;
	let pdu_id = self
		.services
		.timeline
		.get_pdu_id(event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Event not found in this room."))))?;

	(pdu_id.shortroomid() == shortroomid.to_be_bytes())
		.then_some((shortroomid, pdu_id))
		.ok_or_else(|| err!(Request(NotFound("Event not found in this room."))))
}

/// The room's media of one kind that nothing has read yet, newest first: what a
/// bulk run asks for, so it is not handed work that is already done.
#[implement(Service)]
pub async fn missing(
	&self,
	shortroomid: ShortRoomId,
	media: crate::rooms::media_index::MediaKind,
	kind: TextKind,
	from: Option<PduCount>,
	limit: usize,
) -> Vec<RawPduId> {
	let mut missing = Vec::new();
	let mut entries = std::pin::pin!(
		self.services
			.media_index
			.media_ids(shortroomid, media, from)
	);
	while let Some(pdu_id) = entries.next().await {
		if missing.len() >= limit {
			break;
		}
		if !self.has(shortroomid, &pdu_id, kind).await {
			missing.push(pdu_id);
		}
	}
	missing
}

const fn prefix_len() -> usize { size_of::<ShortRoomId>().saturating_add(1) }

fn make_key(shortroomid: ShortRoomId, kind: TextKind, pdu_id: &RawPduId) -> Vec<u8> {
	let mut key = make_prefix(shortroomid, kind);
	key.extend_from_slice(pdu_id.as_ref());
	key
}

fn make_prefix(shortroomid: ShortRoomId, kind: TextKind) -> Vec<u8> {
	let mut key = Vec::with_capacity(prefix_len().saturating_add(size_of::<RawPduId>()));
	key.extend_from_slice(&shortroomid.to_be_bytes());
	key.push(kind as u8);
	key
}

/// The id a count belongs to, for a caller that has one and not the other.
#[must_use]
pub fn pdu_id_for(shortroomid: ShortRoomId, count: PduCount) -> RawPduId {
	PduId { shortroomid, count }.into()
}

/// When the event a piece of media text belongs to was sent, which the search index orders and
/// narrows results by. An event that cannot be read counts as sent at the epoch: its text is still
/// found, just never inside a date range.
#[implement(Service)]
async fn sent(&self, pdu_id: &RawPduId) -> MilliSecondsSinceUnixEpoch {
	self.services
		.timeline
		.get_pdu_from_id(pdu_id)
		.await
		.map_or(MilliSecondsSinceUnixEpoch(UInt::MIN), |pdu| pdu.origin_server_ts())
}
