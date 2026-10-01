//! The rules of a room export: what a request may ask for, and what a file's
//! row in the media manifest says.
//!
//! A client can already export a room by paging `/messages`, which is fine for
//! a small room and hopeless for a chat with a million messages and gigabytes
//! of files. The export endpoints hand out the same events in pages a hundred
//! times larger, and list the room's files from the media index without
//! reading its history.
//!
//! Nothing here touches the database, so all of it can be tested without a
//! server; the endpoints in the api crate do the reading.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tuwunel_core::matrix::pdu::PduCount;

use super::media_index::MediaKind;

/// Which end of the room a page starts from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Order {
	OldestFirst,
	NewestFirst,
}

impl Order {
	/// Reads the `dir` parameter, spelled as `/messages` spells it. An export is read from its
	/// beginning unless asked otherwise: that is the order a file is written in.
	#[must_use]
	pub fn parse(dir: Option<&str>) -> Option<Self> {
		match dir {
			| None | Some("f") => Some(Self::OldestFirst),
			| Some("b") => Some(Self::NewestFirst),
			| Some(_) => None,
		}
	}
}

/// Which of a room's events an export keeps.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Selection {
	/// Events sent at or after this time (milliseconds since the epoch).
	pub since_ts: Option<u64>,

	/// Events sent before this time. The end is left out so that two exports of adjoining
	/// stretches - one month, then the next - share no event and miss none.
	pub until_ts: Option<u64>,

	/// Only these event types; every type when empty.
	pub types: Vec<String>,
}

impl Selection {
	/// Reads the parameters of a request. `types` is a comma-separated list, which no event type
	/// contains, so it needs no escaping beyond the URL's own.
	///
	/// A stretch of time that holds nothing is refused rather than answered with an empty export:
	/// it is a mistake in the request, and an empty file would look like an empty room.
	pub fn parse(
		since_ts: Option<u64>,
		until_ts: Option<u64>,
		types: Option<&str>,
	) -> Result<Self, &'static str> {
		if let (Some(since), Some(until)) = (since_ts, until_ts)
			&& since >= until
		{
			return Err("`since_ts` must be before `until_ts`.");
		}

		let types = types
			.unwrap_or_default()
			.split(',')
			.map(str::trim)
			.filter(|kind| !kind.is_empty())
			.map(ToOwned::to_owned)
			.collect();

		Ok(Self { since_ts, until_ts, types })
	}

	/// Whether an event of this type, sent at this time, belongs in the export.
	#[must_use]
	pub fn admits(&self, kind: &str, ts: u64) -> bool {
		self.since_ts.is_none_or(|since| ts >= since)
			&& self.until_ts.is_none_or(|until| ts < until)
			&& (self.types.is_empty() || self.types.iter().any(|wanted| wanted == kind))
	}
}

/// The kinds of the media index that are files. Links and calls are indexed too, but there is
/// nothing to download for them. The manifest lists the kinds in this order, each newest first.
pub const FILE_KINDS: [MediaKind; 4] =
	[MediaKind::Media, MediaKind::Files, MediaKind::Music, MediaKind::Voice];

/// Where the next page of a manifest continues: the kind the last page stopped in, and the last
/// place it looked at there.
#[must_use]
pub fn format_cursor(kind: MediaKind, count: PduCount) -> String {
	format!("{}:{count}", kind.name())
}

/// Reads a cursor back: the kind's place in [`FILE_KINDS`], and the place within it.
#[must_use]
pub fn parse_cursor(token: &str) -> Option<(usize, PduCount)> {
	let (kind, count) = token.split_once(':')?;
	let kind = MediaKind::parse(kind)?;
	let place = FILE_KINDS
		.iter()
		.position(|listed| *listed == kind)?;

	Some((place, count.parse().ok()?))
}

/// One file of a room, as the manifest lists it. The bytes are not part of an export: a client
/// fetches the files it wants through the media endpoints, and this row is what it needs to choose.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestRow {
	pub event_id: String,
	pub sender: String,

	/// When the event was sent, in milliseconds since the epoch.
	pub ts: u64,

	/// Which list of the media index it is in: `media`, `files`, `music` or `voice`.
	pub kind: String,

	/// The message's `msgtype`; a sticker has none.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub msgtype: Option<String>,

	/// The file's name: `filename` where the sender gave one, and otherwise `body`, which is
	/// where the name was before captions existed.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub filename: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub mimetype: Option<String>,

	/// The size the sender stated in `info`. Nothing checks it against the file.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	/// The file's `mxc://` URI.
	pub url: String,

	/// Whether the file is an encrypted attachment: what downloads is ciphertext, and the key is
	/// in the event.
	pub encrypted: bool,

	/// Whether the bytes are kept on this server. A file that is not is fetched from its own
	/// network by a bridge when someone opens it, which is slow and, on some networks, rationed -
	/// so an export says which files those are and leaves it to the client whether to ask.
	pub stored: bool,
}

/// The manifest's row for a media event, from the event's own fields and its content. Nothing
/// when the content names no file, which is how a redacted event reads.
///
/// The content is read leniently, a field at a time: an event whose `info.size` is a string is
/// still a file someone will want in their export, only without a size.
#[must_use]
pub fn manifest_row(
	event_id: &str,
	sender: &str,
	ts: u64,
	kind: MediaKind,
	content: &Value,
	server_name: &str,
) -> Option<ManifestRow> {
	let plain = text(content, "url");
	let attachment = content
		.get("file")
		.and_then(|file| text(file, "url"));
	let encrypted = plain.is_none() && attachment.is_some();
	let url = plain.or(attachment)?;
	let stored = mxc_is_local(&url, server_name);
	let info = content.get("info");

	Some(ManifestRow {
		event_id: event_id.to_owned(),
		sender: sender.to_owned(),
		ts,
		kind: kind.name().to_owned(),
		msgtype: text(content, "msgtype"),
		filename: text(content, "filename").or_else(|| text(content, "body")),
		mimetype: info.and_then(|info| text(info, "mimetype")),
		size: info
			.and_then(|info| info.get("size"))
			.and_then(Value::as_u64),
		url,
		encrypted,
		stored,
	})
}

/// Whether an `mxc://` URI names this server, which is where its bytes then are. Any other name
/// is a bridge's, serving the file from its network on demand.
///
/// This is the rule `room_stats` sorts a room's bytes by (`bytes_of`), repeated here so that the
/// files a manifest calls stored add up to what the statistics call stored.
#[must_use]
pub fn mxc_is_local(url: &str, server_name: &str) -> bool {
	url.strip_prefix("mxc://")
		.and_then(|rest| rest.split('/').next())
		.is_some_and(|server| server == server_name)
}

fn text(value: &Value, key: &str) -> Option<String> {
	value
		.get(key)
		.and_then(Value::as_str)
		.map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
	use serde_json::json;
	use tuwunel_core::matrix::pdu::PduCount;

	use super::{
		FILE_KINDS, ManifestRow, MediaKind, Order, Selection, Value, format_cursor, manifest_row,
		mxc_is_local, parse_cursor,
	};

	const SERVER: &str = "example.org";

	fn row_for(kind: MediaKind, content: &Value) -> Option<ManifestRow> {
		manifest_row("$event", "@alice:example.org", 1_700_000_000_000, kind, content, SERVER)
	}

	#[test]
	fn an_export_reads_from_the_beginning_unless_asked_otherwise() {
		assert_eq!(Order::parse(None), Some(Order::OldestFirst));
		assert_eq!(Order::parse(Some("f")), Some(Order::OldestFirst));
		assert_eq!(Order::parse(Some("b")), Some(Order::NewestFirst));
		assert_eq!(Order::parse(Some("forward")), None);
	}

	#[test]
	fn no_parameters_select_everything() {
		let all = Selection::parse(None, None, None).expect("nothing to refuse");
		assert_eq!(all, Selection::default());
		assert!(all.admits("m.room.message", 0));
		assert!(all.admits("m.room.member", u64::MAX));
	}

	// Two exports of adjoining months must not both hold the event sent on the stroke of midnight,
	// and neither may drop it.
	#[test]
	fn a_stretch_of_time_includes_its_start_and_not_its_end() {
		let month = Selection::parse(Some(1_000), Some(2_000), None).expect("a real stretch");
		assert!(!month.admits("m.room.message", 999));
		assert!(month.admits("m.room.message", 1_000));
		assert!(month.admits("m.room.message", 1_999));
		assert!(!month.admits("m.room.message", 2_000));

		let next = Selection::parse(Some(2_000), None, None).expect("an open stretch");
		assert!(next.admits("m.room.message", 2_000));
	}

	#[test]
	fn a_stretch_that_holds_nothing_is_refused() {
		assert!(Selection::parse(Some(2_000), Some(1_000), None).is_err());
		assert!(Selection::parse(Some(1_000), Some(1_000), None).is_err());
	}

	#[test]
	fn types_are_a_comma_separated_list() {
		let chosen = Selection::parse(None, None, Some("m.room.message, m.sticker,,"))
			.expect("nothing to refuse");
		assert_eq!(chosen.types, ["m.room.message", "m.sticker"]);
		assert!(chosen.admits("m.sticker", 0));
		assert!(!chosen.admits("m.room.member", 0));
		// A type is matched whole: a prefix is another type.
		assert!(!chosen.admits("m.room.message.feedback", 0));

		// An empty list is no list, not a list that nothing is in.
		let empty = Selection::parse(None, None, Some("")).expect("nothing to refuse");
		assert!(empty.admits("m.room.member", 0));
	}

	#[test]
	fn a_cursor_reads_back_as_it_was_written() {
		// History a bridge imported sits at negative counts, and a room of nothing else has only those.
		for count in [PduCount::Normal(42), PduCount::Backfilled(-42), PduCount::max()] {
			for (place, kind) in FILE_KINDS.into_iter().enumerate() {
				let token = format_cursor(kind, count);
				assert_eq!(parse_cursor(&token), Some((place, count)), "{token}");
			}
		}
	}

	#[test]
	fn a_cursor_that_was_not_written_here_is_refused() {
		assert_eq!(parse_cursor("42"), None, "a bare count, as the media index pages by");
		assert_eq!(parse_cursor("media:"), None);
		assert_eq!(parse_cursor("media:soon"), None);
		assert_eq!(parse_cursor("pictures:42"), None);
		// Indexed, but not files: the manifest never stops in them.
		assert_eq!(parse_cursor("links:42"), None);
		assert_eq!(parse_cursor("calls:42"), None);
	}

	#[test]
	fn a_picture_stored_here() {
		let content = json!({
			"msgtype": "m.image",
			"body": "A day out",
			"filename": "IMG_0001.jpg",
			"url": "mxc://example.org/abcdef",
			"info": {"mimetype": "image/jpeg", "size": 123_456, "w": 800, "h": 600},
		});
		let row = row_for(MediaKind::Media, &content).expect("an image is a file");

		assert_eq!(row.event_id, "$event");
		assert_eq!(row.sender, "@alice:example.org");
		assert_eq!(row.ts, 1_700_000_000_000);
		assert_eq!(row.kind, "media");
		assert_eq!(row.msgtype.as_deref(), Some("m.image"));
		// The caption is not the file's name.
		assert_eq!(row.filename.as_deref(), Some("IMG_0001.jpg"));
		assert_eq!(row.mimetype.as_deref(), Some("image/jpeg"));
		assert_eq!(row.size, Some(123_456));
		assert_eq!(row.url, "mxc://example.org/abcdef");
		assert!(!row.encrypted);
		assert!(row.stored);
	}

	// The open question of the export: 1.68 GB of one chat is not on the server at all. The manifest
	// answers it by saying so per file.
	#[test]
	fn a_file_a_bridge_serves_on_demand_is_not_stored() {
		let content = json!({
			"msgtype": "m.file",
			"body": "report.pdf",
			"url": "mxc://telegram.example.org/abcdef",
			"info": {"mimetype": "application/pdf", "size": 9_000_000},
		});
		let row = row_for(MediaKind::Files, &content).expect("a file is a file");

		assert!(!row.stored);
		// Without a `filename` the body is the name, as it was before captions.
		assert_eq!(row.filename.as_deref(), Some("report.pdf"));
		assert_eq!(row.kind, "files");
	}

	#[test]
	fn an_encrypted_attachment_keeps_its_url_under_file() {
		let content = json!({
			"msgtype": "m.audio",
			"body": "Voice message",
			"file": {"url": "mxc://example.org/cipher", "v": "v2"},
			"info": {"mimetype": "audio/ogg", "size": 4_096},
		});
		let row = row_for(MediaKind::Voice, &content).expect("an attachment is a file");

		assert_eq!(row.url, "mxc://example.org/cipher");
		assert!(row.encrypted);
		assert!(row.stored);
	}

	#[test]
	fn a_sticker_has_no_msgtype() {
		let content = json!({
			"body": "wave",
			"url": "mxc://example.org/sticker",
			"info": {"mimetype": "image/webp"},
		});
		let row = row_for(MediaKind::Media, &content).expect("a sticker is a file");

		assert_eq!(row.msgtype, None);
		assert_eq!(row.size, None);
		let sent = serde_json::to_value(&row).expect("a row is plain data");
		assert!(sent.get("msgtype").is_none(), "what is not known is left out: {sent}");
		assert!(sent.get("size").is_none(), "what is not known is left out: {sent}");
		assert_eq!(sent["stored"], true);
	}

	#[test]
	fn content_without_a_file_is_no_row() {
		// A redacted event, and a message that only looked like media.
		assert_eq!(row_for(MediaKind::Media, &json!({})), None);
		let text = json!({"msgtype": "m.image", "body": "no url"});
		assert_eq!(row_for(MediaKind::Media, &text), None);
	}

	// Bridged history is full of events from other software; one odd field must not cost the file.
	#[test]
	fn odd_fields_cost_only_themselves() {
		let content = json!({
			"msgtype": "m.video",
			"body": 7,
			"url": "mxc://example.org/video",
			"info": {"mimetype": ["video/mp4"], "size": "large"},
		});
		let row = row_for(MediaKind::Media, &content).expect("the url is all a row needs");

		assert_eq!(row.filename, None);
		assert_eq!(row.mimetype, None);
		assert_eq!(row.size, None);
		assert_eq!(row.url, "mxc://example.org/video");
	}

	// The same rule as `room_stats::bytes_of`. If that one changes, change this with it, or the
	// manifest and the statistics stop agreeing on how much of a chat is stored.
	#[test]
	fn a_file_is_stored_where_its_mxc_names_this_server() {
		assert!(mxc_is_local("mxc://example.org/abc", SERVER));
		assert!(!mxc_is_local("mxc://example.org.evil.test/abc", SERVER));
		assert!(!mxc_is_local("mxc://bridge.example.org/abc", SERVER));
		assert!(!mxc_is_local("https://example.org/abc", SERVER));
		assert!(!mxc_is_local("", SERVER));
	}
}
