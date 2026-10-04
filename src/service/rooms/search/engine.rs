//! The index behind message search.
//!
//! Searching text is a solved problem with a name - ranking, a term that survives a typo, an exact
//! count of what matched - and none of those parts should be written by hand. What was here before
//! was: a trigram table and an edit-distance check, used only to rescue a search that had already
//! returned nothing, with no ranking at all and no count short of walking every match.
//!
//! So this is tantivy: Lucene's algorithms, in Rust, embedded in the process with no separate
//! service to run. Three things it brings that we could not:
//!
//! - **A tokenizer that folds.** Case and accents are removed at index time and at query time
//!   alike, so `zurich` finds `Zürich` and `cafe` finds `café`. That is a property of the index, not
//!   a trick at the end, which is why it could never have been bolted on.
//! - **Fuzzy terms as automata.** `FuzzyTermQuery` intersects a Levenshtein automaton with the term
//!   dictionary, so "every word in this room within one edit of what was typed" is a walk over an
//!   FST rather than a scan plus a filter. Transpositions count as one edit, which is what people
//!   actually type. In prefix form it also gives search-as-you-type for free.
//! - **A count that is free.** The `Count` collector answers how many messages matched without
//!   materialising them, so a client can say "3 of 47" and step through them.
//!
//! The shape of this - a Tantivy index of message bodies, a `Count` collector beside a recency
//! ordered `TopDocs` - is Seshat's (matrix-org/seshat `src/index/mod.rs`), which is what Element
//! Desktop already searches with. One deliberate difference: Seshat folds case but not accents, and
//! we fold both, because the client folds both (`utils/search/fuzzy.ts`) and the two halves of a
//! search have to agree about what matches.
//!
//! The index lives beside the database rather than inside it, which also means rolling the server
//! back is still possible: nothing here adds a column family the old binary would not know.

use std::{
	ops::Bound,
	path::Path,
	sync::{
		Mutex,
		atomic::{AtomicBool, Ordering},
	},
};

use tantivy::{
	Index, IndexReader, IndexWriter, Order, ReloadPolicy, TantivyDocument, Term,
	collector::{Count, TopDocs},
	directory::MmapDirectory,
	doc,
	query::{BooleanQuery, FuzzyTermQuery, Occur, Query, RangeQuery, TermQuery, TermSetQuery},
	schema::{
		FAST, Field, INDEXED, IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions,
		Value,
	},
	tokenizer::{AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer},
};
use tuwunel_core::{Result, err};

use crate::rooms::short::ShortRoomId;

/// Our tokenizer: split on anything that is not a letter or digit, then fold case and accents.
const ANALYZER: &str = "folding";

/// Longest token worth keeping. Anything longer is a hash or a URL, never a search.
const WORD_MAX_LEN: usize = 50;

/// How much the writer may buffer before it has to flush to disk.
const WRITER_HEAP: usize = 50 * 1024 * 1024;

/// One writer thread: indexing is a trickle of messages, not a bulk load.
const WRITER_THREADS: usize = 1;

/// Shortest word that can afford to have a character forgiven; the name matcher uses the same.
const MIN_LEN_FOR_TYPO: usize = 3;

/// The columns of one indexed message.
#[derive(Clone, Copy)]
struct Fields {
	/// Which room, so a search stays inside the one asked for.
	room: Field,
	/// The message, tokenized and folded.
	body: Field,
	/// The packed PDU key, given back as the result.
	pdu: Field,
	/// Where the message sits in the server's timeline (`PduCount::into_signed`), so results can
	/// come back newest first. The count is one server-wide counter, so this orders messages of
	/// different rooms against each other as well: by when the server took them in.
	order: Field,
	/// When the message was sent (`origin_server_ts`, milliseconds), for a date range and for
	/// results by time. Imported history has its own old times but sits at the start of `order`
	/// in whatever order it was imported, so only this orders it with everything else.
	ts: Field,
}

/// Which messages a search may return by when they were sent, in milliseconds, both ends
/// included. `None` leaves that end open.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TimeRange {
	pub from: Option<i64>,
	pub to: Option<i64>,
}

/// What results are ordered by, newest first.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SortBy {
	/// Where the server put them in its timeline.
	#[default]
	Timeline,
	/// When they were sent.
	Sent,
}

pub(super) struct Engine {
	reader: IndexReader,
	writer: Mutex<IndexWriter>,
	fields: Fields,
	/// Whether anything was added or removed since the last commit. Tantivy does not keep track
	/// of this itself: a commit with nothing in it still rewrites and syncs `meta.json`, so
	/// committing on a timer cost a file write every tick of a server nobody was writing to. It
	/// is only changed with the writer locked, so a commit cannot clear it for a message it
	/// missed.
	dirty: AtomicBool,
}

/// What a search found: how many matched in total, and the page asked for, newest first.
pub(super) struct Found {
	pub(super) count: usize,
	pub(super) pdus: Vec<Vec<u8>>,
}

impl Engine {
	/// Opens the index at `path`, creating it if this is the first run.
	pub(super) fn open(path: &Path) -> Result<Self> {
		let mut schema = Schema::builder();
		let text = TextOptions::default().set_indexing_options(
			TextFieldIndexing::default()
				.set_tokenizer(ANALYZER)
				// Positions cost space but are what makes a phrase searchable later.
				.set_index_option(IndexRecordOption::WithFreqsAndPositions),
		);
		let fields = Fields {
			room: schema.add_u64_field("room", INDEXED),
			body: schema.add_text_field("body", text),
			pdu: schema.add_bytes_field("pdu", STORED | INDEXED),
			order: schema.add_i64_field("order", FAST),
			ts: schema.add_i64_field("ts", FAST),
		};

		std::fs::create_dir_all(path)
			.map_err(|e| err!(Database("Could not make the search index directory: {e}")))?;
		let directory = MmapDirectory::open(path)
			.map_err(|e| err!(Database("Could not open the search index: {e}")))?;
		let index = Index::open_or_create(directory, schema.build())
			.map_err(|e| err!(Database("Could not read the search index: {e}")))?;

		index.tokenizers().register(
			ANALYZER,
			TextAnalyzer::builder(SimpleTokenizer::default())
				.filter(RemoveLongFilter::limit(WORD_MAX_LEN))
				.filter(LowerCaser)
				.filter(AsciiFoldingFilter)
				.build(),
		);

		let reader = index
			.reader_builder()
			// The writer commits on a timer, and a reader that reloads itself means a message is
			// searchable as soon as it is committed without anything here coordinating the two.
			.reload_policy(ReloadPolicy::OnCommitWithDelay)
			.try_into()
			.map_err(|e| err!(Database("Could not read the search index: {e}")))?;

		let writer = index
			.writer_with_num_threads(WRITER_THREADS, WRITER_HEAP)
			.map_err(|e| err!(Database("Could not write to the search index: {e}")))?;

		Ok(Self {
			reader,
			writer: Mutex::new(writer),
			fields,
			dirty: AtomicBool::new(false),
		})
	}

	/// Adds one message. Cheap: it buffers, and {@link commit} makes it searchable.
	pub(super) fn add(
		&self,
		room: ShortRoomId,
		pdu: &[u8],
		order: i64,
		ts: i64,
		body: &str,
	) -> Result {
		let fields = self.fields;
		let writer = self
			.writer
			.lock()
			.map_err(|e| err!(Database("The search index writer is poisoned: {e}")))?;
		writer
			.add_document(doc!(
				fields.room => u64::from(room),
				fields.body => body,
				fields.pdu => pdu,
				fields.order => order,
				fields.ts => ts,
			))
			.map_err(|e| err!(Database("Could not index a message: {e}")))?;
		self.dirty.store(true, Ordering::Release);

		Ok(())
	}

	/// Forgets one message: redacted, purged, or its text replaced.
	pub(super) fn remove(&self, pdu: &[u8]) -> Result {
		let term = Term::from_field_bytes(self.fields.pdu, pdu);
		let writer = self
			.writer
			.lock()
			.map_err(|e| err!(Database("The search index writer is poisoned: {e}")))?;
		writer.delete_term(term);
		self.dirty.store(true, Ordering::Release);

		Ok(())
	}

	/// Whether nothing has been committed to the index yet.
	pub(super) fn is_empty(&self) -> bool { self.reader.searcher().num_docs() == 0 }

	/// Whether anything is waiting for a commit. Costs nothing, so the timer can ask every tick.
	pub(super) fn is_dirty(&self) -> bool { self.dirty.load(Ordering::Acquire) }

	/// Makes everything added or removed so far searchable, and says whether there was anything:
	/// with nothing waiting it touches no file.
	///
	/// This writes and syncs files, so it blocks: from async code it belongs on a blocking
	/// thread.
	pub(super) fn commit(&self) -> Result<bool> {
		let mut writer = self
			.writer
			.lock()
			.map_err(|e| err!(Database("The search index writer is poisoned: {e}")))?;

		if !self.dirty.swap(false, Ordering::AcqRel) {
			return Ok(false);
		}

		if let Err(e) = writer.commit() {
			// Still waiting, so the next tick tries again rather than forgetting it.
			self.dirty.store(true, Ordering::Release);
			return Err(err!(Database("Could not commit the search index: {e}")));
		}

		Ok(true)
	}

	/// Forgets every message of one room, for a room being deleted.
	pub(super) fn remove_room(&self, room: ShortRoomId) -> Result {
		let query = TermQuery::new(
			Term::from_field_u64(self.fields.room, u64::from(room)),
			IndexRecordOption::Basic,
		);
		let writer = self
			.writer
			.lock()
			.map_err(|e| err!(Database("The search index writer is poisoned: {e}")))?;
		writer
			.delete_query(Box::new(query))
			.map_err(|e| err!(Database("Could not forget a room's messages: {e}")))?;
		self.dirty.store(true, Ordering::Release);

		Ok(())
	}

	/// Throws away everything indexed, for a rebuild from the timeline.
	pub(super) fn clear(&self) -> Result {
		let mut writer = self
			.writer
			.lock()
			.map_err(|e| err!(Database("The search index writer is poisoned: {e}")))?;
		writer
			.delete_all_documents()
			.map_err(|e| err!(Database("Could not clear the search index: {e}")))?;
		writer
			.commit()
			.map_err(|e| err!(Database("Could not commit the search index: {e}")))?;
		// That commit took everything that was waiting with it.
		self.dirty.store(false, Ordering::Release);

		Ok(())
	}

	/// The messages of any of `rooms` matching `term`, newest first across all of them, and how
	/// many there are in total.
	///
	/// `skip` and `want` are the page; `count` is every match, which is what lets a client say which
	/// of how many it is showing. Ordering is recency rather than relevance, because searching inside
	/// one conversation is a way of moving around it - the same reason other chat apps give you
	/// arrows rather than a ranked list.
	///
	/// Several rooms are one query over the whole set rather than one per room: the page, the offset
	/// and the count are then the search's, not each room's, and a search of every room a user is in
	/// costs one walk of the index instead of one per room. The forgiving second pass is decided for
	/// the set as a whole, as it is for one room.
	///
	/// `range` keeps only messages sent within it; `sort` says whether newest means where the
	/// server put a message or when it was sent.
	pub(super) fn search(
		&self,
		rooms: &[ShortRoomId],
		term: &str,
		range: TimeRange,
		sort: SortBy,
		skip: usize,
		want: usize,
	) -> Result<Found> {
		let searcher = self.reader.searcher();
		let Some(query) = self.query(rooms, term, range, false) else {
			return Ok(Found { count: 0, pdus: Vec::new() });
		};

		let mut count = searcher
			.search(&query, &Count)
			.map_err(|e| err!(Database("Could not count what a search matched: {e}")))?;

		/*
		 * Nothing matched as typed, so ask again forgiving a typo.
		 *
		 * Second rather than always, because these results are ordered by recency and not by how
		 * well they match, so a forgiving search has no way to put its worse answers last. Asking
		 * exactly first means a search that was spelled right is never diluted, and one that was
		 * not still finds something rather than nothing.
		 */
		let query = if count > 0 {
			query
		} else {
			let Some(forgiving) = self.query(rooms, term, range, true) else {
				return Ok(Found { count: 0, pdus: Vec::new() });
			};
			count = searcher
				.search(&forgiving, &Count)
				.map_err(|e| err!(Database("Could not count what a search matched: {e}")))?;
			forgiving
		};

		let by = match sort {
			| SortBy::Timeline => "order",
			| SortBy::Sent => "ts",
		};
		let top = TopDocs::with_limit(want)
			.and_offset(skip)
			.order_by_fast_field::<i64>(by, Order::Desc);

		let hits = searcher
			.search(&query, &top)
			.map_err(|e| err!(Database("Could not read what a search matched: {e}")))?;

		let mut pdus = Vec::with_capacity(hits.len());
		for (_, address) in hits {
			let Ok(doc) = searcher.doc::<TantivyDocument>(address) else {
				continue;
			};
			if let Some(pdu) = doc
				.get_first(self.fields.pdu)
				.and_then(|value| value.as_bytes())
			{
				pdus.push(pdu.to_vec());
			}
		}

		Ok(Found { count, pdus })
	}

	/// The query for one search term inside `rooms`: every word must match, as a prefix, optionally
	/// forgiving one typo. No rooms is no query.
	///
	/// A word always matches as a prefix, so searching as the user types works ("phot" finding
	/// "photos"). Tolerance is the caller's choice, and being conservative about it matters here:
	/// results inside a room come back newest first, because that is how you move around a
	/// conversation, which means a query cannot lean on ranking to push a poor match down - a recent
	/// noisy match lands at the top. Precision has to come from the query.
	///
	/// Measured on real history before this was fixed: allowing two edits made `thnaks` match
	/// `thats`, `things` and even `the`, and `thanks` claim 16594 hits. One edit, and that noise is
	/// gone. Telegram draws the same line - its in-chat search is exact and chronological, and the
	/// forgiving search is the global one.
	fn query(
		&self,
		rooms: &[ShortRoomId],
		term: &str,
		range: TimeRange,
		forgiving: bool,
	) -> Option<Box<dyn Query>> {
		let room_term =
			|room: &ShortRoomId| Term::from_field_u64(self.fields.room, u64::from(*room));
		let in_rooms: Box<dyn Query> = match rooms {
			| [] => return None,
			| [room] => Box::new(TermQuery::new(room_term(room), IndexRecordOption::Basic)),
			| rooms => Box::new(TermSetQuery::new(rooms.iter().map(room_term))),
		};
		let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Must, in_rooms)];

		if range != TimeRange::default() {
			let bound = |ts: Option<i64>| {
				ts.map_or(Bound::Unbounded, |ts| {
					Bound::Included(Term::from_field_i64(self.fields.ts, ts))
				})
			};
			clauses.push((
				Occur::Must,
				Box::new(RangeQuery::new(bound(range.from), bound(range.to))),
			));
		}

		let mut words: usize = 0;
		for word in tokenize(term) {
			let term = Term::from_field_text(self.fields.body, &word);
			// One edit at most, and none for a word too short to spare one: forgiving a character
			// of `an` matches most of a conversation. Transpositions count as one edit, because
			// that is what people actually type.
			let distance = u8::from(forgiving && word.chars().count() >= MIN_LEN_FOR_TYPO);
			clauses.push((Occur::Must, Box::new(FuzzyTermQuery::new_prefix(term, distance, true))));
			words = words.saturating_add(1);
		}

		(words > 0).then(|| -> Box<dyn Query> { Box::new(BooleanQuery::new(clauses)) })
	}
}

/// Splits a search term the same way the index splits a message.
///
/// The tokenizer does this at index time; a query has to be cut the same way or the words would not
/// line up with what was stored. Folding is left to tantivy - it applies the same filters to a term
/// query - so this only has to agree about where the boundaries are.
fn tokenize(body: &str) -> impl Iterator<Item = String> + Send + '_ {
	body.split_terminator(|c: char| !c.is_alphanumeric())
		.filter(|word| !word.is_empty() && word.len() <= WORD_MAX_LEN)
		.map(str::to_lowercase)
}

#[cfg(test)]
mod tests {
	use std::{env::temp_dir, fs::remove_dir_all, path::PathBuf};

	use tuwunel_core::{Result, utils::random_string};

	use super::{Engine, Found, SortBy, TimeRange};

	/// An index in a directory of its own, removed when the test is done with it.
	struct Scratch {
		engine: Engine,
		path: PathBuf,
	}

	impl Drop for Scratch {
		fn drop(&mut self) { remove_dir_all(&self.path).ok(); }
	}

	/// Indexes `(room, order, body)` and makes it searchable straight away. The order doubles as
	/// the stored key, so a result says which message it was, and as the time it was sent.
	fn index(messages: &[(u64, i64, &str)]) -> Result<Scratch> {
		let timed: Vec<_> = messages
			.iter()
			.map(|&(room, order, body)| (room, order, order, body))
			.collect();

		index_timed(&timed)
	}

	/// Indexes `(room, order, ts, body)` and makes it searchable straight away.
	fn index_timed(messages: &[(u64, i64, i64, &str)]) -> Result<Scratch> {
		let path = temp_dir()
			.join("tuwunel-search-engine")
			.join(random_string(16));
		let engine = Engine::open(&path)?;
		for &(room, order, ts, body) in messages {
			engine.add(room, &order.to_be_bytes(), order, ts, body)?;
		}
		engine.commit()?;
		engine
			.reader
			.reload()
			.map_err(|e| tuwunel_core::err!("{e}"))?;

		Ok(Scratch { engine, path })
	}

	/// The orders of what a search found, in the order it gave them.
	fn orders(found: &Found) -> Vec<i64> {
		found
			.pdus
			.iter()
			.map(|pdu| i64::from_be_bytes(pdu.as_slice().try_into().expect("an order")))
			.collect()
	}

	/// Two busy rooms whose messages alternate, and a third the search is not asked about. Room 1
	/// ends with a backfilled message, which the server counts below zero.
	const ROOMS: &[(u64, i64, &str)] = &[
		(1, -3, "hello from history"),
		(1, 1, "hello one"),
		(2, 2, "hello two"),
		(1, 3, "hello three"),
		(2, 4, "hello four"),
		(3, 5, "hello elsewhere"),
		(1, 6, "hello six"),
		(2, 7, "hello seven"),
		(2, 8, "goodbye"),
	];

	// The bug was a search of all rooms returning `limit` results per room, grouped by room: with
	// a couple of hundred rooms a page of 10 was thousands of results. The page is the search's.
	#[test]
	fn a_page_of_several_rooms_is_one_page_newest_first_across_them() -> Result {
		let scratch = index(ROOMS)?;

		let found = scratch.engine.search(
			&[1, 2],
			"hello",
			TimeRange::default(),
			SortBy::Timeline,
			0,
			3,
		)?;
		assert_eq!(orders(&found), [7, 6, 4], "not the newest three across both rooms");
		assert_eq!(found.count, 7, "the count is not every match in both rooms");

		Ok(())
	}

	// The offset is into the whole result, so stepping through it page by page sees every match
	// once, in one order, and nothing from a room that was not asked about.
	#[test]
	fn paging_several_rooms_walks_one_list_without_repeats() -> Result {
		let scratch = index(ROOMS)?;

		let mut seen = Vec::new();
		for skip in (0..10).step_by(3) {
			seen.extend(orders(&scratch.engine.search(
				&[1, 2],
				"hello",
				TimeRange::default(),
				SortBy::Timeline,
				skip,
				3,
			)?));
		}
		assert_eq!(seen, [7, 6, 4, 3, 2, 1, -3]);

		Ok(())
	}

	// One room stays what it was: only its own messages, newest first.
	#[test]
	fn one_room_finds_only_its_own_messages() -> Result {
		let scratch = index(ROOMS)?;

		let found = scratch.engine.search(
			&[2],
			"hello",
			TimeRange::default(),
			SortBy::Timeline,
			0,
			10,
		)?;
		assert_eq!(orders(&found), [7, 4, 2]);
		assert_eq!(found.count, 3);

		let found =
			scratch
				.engine
				.search(&[], "hello", TimeRange::default(), SortBy::Timeline, 0, 10)?;
		assert_eq!((found.count, found.pdus.len()), (0, 0), "no rooms found something");

		Ok(())
	}

	// A typo is forgiven only when nothing matched as typed, and that is decided for the whole set:
	// an exact match in one room keeps the near misses of another out of the page.
	#[test]
	fn a_typo_is_forgiven_only_when_no_room_matched_as_typed() -> Result {
		let scratch = index(&[(1, 1, "hallo there"), (2, 2, "hello there"), (2, 3, "help")])?;

		let found = scratch.engine.search(
			&[1, 2],
			"hello",
			TimeRange::default(),
			SortBy::Timeline,
			0,
			10,
		)?;
		assert_eq!(orders(&found), [2], "an exact match was diluted by a forgiven one");

		let found = scratch.engine.search(
			&[1, 2],
			"helo",
			TimeRange::default(),
			SortBy::Timeline,
			0,
			10,
		)?;
		assert_eq!(orders(&found), [3, 2], "nothing exact, yet one edit was not forgiven");

		Ok(())
	}

	/// Imported history: the server took these in in this order, but they were sent long before,
	/// and in another order. Room 1's import came last and is the oldest.
	const IMPORTED: &[(u64, i64, i64, &str)] = &[
		(2, 1, 5_000, "meet at noon"),
		(2, 2, 6_000, "meet at one"),
		(1, 3, 1_000, "meet at nine"),
		(1, 4, 2_000, "meet at ten"),
		(2, 5, 7_000, "meet tomorrow"),
	];

	// A date range keeps what was sent inside it, both ends included, from every room asked
	// about, and the count is of those alone.
	#[test]
	fn a_date_range_keeps_what_was_sent_inside_it() -> Result {
		let scratch = index_timed(IMPORTED)?;
		let range = TimeRange { from: Some(2_000), to: Some(6_000) };

		let found = scratch
			.engine
			.search(&[1, 2], "meet", range, SortBy::Timeline, 0, 10)?;
		assert_eq!(orders(&found), [4, 2, 1]);
		assert_eq!(found.count, 3);

		let open_start = TimeRange { from: None, to: Some(1_999) };
		let found =
			scratch
				.engine
				.search(&[1, 2], "meet", open_start, SortBy::Timeline, 0, 10)?;
		assert_eq!(orders(&found), [3]);

		let open_end = TimeRange { from: Some(6_001), to: None };
		let found = scratch
			.engine
			.search(&[1, 2], "meet", open_end, SortBy::Timeline, 0, 10)?;
		assert_eq!(orders(&found), [5]);

		Ok(())
	}

	// By time, the imported room's messages go where they were sent, not where they were taken in.
	#[test]
	fn results_by_time_are_newest_sent_first_across_rooms() -> Result {
		let scratch = index_timed(IMPORTED)?;

		let found =
			scratch
				.engine
				.search(&[1, 2], "meet", TimeRange::default(), SortBy::Sent, 0, 10)?;
		assert_eq!(orders(&found), [5, 2, 1, 4, 3]);

		let found = scratch.engine.search(
			&[1, 2],
			"meet",
			TimeRange::default(),
			SortBy::Timeline,
			0,
			10,
		)?;
		assert_eq!(orders(&found), [5, 4, 3, 2, 1], "the default order changed");

		Ok(())
	}
}
