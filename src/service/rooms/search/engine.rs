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

use std::{path::Path, sync::Mutex};

use tantivy::{
	Index, IndexReader, IndexWriter, Order, ReloadPolicy, TantivyDocument, Term,
	collector::{Count, TopDocs},
	directory::MmapDirectory,
	doc,
	query::{BooleanQuery, FuzzyTermQuery, Occur, Query, TermQuery},
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
	/// The room's own ordering (`PduCount::into_signed`), so results can come back newest first.
	order: Field,
}

pub(super) struct Engine {
	reader: IndexReader,
	writer: Mutex<IndexWriter>,
	fields: Fields,
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

		Ok(Self { reader, writer: Mutex::new(writer), fields })
	}

	/// Adds one message. Cheap: it buffers, and {@link commit} makes it searchable.
	pub(super) fn add(&self, room: ShortRoomId, pdu: &[u8], order: i64, body: &str) -> Result {
		let fields = self.fields;
		self.writer
			.lock()
			.map_err(|e| err!(Database("The search index writer is poisoned: {e}")))?
			.add_document(doc!(
				fields.room => u64::from(room),
				fields.body => body,
				fields.pdu => pdu,
				fields.order => order,
			))
			.map_err(|e| err!(Database("Could not index a message: {e}")))?;

		Ok(())
	}

	/// Forgets one message: redacted, purged, or its text replaced.
	pub(super) fn remove(&self, pdu: &[u8]) -> Result {
		let term = Term::from_field_bytes(self.fields.pdu, pdu);
		self.writer
			.lock()
			.map_err(|e| err!(Database("The search index writer is poisoned: {e}")))?
			.delete_term(term);

		Ok(())
	}

	/// Makes everything added so far searchable.
	pub(super) fn commit(&self) -> Result {
		self.writer
			.lock()
			.map_err(|e| err!(Database("The search index writer is poisoned: {e}")))?
			.commit()
			.map_err(|e| err!(Database("Could not commit the search index: {e}")))?;

		Ok(())
	}

	/// Forgets every message of one room, for a room being deleted.
	pub(super) fn remove_room(&self, room: ShortRoomId) -> Result {
		let query = TermQuery::new(
			Term::from_field_u64(self.fields.room, u64::from(room)),
			IndexRecordOption::Basic,
		);
		self.writer
			.lock()
			.map_err(|e| err!(Database("The search index writer is poisoned: {e}")))?
			.delete_query(Box::new(query))
			.map_err(|e| err!(Database("Could not forget a room's messages: {e}")))?;

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

		Ok(())
	}

	/// The messages of one room matching `term`, newest first, and how many there are in total.
	///
	/// `skip` and `want` are the page; `count` is every match, which is what lets a client say which
	/// of how many it is showing. Ordering is the room's own order rather than relevance, because
	/// searching inside one conversation is a way of moving around it - the same reason other chat
	/// apps give you arrows rather than a ranked list.
	pub(super) fn search(
		&self,
		room: ShortRoomId,
		term: &str,
		skip: usize,
		want: usize,
	) -> Result<Found> {
		let searcher = self.reader.searcher();
		let Some(query) = self.query(room, term, false) else {
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
			let Some(forgiving) = self.query(room, term, true) else {
				return Ok(Found { count: 0, pdus: Vec::new() });
			};
			count = searcher
				.search(&forgiving, &Count)
				.map_err(|e| err!(Database("Could not count what a search matched: {e}")))?;
			forgiving
		};

		let top = TopDocs::with_limit(want)
			.and_offset(skip)
			.order_by_fast_field::<i64>("order", Order::Desc);

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

	/// The query for one search term: every word must match, as a prefix, optionally forgiving one
	/// typo.
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
	fn query(&self, room: ShortRoomId, term: &str, forgiving: bool) -> Option<Box<dyn Query>> {
		let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(
			Occur::Must,
			Box::new(TermQuery::new(
				Term::from_field_u64(self.fields.room, u64::from(room)),
				IndexRecordOption::Basic,
			)),
		)];

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
