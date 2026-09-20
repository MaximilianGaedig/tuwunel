use std::{collections::BTreeMap, pin::pin, sync::Arc};

use futures::{Stream, StreamExt};
use ruma::{RoomId, UserId, api::client::search::search_events::v3::Criteria};
use tuwunel_core::{
	Result,
	arrayvec::ArrayVec,
	implement,
	matrix::event::{Event, Matches},
	trace,
	utils::{
		ArrayVecExt, IterStream, ReadyExt, set,
		stream::{TryIgnore, WidebandExt},
	},
};
use tuwunel_database::{Map, Txn, keyval::Val};

use crate::rooms::{short::ShortRoomId, timeline::RawPduId};

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
}

struct Data {
	tokenids: Arc<Map>,
	/// `shortroomid | trigram | SEP | word`: the words a room has used, by their three-letter
	/// pieces, so a misspelt search term can be matched against them.
	wordgrams: Arc<Map>,
}

#[derive(Clone, Debug)]
pub struct RoomQuery<'a> {
	pub room_id: &'a RoomId,
	pub user_id: Option<&'a UserId>,
	pub criteria: &'a Criteria,
	pub limit: usize,
	pub skip: usize,
}

type TokenId = ArrayVec<u8, TOKEN_ID_MAX_LEN>;

const TOKEN_ID_MAX_LEN: usize =
	size_of::<ShortRoomId>() + WORD_MAX_LEN + 1 + size_of::<RawPduId>();
const WORD_MAX_LEN: usize = 50;

/// Fuzzy matching (see {@link Service::fuzzy_candidates}).
const GRAM_LEN: usize = 3;
const MIN_FUZZY_LEN: usize = GRAM_LEN;
const SHORT_WORD_LEN: usize = 5;
const MIN_SHARED_GRAMS: usize = 2;
const MAX_FUZZY_CANDIDATES: usize = 8;

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				tokenids: args.db["tokenids"].clone(),
				wordgrams: args.db["roomwordgrams"].clone(),
			},
			services: args.services.clone(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub fn index_pdu(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId, message_body: &str) {
	self.index_words(shortroomid, message_body);

	let items = tokenize(message_body).map(|word| {
		let mut key = shortroomid.to_be_bytes().to_vec();
		key.extend_from_slice(word.as_bytes());
		key.push(0xFF);
		key.extend_from_slice(pdu_id.as_ref()); // TODO: currently we save the room id a second time here

		(key, [])
	});

	Txn::insert(&self.db.tokenids, items).execute();
}

#[implement(Service)]
pub fn deindex_pdu(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId, message_body: &str) {
	let batch = tokenize(message_body).map(|word| {
		let mut key = shortroomid.to_be_bytes().to_vec();
		key.extend_from_slice(word.as_bytes());
		key.push(0xFF);
		key.extend_from_slice(pdu_id.as_ref()); // TODO: currently we save the room id a second time here
		key
	});

	for token in batch {
		self.db.tokenids.remove(&token);
	}
}

/// Records a message's words by their three-letter pieces, so that a search term with a typo in it
/// can still find them (see {@link fuzzy_candidates}). Words shorter than a trigram are skipped:
/// those are already reachable by prefix.
#[implement(Service)]
fn index_words(&self, shortroomid: ShortRoomId, message_body: &str) {
	let items = tokenize(message_body)
		.filter(|word| word.len() >= MIN_FUZZY_LEN)
		.flat_map(move |word| {
			trigrams(&word)
				.map(|gram| (make_gram_key(shortroomid, &gram, &word), []))
				.collect::<Vec<_>>()
		});

	Txn::insert(&self.db.wordgrams, items).execute();
}

/// Words of this room close enough to `word` to be what the user meant: they share at least two
/// three-letter pieces with it and are within an edit or two. Used only when nothing matched the
/// term as typed, so an ordinary search never pays for it.
#[implement(Service)]
async fn fuzzy_candidates(&self, shortroomid: ShortRoomId, word: &str) -> Vec<String> {
	if word.len() < MIN_FUZZY_LEN {
		return Vec::new();
	}

	let mut shared: BTreeMap<String, usize> = BTreeMap::new();
	for gram in trigrams(word) {
		let prefix = make_gram_prefix(shortroomid, &gram);
		let prefix_len = prefix.len();
		let matches = prefix.clone();
		let mut keys = pin!(
			self.db
				.wordgrams
				.raw_keys_from(&prefix)
				.ignore_err()
				.ready_take_while(move |key| key.starts_with(&matches))
		);

		while let Some(key) = keys.next().await {
			let Some(candidate) = key
				.get(prefix_len..)
				.and_then(|bytes| std::str::from_utf8(bytes).ok())
			else {
				continue;
			};

			*shared.entry(candidate.to_owned()).or_default() += 1;
		}
	}

	let max_distance = if word.len() <= SHORT_WORD_LEN { 1 } else { 2 };
	let mut candidates: Vec<_> = shared
		.into_iter()
		.filter(|(candidate, grams)| *grams >= MIN_SHARED_GRAMS && candidate.as_str() != word)
		.filter(|(candidate, _)| within_distance(word, candidate, max_distance))
		.collect();

	// Closest first, and only a few: each one costs an index scan below.
	candidates.sort_by_key(|(candidate, grams)| (usize::MAX - grams, candidate.len()));
	candidates
		.into_iter()
		.take(MAX_FUZZY_CANDIDATES)
		.map(|(candidate, _)| candidate)
		.collect()
}

#[implement(Service)]
pub async fn search_pdus<'a>(
	&'a self,
	query: &'a RoomQuery<'a>,
) -> Result<(usize, impl Stream<Item = impl Event + use<>> + Send + '_)> {
	let pdu_ids: Vec<_> = self.search_pdu_ids(query).await?.collect().await;

	let filter = &query.criteria.filter;
	let count = pdu_ids.len();
	let pdus = pdu_ids
		.into_iter()
		.stream()
		.wide_filter_map(async |result_pdu_id: RawPduId| {
			self.services
				.timeline
				.get_pdu_from_id(&result_pdu_id)
				.await
				.ok()
		})
		.ready_filter(|pdu| !pdu.is_redacted())
		.ready_filter(move |pdu| filter.matches(pdu))
		.wide_filter_map(async |pdu| {
			self.services
				.state_accessor
				.user_can_see_event(query.user_id?, pdu.room_id(), pdu.event_id())
				.await
				.then_some(pdu)
		})
		.skip(query.skip)
		.take(query.limit);

	Ok((count, pdus))
}

// result is modeled as a stream such that callers don't have to be refactored
// though an additional async/wrap still exists for now
#[implement(Service)]
pub async fn search_pdu_ids(
	&self,
	query: &RoomQuery<'_>,
) -> Result<impl Stream<Item = RawPduId> + Send + '_ + use<'_>> {
	let shortroomid = self
		.services
		.short
		.get_shortroomid(query.room_id)
		.await?;

	let pdu_ids = self
		.search_pdu_ids_query_room(query, shortroomid)
		.await;

	let iters = pdu_ids.into_iter().map(IntoIterator::into_iter);

	Ok(set::intersection(iters).stream())
}

#[implement(Service)]
async fn search_pdu_ids_query_room(
	&self,
	query: &RoomQuery<'_>,
	shortroomid: ShortRoomId,
) -> Vec<Vec<RawPduId>> {
	tokenize(&query.criteria.search_term)
		.stream()
		.wide_then(async |word| {
			let mut ids: Vec<_> = self
				.search_pdu_ids_query_words(shortroomid, &word)
				.collect()
				.await;

			// Nothing matched what was typed: try the room's words that are a typo away.
			if ids.is_empty() {
				for candidate in self.fuzzy_candidates(shortroomid, &word).await {
					ids.extend(
						self.search_pdu_ids_query_words(shortroomid, &candidate)
							.collect::<Vec<_>>()
							.await,
					);
				}
			}

			// Prefix and fuzzy matches arrive grouped by the word that matched; the terms are
			// intersected below, which needs one order: newest first.
			ids.sort_unstable_by(|a: &RawPduId, b: &RawPduId| b.as_ref().cmp(a.as_ref()));
			ids.dedup();
			ids
		})
		.collect::<Vec<_>>()
		.await
}

/// Iterate over PduId's whose message has a word starting with `word`.
///
/// Matching a prefix rather than the whole word is what lets a client search as
/// the user types ("phot" finding "photos"), the way other chat apps do. The
/// index key is `shortroomid | word | SEP | pduid`, so every word starting with
/// the term shares the prefix `shortroomid | word` and the id is the key's last
/// bytes, whichever word matched.
#[implement(Service)]
fn search_pdu_ids_query_words<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	word: &'a str,
) -> impl Stream<Item = RawPduId> + Send + '_ {
	self.search_pdu_ids_query_word(shortroomid, word)
		.ready_filter_map(|key| {
			// The id is whatever follows the word's separator; ids come in two lengths, so it
			// cannot be taken as a fixed number of trailing bytes.
			let room_len = size_of::<ShortRoomId>();
			let sep = key
				.get(room_len..)?
				.iter()
				.position(|byte| *byte == tuwunel_database::SEP)?;

			Some(RawPduId::from(key.get(room_len.saturating_add(sep).saturating_add(1)..)?))
		})
}

/// Iterate over raw database results for words starting with `word`
#[implement(Service)]
fn search_pdu_ids_query_word(
	&self,
	shortroomid: ShortRoomId,
	word: &str,
) -> impl Stream<Item = Val<'_>> + Send + '_ + use<'_> {
	// The prefix without the separator, so longer words starting with it match too.
	let prefix = make_word_prefix(shortroomid, word);

	// Newest pdus first, so the scan starts just past this prefix's last possible key: the
	// separator and id that follow the word are all below 0xFF repeated. Seeking to the bare
	// prefix instead would start *before* every key that has it and find nothing.
	let mut end = prefix.clone();
	end.extend_from_slice(&[u8::MAX; size_of::<RawPduId>() + 2]);

	self.db
		.tokenids
		.rev_raw_keys_from(&end)
		.ignore_err()
		.ready_take_while(move |key| key.starts_with(&prefix))
}

#[implement(Service)]
pub async fn delete_all_search_tokenids_for_room(&self, room_id: &RoomId) -> Result {
	let Ok(shortroomid) = self.services.short.get_shortroomid(room_id).await else {
		return Ok(());
	};

	let txn = self
		.db
		.tokenids
		.keys_prefix_raw(&shortroomid)
		.ignore_err()
		.ready_fold(self.services.db.txn(), |mut txn, key| {
			trace!("Removing key: {key:?}");
			txn.del_raw(&self.db.tokenids, key);
			txn
		})
		.await;

	txn.execute();

	Ok(())
}

/// Splits a string into tokens used as keys in the search inverted index
///
/// This may be used to tokenize both message bodies (for indexing) or search
/// queries (for querying).
fn tokenize(body: &str) -> impl Iterator<Item = String> + Send + '_ {
	body.split_terminator(|c: char| !c.is_alphanumeric())
		.filter(|s| !s.is_empty())
		.filter(|word| word.len() <= WORD_MAX_LEN)
		.map(str::to_lowercase)
}


fn make_prefix(shortroomid: ShortRoomId, word: &str) -> TokenId {
	let mut key = make_word_prefix(shortroomid, word);
	key.push(tuwunel_database::SEP);
	key
}

fn make_gram_key(shortroomid: ShortRoomId, gram: &str, word: &str) -> Vec<u8> {
	let mut key = make_gram_prefix(shortroomid, gram);
	key.extend_from_slice(word.as_bytes());
	key
}

fn make_gram_prefix(shortroomid: ShortRoomId, gram: &str) -> Vec<u8> {
	let mut key = Vec::with_capacity(size_of::<ShortRoomId>() + GRAM_LEN + 1);
	key.extend_from_slice(&shortroomid.to_be_bytes());
	key.extend_from_slice(gram.as_bytes());
	key.push(tuwunel_database::SEP);
	key
}

/// A word's overlapping three-letter pieces ("photo" -> pho, hot, oto).
fn trigrams(word: &str) -> impl Iterator<Item = String> + '_ {
	let chars: Vec<char> = word.chars().collect();
	(0..chars.len().saturating_sub(GRAM_LEN.saturating_sub(1)))
		.map(move |i| chars[i..i.saturating_add(GRAM_LEN)].iter().collect())
}

/// Whether two words are within `max` edits of each other (insert, delete or replace).
fn within_distance(a: &str, b: &str, max: usize) -> bool {
	let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
	if a.len().abs_diff(b.len()) > max {
		return false;
	}

	let mut previous: Vec<usize> = (0..=b.len()).collect();
	let mut current = vec![0_usize; b.len().saturating_add(1)];
	for (i, ca) in a.iter().enumerate() {
		current[0] = i.saturating_add(1);
		for (j, cb) in b.iter().enumerate() {
			let cost = usize::from(ca != cb);
			current[j.saturating_add(1)] = previous[j]
				.saturating_add(cost)
				.min(previous[j.saturating_add(1)].saturating_add(1))
				.min(current[j].saturating_add(1));
		}
		if current.iter().min().is_some_and(|best| *best > max) {
			return false; // every way through this row is already too far
		}
		std::mem::swap(&mut previous, &mut current);
	}

	previous[b.len()] <= max
}

/// The key prefix every word starting with `word` shares.
fn make_word_prefix(shortroomid: ShortRoomId, word: &str) -> TokenId {
	let mut key = TokenId::new();
	key.extend_from_slice(&shortroomid.to_be_bytes());
	key.extend_from_slice(word.as_bytes());
	key
}

