//! The one rule for whether a name matches what somebody typed.
//!
//! Message search gets its matching from tantivy (see engine.rs). Searching *names* - the user
//! directory - cannot use an index the same way, because the candidates are streamed out of the
//! user table rather than indexed. What it must not do is diverge: a search that finds `Zürich` in
//! a message but not in a display name is a bug the reader will never be able to explain.
//!
//! So the rule lives here, once, and is the same rule the client applies (`utils/search/fuzzy.ts`):
//!
//! - **Fold first.** Case and accents are removed with the same tokenizer the index uses, so
//!   `zurich` finds `Zürich` and `cafe` finds `café`.
//! - **Every typed word must match, in any order.** `carter bob` finds `Bob Carter`.
//! - **A word matches at the start of a word.** Typing `man` finds `mega man`, not `tasmania`.
//!   This is the rule that keeps a short query from matching half the directory.
//! - **One typo per word, for words long enough to afford it.** Nothing for one or two characters,
//!   one edit beyond that - insert, delete, substitute or transpose - and never in the first
//!   character. People get the first letter right, and forgiving it turns a short query into a
//!   trawl: `deatrix` should not find `Beatrix`. The client has the same exception, so this is what
//!   keeps the two halves of a search agreeing rather than nearly agreeing.
//!
//! The typo tolerance is `levenshtein_automata`, which is the same crate, built the same way, that
//! tantivy's own `FuzzyTermQuery` uses: a DFA per query word, evaluated against each candidate
//! word. Prefix DFAs are what make "matches at the start of a word" and "within one edit" the same
//! question, answered once per word rather than per character.

use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA};
use tantivy::tokenizer::{
	AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, TokenStream,
};

/// Longest word worth considering, matching the index.
const WORD_MAX_LEN: usize = 50;

/// Below this many characters a word is matched exactly: forgiving a character of a two-letter
/// query matches most of the directory, which is worse than finding nothing.
const MIN_LEN_FOR_TYPO: usize = 3;

/// One typed search, compiled once and then asked about many candidates.
pub struct Matcher {
	/// Per typed word: the letter it must start with, and an automaton for the rest of the job -
	/// matching any word that starts within one edit of it.
	words: Vec<(char, DFA)>,
}

/// How good a match was. Bigger is better; `None` means it did not match at all.
pub type Score = u32;

impl Matcher {
	/// Compiles what was typed. Building the automata is the expensive part, so it happens once
	/// here rather than once per candidate.
	pub fn new(term: &str) -> Option<Self> {
		let exact = LevenshteinAutomatonBuilder::new(0, false);
		let forgiving = LevenshteinAutomatonBuilder::new(1, true);

		let words: Vec<_> = tokenize(term)
			.into_iter()
			.filter_map(|word| {
				let first = word.chars().next()?;
				let dfa = if word.chars().count() < MIN_LEN_FOR_TYPO {
					exact.build_prefix_dfa(&word)
				} else {
					forgiving.build_prefix_dfa(&word)
				};

				Some((first, dfa))
			})
			.collect();

		(!words.is_empty()).then_some(Self { words })
	}

	/// How well `candidate` answers the search, or `None` if it does not.
	///
	/// The score is deliberately coarse: what it has to get right is that a name matching exactly
	/// beats one matching through a typo, and that both beat a name that only matched a later word.
	/// Anything finer would be inventing a relevance model for a list of names.
	pub fn score(&self, candidate: &str) -> Option<Score> {
		let candidate_words = tokenize(candidate);
		if candidate_words.is_empty() {
			return None;
		}

		let mut total: Score = 0;
		for (first, dfa) in &self.words {
			let mut best: Option<Score> = None;
			for (at, word) in candidate_words.iter().enumerate() {
				// The first letter is not negotiable; see the note at the top.
				if word.chars().next() != Some(*first) {
					continue;
				}
				let hit = match dfa.eval(word) {
					| Distance::Exact(0) => 3,
					| Distance::Exact(_) => 2,
					| Distance::AtLeast(_) => continue,
				};

				// Matching the first word of a name is worth more than matching a later one, so
				// "Baker Alice" comes before "Alice Baker" for a search of "baker".
				let placed = if at == 0 { hit + 1 } else { hit };
				best = Some(best.map_or(placed, |previous: Score| previous.max(placed)));
			}

			// Every typed word has to land somewhere, or this is not the person.
			total = total.checked_add(best?)?;
		}

		Some(total)
	}
}

/// Splits and folds text the way the message index does, so both agree on what a word is.
fn tokenize(text: &str) -> Vec<String> {
	let analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
		.filter(RemoveLongFilter::limit(WORD_MAX_LEN))
		.filter(LowerCaser)
		.filter(AsciiFoldingFilter)
		.build();

	let mut analyzer = analyzer;
	let mut stream = analyzer.token_stream(text);
	let mut words = Vec::new();
	while stream.advance() {
		words.push(stream.token().text.clone());
	}

	words
}

#[cfg(test)]
mod tests {
	use super::Matcher;

	/// Whether `candidate` answers a search for `term`.
	fn finds(term: &str, candidate: &str) -> bool {
		Matcher::new(term).is_some_and(|matcher| matcher.score(candidate).is_some())
	}

	/*
	 * These are the same cases the client asserts (`apps/web/src/utils/search/fuzzy.test.ts`).
	 * Two implementations of one rule only stay one rule if both are held to the same table, and a
	 * search that behaves differently depending on which half answered is worse than one that is
	 * simply strict.
	 */

	#[test]
	fn finds_text_through_its_accents() {
		assert!(finds("zurich", "Zürich café"));
		assert!(finds("cafe", "Zürich café"));
		assert!(finds("naive", "naïve"));
	}

	#[test]
	fn takes_the_words_in_any_order() {
		assert!(finds("baker alice", "Alice Baker"));
		assert!(finds("bak al", "Alice Baker"));
	}

	#[test]
	fn forgives_one_typo_but_never_the_first_letter() {
		assert!(finds("beatirx", "Beatrix"), "transposed");
		assert!(finds("beatrux", "Beatrix"), "substituted");
		assert!(finds("betrix", "Beatrix"), "dropped");
		assert!(!finds("deatrix", "Beatrix"), "the first letter is not forgiven");
	}

	#[test]
	fn only_matches_where_a_word_starts() {
		assert!(finds("man", "mega man"));
		assert!(finds("man", "walk man"));
		assert!(!finds("man", "Tasmania"));
	}

	#[test]
	fn a_short_query_gets_no_typo_allowance() {
		// Forgiving a character of "an" would match most of a directory.
		assert!(finds("an", "Anna"));
		assert!(!finds("an", "Bob"));
	}

	#[test]
	fn every_typed_word_has_to_land() {
		assert!(!finds("alice carter", "Alice Baker"));
	}

	#[test]
	fn a_name_starting_with_the_query_scores_higher() {
		let matcher = Matcher::new("baker").expect("a term");
		let first = matcher.score("Baker Alice").expect("matches");
		let second = matcher.score("Alice Baker").expect("matches");
		assert!(first > second, "{first} should beat {second}");
	}

	#[test]
	fn nothing_typed_matches_nothing() {
		assert!(Matcher::new("   ").is_none());
	}
}
