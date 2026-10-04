mod engine;
pub mod matcher;

use std::{pin::pin, sync::Arc};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use ruma::{
	MilliSecondsSinceUnixEpoch, RoomId, UserId,
	api::client::search::search_events::v3::Criteria,
	events::{TimelineEventType, room::topic::RoomTopicEventContent},
};
use tokio::time::{Duration, sleep};
use tuwunel_core::{
	Result, Server, error, implement, info,
	matrix::{
		event::{Event, Matches},
		pdu::PduId,
	},
	utils::{
		IterStream, ReadyExt,
		stream::{TryIgnore, WidebandExt},
	},
};
use tuwunel_database::{Deserialized, Map};

use self::engine::Engine;
pub use self::engine::{SortBy, TimeRange};
use crate::rooms::{short::ShortRoomId, state_accessor::plain_text_topic, timeline::RawPduId};

pub struct Service {
	engine: Engine,
	global: Arc<Map>,
	server: Arc<Server>,
	services: Arc<crate::services::OnceServices>,
}

/// A search of one or more rooms, paged across all of them.
#[derive(Clone, Debug)]
pub struct RoomQuery<'a> {
	pub room_ids: &'a [&'a RoomId],
	pub user_id: Option<&'a UserId>,
	pub criteria: &'a Criteria,
	/// Only messages sent within it.
	pub range: TimeRange,
	/// Newest first by where the server put them, or by when they were sent.
	pub sort: SortBy,
	pub limit: usize,
	pub skip: usize,
}

/// How long a message may wait before it is committed, which is what makes it searchable.
///
/// Committing per message would cost a file write per message, so the worker commits on a timer,
/// which is what Elasticsearch calls a refresh interval and for the same reason. Elasticsearch's
/// default is this same second. The reader notices a commit within another half second (tantivy
/// polls for it), so a message is found at most a second and a half after it was sent, plus the
/// commit itself; half the interval would only halve the first part, and would write twice as
/// many segments while a bridge is filling in history.
///
/// A tick with nothing to commit does nothing at all: see [`Engine::commit`].
const COMMIT_EVERY: Duration = Duration::from_secs(1);

/// Which rules the index was built by. Raise it whenever what is indexed, or how it is cut into
/// words, changes (see [`searchable_text`] and the tokenizer in `engine`): history indexed by the
/// old rules is then indexed again on the next start, without anyone having to ask.
///
/// 1. message bodies and plain-text topics, words folded for case and accents.
/// 2. and when each message was sent, in a new directory ([`INDEX_DIR`]).
const INDEX_VERSION: u64 = 2;
const INDEX_VERSION_KEY: &[u8] = b"search_index_version";

/// Where the index lives, inside the database directory. It changes with the index's columns:
/// tantivy will not open an index whose columns differ, and the old directory is left alone so a
/// rolled-back binary still finds the index it knows (the messages since are missing from it).
const INDEX_DIR: &str = "search.v2";

/// Stored while a rebuild is under way and replaced by [`INDEX_VERSION`] when it finishes. A rebuild
/// that stops part-way therefore leaves a version older than any real one, and the next start does
/// it again; leaving the key as it was would read as "never recorded", which is something else.
const REBUILDING: u64 = 0;

/// What startup does about the index.
#[derive(Debug, Eq, PartialEq)]
enum Startup {
	/// Built by the current rules.
	Current,
	/// Index the history again.
	Rebuild,
	/// Already built, from before versions were recorded: write the version down, change nothing.
	Adopt,
}

/// Decides what to do with an index whose stored version is `stored`, given whether it holds any
/// documents.
///
/// An absent version means one of two things. The live server's index was built long before
/// versions existed, is complete, and must not be thrown away and rebuilt for the sake of a new
/// bookkeeping key - that would leave search empty for the length of a full history walk. A server
/// whose index was never built has no version either, and does need the rebuild. Whether the index
/// has any documents tells them apart; an interrupted rebuild cannot be mistaken for the first kind
/// because it records [`REBUILDING`] before it starts.
///
/// What this cannot tell apart is an index that only ever held the messages that arrived since it
/// was deployed, its history never built: that one is adopted too, and needs the admin command
/// once. No server is known to be in that state.
const fn startup_action(stored: Option<u64>, has_documents: bool) -> Startup {
	match stored {
		| Some(version) if version >= INDEX_VERSION => Startup::Current,
		| Some(_) => Startup::Rebuild,
		| None if has_documents => Startup::Adopt,
		| None => Startup::Rebuild,
	}
}

/// Messages beyond the page asked for, fetched so that the ones the reader may not see can be
/// dropped without leaving the page short.
const OVERFETCH: usize = 16;

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let path = args.server.config.database_path.join(INDEX_DIR);

		Ok(Arc::new(Self {
			engine: Engine::open(&path)?,
			global: args.db["global"].clone(),
			server: args.server.clone(),
			services: args.services.clone(),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		// Brings history up to the current rules if the stored version says it is behind. The walk
		// takes minutes, so it runs on its own task: this loop has to keep committing meanwhile, or
		// nothing indexed in the meantime would become searchable.
		let stored = self
			.global
			.get(INDEX_VERSION_KEY)
			.await
			.deserialized::<u64>()
			.ok();

		match startup_action(stored, !self.engine.is_empty()) {
			| Startup::Current => {},
			| Startup::Adopt => {
				info!(
					current = INDEX_VERSION,
					"Recording the version of the existing search index"
				);
				self.global
					.raw_put(INDEX_VERSION_KEY, INDEX_VERSION);
			},
			| Startup::Rebuild => {
				info!(?stored, current = INDEX_VERSION, "Indexing existing history for search");
				let this = self.clone();
				self.server.runtime().spawn(async move {
					match this.rebuild_words().await {
						| Ok(indexed) => info!("Indexed the words of {indexed} messages."),
						| Err(e) if !this.server.is_running() =>
							info!("Search indexing stopped: {e}"),
						| Err(e) => error!("Rebuilding the search index failed: {e}"),
					}
				});
			},
		}

		while self.server.is_running() {
			tokio::select! {
				() = self.server.until_shutdown() => break,
				() = sleep(COMMIT_EVERY) => {},
			}

			// Most ticks of most servers have nothing waiting, and a commit of nothing is still a
			// file rewritten and synced. So ask first, which is one atomic read.
			if !self.engine.is_dirty() {
				continue;
			}

			// A commit writes and syncs files: keep it off the async threads.
			let this = self.clone();
			match tokio::task::spawn_blocking(move || this.engine.commit()).await {
				| Ok(Ok(_)) => {},
				| Ok(Err(e)) => error!("Could not commit the search index: {e}"),
				| Err(e) => error!("Committing the search index did not finish: {e}"),
			}
		}

		// Whatever arrived since the last tick, so a restart does not lose it. Done here rather
		// than on a blocking thread: the runtime is on its way down, and this must not be
		// skipped.
		self.engine.commit().map(|_| ())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub fn index_pdu(
	&self,
	shortroomid: ShortRoomId,
	pdu_id: &RawPduId,
	sent: MilliSecondsSinceUnixEpoch,
	message_body: &str,
) {
	let order = pdu_id.pdu_count().into_signed();
	let ts = i64::from(sent.get());
	if let Err(e) = self
		.engine
		.add(shortroomid, pdu_id.as_ref(), order, ts, message_body)
	{
		error!("Could not index a message for search: {e}");
	}
}

#[implement(Service)]
pub fn deindex_pdu(&self, _shortroomid: ShortRoomId, pdu_id: &RawPduId, _message_body: &str) {
	// The key identifies the message on its own, so what it used to say does not matter. The
	// argument stays because the callers have it and the old index needed it.
	if let Err(e) = self.engine.remove(pdu_id.as_ref()) {
		error!("Could not remove a message from the search index: {e}");
	}
}

/// Indexes every message of every room, for history that predates this index. Records the version
/// it built when it finishes, so neither a restart nor an admin command's result is forgotten.
#[implement(Service)]
pub async fn rebuild_words(&self) -> Result<usize> {
	self.global.raw_put(INDEX_VERSION_KEY, REBUILDING);
	self.engine.clear()?;

	let rooms: Vec<_> = self
		.services
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let mut indexed: usize = 0;
	for room_id in rooms {
		self.server.check_running()?;
		let Ok(shortroomid) = self
			.services
			.short
			.get_shortroomid(&room_id)
			.await
		else {
			continue;
		};

		let mut pdus = pin!(
			self.services
				.timeline
				.pdus(None, &room_id, None)
				.ignore_err()
		);
		while let Some((count, pdu)) = pdus.next().await {
			if let Some(text) = searchable_text(pdu.event_type(), pdu.content().get()) {
				// The timeline hands back the count; the index is keyed by the packed id.
				let pdu_id: RawPduId = PduId { shortroomid, count }.into();
				self.index_pdu(shortroomid, &pdu_id, pdu.origin_server_ts(), &text);
				indexed = indexed.saturating_add(1);
			}
		}

		// What media said (text read off a picture, a voice message) is not in the events: it is
		// put back from where it is kept, or a rebuild would forget it.
		let media = self
			.services
			.media_text
			.reindex_room(shortroomid)
			.await;
		indexed = indexed.saturating_add(media);
	}

	// The worker's timer has been committing all along; this is the remainder, and it has to be
	// on disk before the version says the history is whole.
	self.engine.commit()?;
	self.global
		.raw_put(INDEX_VERSION_KEY, INDEX_VERSION);

	Ok(indexed)
}

/// The text an event is searched by, if it has any: a message's body or a room's plain-text topic.
/// The timeline hooks index exactly these as events arrive, so a rebuild has to agree or it would
/// quietly drop what they add - topics were missing from it before this was one function.
fn searchable_text(kind: &TimelineEventType, content: &str) -> Option<String> {
	#[derive(serde::Deserialize)]
	struct Body {
		body: Option<String>,
	}

	match kind {
		| TimelineEventType::RoomMessage => serde_json::from_str::<Body>(content)
			.ok()
			.and_then(|content| content.body),
		| TimelineEventType::RoomTopic => serde_json::from_str::<RoomTopicEventContent>(content)
			.ok()
			.and_then(plain_text_topic),
		| _ => None,
	}
}

/// The page of messages matching the query in any of its rooms, newest first across all of them,
/// and how many matched in total.
///
/// One search over the set, not one per room: `limit` is the size of the whole page and `skip` an
/// offset into the whole result, so paging walks one list without repeating or skipping anything,
/// and a search of every room an account is in costs one index walk however many rooms that is.
/// Rooms the server does not know are left out.
///
/// The count is every match, not the size of the page, so a client can say which of how many it is
/// showing and step through them. It counts what the index matched rather than what survives the
/// checks below - a message the reader cannot see is rare inside a room they are searching, and
/// counting them exactly would mean fetching every match to find out.
#[implement(Service)]
pub async fn search_pdus<'a>(
	&'a self,
	query: &'a RoomQuery<'a>,
) -> Result<(usize, impl Stream<Item = impl Event + use<>> + Send + '_)> {
	let shortroomids: Vec<ShortRoomId> = query
		.room_ids
		.iter()
		.stream()
		.wide_filter_map(async |room_id| {
			self.services
				.short
				.get_shortroomid(room_id)
				.await
				.ok()
		})
		.collect()
		.await;

	let want = query
		.skip
		.saturating_add(query.limit)
		.saturating_add(OVERFETCH);

	let found = self.engine.search(
		&shortroomids,
		&query.criteria.search_term,
		query.range,
		query.sort,
		0,
		want,
	)?;

	// What the index found, in its order, less what is redacted, filtered out or not visible to
	// the reader. The offset is applied after those drops, so it counts results the reader got.
	let filter = &query.criteria.filter;
	let pdus = found
		.pdus
		.into_iter()
		.map(|pdu| RawPduId::from(pdu.as_slice()))
		.collect::<Vec<_>>()
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

	Ok((found.count, pdus))
}

/// Forgets a whole room, when the room itself is being deleted.
#[implement(Service)]
pub async fn delete_all_search_tokenids_for_room(&self, room_id: &RoomId) -> Result {
	let Ok(shortroomid) = self.services.short.get_shortroomid(room_id).await else {
		return Ok(());
	};

	self.engine.remove_room(shortroomid)?;
	self.engine.commit().map(|_| ())
}

#[cfg(test)]
mod tests {
	use std::{fs, path::Path, time::SystemTime};

	use ruma::{MilliSecondsSinceUnixEpoch, events::TimelineEventType};
	use tokio::time::{Duration, sleep};
	use tuwunel_core::{Result, config::Figment};

	use super::{
		COMMIT_EVERY, INDEX_DIR, INDEX_VERSION, REBUILDING, SortBy, Startup, TimeRange,
		searchable_text, startup_action,
	};
	use crate::test_utils::{fixture, pdu_id};

	/// When the index last wrote the file that every commit rewrites.
	fn committed_at(index: &Path) -> Result<SystemTime> {
		Ok(fs::metadata(index.join("meta.json"))?.modified()?)
	}

	/// Longer than the coarsest clock a filesystem stamps a file with, so a rewrite shows.
	const A_MOMENT: Duration = Duration::from_millis(50);

	// The reason the worker asks before it commits: tantivy rewrites and syncs `meta.json` on
	// every commit, also on one with nothing in it, and the live server did that twice a second
	// with nobody writing. A commit with nothing waiting must leave the file alone, and one with
	// something waiting must still happen - for an added message, a removed one, and a removed
	// room alike. The first rewrite seen here also shows that the file's time is worth reading.
	#[tokio::test]
	async fn only_a_commit_with_something_waiting_writes_the_index() -> Result {
		let Some(fixture) = fixture(Figment::new()).await? else {
			return Ok(());
		};

		let service = &fixture.services.search;
		let engine = &service.engine;
		let index = service
			.server
			.config
			.database_path
			.join(INDEX_DIR);

		let opened = committed_at(&index)?;
		sleep(A_MOMENT).await;
		assert!(!engine.is_dirty());
		assert!(!engine.commit()?, "a commit of nothing said it committed");
		assert_eq!(committed_at(&index)?, opened, "a commit of nothing wrote the index");

		service.index_pdu(1, &pdu_id(1), MilliSecondsSinceUnixEpoch::now(), "hello there");
		assert!(engine.is_dirty());
		assert!(engine.commit()?);
		assert!(!engine.is_dirty());
		let added = committed_at(&index)?;
		assert_ne!(added, opened, "a commit of a message did not write the index");

		sleep(A_MOMENT).await;
		assert!(!engine.commit()?);
		assert_eq!(committed_at(&index)?, added, "the second commit of one message wrote again");

		service.deindex_pdu(1, &pdu_id(1), "hello there");
		assert!(engine.is_dirty(), "a removed message is not waiting for a commit");
		assert!(engine.commit()?);
		assert!(!engine.commit()?);

		engine.remove_room(1)?;
		assert!(engine.is_dirty(), "a removed room is not waiting for a commit");
		assert!(engine.commit()?);

		// Clearing commits by itself, and takes with it what was waiting.
		service.index_pdu(1, &pdu_id(2), MilliSecondsSinceUnixEpoch::now(), "to be cleared");
		engine.clear()?;
		assert!(!engine.is_dirty());
		let cleared = committed_at(&index)?;
		sleep(A_MOMENT).await;
		assert!(!engine.commit()?);
		assert_eq!(committed_at(&index)?, cleared);

		Ok(())
	}

	// The other half: what is committed can be found, and what is not yet committed cannot. The
	// reader picks a commit up by itself a little later, so this waits for it rather than asking
	// once - but not for long: the interval and the reader's delay are what the comment on
	// COMMIT_EVERY promises.
	#[tokio::test]
	async fn a_message_is_found_once_it_is_committed() -> Result {
		let Some(fixture) = fixture(Figment::new()).await? else {
			return Ok(());
		};

		let service = &fixture.services.search;
		let engine = &service.engine;
		let found = || {
			engine
				.search(&[1], "hello", TimeRange::default(), SortBy::Timeline, 0, 10)
				.map(|found| found.count)
		};

		service.index_pdu(1, &pdu_id(1), MilliSecondsSinceUnixEpoch::now(), "hello there");
		assert_eq!(found()?, 0, "found before it was committed");
		assert!(engine.commit()?);

		let mut waited = Duration::ZERO;
		while found()? == 0 && waited < COMMIT_EVERY.saturating_mul(10) {
			sleep(A_MOMENT).await;
			waited = waited.saturating_add(A_MOMENT);
		}
		assert_eq!(found()?, 1, "not found {waited:?} after it was committed");
		assert!(!engine.is_empty());

		Ok(())
	}

	#[test]
	fn history_indexed_by_older_rules_is_indexed_again() {
		assert_eq!(startup_action(Some(INDEX_VERSION), true), Startup::Current);
		// A newer binary's index, after a rollback: not ours to redo.
		assert_eq!(startup_action(Some(u64::MAX), true), Startup::Current);
		assert_eq!(startup_action(Some(0), true), Startup::Rebuild);
		// Current by its version but empty is still current: a server with no messages.
		assert_eq!(startup_action(Some(INDEX_VERSION), false), Startup::Current);
	}

	// An unfinished rebuild leaves the index full of some of the history. It must not read as a
	// finished one.
	#[test]
	fn a_rebuild_that_stopped_part_way_is_done_again() {
		assert_eq!(startup_action(Some(REBUILDING), true), Startup::Rebuild);
		assert_eq!(startup_action(Some(REBUILDING), false), Startup::Rebuild);
		assert!(REBUILDING < INDEX_VERSION);
	}

	// The live server's index predates versions and is whole: it is kept, not rebuilt. One that was
	// never built has nothing to keep.
	#[test]
	fn an_unversioned_index_is_adopted_if_it_has_documents_and_built_if_it_has_none() {
		assert_eq!(startup_action(None, true), Startup::Adopt);
		assert_eq!(startup_action(None, false), Startup::Rebuild);
	}

	// What the index holds, as INDEX_VERSION names it. If this fails, the rules changed: update the
	// table and raise INDEX_VERSION, or existing history keeps being indexed by the old ones.
	#[test]
	fn the_rules_are_the_ones_the_version_names() {
		assert_eq!(INDEX_VERSION, 2);
		let text = |kind: &TimelineEventType, content| searchable_text(kind, content);
		assert_eq!(
			text(&TimelineEventType::RoomMessage, r#"{"msgtype":"m.text","body":"hello"}"#),
			Some("hello".to_owned())
		);
		assert_eq!(
			text(&TimelineEventType::RoomTopic, r#"{"topic":"all about cats"}"#),
			Some("all about cats".to_owned())
		);
		// Nothing to find in these, so nothing is indexed.
		assert_eq!(text(&TimelineEventType::RoomMessage, r#"{"msgtype":"m.image"}"#), None);
		assert_eq!(text(&TimelineEventType::RoomMessage, "{}"), None);
		assert_eq!(text(&TimelineEventType::RoomTopic, r#"{"topic":""}"#), None);
		assert_eq!(text(&TimelineEventType::RoomName, r#"{"name":"cats","body":"x"}"#), None);
		assert_eq!(text(&TimelineEventType::Sticker, r#"{"body":"sticker"}"#), None);
	}

	// The manager runs only the workers of the services it lists: without the entry the rebuild
	// above never started, and the index kept whatever rules it was first built by.
	#[test]
	fn the_worker_is_started() {
		let services = include_str!("../../services.rs");
		assert!(services.contains("cast!(self.search)"));
	}
}
