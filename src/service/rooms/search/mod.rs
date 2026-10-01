mod engine;
pub mod matcher;

use std::{pin::pin, sync::Arc};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use ruma::{
	RoomId, UserId,
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
use crate::rooms::{short::ShortRoomId, state_accessor::plain_text_topic, timeline::RawPduId};

pub struct Service {
	engine: Engine,
	global: Arc<Map>,
	server: Arc<Server>,
	services: Arc<crate::services::OnceServices>,
}

#[derive(Clone, Debug)]
pub struct RoomQuery<'a> {
	pub room_id: &'a RoomId,
	pub user_id: Option<&'a UserId>,
	pub criteria: &'a Criteria,
	pub limit: usize,
	pub skip: usize,
}

/// How long a message may wait before it is searchable.
///
/// An index becomes readable when it is committed, and committing per message would cost a file
/// write per message. So the worker commits on a timer, which is what Elasticsearch calls a refresh
/// interval and for the same reason. Half a second is below what anyone notices between sending a
/// message and finding it.
const COMMIT_EVERY: Duration = Duration::from_millis(500);

/// Which rules the index was built by. Raise it whenever what is indexed, or how it is cut into
/// words, changes (see [`searchable_text`] and the tokenizer in `engine`): history indexed by the
/// old rules is then indexed again on the next start, without anyone having to ask.
///
/// 1. message bodies and plain-text topics, words folded for case and accents.
const INDEX_VERSION: u64 = 1;
const INDEX_VERSION_KEY: &[u8] = b"search_index_version";

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
		let path = args.server.config.database_path.join("search");

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
				info!(current = INDEX_VERSION, "Recording the version of the existing search index");
				self.global.raw_put(INDEX_VERSION_KEY, INDEX_VERSION);
			},
			| Startup::Rebuild => {
				info!(?stored, current = INDEX_VERSION, "Indexing existing history for search");
				let this = self.clone();
				self.server.runtime().spawn(async move {
					match this.rebuild_words().await {
						| Ok(indexed) => info!("Indexed the words of {indexed} messages."),
						| Err(e) if !this.server.is_running() => info!("Search indexing stopped: {e}"),
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

			if let Err(e) = self.engine.commit() {
				error!("Could not commit the search index: {e}");
			}
		}

		// Whatever arrived since the last tick, so a restart does not lose it.
		self.engine.commit()
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub fn index_pdu(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId, message_body: &str) {
	let order = pdu_id.pdu_count().into_signed();
	if let Err(e) = self
		.engine
		.add(shortroomid, pdu_id.as_ref(), order, message_body)
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
		let Ok(shortroomid) = self.services.short.get_shortroomid(&room_id).await else {
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
				self.index_pdu(shortroomid, &pdu_id, &text);
				indexed = indexed.saturating_add(1);
			}
		}
	}

	self.engine.commit()?;
	self.global.raw_put(INDEX_VERSION_KEY, INDEX_VERSION);

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

/// The page of a room's messages matching the query, and how many matched in total.
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
	let shortroomid = self
		.services
		.short
		.get_shortroomid(query.room_id)
		.await?;

	let want = query
		.skip
		.saturating_add(query.limit)
		.saturating_add(OVERFETCH);

	let found = self
		.engine
		.search(shortroomid, &query.criteria.search_term, 0, want)?;

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
	self.engine.commit()
}

#[cfg(test)]
mod tests {
	use ruma::events::TimelineEventType;

	use super::{INDEX_VERSION, REBUILDING, Startup, searchable_text, startup_action};

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
		assert_eq!(INDEX_VERSION, 1);
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
