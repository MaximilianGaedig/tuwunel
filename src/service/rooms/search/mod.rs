mod engine;
pub mod matcher;

use std::{pin::pin, sync::Arc};

use futures::{Stream, StreamExt};
use ruma::{RoomId, UserId, api::client::search::search_events::v3::Criteria};
use tokio::time::{Duration, sleep};
use async_trait::async_trait;
use tuwunel_core::{
	Result, Server, error, implement,
	matrix::{
		event::{Event, Matches},
		pdu::PduId,
	},
	utils::{
		IterStream, ReadyExt,
		stream::{TryIgnore, WidebandExt},
	},
};

use self::engine::Engine;
use crate::rooms::{short::ShortRoomId, timeline::RawPduId};

pub struct Service {
	engine: Engine,
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

/// Messages beyond the page asked for, fetched so that the ones the reader may not see can be
/// dropped without leaving the page short.
const OVERFETCH: usize = 16;

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let path = args.server.config.database_path.join("search");

		Ok(Arc::new(Self {
			engine: Engine::open(&path)?,
			server: args.server.clone(),
			services: args.services.clone(),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
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

/// Indexes every message of every room, for history that predates this index.
#[implement(Service)]
pub async fn rebuild_words(&self) -> Result<usize> {
	#[derive(serde::Deserialize)]
	struct Body {
		body: Option<String>,
	}

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
			if *pdu.event_type() != ruma::events::TimelineEventType::RoomMessage {
				continue;
			}
			if let Ok(Body { body: Some(body) }) = pdu.get_content() {
				// The timeline hands back the count; the index is keyed by the packed id.
				let pdu_id: RawPduId = PduId { shortroomid, count }.into();
				self.index_pdu(shortroomid, &pdu_id, &body);
				indexed = indexed.saturating_add(1);
			}
		}
	}

	self.engine.commit()?;

	Ok(indexed)
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
