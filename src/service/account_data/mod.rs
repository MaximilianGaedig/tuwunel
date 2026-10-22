//! User and room account data storage.
//!
//! The service stores raw account data events behind monotonic update counters and maintains a
//! secondary index by room, user, and event type. Its streams expose changes for incremental sync.

mod appservice;
mod direct;
mod push_rules;
mod room_tags;

use std::sync::Arc;

use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt, pin_mut};
use ruma::{
	OwnedUserId, RoomId, UserId,
	events::{
		AnyGlobalAccountDataEvent, AnyRawAccountDataEvent, AnyRoomAccountDataEvent,
		GlobalAccountDataEventType, RoomAccountDataEventType,
	},
	push::{RuleKind, Ruleset},
	serde::Raw,
};
use serde::Deserialize;
use serde_json::json;
use tuwunel_core::{
	Err, Result, at, err, implement, info,
	utils::{MutexMap, ReadyExt, TryReadyExt, result::LogErr, stream::TryIgnore},
	warn,
};
use tuwunel_database::{
	Deserialized, Handle, Ignore, Interfix, Json, Map, deserialize_from_slice,
};

/// Maximum number of push rules one account may hold.
///
/// The ruleset is a single account-data blob rewritten in full on every
/// mutation and matched against every event for every local recipient, so the
/// count bounds the write cost and the per-event matching work alike.
pub const MAX_RULES: usize = 10_000;

/// Longest rule ID stored, in bytes.
///
/// Room IDs reach 255 bytes and are routinely used as rule IDs, so the ceiling
/// sits above that rather than at it.
pub const MAX_RULE_ID_BYTES: usize = 300;

/// Largest match and action data stored for one rule, in bytes.
///
/// Rule IDs are excluded and bounded separately by [`MAX_RULE_ID_BYTES`].
pub const MAX_RULE_BYTES: usize = 1024;

/// Stores and queries global and room-scoped account data.
///
/// Updates commit the secondary pointer and new event blob in one transaction while retaining
/// monotonic counters for sync. Typed accessors deserialize the complete account data event
/// envelope.
pub struct Service {
	services: Arc<crate::services::OnceServices>,
	db: Data,

	/// Serializes [`update`](Service::update) per user.
	///
	/// An update reads the pointer to the revision it supersedes, then deletes
	/// that revision in its own transaction. Two concurrent updates of one type
	/// read the same pointer, so the revision written by whichever commits first
	/// was never deleted and stayed in sync responses beside the current one.
	write_mutex: MutexMap<OwnedUserId, ()>,
}

/// Key of one stored revision: scope, owner, counter and event type.
type RevisionKey<'a> = (Option<&'a RoomId>, &'a UserId, u64, &'a str);

struct Data {
	roomuserdataid_accountdata: Arc<Map>,
	roomusertype_roomuserdataid: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: args.services.clone(),
			db: Data {
				roomuserdataid_accountdata: args.db["roomuserdataid_accountdata"].clone(),
				roomusertype_roomuserdataid: args.db["roomusertype_roomuserdataid"].clone(),
			},
			write_mutex: MutexMap::new(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Whether a ruleset has room for the rule with the given kind and ID.
///
/// A rule replacing one already present adds nothing, so only an unseen ID is
/// held against [`MAX_RULES`]. The ID is bounded here rather than at each call
/// site because a room ID serves as the rule ID for room rules and carries no
/// length of its own once arbitrary-length identifiers are accepted.
#[must_use]
pub fn admits_rule(ruleset: &Ruleset, kind: RuleKind, rule_id: &str) -> bool {
	rule_id.len() <= MAX_RULE_ID_BYTES
		&& (ruleset.get(kind, rule_id).is_some()
			|| ruleset.iter().take(MAX_RULES).count() < MAX_RULES)
}

/// Stores an account data event and replaces its previous revision.
///
/// The input must contain complete `type` and `content` fields. A new global counter value keys the
/// event blob, secondary pointer, and removal of the superseded blob in one transaction.
///
/// # Panics
///
/// Panics when dispatching the global sequence number fails.
#[implement(Service)]
pub async fn update(
	&self,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	event_type: RoomAccountDataEventType,
	data: &serde_json::Value,
) -> Result {
	if data.get("type").is_none() || data.get("content").is_none() {
		return Err!(Request(InvalidParam("Account data doesn't have all required fields.")));
	}

	let write_lock = self.write_mutex.lock(user_id).await;
	let count = self.services.globals.next_count();
	let roomuserdataid = (room_id, user_id, *count, &event_type);
	let key = (room_id, user_id, &event_type);
	let prev = self
		.db
		.roomusertype_roomuserdataid
		.qry(&key)
		.await;

	let bridged = appservice::is_bridged(room_id, event_type.to_string().as_str());
	let prev_data: Option<serde_json::Value> = match &prev {
		| Ok(prev) if bridged => self
			.db
			.roomuserdataid_accountdata
			.get(prev)
			.await
			.deserialized()
			.ok(),
		| _ => None,
	};

	let mut txn = self.services.db.txn();

	txn.put(&self.db.roomuserdataid_accountdata, roomuserdataid, Json(data));
	txn.put(&self.db.roomusertype_roomuserdataid, key, roomuserdataid);

	if let Ok(prev) = prev {
		txn.del_raw(&self.db.roomuserdataid_accountdata, prev);
	}

	txn.execute();
	drop(write_lock);

	if bridged {
		self.notify_appservices(
			room_id,
			user_id,
			&event_type.to_string(),
			data,
			prev_data.as_ref(),
		)
		.await;
	}

	Ok(())
}

/// Replaces an account data event with an MSC3391 tombstone.
///
/// Delta sync surfaces the empty content so clients can apply the deletion. Zero-token V3 and V5
/// sync responses and client account-data GET routes treat the tombstone as absent.
///
/// # Panics
///
/// Panics when dispatching the global sequence number fails.
#[implement(Service)]
pub async fn delete(
	&self,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	event_type: RoomAccountDataEventType,
) -> Result {
	let tombstone = json!({
		"type": event_type.to_string(),
		"content": {},
	});

	self.update(room_id, user_id, event_type, &tombstone)
		.await
}

/// Searches the global account data for a specific kind.
///
/// Global data is stored under no room, so one kind can be held once globally
/// and once in every room without collision. The record is the whole
/// `{type, content}` event, so `T` names the event rather than its content.
#[implement(Service)]
pub async fn get_global<T>(&self, user_id: &UserId, kind: GlobalAccountDataEventType) -> Result<T>
where
	T: for<'de> Deserialize<'de>,
{
	self.get_raw(None, user_id, &kind.to_string())
		.await
		.deserialized()
}

/// Searches the room account data for a specific kind.
///
/// The room scopes the lookup, so one kind may hold different data in each of
/// them. The record is the whole `{type, content}` event, so `T` names the
/// event rather than its content.
#[implement(Service)]
pub async fn get_room<T>(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	kind: RoomAccountDataEventType,
) -> Result<T>
where
	T: for<'de> Deserialize<'de>,
{
	self.get_raw(Some(room_id), user_id, &kind.to_string())
		.await
		.deserialized()
}

/// Loads a stored account data event without deserializing it.
///
/// The optional room ID selects room-scoped or global account data. The lookup resolves the
/// current secondary index entry before borrowing the raw event from storage.
#[implement(Service)]
pub async fn get_raw(
	&self,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	kind: &str,
) -> Result<Handle<'_>> {
	let key = (room_id, user_id, kind.to_owned());
	self.db
		.roomusertype_roomuserdataid
		.qry(&key)
		.and_then(|roomuserdataid| {
			self.db
				.roomuserdataid_accountdata
				.get(&roomuserdataid)
		})
		.await
}

/// Streams account data changes after a counter value.
///
/// The optional upper bound is inclusive. Cursor and decoding failures are logged and omitted from
/// this convenience stream.
#[implement(Service)]
pub fn changes_since<'a>(
	&'a self,
	room_id: Option<&'a RoomId>,
	user_id: &'a UserId,
	since: u64,
	to: Option<u64>,
) -> impl Stream<Item = AnyRawAccountDataEvent> + Send + 'a {
	self.changes_since_fallible(room_id, user_id, since, to)
		.map(LogErr::log_err)
		.ignore_err()
}

/// Returns bounded account-data changes without suppressing failures.
///
/// The lower bound is exclusive and the optional upper bound is inclusive.
/// Cursor, decode, and deserialization failures remain in the stream for an
/// atomic caller to handle.
#[implement(Service)]
pub fn changes_since_fallible<'a>(
	&'a self,
	room_id: Option<&'a RoomId>,
	user_id: &'a UserId,
	since: u64,
	to: Option<u64>,
) -> impl Stream<Item = Result<AnyRawAccountDataEvent>> + Send + 'a {
	// Skip the data that's exactly at since, because we sent that last time
	let first_possible = (room_id, user_id, since.saturating_add(1));

	self.db
		.roomuserdataid_accountdata
		.stream_from_raw(&first_possible)
		.ready_and_then(move |(key, val)| {
			let (room_id_, user_id_, count, kind): RevisionKey<'_> = deserialize_from_slice(key)?;

			if room_id != room_id_ || user_id != user_id_ || to.is_some_and(|to| count > to) {
				return Ok(None);
			}

			match room_id {
				| Some(_) => serde_json::from_slice::<Raw<AnyRoomAccountDataEvent>>(val)
					.map(AnyRawAccountDataEvent::Room),
				| None => serde_json::from_slice::<Raw<AnyGlobalAccountDataEvent>>(val)
					.map(AnyRawAccountDataEvent::Global),
			}
			.map(|event| Some((key.to_vec(), kind.to_owned(), event)))
			.map_err(|e| err!(Database("Database contains invalid account data: {e}")))
		})
		.ready_try_take_while(|revision| Ok(revision.is_some()))
		.try_filter_map(move |revision| async move {
			let Some((key, kind, event)) = revision else {
				return Ok(None);
			};

			Ok(self
				.is_current_revision(room_id, user_id, &kind, &key)
				.await
				.then_some(event))
		})
}

/// Whether a stored revision is the one the type's pointer names.
///
/// A superseded revision left behind by an update is not, and is never served:
/// only the latest content of each type reaches a client. A revision no pointer
/// names at all is kept, since no update can have superseded it.
#[implement(Service)]
async fn is_current_revision(
	&self,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	kind: &str,
	roomuserdataid: &[u8],
) -> bool {
	self.db
		.roomusertype_roomuserdataid
		.qry(&(room_id, user_id, kind))
		.await
		.ok()
		.is_none_or(|current| *current == *roomuserdataid)
}

/// Deletes every stored revision a newer one of the same type superseded.
///
/// Such revisions were left by concurrent updates before
/// [`update`](Service::update) was serialized. Returns how many revisions were
/// read and how many were deleted.
#[implement(Service)]
pub async fn remove_superseded_revisions(&self) -> Result<(usize, usize)> {
	let keys: Vec<Vec<u8>> = self
		.db
		.roomuserdataid_accountdata
		.raw_keys()
		.map_ok(<[u8]>::to_vec)
		.try_collect()
		.await?;

	let mut txn = self.services.db.txn();
	let mut removed: usize = 0;
	for key in &keys {
		let Ok((room_id, user_id, _, kind)) = deserialize_from_slice::<RevisionKey<'_>>(key)
		else {
			warn!(?key, "Skipping undecodable account data key.");
			continue;
		};

		if !self
			.is_current_revision(room_id, user_id, kind, key)
			.await
		{
			txn.del_raw(&self.db.roomuserdataid_accountdata, key);
			removed = removed.saturating_add(1);
		}
	}

	if removed > 0 {
		txn.execute();
	}

	info!(total = keys.len(), removed, "Removed superseded account data revisions.");

	Ok((keys.len(), removed))
}

/// Erases a user's account data within one MSC4025 namespace.
///
/// An absent room selects global data, while a room selects only that room's data. Rows yielded
/// successfully from both prefix scans are deleted together; scan errors are skipped.
#[implement(Service)]
pub async fn erase_user(&self, user_id: &UserId, room_id: Option<&RoomId>) {
	let prefix = (room_id, user_id, Interfix);
	let mut txn = self.services.db.txn();

	self.db
		.roomuserdataid_accountdata
		.keys_prefix_raw(&prefix)
		.ignore_err()
		.ready_for_each(|key| txn.del_raw(&self.db.roomuserdataid_accountdata, key))
		.await;

	self.db
		.roomusertype_roomuserdataid
		.keys_prefix_raw(&prefix)
		.ignore_err()
		.ready_for_each(|key| txn.del_raw(&self.db.roomusertype_roomuserdataid, key))
		.await;

	txn.execute();
}

/// Returns the latest account data counter at or below an optional bound.
///
/// The lookup is scoped to one user and either global data or a single room. An empty scope produces
/// a not-found request error.
#[implement(Service)]
pub async fn last_count<'a>(
	&'a self,
	room_id: Option<&'a RoomId>,
	user_id: &'a UserId,
	upper: Option<u64>,
) -> Result<u64> {
	type Key<'a> = (Option<&'a RoomId>, &'a UserId, u64, Ignore);

	let upper = upper.unwrap_or(u64::MAX);
	let key = (room_id, user_id, upper, Interfix);
	let keys = self
		.db
		.roomuserdataid_accountdata
		.rev_keys_from(&key)
		.ignore_err()
		.ready_take_while(move |(room_id_, user_id_, ..): &Key<'_>| {
			room_id == *room_id_ && user_id == *user_id_
		})
		.map(at!(2));

	pin_mut!(keys);
	keys.next()
		.await
		.ok_or_else(|| err!(Request(NotFound("No account data found."))))
}
