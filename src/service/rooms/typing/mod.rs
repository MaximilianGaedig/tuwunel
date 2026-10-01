use std::{collections::BTreeMap, sync::Arc};

use futures::{FutureExt, TryStreamExt, future::try_join};
use ruma::{
	CanonicalJsonValue, OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::federation::transactions::edu::{Edu, TypingContent},
	events::{GlobalAccountDataEventType, ignored_user_list::IgnoredUserListEvent},
};
use serde::{Serialize, Serializer};
use serde_json::{Value as JsonValue, json};
use tokio::sync::{RwLock, broadcast};
use tuwunel_core::{
	Result, Server,
	debug::INFO_SPAN_LEVEL,
	debug_info, trace,
	utils::{BoolExt, IterStream, millis_since_unix_epoch},
};

use crate::sending::EduBuf;

pub struct Service {
	server: Arc<Server>,
	services: Arc<crate::services::OnceServices>,
	typing: RwLock<BTreeMap<OwnedRoomId, RoomTyping>>,
	pub typing_update_sender: broadcast::Sender<OwnedRoomId>,
}

/// The field of the typing request's body that says what the user is doing.
pub const TYPING_KIND_FIELD: &str = "im.mxg.typing.kind";

/// What a typing user is doing. Other networks tell their users that a contact
/// is recording a voice message or sending a photo; `m.typing` alone can only
/// say "typing", so the kind travels beside it.
///
/// The vocabulary is closed. Anything else read off the wire is plain typing,
/// so a newer client's kind degrades to what `m.typing` always meant instead
/// of failing the request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TypingKind {
	#[default]
	Text,
	RecordingVoice,
	RecordingVideo,
	UploadingPhoto,
	UploadingVideo,
	UploadingFile,
	UploadingVoice,
	ChoosingSticker,
}

impl TypingKind {
	/// Reads a kind as written on the wire; an unknown one is plain typing.
	#[must_use]
	pub fn parse(kind: &str) -> Self {
		match kind {
			| "recording_voice" => Self::RecordingVoice,
			| "recording_video" => Self::RecordingVideo,
			| "uploading_photo" => Self::UploadingPhoto,
			| "uploading_video" => Self::UploadingVideo,
			| "uploading_file" => Self::UploadingFile,
			| "uploading_voice" => Self::UploadingVoice,
			| "choosing_sticker" => Self::ChoosingSticker,
			| _ => Self::Text,
		}
	}

	/// Reads the kind out of a typing request's body.
	///
	/// Ruma's request type has no field for it, so it is taken from the parsed
	/// JSON. A missing body, a missing field or one that is not a string are
	/// all plain typing.
	#[must_use]
	pub fn from_request_body(body: Option<&CanonicalJsonValue>) -> Self {
		body.and_then(CanonicalJsonValue::as_object)
			.and_then(|body| body.get(TYPING_KIND_FIELD))
			.and_then(CanonicalJsonValue::as_str)
			.map(Self::parse)
			.unwrap_or_default()
	}

	/// The kind as written on the wire.
	#[must_use]
	pub const fn as_str(self) -> &'static str {
		match self {
			| Self::Text => "text",
			| Self::RecordingVoice => "recording_voice",
			| Self::RecordingVideo => "recording_video",
			| Self::UploadingPhoto => "uploading_photo",
			| Self::UploadingVideo => "uploading_video",
			| Self::UploadingFile => "uploading_file",
			| Self::UploadingVoice => "uploading_voice",
			| Self::ChoosingSticker => "choosing_sticker",
		}
	}
}

impl Serialize for TypingKind {
	fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(self.as_str())
	}
}

/// The content of an `m.typing` event: who is typing, and for those doing
/// something other than typing text, what.
///
/// This replaces ruma's `TypingEventContent`, which has nowhere to put the
/// kinds. A room where everybody types text serializes exactly as before, so
/// clients and appservices that know nothing of kinds see no difference.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TypingUsers {
	pub user_ids: Vec<OwnedUserId>,

	#[serde(
		rename = "im.mxg.typing.kinds",
		skip_serializing_if = "BTreeMap::is_empty"
	)]
	pub kinds: BTreeMap<OwnedUserId, TypingKind>,
}

impl TypingUsers {
	/// Lists every user as typing, and in `kinds` only those not typing text.
	pub fn new<Users>(users: Users) -> Self
	where
		Users: IntoIterator<Item = (OwnedUserId, TypingKind)>,
	{
		let mut content = Self::default();
		for (user_id, kind) in users {
			if kind != TypingKind::Text {
				content.kinds.insert(user_id.clone(), kind);
			}

			content.user_ids.push(user_id);
		}

		content
	}

	/// The `m.typing` event as a client receives it in a sync response.
	#[must_use]
	pub fn sync_event(&self) -> JsonValue {
		json!({
			"type": "m.typing",
			"content": self,
		})
	}

	/// The `m.typing` event as an appservice receives it, naming its room.
	#[must_use]
	pub fn room_event(&self, room_id: &RoomId) -> JsonValue {
		json!({
			"type": "m.typing",
			"content": self,
			"room_id": room_id.as_str(),
		})
	}
}

#[derive(Clone, Copy)]
struct Typing {
	// Unix epoch millisecond timestamp when the typing indicator expires.
	timeout: u64,
	// Kept with the timeout so that it goes when the typing does, whether the
	// user stops or times out.
	kind: TypingKind,
}

#[derive(Default)]
struct RoomTyping {
	users: BTreeMap<OwnedUserId, Typing>,
	// Global stream position of the last change to this room. The count permit must
	// retire only after releasing the typing state lock.
	update: u64,
}

impl RoomTyping {
	fn users(&self) -> impl Iterator<Item = (OwnedUserId, TypingKind)> + '_ {
		self.users
			.iter()
			.map(|(user_id, typing)| (user_id.clone(), typing.kind))
	}
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			server: args.server.clone(),
			services: args.services.clone(),
			typing: RwLock::new(BTreeMap::new()),
			typing_update_sender: broadcast::channel(100).0,
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Sets a user as typing until the timeout timestamp is reached or
	/// roomtyping_remove is called. Calling it again for a user who is already
	/// typing replaces the kind as well as the timeout.
	#[tracing::instrument(
		name = "typing_start"
		level = INFO_SPAN_LEVEL,
		skip_all,
		 fields(
			%room_id,
			%user_id,
			%timeout,
		)
	)]
	pub async fn typing_add(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		timeout: u64,
		kind: TypingKind,
	) -> Result {
		debug_info!(
			"typing started {user_id:?} in {room_id:?} timeout:{timeout:?} kind:{kind:?}"
		);

		// update clients
		let mut typing = self.typing.write().await;
		let room = typing.entry(room_id.to_owned()).or_default();
		room.users
			.insert(user_id.to_owned(), Typing { timeout, kind });

		let count = self.services.globals.next_count();

		room.update = *count;

		drop(typing);
		drop(count);

		if self
			.typing_update_sender
			.send(room_id.to_owned())
			.is_err()
		{
			trace!("receiver found what it was looking for and is no longer interested");
		}

		self.services
			.activity_log
			.log_typing(user_id, room_id)
			.await;

		// update appservices
		let appservice_send = self.appservice_send(room_id);

		// update federation
		let federation_send = self
			.services
			.globals
			.user_is_local(user_id)
			.then_async(|| self.federation_send(room_id, user_id, true))
			.map(Option::transpose);

		try_join(appservice_send, federation_send)
			.await
			.map(|_| ())
	}

	/// Removes a user from typing before the timeout is reached.
	#[tracing::instrument(
		name = "typing_stop"
		level = INFO_SPAN_LEVEL,
		skip_all,
		 fields(
			%room_id,
			%user_id,
		)
	)]
	pub async fn typing_remove(&self, user_id: &UserId, room_id: &RoomId) -> Result {
		debug_info!("typing stopped {user_id:?} in {room_id:?}");

		// update clients
		let mut typing = self.typing.write().await;
		let room = typing.entry(room_id.to_owned()).or_default();
		room.users.remove(user_id);

		let count = self.services.globals.next_count();

		room.update = *count;

		drop(typing);
		drop(count);

		if self
			.typing_update_sender
			.send(room_id.to_owned())
			.is_err()
		{
			trace!("receiver found what it was looking for and is no longer interested");
		}

		// update appservices
		let appservice_send = self.appservice_send(room_id);

		// update federation
		let federation_send = self
			.services
			.globals
			.user_is_local(user_id)
			.then_async(|| self.federation_send(room_id, user_id, false))
			.map(Option::transpose);

		try_join(appservice_send, federation_send)
			.await
			.map(|_| ())
	}

	pub async fn wait_for_update(&self, room_id: &RoomId) {
		let mut receiver = self.typing_update_sender.subscribe();
		while let Ok(next) = receiver.recv().await {
			if next == room_id {
				break;
			}
		}
	}

	/// Makes sure that typing events with old timestamps get removed.
	async fn typings_maintain(&self, room_id: &RoomId) -> Result {
		let current_timestamp = millis_since_unix_epoch();
		let typing = self.typing.read().await;
		let has_expired = typing.get(room_id).is_some_and(|room| {
			room.users
				.values()
				.any(|typing| typing.timeout < current_timestamp)
		});

		drop(typing);

		if !has_expired {
			return Ok(());
		}

		let current_timestamp = millis_since_unix_epoch();
		let mut removable = Vec::new();
		let mut typing = self.typing.write().await;
		let Some(room) = typing.get_mut(room_id) else {
			return Ok(());
		};

		room.users.retain(|user, typing| {
			let expired = typing.timeout < current_timestamp;
			if expired {
				removable.push(user.clone());
			}

			expired.is_false()
		});

		if removable.is_empty() {
			return Ok(());
		}

		// update clients
		let count = self.services.globals.next_count();

		room.update = *count;

		drop(typing);
		drop(count);

		for user in &removable {
			debug_info!("typing timeout {user:?} in {room_id:?}");
		}

		if self
			.typing_update_sender
			.send(room_id.to_owned())
			.is_err()
		{
			trace!("receiver found what it was looking for and is no longer interested");
		}

		// update appservices
		let appservice_send = self.appservice_send(room_id);

		// update federation
		let federation_sends = removable
			.iter()
			.filter(|user_id| self.services.globals.user_is_local(user_id))
			.try_stream()
			.try_for_each(|user_id| self.federation_send(room_id, user_id, false));

		try_join(appservice_send, federation_sends)
			.boxed()
			.await
			.map(|_| ())
	}

	/// Returns the count of the last typing update in this room.
	pub async fn last_typing_update(&self, room_id: &RoomId) -> Result<u64> {
		self.typings_maintain(room_id).await?;

		self.typing
			.read()
			.await
			.get(room_id)
			.map(|room| room.update)
			.map(Ok)
			.unwrap_or(Ok(0))
	}

	/// Returns the typing content with all typing users in the room.
	async fn typings_content(&self, room_id: &RoomId) -> TypingUsers {
		let typing = self.typing.read().await;
		let users = typing
			.get(room_id)
			.into_iter()
			.flat_map(|room| room.users());

		TypingUsers::new(users)
	}

	/// Sends a typing EDU to all appservices interested in the room.
	async fn appservice_send(&self, room_id: &RoomId) -> Result {
		// Written by hand rather than through ruma's `EphemeralData`, whose typing
		// content cannot carry the kinds.
		let edu = self
			.typings_content(room_id)
			.await
			.room_event(room_id);

		self.services
			.sending
			.send_edu_room_appservices(room_id, |buf| Ok(serde_json::to_writer(buf, &edu)?))
			.await
	}

	/// Returns a new typing EDU.
	pub async fn typing_users_for_user(
		&self,
		room_id: &RoomId,
		sender_user: &UserId,
	) -> Result<TypingUsers> {
		let typing = self.typing.read().await;
		let users: Vec<_> = typing
			.get(room_id)
			.into_iter()
			.flat_map(|room| room.users())
			.collect();
		drop(typing);

		let users = self.filter_typing_users(users, sender_user).await;

		Ok(TypingUsers::new(users))
	}

	/// Returns one coherent typing update token and visible user snapshot.
	///
	/// The predicate checks the token under the same lock and can skip cloning
	/// stale users. Selected user IDs are captured before ignored users are
	/// filtered after releasing the lock.
	pub async fn typing_snapshot_for_user<Select>(
		&self,
		room_id: &RoomId,
		sender_user: &UserId,
		select: Select,
	) -> Result<Option<(u64, TypingUsers)>>
	where
		Select: FnOnce(u64) -> bool + Send,
	{
		self.typings_maintain(room_id).await?;

		let typing = self.typing.read().await;
		let room = typing.get(room_id);
		let update = room.map_or(0, |room| room.update);

		if !select(update) {
			return Ok(None);
		}

		let users: Vec<_> = room
			.into_iter()
			.flat_map(|room| room.users())
			.collect();

		drop(typing);

		let users = self.filter_typing_users(users, sender_user).await;

		Ok(Some((update, TypingUsers::new(users))))
	}

	async fn filter_typing_users(
		&self,
		users: Vec<(OwnedUserId, TypingKind)>,
		sender_user: &UserId,
	) -> Vec<(OwnedUserId, TypingKind)> {
		if users.is_empty() {
			return users;
		}

		let ignored: Option<IgnoredUserListEvent> = self
			.services
			.account_data
			.get_global(sender_user, GlobalAccountDataEventType::IgnoredUserList)
			.await
			.ok();

		users
			.into_iter()
			.filter(|(user_id, _)| {
				ignored.as_ref().is_none_or(|ignored| {
					!ignored
						.content
						.ignored_users
						.contains_key::<UserId>(user_id.as_ref())
				})
			})
			.collect()
	}

	/// The kind is deliberately left out: the federation EDU has no field for
	/// it, so other servers see plain typing.
	async fn federation_send(&self, room_id: &RoomId, user_id: &UserId, typing: bool) -> Result {
		debug_assert!(
			self.services.globals.user_is_local(user_id),
			"tried to broadcast typing status of remote user",
		);

		if !self.server.config.allow_outgoing_typing {
			return Ok(());
		}

		let content = TypingContent::new(room_id.to_owned(), user_id.to_owned(), typing);
		let edu = Edu::Typing(content);

		let mut buf = EduBuf::new();
		serde_json::to_writer(&mut buf, &edu).expect("Serialized Edu::Typing");

		self.services
			.sending
			.send_edu_room(room_id, buf)
			.await?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use ruma::{CanonicalJsonValue, OwnedUserId, room_id, user_id};
	use serde_json::json;

	use super::{TypingKind, TypingUsers};

	const KINDS: [TypingKind; 8] = [
		TypingKind::Text,
		TypingKind::RecordingVoice,
		TypingKind::RecordingVideo,
		TypingKind::UploadingPhoto,
		TypingKind::UploadingVideo,
		TypingKind::UploadingFile,
		TypingKind::UploadingVoice,
		TypingKind::ChoosingSticker,
	];

	fn ada() -> OwnedUserId { user_id!("@ada:example.com").to_owned() }

	fn grace() -> OwnedUserId { user_id!("@grace:example.com").to_owned() }

	fn body(json: serde_json::Value) -> CanonicalJsonValue {
		serde_json::from_value(json).expect("canonical JSON")
	}

	#[test]
	fn typing_kind_survives_the_wire() {
		for kind in KINDS {
			assert_eq!(TypingKind::parse(kind.as_str()), kind);
		}
	}

	#[test]
	fn typing_kind_unknown_is_text() {
		assert_eq!(TypingKind::parse("playing_a_game"), TypingKind::Text);
		assert_eq!(TypingKind::parse(""), TypingKind::Text);
		assert_eq!(TypingKind::parse("Recording_Voice"), TypingKind::Text);
	}

	#[test]
	fn typing_kind_is_read_from_the_request_body() {
		let recording = body(json!({
			"typing": true,
			"timeout": 30000,
			"im.mxg.typing.kind": "recording_voice",
		}));

		assert_eq!(TypingKind::from_request_body(Some(&recording)), TypingKind::RecordingVoice);
	}

	#[test]
	fn typing_kind_missing_or_malformed_is_text() {
		let plain = body(json!({ "typing": true, "timeout": 30000 }));
		let unknown = body(json!({ "typing": true, "im.mxg.typing.kind": "playing_a_game" }));
		let number = body(json!({ "typing": true, "im.mxg.typing.kind": 3 }));
		let object = body(json!({ "typing": true, "im.mxg.typing.kind": { "a": "b" } }));
		let array = body(json!(["recording_voice"]));

		assert_eq!(TypingKind::from_request_body(None), TypingKind::Text);
		for request in [plain, unknown, number, object, array] {
			assert_eq!(
				TypingKind::from_request_body(Some(&request)),
				TypingKind::Text,
				"{request:?}"
			);
		}
	}

	#[test]
	fn typing_kinds_list_only_users_not_typing_text() {
		let content =
			TypingUsers::new([(ada(), TypingKind::RecordingVoice), (grace(), TypingKind::Text)]);

		assert_eq!(content.user_ids, [ada(), grace()]);
		assert_eq!(
			serde_json::to_value(&content).expect("typing content serializes"),
			json!({
				"user_ids": ["@ada:example.com", "@grace:example.com"],
				"im.mxg.typing.kinds": { "@ada:example.com": "recording_voice" },
			}),
		);
	}

	#[test]
	fn typing_without_kinds_is_the_plain_event() {
		let content = TypingUsers::new([(ada(), TypingKind::Text)]);

		assert_eq!(
			serde_json::to_value(&content).expect("typing content serializes"),
			json!({ "user_ids": ["@ada:example.com"] }),
		);

		let nobody = TypingUsers::new(Vec::new());

		assert_eq!(
			serde_json::to_value(&nobody).expect("typing content serializes"),
			json!({ "user_ids": [] }),
		);
	}

	#[test]
	fn typing_events_carry_the_kinds_to_clients_and_appservices() {
		let content = TypingUsers::new([(ada(), TypingKind::UploadingPhoto)]);
		let inner = json!({
			"user_ids": ["@ada:example.com"],
			"im.mxg.typing.kinds": { "@ada:example.com": "uploading_photo" },
		});

		assert_eq!(content.sync_event(), json!({ "type": "m.typing", "content": inner }));
		assert_eq!(
			content.room_event(room_id!("!room:example.com")),
			json!({
				"type": "m.typing",
				"content": inner,
				"room_id": "!room:example.com",
			}),
		);
	}
}
