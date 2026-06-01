use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::TryStreamExt;
use http::StatusCode;
use ruma::{
	OwnedDeviceId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId, api::error::ErrorKind,
};
use serde::{Deserialize, Serialize};
use serde_json::value::to_raw_value;
use tokio::sync::Mutex as AsyncMutex;
use tuwunel_core::{
	Err, Error, Result, err, implement, trace,
	utils::{TryReadyExt, millis_since_unix_epoch},
};
use tuwunel_database::{Deserialized, Json, Map};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DelayedEvent {
	pub delay_id: String,
	pub room_id: OwnedRoomId,
	pub sender_user: OwnedUserId,
	pub sender_device: Option<OwnedDeviceId>,
	pub event_type: String,
	pub state_key: Option<String>,
	pub txn_id: Option<String>,
	pub content: serde_json::Value,
	pub delay_ms: u64,
	pub timeout: u64,
	pub running_since: u64,
	pub status: DelayedEventStatus,
	pub error: Option<serde_json::Value>,
	pub event_id: Option<OwnedEventId>,
	pub finalised_ts: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum DelayedEventStatus {
	Scheduled,
	Finalized,
}

pub struct Service {
	services: Arc<crate::services::OnceServices>,
	delayid_event: Arc<Map>,
	delayedevent_timeout: Arc<Map>,
	sender_delayedevents: Arc<Map>,
	/// Per-user async mutex to serialize add() calls and prevent races on
	/// the max_delayed_events_per_user check.
	user_locks: std::sync::Mutex<HashMap<OwnedUserId, Arc<AsyncMutex<()>>>>,
	/// Per-delay_id async mutex to serialize send/cancel/restart operations
	/// and prevent duplicate sends or races with the worker.
	delay_id_locks: std::sync::Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: args.services.clone(),
			delayid_event: args.db["delayid_event"].clone(),
			delayedevent_timeout: args.db["delayedevent_timeout"].clone(),
			sender_delayedevents: args.db["sender_delayedevents"].clone(),
			user_locks: std::sync::Mutex::new(HashMap::new()),
			delay_id_locks: std::sync::Mutex::new(HashMap::new()),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		let mut cleanup_interval = tokio::time::interval(Duration::from_secs(300));
		cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

		loop {
			tokio::select! {
				() = tokio::time::sleep(Duration::from_millis(100)) => {
					self.process_due_events().await;
				},
				_ = cleanup_interval.tick() => {
					self.cleanup_old_finalized().await;
				},
				() = self.services.server.until_shutdown() => return Ok(()),
			}
		}
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
fn user_lock(&self, sender_user: &UserId) -> Arc<AsyncMutex<()>> {
	let mut locks = self
		.user_locks
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner);
	locks
		.entry(sender_user.to_owned())
		.or_insert_with(|| Arc::new(AsyncMutex::new(())))
		.clone()
}

#[implement(Service)]
async fn with_delay_id_lock<F, Fut, R>(&self, delay_id: &str, f: F) -> R
where
	F: FnOnce() -> Fut,
	Fut: Future<Output = R>,
{
	let lock = {
		let mut locks = self
			.delay_id_locks
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		locks
			.entry(delay_id.to_owned())
			.or_insert_with(|| Arc::new(AsyncMutex::new(())))
			.clone()
	};
	let guard = lock.lock().await;
	let result = f().await;
	drop(guard);
	let mut locks = self
		.delay_id_locks
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner);
	// `lock` itself still holds one strong ref, and the hashmap holds another
	// while the entry exists. Remove only when no other tasks have cloned it.
	if Arc::strong_count(&lock) == 2 {
		locks.remove(delay_id);
	}
	result
}

#[implement(Service)]
async fn check_max_events(&self, sender_user: &UserId) -> Result {
	let max = self.services.config.max_delayed_events_per_user;
	if max == 0 {
		return Ok(());
	}

	let count = self.count_scheduled_by_sender(sender_user).await;

	if count >= usize::try_from(max).unwrap_or(usize::MAX) {
		return Err(Error::Request(
			ErrorKind::LimitExceeded(ruma::api::error::LimitExceededErrorData {
				retry_after: None,
			}),
			"Maximum number of scheduled delayed events reached.".into(),
			StatusCode::TOO_MANY_REQUESTS,
		));
	}

	Ok(())
}

#[implement(Service)]
#[allow(clippy::too_many_arguments)]
pub async fn add(
	&self,
	delay_id: &str,
	room_id: &RoomId,
	sender_user: &UserId,
	sender_device: Option<&OwnedDeviceId>,
	event_type: &str,
	state_key: Option<String>,
	txn_id: Option<String>,
	content: serde_json::Value,
	delay_ms: u64,
) -> Result {
	let max_delay = self.services.config.max_event_delay_ms;
	if max_delay == 0 {
		return Err!(Request(Unknown("Delayed events are not supported on this server")));
	}

	if delay_ms > max_delay {
		return Err!(Request(InvalidParam("Delay exceeds maximum allowed delay")));
	}

	let lock = self.user_lock(sender_user);
	let _guard = lock.lock().await;
	self.check_max_events(sender_user).await?;

	let now = millis_since_unix_epoch();
	let timeout = now.saturating_add(delay_ms);
	let event = DelayedEvent {
		delay_id: delay_id.to_owned(),
		room_id: room_id.to_owned(),
		sender_user: sender_user.to_owned(),
		sender_device: sender_device.cloned(),
		event_type: event_type.to_owned(),
		state_key,
		txn_id,
		content,
		delay_ms,
		timeout,
		running_since: now,
		status: DelayedEventStatus::Scheduled,
		error: None,
		event_id: None,
		finalised_ts: None,
	};

	self.delayid_event.raw_put(delay_id, Json(&event));
	self.delayedevent_timeout
		.put_raw((timeout, delay_id), []);
	self.sender_delayedevents
		.put_raw((sender_user.as_str(), delay_id), []);

	Ok(())
}

#[implement(Service)]
pub async fn get(&self, delay_id: &str) -> Result<Option<DelayedEvent>> {
	match self
		.delayid_event
		.get(delay_id)
		.await
		.deserialized()
	{
		| Ok(event) => Ok(Some(event)),
		| Err(_) => Ok(None),
	}
}

#[implement(Service)]
pub async fn get_by_sender(&self, sender_user: &UserId) -> Result<Vec<DelayedEvent>> {
	let mut events = Vec::new();

	let keys: Vec<String> = self
		.sender_delayedevents
		.keys::<(&str, &str)>()
		.ready_try_take_while(|(sender, _)| Ok(*sender == sender_user.as_str()))
		.map_ok(|(_, delay_id): (&str, &str)| delay_id.to_owned())
		.try_collect()
		.await
		.unwrap_or_default();

	for delay_id in keys {
		if let Ok(Some(event)) = self.get(&delay_id).await {
			events.push(event);
		}
	}

	Ok(events)
}

#[implement(Service)]
pub async fn count_scheduled_by_sender(&self, sender_user: &UserId) -> usize {
	self.get_by_sender(sender_user)
		.await
		.map(|events| {
			events
				.into_iter()
				.filter(|e| matches!(e.status, DelayedEventStatus::Scheduled))
				.count()
		})
		.unwrap_or(0)
}

#[implement(Service)]
pub async fn remove(&self, delay_id: &str) -> Result {
	self.with_delay_id_lock(delay_id, || async move {
		if let Ok(Some(event)) = self.get(delay_id).await {
			self.delayedevent_timeout
				.del((event.timeout, delay_id));
			self.sender_delayedevents
				.del((event.sender_user.as_str(), delay_id));
		}

		self.delayid_event.remove(delay_id);
		Ok(())
	})
	.await
}

#[implement(Service)]
pub async fn cancel(&self, delay_id: &str, sender_user: &UserId) -> Result {
	trace!(?delay_id, "Cancelling delayed event");

	self.with_delay_id_lock(delay_id, || async move {
		let Some(event) = self.get(delay_id).await? else {
			return Ok(()); // Already gone = idempotent success
		};

		if event.sender_user != sender_user {
			return Err!(Request(NotFound("Delayed event not found")));
		}

		if matches!(event.status, DelayedEventStatus::Finalized) {
			return Ok(());
		}

		self.delayedevent_timeout
			.del((event.timeout, delay_id));
		self.sender_delayedevents
			.del((sender_user.as_str(), delay_id));
		self.delayid_event.remove(delay_id);

		Ok(())
	})
	.await
}

#[implement(Service)]
pub async fn send_now(
	&self,
	delay_id: &str,
	sender_user: &UserId,
) -> Result<Option<OwnedEventId>> {
	self.with_delay_id_lock(delay_id, || async move {
		let Some(event) = self.get(delay_id).await? else {
			return Ok(None); // Already sent and cleaned up
		};

		if event.sender_user != sender_user {
			return Err!(Request(NotFound("Delayed event not found")));
		}

		if matches!(event.status, DelayedEventStatus::Finalized) {
			return Ok(event.event_id.clone());
		}

		// If the event was restarted to a later time after the worker scanned
		// the timeout queue, don't send it yet. Re-insert the new timeout.
		let now = millis_since_unix_epoch();
		if event.timeout > now {
			self.delayedevent_timeout
				.put_raw((event.timeout, delay_id), []);
			return Ok(None);
		}

		// Remove from timeout queue so the worker doesn't retry
		self.delayedevent_timeout
			.del((event.timeout, delay_id));

		// Validate at send time
		if let Err(e) = self.validate_send_time(&event).await {
			trace!(?delay_id, "Delayed event validation failed: {e}");
			let error_json = serde_json::json!({
				"errcode": format!("{e:?}"),
				"error": e.to_string(),
			});
			let mut event = event;
			event.status = DelayedEventStatus::Finalized;
			event.error = Some(error_json);
			event.finalised_ts = Some(millis_since_unix_epoch());
			// Keep sender_delayedevents so user can see the error in their list
			self.delayid_event.raw_put(delay_id, Json(&event));
			return Err(e);
		}

		let content = to_raw_value(&event.content).map_err(|e| {
			err!(Request(BadJson("Failed to serialize delayed event content: {e}")))
		})?;

		// Build unsigned with transaction_id and delay_id per spec
		let mut unsigned = event
			.txn_id
			.as_ref()
			.map(|id| {
				std::iter::once((
					"transaction_id".to_owned(),
					serde_json::Value::String(id.clone()),
				))
				.collect::<std::collections::BTreeMap<String, serde_json::Value>>()
			})
			.unwrap_or_default();

		unsigned.insert(
			"org.matrix.msc4140.delay_id".to_owned(),
			serde_json::Value::String(delay_id.to_owned()),
		);

		let event_id = self
			.services
			.timeline
			.build_and_append_pdu(
				tuwunel_core::matrix::pdu::PduBuilder {
					event_type: event.event_type.clone().into(),
					content,
					state_key: event.state_key.clone().map(Into::into),
					unsigned: Some(unsigned),
					..Default::default()
				},
				&event.sender_user,
				&event.room_id,
				&self
					.services
					.state
					.mutex
					.lock(&event.room_id)
					.await,
			)
			.await?;

		// Success: clean up everything
		self.sender_delayedevents
			.del((sender_user.as_str(), delay_id));
		self.delayid_event.remove(delay_id);

		Ok(Some(event_id))
	})
	.await
}

#[implement(Service)]
pub async fn restart(&self, delay_id: &str, sender_user: &UserId) -> Result {
	self.with_delay_id_lock(delay_id, || async move {
		let Some(mut event) = self.get(delay_id).await? else {
			return Err!(Request(NotFound("Delayed event not found")));
		};

		if event.sender_user != sender_user {
			return Err!(Request(NotFound("Delayed event not found")));
		}

		if matches!(event.status, DelayedEventStatus::Finalized) {
			return Err!(Request(Unknown("Delayed event has already been finalized")));
		}

		self.delayedevent_timeout
			.del((event.timeout, delay_id));

		let new_timeout = millis_since_unix_epoch().saturating_add(event.delay_ms);
		event.timeout = new_timeout;
		event.running_since = millis_since_unix_epoch();
		self.delayid_event.raw_put(delay_id, Json(&event));
		self.delayedevent_timeout
			.put_raw((new_timeout, delay_id), []);

		Ok(())
	})
	.await
}

#[implement(Service)]
pub async fn cancel_matching_state_events(
	&self,
	room_id: &RoomId,
	event_type: &str,
	state_key: &str,
	sender_user: &UserId,
) -> usize {
	let keys: Vec<String> = self
		.delayid_event
		.keys::<&str>()
		.ready_try_take_while(|_| Ok(true))
		.map_ok(|delay_id: &str| delay_id.to_owned())
		.try_collect()
		.await
		.unwrap_or_default();

	let mut cancelled = 0_usize;

	for delay_id in keys {
		let Ok(Some(event)) = self.get(&delay_id).await else {
			continue;
		};

		if !matches!(event.status, DelayedEventStatus::Scheduled)
			|| event.room_id.as_str() != room_id.as_str()
			|| event.event_type != event_type
			|| event.state_key.as_deref() != Some(state_key)
			|| event.sender_user == sender_user
		{
			continue;
		}

		let _: Result = self.remove(&delay_id).await;
		cancelled = cancelled.saturating_add(1);
	}

	cancelled
}

#[implement(Service)]
async fn validate_send_time(&self, event: &DelayedEvent) -> Result {
	let content_json = serde_json::to_string(&event.content)
		.map_err(|e| err!(Request(BadJson("Failed to serialize delayed event content: {e}"))))?;

	if let Some(state_key) = &event.state_key {
		// State event: validate at send time
		let event_type: ruma::events::StateEventType = event.event_type.clone().into();

		let content: ruma::serde::Raw<ruma::events::AnyStateEventContent> =
			ruma::serde::Raw::from_json_string(content_json)
				.map_err(|e| err!(Request(BadJson("Invalid raw content: {e}"))))?;

		crate::rooms::event_policy::allowed_to_send_state_event(
			&self.services,
			&event.room_id,
			&event_type,
			state_key,
			&content,
		)
		.await
	} else {
		// Message-like event: validate at send time
		let event_type: ruma::events::MessageLikeEventType = event.event_type.clone().into();

		let content: ruma::serde::Raw<ruma::events::AnyMessageLikeEventContent> =
			ruma::serde::Raw::from_json_string(content_json)
				.map_err(|e| err!(Request(BadJson("Invalid raw content: {e}"))))?;

		crate::rooms::event_policy::validate_message_event_policy(
			&self.services,
			&event.sender_user,
			&event_type,
			&content,
			&event.room_id,
		)
		.await
	}
}

#[implement(Service)]
async fn process_due_events(&self) {
	let now = millis_since_unix_epoch();
	let due: Vec<String> = self
		.delayedevent_timeout
		.keys::<(u64, &str)>()
		.ready_try_take_while(|(timeout, _)| Ok(*timeout <= now))
		.map_ok(|(_, delay_id): (u64, &str)| delay_id.to_owned())
		.try_collect()
		.await
		.unwrap_or_default();

	for delay_id in due {
		trace!(?delay_id, "Sending due delayed event");
		// Need to look up the sender to call send_now; if the event is
		// gone (e.g. cancelled concurrently), skip it.
		let Ok(Some(event)) = self.get(&delay_id).await else {
			continue;
		};
		if let Err(e) = self.send_now(&delay_id, &event.sender_user).await {
			trace!(?delay_id, "Failed to send delayed event: {e}");
		}
	}
}

#[implement(Service)]
async fn cleanup_old_finalized(&self) {
	let cutoff = millis_since_unix_epoch().saturating_sub(24 * 60 * 60 * 1000);
	let mut removed: usize = 0;

	let keys: Vec<String> = self
		.delayid_event
		.keys::<&str>()
		.ready_try_take_while(|_| Ok(true))
		.map_ok(|delay_id: &str| delay_id.to_owned())
		.try_collect()
		.await
		.unwrap_or_default();

	for delay_id in keys {
		if let Ok(Some(event)) = self.get(&delay_id).await
			&& matches!(event.status, DelayedEventStatus::Finalized)
			&& let Some(ts) = event.finalised_ts
			&& ts <= cutoff
		{
			self.delayedevent_timeout
				.del((event.timeout, &delay_id));
			self.sender_delayedevents
				.del((event.sender_user.as_str(), &delay_id));
			self.delayid_event.remove(&delay_id);
			removed = removed.saturating_add(1);
		}
	}

	if removed > 0 {
		trace!(removed, "Cleaned up old finalized delayed events");
	}
}
