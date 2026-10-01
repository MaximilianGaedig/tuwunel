//! Presence update pipeline.
//!
//! This module centralizes the write path for presence updates. It keeps the
//! aggregation and timer logic in one place so the public `Service` surface
//! remains small and the update flow is easy to review.

use std::time::Duration;

use futures::TryFutureExt;
use ruma::{
	DeviceId, OwnedUserId, UInt, UserId,
	events::presence::{PresenceEvent, PresenceEventContent},
	presence::PresenceState,
};
use tokio::time::sleep;
use tuwunel_core::{
	Error, Result, debug,
	debug::INFO_SPAN_LEVEL,
	error,
	result::LogErr,
	trace,
	utils::{future::OptionFutureExt, option::OptionExt},
};

use super::{
	Ping, Service, TimerFired,
	aggregate::{self, StatusMsg},
};

impl Service {
	/// How old the stored "last active" may get while explicit updates keep saying the same
	/// thing, before one of them is written after all.
	///
	/// It has to be longer than the few minutes between a bridge's repeats, or every repeat would
	/// still be a row. What bounds it from above is other servers: one that hears nothing about a
	/// user for half an hour takes them for gone. Synapse draws the same two lines, never telling
	/// anyone about a newer "last active" while the user is still active, and repeating itself to
	/// other servers every twenty-five minutes for the sake of their thirty-minute timeout
	/// (`FEDERATION_PING_INTERVAL` and `FEDERATION_TIMEOUT` in its presence handler).
	const REASSERT_REFRESH_MS: u64 = 25 * 60 * 1000;

	fn device_key(device_id: Option<&DeviceId>, is_remote: bool) -> aggregate::DeviceKey {
		if is_remote {
			return aggregate::DeviceKey::Remote;
		}

		match device_id {
			| Some(device_id) => aggregate::DeviceKey::Device(device_id.to_owned()),
			| None => aggregate::DeviceKey::UnknownLocal,
		}
	}

	fn schedule_presence_timer(
		&self,
		user_id: &UserId,
		presence_state: &PresenceState,
		count: u64,
	) -> Result {
		if !(self.timeout_remote_users || self.services.globals.user_is_local(user_id))
			|| user_id == self.services.globals.server_user
		{
			return Ok(());
		}

		let timeout = match presence_state {
			| PresenceState::Online =>
				self.services
					.server
					.config
					.presence_idle_timeout_s,
			| _ =>
				self.services
					.server
					.config
					.presence_offline_timeout_s,
		};

		self.timer_channel
			.0
			.send((user_id.to_owned(), Duration::from_secs(timeout), count))
			.map_err(|e| {
				error!("Failed to add presence timer: {}", e);
				Error::bad_database("Failed to add presence timer")
			})
	}

	fn refresh_skip_decision(
		refresh_window_ms: Option<u64>,
		last_event: Option<&PresenceEvent>,
		last_count: Option<u64>,
	) -> Option<(u64, u64)> {
		let (Some(refresh_ms), Some(event), Some(count)) =
			(refresh_window_ms, last_event, last_count)
		else {
			return None;
		};

		let last_last_active_ago: u64 = event.content.last_active_ago?.into();

		(last_last_active_ago < refresh_ms).then_some((count, last_last_active_ago))
	}

	/// Whether an update would leave everything a reader can see of someone as it is: the state,
	/// whether they are active right now, and the status message. When they were last active is
	/// left to the refresh window, see [`Self::refresh_skip_decision`].
	///
	/// An empty status message is the same as none: it is stored as none.
	fn changes_nothing_visible(
		last: &PresenceEventContent,
		state: &PresenceState,
		currently_active: bool,
		status_msg: Option<&str>,
	) -> bool {
		fn said(msg: Option<&str>) -> Option<&str> { msg.filter(|msg| !msg.is_empty()) }

		last.presence == *state
			&& last.currently_active.unwrap_or(false) == currently_active
			&& said(last.status_msg.as_deref()) == said(status_msg)
	}

	fn timer_is_stale(expected_count: u64, current_count: u64) -> bool {
		expected_count != current_count
	}

	#[tracing::instrument(
		name = "presence",
		level = INFO_SPAN_LEVEL,
		skip_all,
		fields(
			%user_id,
			?device_key,
			%state,
			?currently_active,
		),
	)]
	#[expect(clippy::too_many_arguments)]
	async fn apply_device_presence_update(
		&self,
		user_id: &UserId,
		device_key: aggregate::DeviceKey,
		state: &PresenceState,
		currently_active: Option<bool>,
		last_active_ago: Option<UInt>,
		status_msg: StatusMsg,
		refresh_window_ms: Option<u64>,
	) -> Result {
		let now = tuwunel_core::utils::millis_since_unix_epoch();
		let preserve_status = matches!(status_msg, StatusMsg::Unchanged);

		// 1) Capture per-device presence snapshot for aggregation.
		debug!(
			?user_id,
			?device_key,
			?state,
			currently_active,
			last_active_ago = last_active_ago.map(u64::from),
			"Presence update received"
		);

		self.device_presence
			.update(
				user_id,
				device_key,
				state,
				currently_active,
				last_active_ago,
				status_msg,
				now,
			)
			.await;

		// 2) Compute the aggregated presence across all devices.
		let aggregated = self
			.device_presence
			.aggregate(user_id, now, self.idle_timeout, self.offline_timeout)
			.await;

		debug!(
			?user_id,
			agg_state = ?aggregated.state,
			agg_currently_active = aggregated.currently_active,
			agg_last_active_ts = aggregated.last_active_ts,
			agg_device_count = aggregated.device_count,
			"Presence aggregate computed"
		);

		// 3) Load the last persisted presence to decide whether to skip or merge.
		let last_presence = self.db.get_presence(user_id).await;
		let (last_count, last_event) = match last_presence {
			| Ok((count, event)) => (Some(count), Some(event)),
			| Err(_) => (None, None),
		};

		let last_state = last_event
			.as_ref()
			.map(|event| event.content.presence.clone());

		// 4) Unchanged preserves the last non-empty status; explicit None clears it.
		let fallback_status = || {
			last_event
				.as_ref()
				.and_then(|event| event.content.status_msg.clone())
				.filter(|msg| !msg.is_empty())
		};

		let status_msg = aggregated
			.status_msg
			.clone()
			.or_else(|| preserve_status.then(fallback_status).flatten());

		// 5) An update that shows nothing new, soon after the last one, writes nothing: a new
		// row is a new stream position, and that wakes the sync of everyone who can see this
		// user. The device snapshot above has already taken the update, and the timer is set
		// again here, so going idle is still measured from this moment rather than from the row.
		let nothing_new = last_event.as_ref().is_some_and(|event| {
			Self::changes_nothing_visible(
				&event.content,
				&aggregated.state,
				aggregated.currently_active,
				status_msg.as_deref(),
			)
		});

		if nothing_new
			&& let Some((count, last_last_active_ago)) =
				Self::refresh_skip_decision(refresh_window_ms, last_event.as_ref(), last_count)
		{
			let presence = last_event
				.as_ref()
				.map(|event| &event.content.presence)
				.unwrap_or(state);

			self.schedule_presence_timer(user_id, presence, count)
				.log_err()
				.ok();

			debug!(
				?user_id,
				?state,
				last_last_active_ago,
				"Skipping presence update: refresh window (timer rescheduled)"
			);

			return Ok(());
		}

		// 6) If we just transitioned away from online, flush suppressed pushes.
		if matches!(last_state, Some(PresenceState::Online))
			&& aggregated.state != PresenceState::Online
		{
			debug!(
				?user_id,
				from = ?PresenceState::Online,
				to = ?aggregated.state,
				"Presence went inactive; flushing suppressed pushes"
			);

			self.services
				.sending
				.schedule_flush_suppressed_for_user(
					user_id.to_owned(),
					"presence->inactive (aggregate)",
				);
		}

		let last_active_ago =
			Some(UInt::new_saturating(now.saturating_sub(aggregated.last_active_ts)));

		self.set_presence(
			user_id,
			&aggregated.state,
			Some(aggregated.currently_active),
			last_active_ago,
			status_msg,
		)
		.await
	}

	/// Pings the presence of the given user, defaulting the state to online.
	///
	/// Requests authenticated with an appservice token do not imply user
	/// activity. In particular, they must not update presence or device
	/// last-seen data. Explicit appservice presence updates use
	/// [`Self::set_presence_for_device`] instead.
	pub async fn maybe_ping_presence(&self, user_id: &UserId, args: Ping<'_>) -> Result {
		const REFRESH_TIMEOUT: u64 = 30 * 1000;

		if args.appservice.is_some()
			|| !self.services.server.config.allow_local_presence
			|| self.services.db.is_read_only()
		{
			return Ok(());
		}

		let update_device_seen = args.device_id.map_async(|device_id| {
			self.services
				.users
				.update_device_last_seen(user_id, device_id, args.client_ip, None)
		});

		let new_state = args.new_state.unwrap_or(&PresenceState::Online);
		let currently_active = *new_state == PresenceState::Online;
		let set_presence = self.apply_device_presence_update(
			user_id,
			Self::device_key(args.device_id, false),
			new_state,
			Some(currently_active),
			UInt::new(0),
			StatusMsg::Unchanged,
			Some(REFRESH_TIMEOUT),
		);

		debug!(?user_id, ?new_state, currently_active, "Presence ping accepted");

		futures::future::try_join(set_presence, update_device_seen.unwrap_or(Ok(())))
			.map_ok(|_| ())
			.await
	}

	/// Applies an explicit presence update for a local device.
	///
	/// A bridge sets "online" for each of its users again every few minutes for as long as the
	/// network says so, because presence here goes idle by itself if nobody repeats it. Such a
	/// repeat says nothing new, so it writes nothing, see [`Self::REASSERT_REFRESH_MS`]; it keeps
	/// the user from going idle all the same.
	pub async fn set_presence_for_device(
		&self,
		user_id: &UserId,
		device_id: Option<&DeviceId>,
		state: &PresenceState,
		status_msg: Option<String>,
	) -> Result {
		let currently_active = *state == PresenceState::Online;
		self.apply_device_presence_update(
			user_id,
			Self::device_key(device_id, false),
			state,
			Some(currently_active),
			None,
			StatusMsg::Set(status_msg),
			Some(Self::REASSERT_REFRESH_MS),
		)
		.await
	}

	/// Applies a presence update received over federation.
	pub async fn set_presence_from_federation(
		&self,
		user_id: &UserId,
		state: &PresenceState,
		currently_active: bool,
		last_active_ago: UInt,
		status_msg: Option<String>,
	) -> Result {
		self.apply_device_presence_update(
			user_id,
			Self::device_key(None, true),
			state,
			Some(currently_active),
			Some(last_active_ago),
			StatusMsg::Set(status_msg),
			None,
		)
		.await
	}

	/// Adds a presence event which will be saved until a new event replaces it.
	pub async fn set_presence(
		&self,
		user_id: &UserId,
		state: &PresenceState,
		currently_active: Option<bool>,
		last_active_ago: Option<UInt>,
		status_msg: Option<String>,
	) -> Result {
		let presence_state = match state.as_str() {
			| "" => &PresenceState::Offline, // default an empty string to 'offline'
			| &_ => state,
		};

		let count = self
			.db
			.set_presence(user_id, presence_state, currently_active, last_active_ago, status_msg)
			.await?;

		if let Some(count) = count {
			let is_local = self.services.globals.user_is_local(user_id);
			let is_server_user = user_id == self.services.globals.server_user;
			let allow_timeout = self.timeout_remote_users || is_local;

			if allow_timeout && !is_server_user {
				self.schedule_presence_timer(user_id, presence_state, count)?;
			}
		}

		Ok(())
	}

	pub(super) async fn process_presence_timer(
		&self,
		user_id: &OwnedUserId,
		expected_count: u64,
	) -> Result {
		let Ok((current_count, presence)) = self.db.get_presence_raw(user_id).await else {
			return Ok(());
		};

		if Self::timer_is_stale(expected_count, current_count) {
			trace!(?user_id, expected_count, current_count, "Skipping stale presence timer");
			return Ok(());
		}

		let presence_state = presence.state.clone();
		let now = tuwunel_core::utils::millis_since_unix_epoch();
		let aggregated = self
			.device_presence
			.aggregate(user_id, now, self.idle_timeout, self.offline_timeout)
			.await;

		if aggregated.device_count == 0 {
			let last_active_ago =
				Some(UInt::new_saturating(now.saturating_sub(presence.last_active_ts)));
			let status_msg = presence.status_msg;

			let new_state = match (&presence_state, last_active_ago.map(u64::from)) {
				| (PresenceState::Online, Some(ago)) if ago >= self.idle_timeout =>
					Some(PresenceState::Unavailable),
				| (PresenceState::Unavailable, Some(ago)) if ago >= self.offline_timeout =>
					Some(PresenceState::Offline),
				| _ => None,
			};

			debug!(
				"Processed presence timer for user '{user_id}': Old state = {presence_state}, \
				 New state = {new_state:?}"
			);

			if let Some(new_state) = new_state {
				if matches!(new_state, PresenceState::Unavailable | PresenceState::Offline) {
					self.services
						.sending
						.schedule_flush_suppressed_for_user(
							user_id.to_owned(),
							"presence->inactive",
						);
				}
				self.set_presence(user_id, &new_state, Some(false), last_active_ago, status_msg)
					.await?;
			}

			return Ok(());
		}

		if aggregated.state == presence_state {
			self.schedule_presence_timer(user_id, &presence_state, current_count)
				.log_err()
				.ok();
			return Ok(());
		}

		if matches!(aggregated.state, PresenceState::Unavailable | PresenceState::Offline) {
			self.services
				.sending
				.schedule_flush_suppressed_for_user(user_id.to_owned(), "presence->inactive");
		}

		let status_msg = aggregated.status_msg.or(presence.status_msg);
		let last_active_ago =
			Some(UInt::new_saturating(now.saturating_sub(aggregated.last_active_ts)));

		self.set_presence(
			user_id,
			&aggregated.state,
			Some(aggregated.currently_active),
			last_active_ago,
			status_msg,
		)
		.await?;

		Ok(())
	}
}

pub(super) async fn presence_timer(
	user_id: OwnedUserId,
	timeout: Duration,
	count: u64,
) -> TimerFired {
	sleep(timeout).await;

	(user_id, count)
}

#[cfg(test)]
mod tests {
	use futures::StreamExt;
	use ruma::{uint, user_id};
	use tuwunel_core::{
		config::Figment,
		utils::{ReadyExt, millis_since_unix_epoch},
	};

	use super::*;
	use crate::{
		activity_log::{Entry, Kind},
		test_utils::fixture,
	};

	#[test]
	fn refresh_window_skip_decision() {
		let user_id = user_id!("@alice:example.com");
		let event = PresenceEvent {
			sender: user_id.to_owned(),
			content: ruma::events::presence::PresenceEventContent {
				presence: PresenceState::Online,
				status_msg: None,
				currently_active: Some(true),
				last_active_ago: Some(uint!(10)),
				avatar_url: None,
				displayname: None,
			},
		};

		let decision = Service::refresh_skip_decision(Some(20), Some(&event), Some(5));
		assert_eq!(decision, Some((5, 10)));

		let decision = Service::refresh_skip_decision(Some(5), Some(&event), Some(5));
		assert_eq!(decision, None);

		let event_missing_ago = PresenceEvent {
			sender: user_id.to_owned(),
			content: ruma::events::presence::PresenceEventContent {
				presence: PresenceState::Online,
				status_msg: None,
				currently_active: Some(true),
				last_active_ago: None,
				avatar_url: None,
				displayname: None,
			},
		};

		let decision =
			Service::refresh_skip_decision(Some(20), Some(&event_missing_ago), Some(5));
		assert_eq!(decision, None);

		let decision = Service::refresh_skip_decision(Some(20), None, Some(5));
		assert_eq!(decision, None);
	}

	#[test]
	fn timer_stale_detection() {
		assert!(Service::timer_is_stale(2, 3));
		assert!(!Service::timer_is_stale(2, 2));
	}

	fn shown(
		presence: PresenceState,
		currently_active: Option<bool>,
		status_msg: Option<&str>,
		last_active_ago: u64,
	) -> PresenceEvent {
		PresenceEvent {
			sender: user_id!("@ghost:example.com").to_owned(),
			content: PresenceEventContent {
				presence,
				status_msg: status_msg.map(ToOwned::to_owned),
				currently_active,
				last_active_ago: Some(UInt::new_saturating(last_active_ago)),
				avatar_url: None,
				displayname: None,
			},
		}
	}

	// A repeat is the same state, the same "active now" and the same status message. Anything
	// else is news and must be written: a missed case here is a change nobody is told about.
	#[test]
	fn a_repeat_shows_nothing_new_and_a_change_does() {
		let online = shown(PresenceState::Online, Some(true), None, 0).content;
		let same = |state: &PresenceState, active, msg| {
			Service::changes_nothing_visible(&online, state, active, msg)
		};

		assert!(same(&PresenceState::Online, true, None));
		// Stored as none, so an empty message is not a new one.
		assert!(same(&PresenceState::Online, true, Some("")));

		assert!(!same(&PresenceState::Unavailable, true, None));
		assert!(!same(&PresenceState::Offline, true, None));
		assert!(!same(&PresenceState::Online, false, None));
		assert!(!same(&PresenceState::Online, true, Some("in a call")));

		let busy = shown(PresenceState::Online, Some(true), Some("in a call"), 0).content;
		assert!(Service::changes_nothing_visible(
			&busy,
			&PresenceState::Online,
			true,
			Some("in a call")
		));
		// Clearing a message and changing one are both news.
		assert!(!Service::changes_nothing_visible(&busy, &PresenceState::Online, true, None));
		assert!(!Service::changes_nothing_visible(
			&busy,
			&PresenceState::Online,
			true,
			Some("in a meeting")
		));

		// A row that never said whether they were active reads as not active.
		let unsaid = shown(PresenceState::Unavailable, None, None, 0).content;
		assert!(Service::changes_nothing_visible(
			&unsaid,
			&PresenceState::Unavailable,
			false,
			None
		));
		assert!(!Service::changes_nothing_visible(
			&unsaid,
			&PresenceState::Unavailable,
			true,
			None
		));
	}

	// The window is what keeps the stored "last active" from growing old without bound: a repeat
	// is skipped while the row is younger than the window and written from the window on. One
	// millisecond either side of it, and no window at all (federation), which never skips.
	#[test]
	fn a_repeat_is_skipped_only_inside_the_refresh_window() {
		let window = Service::REASSERT_REFRESH_MS;
		let aged = |ago| shown(PresenceState::Online, Some(true), None, ago);
		let decide =
			|ago, window| Service::refresh_skip_decision(window, Some(&aged(ago)), Some(7));

		assert_eq!(decide(0, Some(window)), Some((7, 0)));
		let inside = window.saturating_sub(1);
		assert_eq!(decide(inside, Some(window)), Some((7, inside)));
		assert_eq!(decide(window, Some(window)), None);
		assert_eq!(decide(window.saturating_add(1), Some(window)), None);
		assert_eq!(decide(0, None), None);

		// Longer than the four minutes between a bridge's repeats, or every repeat is a row;
		// under the half hour after which another server takes a user it hears nothing of for
		// gone, with one more repeat's worth to spare, since a row is only written when a repeat
		// arrives.
		let bridge_repeats_every = 4 * 60 * 1000;
		let other_servers_give_up_after = 30 * 60 * 1000;
		assert!(window > bridge_repeats_every);
		assert!(window.saturating_add(bridge_repeats_every) < other_servers_give_up_after);
	}

	async fn rows_of(service: &Service, user_id: &UserId) -> Vec<u64> {
		service
			.presence_since(0, None)
			.ready_filter_map(|(user, count, _)| (user == user_id).then_some(count))
			.collect()
			.await
	}

	/// The newest timer asked for since the last look, the way the worker would be left with it:
	/// a later one for the same user replaces an earlier one.
	fn timer_set(service: &Service) -> Option<(OwnedUserId, Duration, u64)> {
		let mut newest = None;
		while let Ok(timer) = service.timer_channel.1.try_recv() {
			newest = Some(timer);
		}

		newest
	}

	async fn logged(services: &crate::Services, user_id: &UserId) -> Vec<Entry> {
		services
			.activity_log
			.entries(user_id, 0, u64::MAX)
			.collect()
			.await
	}

	fn kinds(entries: &[Entry]) -> Vec<Kind> { entries.iter().map(|entry| entry.kind).collect() }

	// What a bridge does to one of its users, through the real tables: "online", the same again,
	// and then nothing. The repeat must not be a new row (a new row wakes every sync that can see
	// the user), must not be a row in the activity log either, and must still push going idle
	// back to a full timeout after it; the changes that follow are rows in both, as before.
	//
	// The worker that sleeps on the timers is not running here, so the test reads which timer was
	// asked for and fires it by hand once that long has passed.
	#[tokio::test]
	async fn a_repeated_online_keeps_its_row_and_the_user_still_goes_offline() -> Result {
		const IDLE: Duration = Duration::from_secs(1);
		// Long enough after going idle that a slow machine does not get there early.
		const OFFLINE: Duration = Duration::from_secs(5);
		const MARGIN: Duration = Duration::from_millis(100);

		let config = Figment::new()
			.merge(("presence_idle_timeout_s", IDLE.as_secs()))
			.merge(("presence_offline_timeout_s", OFFLINE.as_secs()));
		let Some(fixture) = fixture(config).await? else {
			return Ok(());
		};

		let services = &fixture.services;
		let service = &services.presence;
		let ghost = user_id!("@ghost:localhost");
		let online = &PresenceState::Online;

		service
			.set_presence_for_device(ghost, None, online, None)
			.await?;
		let first = rows_of(service, ghost).await;
		assert_eq!(first.len(), 1, "{first:?}");
		assert_eq!(timer_set(service), Some((ghost.to_owned(), IDLE, first[0])));
		assert_eq!(kinds(&logged(services, ghost).await), [Kind::Online]);

		// The same again: no new row, and the timer set again for the row there is.
		service
			.set_presence_for_device(ghost, None, online, None)
			.await?;
		assert_eq!(rows_of(service, ghost).await, first, "a repeat was written as a new row");
		assert_eq!(timer_set(service), Some((ghost.to_owned(), IDLE, first[0])));
		assert_eq!(kinds(&logged(services, ghost).await), [Kind::Online]);

		// A status message is news, once: written when it appears, not when it is repeated, and
		// written again when it goes.
		let in_a_call = || Some("in a call".to_owned());
		service
			.set_presence_for_device(ghost, None, online, in_a_call())
			.await?;
		let second = rows_of(service, ghost).await;
		assert_eq!(second.len(), 1, "{second:?}");
		assert!(second[0] > first[0], "a new status message was not written");
		service
			.set_presence_for_device(ghost, None, online, in_a_call())
			.await?;
		assert_eq!(rows_of(service, ghost).await, second);
		service
			.set_presence_for_device(ghost, None, online, None)
			.await?;
		let third = rows_of(service, ghost).await;
		assert!(third[0] > second[0], "a cleared status message was not written");
		// None of that was coming or going.
		assert_eq!(kinds(&logged(services, ghost).await), [Kind::Online]);

		// The last repeat, and then silence. Idle is counted from this, not from the row.
		timer_set(service);
		let repeated_at = millis_since_unix_epoch();
		service
			.set_presence_for_device(ghost, None, online, None)
			.await?;
		assert_eq!(rows_of(service, ghost).await, third);
		let (user, wait, count) = timer_set(service).expect("the repeat set the timer again");
		assert_eq!((&*user, wait, count), (ghost, IDLE, third[0]));

		sleep(wait.saturating_add(MARGIN)).await;
		service.process_presence_timer(&user, count).await?;
		let idle = logged(services, ghost).await;
		assert_eq!(kinds(&idle), [Kind::Online, Kind::Unavailable]);
		assert!(
			idle[1].value >= repeated_at,
			"last active is when the last repeat came, not when the row was written: {idle:?}"
		);
		let now = service.get_presence(ghost).await?.content.presence;
		assert_eq!(now, PresenceState::Unavailable);

		let (user, wait, count) = timer_set(service).expect("going idle set the offline timer");
		assert_eq!((&*user, wait), (ghost, OFFLINE));
		assert_eq!(rows_of(service, ghost).await, [count]);

		sleep(wait.saturating_add(MARGIN)).await;
		service.process_presence_timer(&user, count).await?;
		assert_eq!(kinds(&logged(services, ghost).await), [
			Kind::Online,
			Kind::Unavailable,
			Kind::Offline
		]);
		let now = service.get_presence(ghost).await?.content.presence;
		assert_eq!(now, PresenceState::Offline);

		Ok(())
	}
}
