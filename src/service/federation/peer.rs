//! Per-server reachability store backed by the `servername_status` CF.
//!
//! Bucket key layout: `servername || u32_be(now.as_secs() / window_secs)`. The
//! one-byte value is the [`Classification`]. Bursts within the same window
//! collide on the same key, which is a correct collision (the window is the
//! coalescing quantum). The storage layout is the batch.
//!
//! `window_secs` is sourced from `sender_timeout` at service build time so the
//! peer-status curve does not drift from the sender's existing quadratic
//! backoff when both observe the same peer.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{Stream, StreamExt};
use ruma::ServerName;
use tuwunel_core::{
	implement,
	utils::{stream::TryIgnore, time::now_secs},
};

/// Backoff ceiling, matching `sender_retry_backoff_limit`'s 24h default.
pub(super) const MAX_BACKOFF: Duration = Duration::from_hours(24);

/// Permanence classification supplied alongside a failure.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Classification {
	#[default]
	Transient,
	Permanent,
}

impl Classification {
	/// Unknown bytes downgrade to `Transient`; a future encoding can only
	/// soften a verdict, never wrongly escalate one against an old binary.
	#[inline]
	#[must_use]
	fn from_byte(byte: u8) -> Self {
		match byte {
			| 1 => Self::Permanent,
			| _ => Self::Transient,
		}
	}
}

impl From<Classification> for u8 {
	#[inline]
	fn from(c: Classification) -> Self {
		match c {
			| Classification::Transient => 0,
			| Classification::Permanent => 1,
		}
	}
}

/// Verdict for [`Service::should_attempt`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShouldAttempt {
	Yes,
	No {
		earliest_retry: SystemTime,
	},

	/// Eligible but should be sorted to the back of any candidate list
	/// rather than skipped outright.
	#[allow(dead_code)]
	Deprioritize,
}

#[implement(super::Service)]
pub fn record_success(&self, server: &ServerName) {
	self.statuses.del((server, self.current_bucket()));
}

#[implement(super::Service)]
pub fn record_failure(&self, server: &ServerName, classification: Classification) {
	self.statuses
		.put_raw((server, self.current_bucket()), [u8::from(classification)]);
}

#[implement(super::Service)]
#[tracing::instrument(skip(self), fields(%server), level = "trace")]
pub async fn should_attempt(&self, server: &ServerName) -> ShouldAttempt {
	let now_bucket = self.current_bucket();

	let Ok(handle) = self.statuses.qry(&(server, now_bucket)).await else {
		return ShouldAttempt::Yes;
	};

	if matches!(classify(handle.as_ref()), Classification::Permanent) {
		return ShouldAttempt::No {
			earliest_retry: self
				.bucket_start(now_bucket)
				.checked_add(MAX_BACKOFF)
				.unwrap_or_else(SystemTime::now),
		};
	}

	// streak walks back until the first gap; async `contains` predicate
	// forces an imperative loop rather than `take_while`.
	let mut streak: u32 = 1;
	while streak < self.n_max {
		let prior = now_bucket.saturating_sub(streak);
		if !self.statuses.contains(&(server, prior)).await {
			break;
		}
		streak = streak.saturating_add(1);
	}

	ShouldAttempt::No {
		earliest_retry: self.earliest_retry(now_bucket, streak),
	}
}

/// Yields one tuple per populated bucket, ordered by `(server, bucket_start)`.
/// The admin/metrics consumer groups adjacent rows per server to reconstruct
/// streak and latest-failure information.
#[implement(super::Service)]
pub fn peer_snapshot(
	&self,
) -> impl Stream<Item = (&ServerName, SystemTime, Classification)> + Send + '_ {
	self.statuses.stream().ignore_err().map(
		move |((server, bucket), value): ((&ServerName, u32), &[u8])| {
			(server, self.bucket_start(bucket), classify(value))
		},
	)
}

#[implement(super::Service)]
#[inline]
#[must_use]
fn current_bucket(&self) -> u32 {
	let bucket = now_secs()
		.checked_div(self.window_secs.max(1))
		.unwrap_or(0);

	u32::try_from(bucket).unwrap_or(u32::MAX)
}

/// Wall-clock instant at the start of `bucket`.
#[implement(super::Service)]
#[inline]
#[must_use]
fn bucket_start(&self, bucket: u32) -> SystemTime {
	let offset = u64::from(bucket).saturating_mul(self.window_secs);

	UNIX_EPOCH
		.checked_add(Duration::from_secs(offset))
		.unwrap_or(UNIX_EPOCH)
}

#[implement(super::Service)]
#[inline]
#[must_use]
fn earliest_retry(&self, current_bucket: u32, streak: u32) -> SystemTime {
	let window = Duration::from_secs(self.window_secs);
	let delay = window
		.saturating_mul(streak)
		.saturating_mul(streak)
		.min(MAX_BACKOFF);

	self.bucket_start(current_bucket)
		.checked_add(delay)
		.unwrap_or_else(SystemTime::now)
}

#[inline]
#[must_use]
fn classify(bytes: &[u8]) -> Classification {
	bytes
		.first()
		.copied()
		.map_or(Classification::Transient, Classification::from_byte)
}
