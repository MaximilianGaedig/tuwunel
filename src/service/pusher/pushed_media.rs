//! Media of events just pushed in full to a device.
//!
//! A notification extension (Element X's on iOS) that receives a full push
//! for a message with an image, a video or audio downloads the original file
//! from the device's own session right away. The media service uses this
//! record, together with the extension's user agent, to recognise those
//! downloads (see `nse_media_user_agents`): a download of media this server
//! just pushed to the same device within [`PUSHED_MEDIA_TTL`].
//!
//! In memory only, and only kept while `nse_media_user_agents` is set.

use std::{
	collections::HashMap,
	sync::{Mutex, MutexGuard, PoisonError},
	time::{Duration, Instant},
};

use ruma::{DeviceId, OwnedDeviceId, OwnedUserId, UserId};
use serde_json::Value;
use tuwunel_core::{debug, implement, matrix::Event};

/// How long after a full push a download of its media counts as the
/// extension's. The extension runs for at most 30 seconds.
pub const PUSHED_MEDIA_TTL: Duration = Duration::from_secs(120);

/// Entries beyond this many trigger a sweep of the expired ones.
const SWEEP_THRESHOLD: usize = 1024;

/// How deep into the content to look for media URLs (`content.file.url`,
/// `content.itemtypes[n].file.url`).
const MAX_DEPTH: usize = 4;

type Key = (OwnedUserId, OwnedDeviceId);
type Pushed = Vec<(String, Instant)>;

#[derive(Default)]
pub(super) struct PushedMedia {
	inner: Mutex<HashMap<Key, Pushed>>,
}

impl PushedMedia {
	fn lock(&self) -> MutexGuard<'_, HashMap<Key, Pushed>> {
		self.inner
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}
}

/// Remember the media of an event just pushed in full to `pushkey`.
#[implement(super::Service)]
pub(super) async fn note_pushed_media<E>(&self, user_id: &UserId, pushkey: &str, event: &E)
where
	E: Event,
{
	if self
		.services
		.config
		.nse_media_user_agents
		.is_empty()
	{
		return;
	}

	let mut uris = Vec::new();
	collect_mxc(&event.get_content_as_value(), 0, &mut uris);
	if uris.is_empty() {
		return;
	}

	let Ok(device) = self.get_pusher_device(pushkey).await else {
		debug!(%user_id, "Pusher has no device; its pushed media are not recorded");
		return;
	};

	let now = Instant::now();
	let mut pushed = self.pushed_media.lock();
	let entries = pushed
		.entry((user_id.to_owned(), device))
		.or_default();

	entries.retain(|(_, at)| now.saturating_duration_since(*at) < PUSHED_MEDIA_TTL);
	entries.extend(uris.into_iter().map(|uri| (uri, now)));

	if pushed.len() > SWEEP_THRESHOLD {
		pushed.retain(|_, entries| {
			entries.retain(|(_, at)| now.saturating_duration_since(*at) < PUSHED_MEDIA_TTL);
			!entries.is_empty()
		});
	}
}

/// Whether `mxc` (`mxc://server/id`) was in an event pushed in full to this
/// user's `device_id` within [`PUSHED_MEDIA_TTL`].
#[implement(super::Service)]
#[must_use]
pub fn media_recently_pushed(&self, user_id: &UserId, device_id: &DeviceId, mxc: &str) -> bool {
	let now = Instant::now();
	let key = (user_id.to_owned(), device_id.to_owned());

	self.pushed_media
		.lock()
		.get(&key)
		.is_some_and(|entries| {
			entries.iter().any(|(uri, at)| {
				uri == mxc && now.saturating_duration_since(*at) < PUSHED_MEDIA_TTL
			})
		})
}

/// Collect the `mxc://` values of every `url` key in the content.
fn collect_mxc(value: &Value, depth: usize, out: &mut Vec<String>) {
	if depth > MAX_DEPTH {
		return;
	}

	let next = depth.saturating_add(1);
	match value {
		| Value::Object(map) =>
			for (key, value) in map {
				match value {
					| Value::String(uri) if key == "url" && uri.starts_with("mxc://") => {
						out.push(uri.clone());
					},
					| _ => collect_mxc(value, next, out),
				}
			},
		| Value::Array(items) =>
			for item in items {
				collect_mxc(item, next, out);
			},
		| _ => (),
	}
}
