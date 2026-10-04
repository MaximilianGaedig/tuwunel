use ruma::api::client::{filter::FilterDefinition, sync::sync_events::v5::response};
use tuwunel_core::Result;
use tuwunel_service::sync::Connection;

use super::SyncInfo;
use crate::client::sync::v3::{build_presence_events, process_presence_updates};

/// The `im.mxg.presence` extension: `m.presence` events for the users the
/// sender shares a room with.
///
/// It reads what `/sync` v2 reads, by the same code: the whole picture when
/// the connection is new, and the changes after `globalsince` up to
/// `next_batch` once it is resumed.
#[tracing::instrument(name = "presence", level = "trace", skip_all)]
pub(super) async fn collect(
	SyncInfo { services, sender_user, .. }: SyncInfo<'_>,
	conn: &Connection,
) -> Result<response::Presence> {
	if !services.config.allow_local_presence {
		return Ok(response::Presence::default());
	}

	let (since, to) = bounds(conn.presence_owed(), conn.globalsince, conn.next_batch);

	let updates =
		process_presence_updates(services, since, to, sender_user, &FilterDefinition::default())
			.await;

	let events = build_presence_events(Some(updates));

	Ok(response::Presence { events })
}

/// The range of presence changes to send, as `(after, up to and including)`.
///
/// A connection owed the whole picture reads from the start; any other reads
/// what changed after `globalsince`. Neither reads past `next_batch`: a change
/// beyond it has no place in this response, and the connection reaches it on
/// its next one.
fn bounds(owed: bool, globalsince: u64, next_batch: u64) -> (u64, u64) {
	let since = if owed { 0 } else { globalsince };

	(since.min(next_batch), next_batch)
}

/// Whether a presence row written at `count` is inside the bounds.
#[cfg(test)]
fn in_bounds(count: u64, (since, to): (u64, u64)) -> bool { count > since && count <= to }

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn new_connection_reads_everything_up_to_next_batch() {
		let bounds = bounds(true, 0, 9);

		assert_eq!(bounds, (0, 9));
		assert!(in_bounds(1, bounds));
		assert!(in_bounds(9, bounds));
		assert!(!in_bounds(10, bounds), "a change past next_batch waits for the next response");
	}

	#[test]
	fn resumed_connection_reads_only_changes_since_globalsince() {
		let bounds = bounds(false, 5, 9);

		assert_eq!(bounds, (5, 9));
		assert!(!in_bounds(5, bounds), "what the client already has is not repeated");
		assert!(in_bounds(6, bounds));
		assert!(in_bounds(9, bounds));
		assert!(!in_bounds(10, bounds));
	}

	#[test]
	fn replayed_connection_owed_the_picture_reads_everything_again() {
		// The client replays from before the response that carried the picture.
		let bounds = bounds(true, 3, 9);

		assert_eq!(bounds, (0, 9));
		assert!(in_bounds(2, bounds));
	}

	#[test]
	fn since_never_passes_next_batch() {
		assert_eq!(bounds(false, 12, 9), (9, 9));
		assert!(!in_bounds(9, bounds(false, 12, 9)));
	}
}
