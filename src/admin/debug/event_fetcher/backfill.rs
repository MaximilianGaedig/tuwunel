use std::num::NonZeroUsize;

use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName};
use tuwunel_core::Result;
use tuwunel_service::fetcher::{Op, Opts};

use super::{base_opts, run};
use crate::admin_command;

#[admin_command]
pub(super) async fn event_fetcher_backfill(
	&self,
	room_id: OwnedRoomId,
	event_id: OwnedEventId,
	server: Option<OwnedServerName>,
	attempt_limit: Option<usize>,
	limit: Option<usize>,
	verify: bool,
) -> Result {
	let opts = Opts {
		backfill_limit: limit.and_then(NonZeroUsize::new),
		..base_opts(Op::Backfill, room_id, event_id, server, attempt_limit, verify)
	};

	run(self, opts).await
}
