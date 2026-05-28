use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName};
use tuwunel_core::Result;
use tuwunel_service::fetcher::Op;

use super::{base_opts, run};
use crate::admin_command;

#[admin_command]
pub(super) async fn event_fetcher_event(
	&self,
	room_id: OwnedRoomId,
	event_id: OwnedEventId,
	server: Option<OwnedServerName>,
	attempt_limit: Option<usize>,
	verify: bool,
) -> Result {
	let opts = base_opts(Op::Event, room_id, event_id, server, attempt_limit, verify);
	run(self, opts).await
}
