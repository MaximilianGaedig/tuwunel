use ruma::OwnedServerName;
use tokio::time::Instant;
use tuwunel_core::{Result, utils::time};
use tuwunel_service::federation::ShouldAttempt;

use crate::admin_command;

#[admin_command]
pub(super) async fn peer_status_should_attempt(&self, server_name: OwnedServerName) -> Result {
	let timer = Instant::now();
	let verdict = self
		.services
		.federation
		.should_attempt(&server_name)
		.await;
	let query_time = timer.elapsed();

	let line = match verdict {
		| ShouldAttempt::Yes => "Yes; the sender would dispatch immediately.".to_owned(),
		| ShouldAttempt::No { earliest_retry } => {
			let at = time::format(earliest_retry, "%+");
			format!("No; earliest retry at {at}.")
		},
		| ShouldAttempt::Deprioritize =>
			"Deprioritize; eligible but should sort to the back of any candidate list.".to_owned(),
	};

	write!(self, "{server_name}: {line}\n\nResolved in {query_time:?}.").await
}
