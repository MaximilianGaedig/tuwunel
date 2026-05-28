mod execute;
mod format;
mod peer;
pub mod scheme;

use std::{sync::Arc, time::Duration};

use tuwunel_core::{Result, utils::exponential_backoff_streak_cap};
use tuwunel_database::Map;

use self::peer::MAX_BACKOFF;
pub use self::peer::{Classification, ShouldAttempt};
use crate::services::OnceServices;

pub struct Service {
	services: Arc<OnceServices>,
	statuses: Arc<Map>,

	/// Width of one peer-status bucket in seconds. Aligned with
	/// `sender_timeout` so the streak count walking back across adjacent
	/// buckets matches the sender's `consecutive_failures` notion at the
	/// cutover.
	window_secs: u64,

	/// Walk-back cap = `ceil(sqrt(MAX_BACKOFF / window_secs))`. Beyond this
	/// streak length the quadratic curve `window * n²` saturates at
	/// [`MAX_BACKOFF`] and further steps cannot change the verdict.
	n_max: u32,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let window_secs = args.server.config.sender_timeout.max(1);
		let n_max = exponential_backoff_streak_cap(Duration::from_secs(window_secs), MAX_BACKOFF);

		Ok(Arc::new(Self {
			services: args.services.clone(),
			statuses: args.db["servername_status"].clone(),
			window_secs,
			n_max,
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}
