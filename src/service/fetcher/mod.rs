mod opts;
mod request;
mod select;
mod transport;
mod validate;
mod worker;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use async_trait::async_trait;
use loole::{Receiver, Sender, unbounded};
use tokio::sync::{
	oneshot::{self, channel},
	watch,
};
use tuwunel_core::{Result, implement};

pub use self::opts::{Op, Opts, Outcome};
use self::{
	opts::{Failure, Key},
	request::{SharedResult, Subscription},
	select::{RoomCandidates, Select},
	transport::{FederationTransport, Transport},
};
use crate::services::OnceServices;

/// Upper bound on concurrent in-flight fetches across all keys.
const REQUESTS_MAX: usize = 256;

pub struct Service {
	services: Arc<OnceServices>,
	channel: (Sender<Msg>, Receiver<Msg>),
	transport: Arc<dyn Transport>,
	select: Arc<dyn Select>,
	capacity: usize,
}

/// Request to the worker. The worker replies with a subscription to the
/// coalesced result, deferring the reply under backpressure until a slot frees.
struct Msg {
	key: Key,
	opts: Opts,
	reply: oneshot::Sender<Subscription>,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let services = args.services.clone();
		let transport: Arc<dyn Transport> =
			Arc::new(FederationTransport { services: services.clone() });

		let select: Arc<dyn Select> = Arc::new(RoomCandidates { services: services.clone() });

		Ok(Arc::new(Self {
			services,
			channel: unbounded(),
			transport,
			select,
			capacity: REQUESTS_MAX,
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		self.run_worker().await;
		Ok(())
	}

	async fn interrupt(&self) {
		let (sender, _) = &self.channel;
		if !sender.is_closed() {
			sender.close();
		}
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Fetch raw response bytes for an event over federation, coalescing concurrent
/// callers for the same key onto a single network attempt. Server selection,
/// failover, and poison detection happen internally; the future resolves only
/// once a clean response arrives or every candidate is exhausted.
#[implement(Service)]
pub async fn fetch(&self, opts: Opts) -> Result<Arc<Outcome>> {
	let key = Key::new(&opts);
	let (reply, reply_rx) = channel();

	self.channel
		.0
		.send(Msg { key, opts, reply })
		.map_err(|_| Failure::Cancelled)?;

	// Hold the strong interest token across the wait; its drop cancels the fetch.
	let (rx, _interest) = reply_rx.await.map_err(|_| Failure::Cancelled)?;

	await_result(rx).await.map_err(Into::into)
}

async fn await_result(mut rx: watch::Receiver<Option<SharedResult>>) -> SharedResult {
	rx.wait_for(Option::is_some)
		.await
		.map_or(Err(Failure::Cancelled), |value| value.clone().expect("present by predicate"))
}
