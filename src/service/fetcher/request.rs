use std::sync::{Arc, Weak};

use tokio::sync::watch::{Receiver, Sender};

use super::{Failure, Opts, Outcome};

/// Outcome shared by every caller coalesced onto one fetch. Cheap to clone so
/// the worker can broadcast it down each subscriber's channel.
pub(super) type SharedResult = Result<Arc<Outcome>, Failure>;

/// Reply handed to a caller: the channel it awaits the outcome on, plus the
/// sole strong liveness token whose drop cancels the in-flight fetch.
pub(super) type Subscription = (Receiver<Option<SharedResult>>, Arc<()>);

/// One in-flight fetch, owned by the worker. The worker is the sole mutator, so
/// no lock guards it; coalesced callers reach it only through their channels.
pub(super) struct Inflight {
	/// Result channel. Coalesced callers subscribe to await the outcome.
	pub(super) tx: Sender<Option<SharedResult>>,

	/// Liveness signal. The strong token rides to the callers; the worker holds
	/// this weak ref and the fetch bails once it can no longer upgrade it.
	pub(super) interest: Weak<()>,

	/// Retained (shared) so a re-armed key re-dispatches without re-cloning it.
	pub(super) opts: Arc<Opts>,
}
