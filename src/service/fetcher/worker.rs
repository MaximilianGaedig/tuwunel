use std::{
	collections::{HashMap, VecDeque},
	sync::{Arc, Weak},
};

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use ruma::OwnedServerName;
use tokio::sync::watch::channel;
use tuwunel_core::{debug_warn, implement, trace};

use super::{
	Failure, Msg, Opts, Outcome, Service,
	opts::Key,
	request::{Inflight, SharedResult},
};

/// One in-flight fetch, borrowing the worker for the service's lifetime and
/// yielding its key alongside the result so the worker can route it.
type FetchFuture<'a> = BoxFuture<'a, (Key, SharedResult)>;
type FetchFutures<'a> = FuturesUnordered<FetchFuture<'a>>;

/// Service worker. Owns the request map, the deferral queue, and every
/// in-flight fetch on its stack, so no lock guards any of them.
#[implement(Service)]
pub(super) async fn run_worker(self: Arc<Self>) {
	let mut inflight: HashMap<Key, Inflight> = HashMap::new();
	let mut pending: VecDeque<Msg> = VecDeque::new();
	let mut futures: FetchFutures<'_> = FuturesUnordered::new();

	self.work_loop(&mut inflight, &mut pending, &mut futures)
		.await;
}

#[implement(Service)]
async fn work_loop<'a>(
	&'a self,
	inflight: &mut HashMap<Key, Inflight>,
	pending: &mut VecDeque<Msg>,
	futures: &mut FetchFutures<'a>,
) {
	let rx = self.channel.1.clone();
	while !rx.is_closed() {
		// Coalesce co-arriving callers before any completion evicts their entry.
		while let Ok(msg) = rx.try_recv() {
			self.on_request(msg, inflight, pending, futures);
		}

		tokio::select! {
			Some((key, result)) = futures.next() =>
				self.on_complete(key, result, inflight, pending, futures),
			msg = rx.recv_async() => match msg {
				| Ok(msg) => self.on_request(msg, inflight, pending, futures),
				| Err(_) => break,
			},
		}
	}
}

#[implement(Service)]
fn on_request<'a>(
	&'a self,
	msg: Msg,
	inflight: &mut HashMap<Key, Inflight>,
	pending: &mut VecDeque<Msg>,
	futures: &FetchFutures<'a>,
) {
	let Some(entry) = inflight.get_mut(&msg.key) else {
		// no in-flight request for this key: dispatch, or defer at the cap
		if futures.len() >= self.capacity {
			pending.push_back(msg);
		} else {
			self.dispatch(msg, inflight, futures);
		}

		return;
	};

	match entry.interest.upgrade() {
		// live callers: coalesce onto the running future
		| Some(strong) => {
			msg.reply
				.send((entry.tx.subscribe(), strong))
				.ok();
		},
		// every prior caller dropped and the future is draining toward
		// Cancelled; re-arm so it revives at its next attempt boundary
		| None => {
			let interest = Arc::new(());
			entry.interest = Arc::downgrade(&interest);
			msg.reply
				.send((entry.tx.subscribe(), interest))
				.ok();
		},
	}
}

#[implement(Service)]
fn dispatch<'a>(
	&'a self,
	msg: Msg,
	inflight: &mut HashMap<Key, Inflight>,
	futures: &FetchFutures<'a>,
) {
	let Msg { key, opts, reply } = msg;
	let interest = Arc::new(());
	let (tx, rx) = channel(None);

	// caller already gone: do not touch the network
	if reply.send((rx, interest.clone())).is_err() {
		return;
	}

	let opts = Arc::new(opts);
	let weak = Arc::downgrade(&interest);
	inflight.insert(key.clone(), Inflight {
		tx,
		interest: weak.clone(),
		opts: opts.clone(),
	});

	self.push_attempt(futures, key, opts, weak);
}

/// Push one attempt future onto the worker's set, yielding its key with the
/// result so the worker can route it back. The lone construction site for the
/// borrowed-future shape, shared by the fresh-dispatch and re-arm paths.
#[implement(Service)]
fn push_attempt<'a>(
	&'a self,
	futures: &FetchFutures<'a>,
	key: Key,
	opts: Arc<Opts>,
	weak: Weak<()>,
) {
	futures.push(async move { (key, self.run_attempts(&opts, &weak).await) }.boxed());
}

#[implement(Service)]
fn on_complete<'a>(
	&'a self,
	key: Key,
	result: SharedResult,
	inflight: &mut HashMap<Key, Inflight>,
	pending: &mut VecDeque<Msg>,
	futures: &FetchFutures<'a>,
) {
	let Some(entry) = inflight.get(&key) else {
		return;
	};

	// a fresh caller re-armed after the future gave up: revive from the
	// retained opts rather than publishing a stale Cancelled
	if matches!(&result, Err(Failure::Cancelled)) && entry.interest.upgrade().is_some() {
		let opts = entry.opts.clone();
		let weak = entry.interest.clone();
		self.push_attempt(futures, key, opts, weak);
		return;
	}

	entry.tx.send(Some(result)).ok();
	inflight.remove(&key);

	// a slot freed: re-admit deferred requests up to the cap, routing through
	// on_request so a deferred same-key pair coalesces instead of
	// double-dispatching
	while futures.len() < self.capacity {
		let Some(msg) = pending.pop_front() else {
			break;
		};

		self.on_request(msg, inflight, pending, futures);
	}
}

#[implement(Service)]
async fn run_attempts(&self, opts: &Opts, interest: &Weak<()>) -> SharedResult {
	let candidates = self.select.candidates(opts).await;
	if candidates.is_empty() {
		return Err(Failure::NoCandidates);
	}

	let limit = opts
		.attempt_limit
		.map_or(candidates.len(), |n| n.get().min(candidates.len()));

	let mut attempted: Vec<OwnedServerName> = Vec::new();
	for server in candidates.into_iter().take(limit) {
		if interest.strong_count() == 0 {
			return Err(Failure::Cancelled);
		}

		match self
			.transport
			.fetch_raw(opts.op, &server, opts)
			.await
		{
			| Err(error) => debug_warn!(%server, "fetch attempt failed: {error}"),
			| Ok(bytes) => match self.validate(opts, &bytes).await {
				| Err(error) => debug_warn!(%server, "rejecting poisoned response: {error}"),
				| Ok(()) => {
					trace!(%server, "fetch satisfied");
					return Ok(Arc::new(Outcome { bytes, origin: server }));
				},
			},
		}

		attempted.push(server);
	}

	Err(Failure::NotFound { attempted })
}
