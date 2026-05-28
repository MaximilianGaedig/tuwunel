use std::{
	collections::HashMap,
	sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use loole::unbounded;
use ruma::{
	OwnedEventId, OwnedRoomId, OwnedServerName, ServerName, event_id, room_id, server_name,
};
use tokio::{
	sync::Notify,
	task::{spawn, yield_now},
};
use tuwunel_core::{Result, err};

use super::*;
use crate::services::OnceServices;

impl Service {
	fn test(
		transport: Arc<dyn Transport>,
		select: Arc<dyn Select>,
		capacity: usize,
	) -> Arc<Self> {
		Arc::new(Self {
			services: Arc::new(OnceServices::default()),
			channel: unbounded(),
			transport,
			select,
			capacity,
		})
	}

	fn test_spawn(
		transport: Arc<dyn Transport>,
		select: Arc<dyn Select>,
		capacity: usize,
	) -> Arc<Self> {
		let svc = Self::test(transport, select, capacity);
		spawn(svc.clone().run_worker());
		svc
	}
}

enum Behavior {
	/// Valid JSON object.
	Good,

	/// Invalid JSON; fails the conform check.
	Garbage,

	/// Transport-level failure.
	Fail,

	/// Block until released, then answer with garbage.
	BlockGarbage,

	/// Block until released, then answer with a valid JSON object.
	BlockGood,
}

struct MockTransport {
	behaviors: HashMap<OwnedServerName, Behavior>,
	calls: Mutex<Vec<OwnedServerName>>,
	releases: HashMap<OwnedServerName, Notify>,
}

impl MockTransport {
	fn new(behaviors: impl IntoIterator<Item = (OwnedServerName, Behavior)>) -> Self {
		let behaviors: HashMap<OwnedServerName, Behavior> = behaviors.into_iter().collect();
		let releases = behaviors
			.iter()
			.filter(|(_, behavior)| {
				matches!(behavior, Behavior::BlockGarbage | Behavior::BlockGood)
			})
			.map(|(server, _)| (server.clone(), Notify::new()))
			.collect();

		Self {
			behaviors,
			calls: Mutex::new(Vec::new()),
			releases,
		}
	}

	fn calls(&self) -> Vec<OwnedServerName> {
		self.calls
			.lock()
			.expect("calls not poisoned")
			.clone()
	}

	fn call_count(&self) -> usize {
		self.calls
			.lock()
			.expect("calls not poisoned")
			.len()
	}

	fn calls_to(&self, server: &ServerName) -> usize {
		self.calls
			.lock()
			.expect("calls not poisoned")
			.iter()
			.filter(|called| called.as_str() == server.as_str())
			.count()
	}

	fn release(&self, server: &ServerName) {
		self.releases
			.get(server)
			.expect("server has a pending block to release")
			.notify_one();
	}
}

#[async_trait]
impl Transport for MockTransport {
	async fn fetch_raw(&self, _op: Op, server: &ServerName, _opts: &Opts) -> Result<Bytes> {
		self.calls
			.lock()
			.expect("calls not poisoned")
			.push(server.to_owned());

		match self
			.behaviors
			.get(server)
			.unwrap_or(&Behavior::Fail)
		{
			| Behavior::Good => Ok(Bytes::from_static(b"{\"ok\":true}")),
			| Behavior::Garbage => Ok(Bytes::from_static(b"not json")),
			| Behavior::Fail => Err(err!("mock transport failure")),
			| Behavior::BlockGarbage => {
				self.blocked(server).await;
				Ok(Bytes::from_static(b"not json"))
			},
			| Behavior::BlockGood => {
				self.blocked(server).await;
				Ok(Bytes::from_static(b"{\"ok\":true}"))
			},
		}
	}
}

impl MockTransport {
	async fn blocked(&self, server: &ServerName) {
		self.releases
			.get(server)
			.expect("blocking server has a release")
			.notified()
			.await;
	}
}

struct MockSelect {
	by_event: HashMap<OwnedEventId, Vec<OwnedServerName>>,
}

impl MockSelect {
	fn new(by_event: impl IntoIterator<Item = (OwnedEventId, Vec<OwnedServerName>)>) -> Self {
		Self { by_event: by_event.into_iter().collect() }
	}
}

#[async_trait]
impl Select for MockSelect {
	async fn candidates(&self, opts: &Opts) -> Vec<OwnedServerName> {
		opts.event_id
			.as_ref()
			.and_then(|event| self.by_event.get(event))
			.cloned()
			.unwrap_or_default()
	}
}

fn room() -> OwnedRoomId { room_id!("!room:test.local").to_owned() }

fn test_opts(event: &OwnedEventId) -> Opts {
	Opts {
		check_event_id: false,
		check_hashes: false,
		check_signature: false,
		..Opts::new(Op::Event, room()).event_id(event.clone())
	}
}

async fn settle() {
	for _ in 0..16 {
		yield_now().await;
	}
}

async fn wait_for(mut cond: impl FnMut() -> bool) {
	for _ in 0..1000 {
		if cond() {
			return;
		}

		yield_now().await;
	}

	panic!("condition was never met");
}

#[tokio::test]
async fn coalesces_concurrent_fetches() {
	let server = server_name!("a.test.local").to_owned();
	let event = event_id!("$ev:test.local").to_owned();

	let mock = Arc::new(MockTransport::new([(server.clone(), Behavior::Good)]));
	let select = Arc::new(MockSelect::new([(event.clone(), vec![server.clone()])]));
	let svc = Service::test_spawn(mock.clone(), select, 4);

	let opts = test_opts(&event);
	let (a, b) = tokio::join!(svc.fetch(opts.clone()), svc.fetch(opts));

	let a = a.expect("first fetch ok");
	let b = b.expect("second fetch ok");

	assert_eq!(a.origin, server);
	assert_eq!(b.origin, server);
	assert!(Arc::ptr_eq(&a, &b), "coalesced callers share one outcome");
	assert_eq!(mock.call_count(), 1, "one network attempt for two callers");
}

#[tokio::test]
async fn fails_over_past_poisoned_server() {
	let a = server_name!("a.test.local").to_owned();
	let b = server_name!("b.test.local").to_owned();
	let event = event_id!("$ev:test.local").to_owned();

	let mock = Arc::new(MockTransport::new([
		(a.clone(), Behavior::Garbage),
		(b.clone(), Behavior::Good),
	]));
	let select = Arc::new(MockSelect::new([(event.clone(), vec![a.clone(), b.clone()])]));
	let svc = Service::test_spawn(mock.clone(), select, 4);

	let outcome = svc
		.fetch(test_opts(&event))
		.await
		.expect("fails over to the good server");

	assert_eq!(outcome.origin, b);
	assert_eq!(mock.calls(), vec![a, b], "poisoned server attempted before the good one");
}

#[tokio::test]
async fn errors_when_all_servers_fail() {
	let a = server_name!("a.test.local").to_owned();
	let b = server_name!("b.test.local").to_owned();
	let event = event_id!("$ev:test.local").to_owned();

	let mock =
		Arc::new(MockTransport::new([(a.clone(), Behavior::Fail), (b.clone(), Behavior::Fail)]));
	let select = Arc::new(MockSelect::new([(event.clone(), vec![a.clone(), b.clone()])]));
	let svc = Service::test_spawn(mock.clone(), select, 4);

	let error = svc
		.fetch(test_opts(&event))
		.await
		.expect_err("no server has the event");

	assert!(error.to_string().contains("not found"), "unexpected error: {error}");
	assert_eq!(mock.calls(), vec![a, b], "both servers attempted before giving up");
}

#[tokio::test]
async fn capacity_backpressures_extra_keys() {
	let a = server_name!("a.test.local").to_owned();
	let b = server_name!("b.test.local").to_owned();
	let ev1 = event_id!("$one:test.local").to_owned();
	let ev2 = event_id!("$two:test.local").to_owned();

	let mock = Arc::new(MockTransport::new([
		(a.clone(), Behavior::BlockGarbage),
		(b.clone(), Behavior::Good),
	]));
	let select = Arc::new(MockSelect::new([
		(ev1.clone(), vec![a.clone()]),
		(ev2.clone(), vec![b.clone()]),
	]));
	let svc = Service::test_spawn(mock.clone(), select, 1);

	let g1 = {
		let svc = svc.clone();
		let opts = test_opts(&ev1);
		spawn(async move { svc.fetch(opts).await })
	};

	wait_for(|| mock.call_count() >= 1).await;

	let g2 = {
		let svc = svc.clone();
		let opts = test_opts(&ev2);
		spawn(async move { svc.fetch(opts).await })
	};

	settle().await;
	assert_eq!(mock.call_count(), 1, "second key cannot start while the slot is held");
	assert!(!g2.is_finished());

	mock.release(&a);

	drop(g1.await.expect("join g1"));
	let r2 = g2
		.await
		.expect("join g2")
		.expect("second key fetched once the slot freed");

	assert_eq!(r2.origin, b);
	assert_eq!(mock.call_count(), 2);
}

#[tokio::test]
async fn worker_stops_when_callers_drop() {
	let a = server_name!("a.test.local").to_owned();
	let b = server_name!("b.test.local").to_owned();
	let event = event_id!("$ev:test.local").to_owned();

	let mock = Arc::new(MockTransport::new([
		(a.clone(), Behavior::BlockGarbage),
		(b.clone(), Behavior::Good),
	]));
	let select = Arc::new(MockSelect::new([(event.clone(), vec![a.clone(), b.clone()])]));
	let svc = Service::test_spawn(mock.clone(), select, 4);

	let g = {
		let svc = svc.clone();
		let opts = test_opts(&event);
		spawn(async move { svc.fetch(opts).await })
	};

	wait_for(|| mock.call_count() >= 1).await;

	// drop the only caller; at its next attempt boundary the worker sees the
	// dead interest token and abandons the fetch before trying B
	g.abort();
	drop(g.await);

	mock.release(&a);
	settle().await;

	assert_eq!(mock.calls(), vec![a], "worker aborts at the attempt boundary; B untried");
}

#[tokio::test]
async fn coalesces_same_key_deferred_under_backpressure() {
	// Two callers for one key, both deferred while the cap is saturated by
	// other keys, must still collapse onto a single network attempt once slots
	// free, and neither may observe a spurious cancellation.
	let b1 = server_name!("b1.test.local").to_owned();
	let b2 = server_name!("b2.test.local").to_owned();
	let t = server_name!("t.test.local").to_owned();
	let evb1 = event_id!("$b1:test.local").to_owned();
	let evb2 = event_id!("$b2:test.local").to_owned();
	let target = event_id!("$target:test.local").to_owned();

	let mock = Arc::new(MockTransport::new([
		(b1.clone(), Behavior::BlockGarbage),
		(b2.clone(), Behavior::BlockGarbage),
		(t.clone(), Behavior::BlockGood),
	]));
	let select = Arc::new(MockSelect::new([
		(evb1.clone(), vec![b1.clone()]),
		(evb2.clone(), vec![b2.clone()]),
		(target.clone(), vec![t.clone()]),
	]));
	let svc = Service::test_spawn(mock.clone(), select, 2);

	// saturate both slots with distinct keys
	let gb1 = {
		let svc = svc.clone();
		let opts = test_opts(&evb1);
		spawn(async move { svc.fetch(opts).await })
	};
	let gb2 = {
		let svc = svc.clone();
		let opts = test_opts(&evb2);
		spawn(async move { svc.fetch(opts).await })
	};

	wait_for(|| mock.call_count() >= 2).await;

	// both same-key callers arrive while at capacity, so both are deferred with
	// no in-flight entry to coalesce onto yet
	let gt1 = {
		let svc = svc.clone();
		let opts = test_opts(&target);
		spawn(async move { svc.fetch(opts).await })
	};
	let gt2 = {
		let svc = svc.clone();
		let opts = test_opts(&target);
		spawn(async move { svc.fetch(opts).await })
	};

	settle().await;
	assert_eq!(mock.call_count(), 2, "deferred callers do not start while saturated");

	// free one slot: the first deferred caller dispatches and blocks on t
	mock.release(&b1);
	wait_for(|| mock.calls_to(&t) >= 1).await;
	assert_eq!(mock.calls_to(&t), 1, "first deferred caller dispatched once");

	// free the second slot: the second deferred caller must coalesce onto the
	// still-in-flight target fetch, not dispatch a second one
	mock.release(&b2);
	settle().await;
	assert_eq!(mock.calls_to(&t), 1, "second deferred caller coalesced; no second attempt");

	// release the target: both callers receive the one shared outcome
	mock.release(&t);
	let r1 = gt1
		.await
		.expect("join gt1")
		.expect("first target caller ok");
	let r2 = gt2
		.await
		.expect("join gt2")
		.expect("second target caller ok");

	assert_eq!(r1.origin, t);
	assert!(Arc::ptr_eq(&r1, &r2), "deferred callers share one outcome");
	assert_eq!(mock.calls_to(&t), 1, "exactly one network attempt for both callers");

	drop(gb1.await);
	drop(gb2.await);
}
