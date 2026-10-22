#![cfg(test)]

//! Each account data type reaches a client once, with its latest content
//! (MEO-188).
//!
//! An update deletes the revision it supersedes. Two updates of one type at
//! the same time both read the same superseded revision, so the revision the
//! first of them wrote was never deleted, and every sync served it beside the
//! current one: one account held five breadcrumbs events and sixteen local
//! notification settings for one device.
//!
//! The race is driven through the client API, and a superseded revision is
//! also planted directly, which is the state databases are already in. Initial
//! `/v3` sync, incremental `/v3` sync from before the plant, and sliding sync
//! must each carry the type once, with what `GET` answers; the one-time cleanup
//! then removes the planted row.

#[expect(
	dead_code,
	reason = "the shared client harness exposes helpers used by sibling integration tests"
)]
mod client;

use std::{fs::remove_dir_all, net::TcpListener};

use futures::{
	StreamExt,
	future::{join, try_join_all},
};
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, implement,
	ruma::{RoomId, UserId},
	utils::{ReadyExt, stream::TryIgnore},
};
use tuwunel_database::Json;
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

const TOKEN: &str = "account-data-superseded-access-token-0000";

/// A type written once through the API, with an older revision planted beside it.
const PLANTED: &str = "org.example.meo188.planted";

/// A type written by many concurrent requests.
const RACED: &str = "org.example.meo188.raced";

/// How many writes of `RACED` are in flight at once.
const WRITERS: usize = 32;

type RevisionKey<'a> = (Option<&'a RoomId>, &'a UserId, u64, &'a str);

#[test]
fn each_type_is_served_once_with_its_latest_content() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = Args::test_database_path("account-data-superseded");
	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option(format!("database_path={db_path:?}"))
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("allow_local_presence=false")
		.with_option("allow_outgoing_presence=false");

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let exercise = async {
			let outcome = exercise(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};

		let (run, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run.and(outcome)
	});

	drop(runtime);
	remove_dir_all(&db_path).ok();

	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let user_id = register(services, "adsuperseded", TOKEN).await?;
	let alice = Client { services, base, token: TOKEN };
	let path = |kind: &str| format!("user/{user_id}/account_data/{kind}");

	// The planted revision takes a count from before the real write, which is
	// where a superseded revision sits.
	let before = alice.sync_v3(None).await?;
	let before = field(&before, "next_batch")?.to_owned();
	let older = *services.globals.next_count();

	alice
		.put(&path(PLANTED), &json!({ "revision": "latest" }))
		.await?;

	services.db["roomuserdataid_accountdata"].put(
		(None::<&RoomId>, &*user_id, older, PLANTED),
		Json(json!({ "type": PLANTED, "content": { "revision": "superseded" } })),
	);

	if stored(services, &user_id, PLANTED).await != 2 {
		return Err!("the superseded revision was not planted");
	}

	let raced_path = path(RACED);
	let bodies: Vec<Value> = (0..WRITERS)
		.map(|writer| json!({ "writer": writer }))
		.collect();

	try_join_all(
		bodies
			.iter()
			.map(|body| alice.put(&raced_path, body)),
	)
	.await?;

	if stored(services, &user_id, RACED).await != 1 {
		return Err!("concurrent writes of one type left more than one revision stored");
	}

	let raced = alice.get(&raced_path).await?;
	let expected = [(PLANTED, json!({ "revision": "latest" })), (RACED, raced)];

	let initial = alice.sync_v3(None).await?;
	let incremental = alice.sync_v3(Some(&before)).await?;
	let sliding = alice.sync_v5().await?;

	for (name, events) in [
		("initial /v3 sync", &initial["account_data"]["events"]),
		("incremental /v3 sync", &incremental["account_data"]["events"]),
		("sliding sync", &sliding["extensions"]["account_data"]["global"]),
	] {
		for (kind, content) in &expected {
			let served: Vec<&Value> = events
				.as_array()
				.into_iter()
				.flatten()
				.filter(|event| event["type"] == *kind)
				.map(|event| &event["content"])
				.collect();

			if served != [content] {
				return Err!("{name} served {kind} as {served:?}, not once as {content}");
			}
		}
	}

	let (_, removed) = services
		.account_data
		.remove_superseded_revisions()
		.await?;

	if removed != 1 || stored(services, &user_id, PLANTED).await != 1 {
		return Err!("the cleanup removed {removed} revisions, not the one planted");
	}

	if alice.get(&path(PLANTED)).await? != json!({ "revision": "latest" }) {
		return Err!("the cleanup removed the current revision");
	}

	Ok(())
}

/// How many revisions of one global type are stored for a user.
async fn stored(services: &Services, user_id: &UserId, kind: &str) -> usize {
	services.db["roomuserdataid_accountdata"]
		.keys()
		.ignore_err()
		.ready_filter(|(room_id, user_id_, _, kind_): &RevisionKey<'_>| {
			room_id.is_none() && *user_id_ == user_id && *kind_ == kind
		})
		.count()
		.await
}

#[implement(Client, params = "<'_>")]
async fn put(&self, path: &str, body: &Value) -> Result<Value> {
	self.services
		.client
		.clients
		.default
		.put(self.url(path))
		.bearer_auth(self.token)
		.json(body)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}

#[implement(Client, params = "<'_>")]
async fn get(&self, path: &str) -> Result<Value> {
	self.services
		.client
		.clients
		.default
		.get(self.url(path))
		.bearer_auth(self.token)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}

/// A `/v3` sync with no rooms, from the start or from a token.
#[implement(Client, params = "<'_>")]
async fn sync_v3(&self, since: Option<&str>) -> Result<Value> {
	let since = since.map(|since| ("since", since));

	self.services
		.client
		.clients
		.default
		.get(self.url("sync"))
		.bearer_auth(self.token)
		.query(&[("timeout", "0"), ("filter", r#"{"room":{"rooms":[]}}"#)])
		.query(since.as_slice())
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}

/// A new sliding sync connection asking only for account data.
#[implement(Client, params = "<'_>")]
async fn sync_v5(&self) -> Result<Value> {
	let url = format!(
		"{}/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?timeout=0",
		self.base
	);

	let body = json!({
		"conn_id": "meo188",
		"lists": {},
		"extensions": { "account_data": { "enabled": true } },
	});

	self.services
		.client
		.clients
		.default
		.post(url)
		.bearer_auth(self.token)
		.json(&body)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}
