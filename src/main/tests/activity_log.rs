#![cfg(test)]

//! The activity log end to end: what a person does over the client API is
//! written as rows, and the two endpoints answer for them.
//!
//! The service's own tests cover only how rows fold into a week. Nothing there
//! would notice a hook that was never called, or an endpoint that told one
//! person where another had been.

mod client;

use std::{collections::BTreeSet, net::TcpListener, time::Duration};

use futures::future::join;
use reqwest::RequestBuilder;
use serde_json::{Value, json};
use tokio::time::sleep;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{OwnedEventId, RoomId, UserId},
	utils::millis_since_unix_epoch,
};
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

const ALICE: &str = "activity-log-alice-token";
const BOB: &str = "activity-log-bob-token";
const CAROL: &str = "activity-log-carol-token";

/// Rows are keyed by the millisecond, so two of a kind in the same one would be one row. Nothing
/// here is that fast in practice; the pause makes the order a fact rather than a likelihood.
const PAUSE: Duration = Duration::from_millis(5);

const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;

/// The asker's distance from UTC in the week request, in minutes.
const OFFSET_MINUTES: u64 = 120;

#[test]
fn what_people_do_is_logged_and_shown_to_those_who_share_a_room() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	// Presence stays on (the default): its changes are rows too.
	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true");

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let exercise = async {
			let outcome = activity(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};

		let (run, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run.and(outcome)
	});

	drop(runtime);
	result
}

async fn activity(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let alice_id = register(services, "activityalice", ALICE).await?;
	register(services, "activitybob", BOB).await?;
	let carol_id = register(services, "activitycarol", CAROL).await?;
	let alice = Client { services, base, token: ALICE };
	let bob = Client { services, base, token: BOB };
	let carol = Client { services, base, token: CAROL };

	// Alice and Bob share one room; Alice has another to herself; Carol is in neither.
	let shared = alice
		.create_room(&json!({"preset": "public_chat"}))
		.await?;
	let alone = alice.create_room(&json!({})).await?;
	bob.post(&format!("rooms/{shared}/join"), &json!({}))
		.await?;

	let started = millis_since_unix_epoch();

	// A message in each room, typing, and a receipt for something Bob said: one row each.
	send(&alice, &shared, "alice-shared").await?;
	sleep(PAUSE).await;
	send(&alice, &alone, "alice-alone").await?;
	sleep(PAUSE).await;
	let typing = put(
		&alice,
		alice.url(&format!("rooms/{shared}/typing/{alice_id}")),
		&json!({"typing": true, "timeout": 30_000}),
	)
	.await?;
	assert_eq!(typing.0, 200, "typing: {}", typing.1);
	sleep(PAUSE).await;
	let theirs = send(&bob, &shared, "bob-shared").await?;
	sleep(PAUSE).await;
	alice
		.post(&format!("rooms/{shared}/receipt/m.read/{theirs}"), &json!({}))
		.await?;

	let (status, log) = get(&alice, log_url(base, &alice_id, "")).await?;
	assert_eq!(status, 200, "own log: {log}");
	let rows = chunk(&log)?;
	assert!(newest_first(rows), "rows are not newest first: {log}");
	let deeds = deeds_of(rows);
	assert_eq!(kinds(&deeds), ["read", "typing", "sent", "sent"], "{log}");
	assert_eq!(
		rooms(&deeds),
		[Some(shared.as_str()), Some(shared.as_str()), Some(alone.as_str()), Some(shared.as_str())],
		"the person themselves is told every room: {log}"
	);

	// Bob shares one of the two rooms: he sees all four rows, and is told only that room.
	let (status, log) = get(&bob, log_url(base, &alice_id, "")).await?;
	assert_eq!(status, 200, "a room-mate's log: {log}");
	let deeds = deeds_of(chunk(&log)?);
	assert_eq!(kinds(&deeds), ["read", "typing", "sent", "sent"], "{log}");
	assert_eq!(
		rooms(&deeds),
		[Some(shared.as_str()), Some(shared.as_str()), None, Some(shared.as_str())],
		"a room the asker is not in was named, or one they are in was not: {log}"
	);

	// Carol shares nothing with Alice.
	let (status, refused) = get(&carol, log_url(base, &alice_id, "")).await?;
	assert_eq!((status, refused["errcode"].as_str()), (403, Some("M_FORBIDDEN")), "{refused}");

	// Presence: Carol has done nothing else, so her log is exactly her two changes.
	for state in ["online", "offline"] {
		let set = put(
			&carol,
			carol.url(&format!("presence/{carol_id}/status")),
			&json!({"presence": state}),
		)
		.await?;
		assert_eq!(set.0, 200, "presence {state}: {}", set.1);
		sleep(PAUSE).await;
	}
	let (status, log) = get(&carol, log_url(base, &carol_id, "")).await?;
	assert_eq!(status, 200, "{log}");
	let rows = chunk(&log)?;
	let all: Vec<&Value> = rows.iter().collect();
	assert_eq!(kinds(&all), ["offline", "online"], "{log}");
	for row in rows {
		assert!(row["last_active_ts"].is_u64(), "a presence row says when they were last active: {log}");
		assert!(row.get("room_id").is_none(), "a presence change happens in no room: {log}");
	}

	// A last-active time: from oneself and in the past, or not at all.
	let seen_at = started.saturating_sub(HOUR_MS);
	let seen_url = format!("{}/seen", log_url(base, &alice_id, ""));
	let accepted = put(&alice, seen_url.clone(), &json!({"ts": seen_at})).await?;
	assert_eq!(accepted.0, 200, "{}", accepted.1);
	let other = put(&bob, seen_url.clone(), &json!({"ts": seen_at.saturating_add(1)})).await?;
	assert_eq!((other.0, other.1["errcode"].as_str()), (403, Some("M_FORBIDDEN")), "{}", other.1);
	let future = millis_since_unix_epoch().saturating_add(DAY_MS);
	let future = put(&alice, seen_url, &json!({"ts": future})).await?;
	assert_eq!(
		(future.0, future.1["errcode"].as_str()),
		(400, Some("M_INVALID_PARAM")),
		"{}",
		future.1
	);

	let (_, log) = get(&alice, log_url(base, &alice_id, "")).await?;
	let rows = chunk(&log)?;
	let seen: Vec<u64> = rows
		.iter()
		.filter(|row| row["kind"] == "seen")
		.map(ts)
		.collect();
	assert_eq!(seen, [seen_at], "only the accepted time is a row: {log}");
	assert_eq!(rows.last().map(ts), Some(seen_at), "the oldest row comes last: {log}");

	// The week, in the hours of someone two hours east of UTC.
	let before = millis_since_unix_epoch();
	let query = format!("?week=true&utc_offset_minutes={OFFSET_MINUTES}");
	let (status, answer) = get(&alice, log_url(base, &alice_id, &query)).await?;
	let after = millis_since_unix_epoch();
	assert_eq!(status, 200, "{answer}");
	assert_eq!(answer["chunk"], json!([]), "the week comes instead of the rows: {answer}");
	let week = &answer["week"];
	let grid = week["seen"]
		.as_array()
		.ok_or_else(|| err!("no week in {answer}"))?;
	assert_eq!(grid.len(), 7, "{answer}");
	assert!(
		grid.iter()
			.all(|day| day.as_array().is_some_and(|day| day.len() == 24)),
		"{answer}"
	);
	assert_eq!(week["entries"].as_u64(), u64::try_from(rows.len()).ok(), "{answer}");

	let mut marked = BTreeSet::new();
	for (weekday, day) in grid.iter().enumerate() {
		for (hour, count) in day.as_array().into_iter().flatten().enumerate() {
			match count.as_u64() {
				| Some(0) => {},
				// Everything happened within the hour or two this test covers: one day each.
				| Some(1) => {
					marked.insert((weekday, hour));
				},
				| _ => panic!("{weekday} {hour} holds {count}: {answer}"),
			}
		}
	}

	// Every hour something was done in is marked, at the asker's hour rather than UTC's ...
	let done: BTreeSet<_> = deeds_of(rows)
		.into_iter()
		.map(|row| cell(ts(row)))
		.collect();
	assert!(done.contains(&cell(seen_at)) && done.len() >= 2, "{log}");
	assert!(marked.is_superset(&done), "marked {marked:?}, done {done:?}: {answer}");

	// ... and nothing else is, but for the hours Alice's presence covers: typing put her online,
	// and she still is when the week is asked for.
	let mut allowed: BTreeSet<_> = rows.iter().map(|row| cell(ts(row))).collect();
	allowed.insert(cell(before));
	allowed.insert(cell(after));
	assert!(marked.is_subset(&allowed), "marked {marked:?}, allowed {allowed:?}: {answer}");

	Ok(())
}

/// The weekday (Monday first) and hour a time falls in for the week's asker.
fn cell(ts: u64) -> (usize, usize) {
	let local = ts.saturating_add(OFFSET_MINUTES.saturating_mul(60_000));
	// The epoch was a Thursday.
	let weekday = (local / DAY_MS).saturating_add(3) % 7;
	let hour = local % DAY_MS / HOUR_MS;

	(usize::try_from(weekday).unwrap_or(0), usize::try_from(hour).unwrap_or(0))
}

fn log_url(base: &str, user: &UserId, query: &str) -> String {
	format!("{base}/_matrix/client/unstable/im.mxg.activity/users/{user}{query}")
}

fn chunk(log: &Value) -> Result<&Vec<Value>> {
	log.get("chunk")
		.and_then(Value::as_array)
		.ok_or_else(|| err!("no chunk in {log}"))
}

fn ts(row: &Value) -> u64 { row["ts"].as_u64().unwrap_or(0) }

fn newest_first(rows: &[Value]) -> bool {
	rows.windows(2)
		.all(|pair| ts(&pair[0]) >= ts(&pair[1]))
}

/// The rows for what someone did, without their presence changes: answering typing and receipts
/// marks a person online, which is right and beside the point of most checks here.
fn deeds_of(rows: &[Value]) -> Vec<&Value> {
	rows.iter()
		.filter(|row| !matches!(row["kind"].as_str(), Some("online" | "unavailable" | "offline")))
		.collect()
}

fn kinds<'a>(rows: &[&'a Value]) -> Vec<&'a str> {
	rows.iter()
		.map(|row| row["kind"].as_str().unwrap_or("(no kind)"))
		.collect()
}

fn rooms<'a>(rows: &[&'a Value]) -> Vec<Option<&'a str>> {
	rows.iter()
		.map(|row| row.get("room_id").and_then(Value::as_str))
		.collect()
}

async fn send(client: &Client<'_>, room: &RoomId, txn: &str) -> Result<OwnedEventId> {
	let (status, response) = put(
		client,
		client.url(&format!("rooms/{room}/send/m.room.message/{txn}")),
		&json!({"msgtype": "m.text", "body": "activity log check"}),
	)
	.await?;
	assert_eq!(status, 200, "send: {response}");

	Ok(field(&response, "event_id")?.try_into()?)
}

async fn get(client: &Client<'_>, url: String) -> Result<(u16, Value)> {
	answer(client, client.services.client.clients.default.get(url)).await
}

async fn put(client: &Client<'_>, url: String, body: &Value) -> Result<(u16, Value)> {
	let request = client
		.services
		.client
		.clients
		.default
		.put(url)
		.json(body);

	answer(client, request).await
}

/// The status and body of a request made as this user, whatever the status: refusals are half of
/// what is checked here.
async fn answer(client: &Client<'_>, request: RequestBuilder) -> Result<(u16, Value)> {
	let response = request.bearer_auth(client.token).send().await?;
	let status = response.status().as_u16();

	Ok((status, response.json().await?))
}
