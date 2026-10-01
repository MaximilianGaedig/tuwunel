#![cfg(test)]

//! An incremental `/sync` loads only the rooms that were written to, and must
//! still say everything it said when it loaded all of them.
//!
//! Every round here is taken twice from the same token: once as served, and
//! once with the activity record switched off, which is the walk over every
//! room that the record replaces. The two answers must be the same, and on top
//! of that each round names the room that has to be in it and the rooms that
//! must not be.
//!
//! A filter that lets no room event through is driven the same way: it gets no
//! rooms, and still gets its to-device messages and the device lists that
//! changed in the rooms it is in.

mod client;

use std::{
	collections::BTreeSet,
	fs::remove_dir_all,
	net::TcpListener,
	time::{Duration, Instant},
};

use futures::future::join;
use serde_json::{Map, Value, json};
use tokio::time::sleep;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, implement,
	ruma::{RoomId, UserId},
	utils::BoolExt,
};
use tuwunel_service::{Services, users::DeviceListChange};

use self::client::{Client, field, poll_until, register, wait_until_ready};

const ALICE: &str = "sync-skip-unchanged-access-token-alice";
const BOB: &str = "sync-skip-unchanged-access-token-bob";
const DAVE: &str = "sync-skip-unchanged-access-token-dave";
const BOT: &str = "sync-skip-unchanged-access-token-bot";

/// What mautrix's crypto syncer asks for: nothing of any room.
const BOT_FILTER: &str = r#"{"presence":{"not_types":["*"]},"account_data":{"not_types":["*"]},"room":{"include_leave":false,"ephemeral":{"not_types":["*"]},"account_data":{"not_types":["*"]},"state":{"not_types":["*"]},"timeline":{"not_types":["*"]}}}"#;

/// A filter no room can pass.
const NO_ROOMS_FILTER: &str = r#"{"room":{"rooms":[]}}"#;

/// How long a notification count is polled for before the case gives up.
const COUNT_DEADLINE: Duration = Duration::from_secs(5);

/// A long poll that is not woken returns after this, far past `WOKEN_WITHIN`.
const LONG_POLL: &str = "30000";

/// How long the long poll is left waiting before the write that must wake it.
const WAKE_DELAY: Duration = Duration::from_millis(500);

/// A woken long poll answers well within this; one that slept through does not.
const WOKEN_WITHIN: Duration = Duration::from_secs(10);

/// Fields that differ between two answers to the same question: how old an
/// event is by now, and the token, which moves with any count handed out.
const VOLATILE: [&str; 2] = ["age", "next_batch"];

#[test]
fn only_changed_rooms_are_loaded_and_nothing_is_lost() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = Args::test_database_path("sync-skip-unchanged");

	// Presence is off so that nothing but the steps below moves the counter,
	// and the shortest long poll is short so that an empty round is quick.
	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option(format!("database_path={db_path:?}"))
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("allow_local_presence=false")
		.with_option("allow_outgoing_presence=false")
		.with_option("client_sync_timeout_min=200");

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

#[expect(clippy::too_many_lines)]
async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let alice_id = register(services, "skipalice", ALICE).await?;
	let bob_id = register(services, "skipbob", BOB).await?;
	let dave_id = register(services, "skipdave", DAVE).await?;
	let bot_id = register(services, "skipbot", BOT).await?;

	let alice = Client { services, base, token: ALICE };
	let bob = Client { services, base, token: BOB };
	let dave = Client { services, base, token: DAVE };
	let bot = Client { services, base, token: BOT };

	let public = json!({ "preset": "public_chat" });
	let encrypted = json!({
		"preset": "public_chat",
		"initial_state": [{
			"type": "m.room.encryption",
			"state_key": "",
			"content": { "algorithm": "m.megolm.v1.aes-sha2" },
		}],
	});

	// One room per thing a sync can report, and two in which nothing happens.
	let message = alice.create_room(&public).await?;
	let state = alice.create_room(&public).await?;
	let receipt = alice.create_room(&public).await?;
	let typing = alice.create_room(&public).await?;
	let account = alice.create_room(&public).await?;
	let tag = alice.create_room(&public).await?;
	let member = alice.create_room(&encrypted).await?;
	let unread = alice.create_room(&public).await?;
	let quiet_shared = alice.create_room(&public).await?;
	let quiet_alone = alice.create_room(&public).await?;

	let (message, state, receipt, typing, account): (&RoomId, &RoomId, &RoomId, &RoomId, &RoomId) =
		(&message, &state, &receipt, &typing, &account);
	let (tag, member, unread, quiet_shared, quiet_alone): (
		&RoomId,
		&RoomId,
		&RoomId,
		&RoomId,
		&RoomId,
	) = (&tag, &member, &unread, &quiet_shared, &quiet_alone);

	let all: [&RoomId; 10] = [
		message,
		state,
		receipt,
		typing,
		account,
		tag,
		member,
		unread,
		quiet_shared,
		quiet_alone,
	];

	for room in [message, receipt, typing, unread, quiet_shared] {
		bob.join(room).await?;
	}

	// The bot shares a plain room with Bob and the encrypted one with Alice.
	for room in [message, member] {
		bot.join(room).await?;
	}

	let to_read = alice.send(receipt, "to-read").await?;
	let unread_event = bob.send(unread, "unread").await?;

	count_settles(services, &alice_id, unread, 1)
		.await
		.into_option()
		.ok_or_else(|| err!("Bob's message did not notify Alice"))?;

	// Sending an event leaves its sender a read cursor, so every room Alice
	// made has one. Rooms from before that was so have none, and such a room
	// reports its unread count on every round. That state is made here by
	// taking the cursor away, since nothing in the client API leads to it.
	services
		.db
		.get("roomuserid_lastnotificationread")?
		.del((unread, &*alice_id));

	// An initial sync carries every room, with or without the record.
	let opening = alice.sync(None, None, "0").await?;

	services.sync.activity.set_disabled(true);
	let walked = alice.sync(None, None, "0").await;
	services.sync.activity.set_disabled(false);

	assert_eq!(
		normalised(&opening),
		normalised(&walked?),
		"an initial sync differs with the activity record on and off"
	);

	expect(&opening, "initial", &all, &[])?;

	let mut since = next_batch(&opening)?.to_owned();

	let bot_opening = bot.sync(None, Some(BOT_FILTER), "0").await?;
	let bot_since = next_batch(&bot_opening)?.to_owned();

	assert!(
		joined(&bot_opening).is_empty(),
		"a filter that rejects every room event was given rooms on its initial sync: \
		 {bot_opening}"
	);

	// The room without a read cursor reports its unread count although nothing
	// was written to it since the token: the record alone would skip it.
	alice
		.put(&format!("user/{alice_id}/account_data/org.example.round"), &json!({ "round": 1 }))
		.await?;

	let standing = round(&alice, &since, None).await?;
	since = next_batch(&standing)?.to_owned();

	expect(&standing, "standing unread count", &[unread], &except(&all, &[unread]))?;
	assert_eq!(
		notification_count(&standing, unread),
		Some(1),
		"the unread count of a room without a read cursor: {standing}"
	);

	// The round that judged the untouched rooms is the proof that rounds here
	// go through the record at all, rather than loading every room.
	let stamp = services
		.sync
		.room_activity(quiet_alone)
		.await
		.ok_or_else(|| err!("the untouched room has no stamp"))?;

	assert!(
		services
			.sync
			.activity
			.is_quiet(&alice_id, quiet_alone, stamp),
		"the untouched room was never judged, so no round here skipped anything"
	);

	// Reading it moves the cursor: the zeroed count is reported once, and the
	// room is silent from then on.
	alice
		.post(&format!("rooms/{unread}/receipt/m.read/{unread_event}"), &json!({}))
		.await?;

	count_settles(services, &alice_id, unread, 0)
		.await
		.into_option()
		.ok_or_else(|| err!("the read receipt did not clear Alice's count"))?;

	let read = round(&alice, &since, None).await?;
	since = next_batch(&read)?.to_owned();

	expect(&read, "read cursor", &[unread], &except(&all, &[unread]))?;
	assert_eq!(
		notification_count(&read, unread),
		Some(0),
		"the count a moved read cursor zeroes: {read}"
	);

	// A new message.
	let sent = alice.send(message, "fresh").await?;
	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "message", &[message], &except(&all, &[message]))?;
	assert!(
		events(&seen, message, "timeline")
			.iter()
			.any(|event| event["event_id"].as_str() == Some(sent.as_str())),
		"the new message is not in the timeline: {seen}"
	);

	// A state change.
	alice
		.put(&format!("rooms/{state}/state/m.room.topic"), &json!({ "topic": "changed" }))
		.await?;

	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "state", &[state], &except(&all, &[state]))?;
	assert!(
		has_type(&seen, state, "timeline", "m.room.topic"),
		"the new topic is not in the timeline: {seen}"
	);

	// Somebody else's read receipt.
	bob.post(&format!("rooms/{receipt}/receipt/m.read/{to_read}"), &json!({}))
		.await?;

	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "receipt", &[receipt], &except(&all, &[receipt]))?;
	assert!(
		has_type(&seen, receipt, "ephemeral", "m.receipt"),
		"Bob's receipt is not among the ephemeral events: {seen}"
	);

	// Typing starting, and stopping.
	let typing_path = format!("rooms/{typing}/typing/{bob_id}");
	bob.put(&typing_path, &json!({ "typing": true, "timeout": 30_000 }))
		.await?;

	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "typing", &[typing], &except(&all, &[typing]))?;
	assert_eq!(typists(&seen, typing), [bob_id.as_str()], "who is typing: {seen}");

	bob.put(&typing_path, &json!({ "typing": false }))
		.await?;

	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "typing stopped", &[typing], &except(&all, &[typing]))?;
	assert!(typists(&seen, typing).is_empty(), "nobody is typing any more: {seen}");

	// Room account data.
	alice
		.put(
			&format!("user/{alice_id}/rooms/{account}/account_data/org.example.note"),
			&json!({ "note": "kept" }),
		)
		.await?;

	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "room account data", &[account], &except(&all, &[account]))?;
	assert!(
		has_type(&seen, account, "account_data", "org.example.note"),
		"the room account data is missing: {seen}"
	);

	// A tag.
	alice
		.put(&format!("user/{alice_id}/rooms/{tag}/tags/u.work"), &json!({}))
		.await?;

	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "tag", &[tag], &except(&all, &[tag]))?;
	assert!(
		has_type(&seen, tag, "account_data", "m.tag"),
		"the tag is missing: {seen}"
	);

	// A new member of an encrypted room, whose devices Alice now has to know.
	dave.join(member).await?;

	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "new member", &[member], &except(&all, &[member]))?;
	assert!(
		events(&seen, member, "timeline")
			.iter()
			.any(|event| event["state_key"].as_str() == Some(dave_id.as_str())),
		"Dave's join is not in the timeline: {seen}"
	);
	assert!(
		device_list_changed(&seen, &dave_id),
		"the new member of an encrypted room is not a device-list change: {seen}"
	);

	// From here on the encrypted room may report on its own: the join may have
	// left Alice a notification, and she has no read cursor there.
	let settled = except(&all, &[member]);

	// A device-list change of somebody Alice shares rooms with. It is written
	// to each of those rooms and reported outside all of them.
	services
		.users
		.mark_device_key_update(&bob_id, DeviceListChange::Resync)
		.await;

	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "device list", &[], &settled)?;
	assert!(device_list_changed(&seen, &bob_id), "Bob's new device list: {seen}");

	// A filter no room can pass gets no room, whatever happened in one. The
	// token is not advanced, so the same message is still owed afterwards.
	let unseen = alice.send(message, "unseen").await?;
	let none = round(&alice, &since, Some(NO_ROOMS_FILTER)).await?;

	assert!(joined(&none).is_empty(), "a filter with no rooms was given some: {none}");

	let seen = round(&alice, &since, None).await?;
	since = next_batch(&seen)?.to_owned();

	expect(&seen, "message after an empty filter", &[message], &except(&settled, &[message]))?;
	assert!(
		events(&seen, message, "timeline")
			.iter()
			.any(|event| event["event_id"].as_str() == Some(unseen.as_str())),
		"the message the empty filter skipped was lost: {seen}"
	);

	// A long poll is woken by typing in one of its rooms. All the rooms of a
	// connection share one typing receiver, and this is what it is for.
	let started = Instant::now();
	let (woken, typed) = join(alice.sync(Some(&since), None, LONG_POLL), async {
		sleep(WAKE_DELAY).await;
		bob.put(&typing_path, &json!({ "typing": true, "timeout": 30_000 }))
			.await
	})
	.await;

	typed?;
	let woken = woken?;

	assert!(
		started.elapsed() < WOKEN_WITHIN,
		"typing did not wake the long poll, which ran its whole timeout"
	);
	assert_eq!(typists(&woken, typing), [bob_id.as_str()], "who woke the long poll: {woken}");

	bob.put(&typing_path, &json!({ "typing": false }))
		.await?;

	// The bot. Since its initial sync Bob's device list changed in a room they
	// share, and Dave joined the encrypted room it is in. A to-device message
	// is on its way as well.
	let bot_device = bot.device_id().await?;
	alice
		.put(
			"sendToDevice/org.example.ping/sync-skip-unchanged-ping",
			&json!({ "messages": { bot_id.as_str(): { bot_device: { "marker": "ping" } } } }),
		)
		.await?;

	let caught_up = round(&bot, &bot_since, Some(BOT_FILTER)).await?;
	let bot_since = next_batch(&caught_up)?.to_owned();

	assert!(
		joined(&caught_up).is_empty(),
		"a filter that rejects every room event was given rooms: {caught_up}"
	);
	assert!(
		caught_up["to_device"]["events"]
			.as_array()
			.into_iter()
			.flatten()
			.any(|event| event["content"]["marker"].as_str() == Some("ping")),
		"the to-device message did not arrive: {caught_up}"
	);
	assert!(
		device_list_changed(&caught_up, &bob_id),
		"a device-list change in a shared room did not reach the bot: {caught_up}"
	);
	assert!(
		device_list_changed(&caught_up, &dave_id),
		"a new member of an encrypted room did not reach the bot: {caught_up}"
	);

	// The bot watches no room. A device-list change in one must still wake it.
	let started = Instant::now();
	let (woken, ()) = join(bot.sync(Some(&bot_since), Some(BOT_FILTER), LONG_POLL), async {
		sleep(WAKE_DELAY).await;
		services
			.users
			.mark_device_key_update(&bob_id, DeviceListChange::Resync)
			.await;
	})
	.await;

	let woken = woken?;

	assert!(
		started.elapsed() < WOKEN_WITHIN,
		"a device-list change did not wake the bot's long poll, which ran its whole timeout"
	);
	assert!(
		device_list_changed(&woken, &bob_id),
		"the bot was woken without the device-list change: {woken}"
	);
	assert!(joined(&woken).is_empty(), "the woken bot was given rooms: {woken}");

	Ok(())
}

/// One incremental round, answered twice from the same token.
///
/// The first answer is the one served. The second is taken with the activity
/// record switched off, so every room is loaded. They must agree on everything
/// but the fields that cannot.
async fn round(client: &Client<'_>, since: &str, filter: Option<&str>) -> Result<Value> {
	let services = client.services;
	let token: u64 = since
		.parse()
		.map_err(|error| err!("the token {since} is not a count: {error}"))?;

	assert!(
		services.sync.activity_covers(token),
		"the activity record does not cover the token {since}, so this round would load every \
		 room and prove nothing"
	);

	let served = client.sync(Some(since), filter, "0").await?;

	services.sync.activity.set_disabled(true);
	let walked = client.sync(Some(since), filter, "0").await;
	services.sync.activity.set_disabled(false);

	assert_eq!(
		normalised(&served),
		normalised(&walked?),
		"the round from {since} differs with the activity record on and off"
	);

	Ok(served)
}

/// A response with what cannot be compared taken out.
///
/// Lists are sorted, because most of them are filled in the order their parts
/// happen to finish loading. The timeline keeps its order, which is the one
/// thing a client reads off it.
fn normalised(response: &Value) -> Value { normalise(response, ("", "")) }

fn normalise(value: &Value, path: (&str, &str)) -> Value {
	match value {
		| Value::Object(fields) => fields
			.iter()
			.filter(|(name, _)| !VOLATILE.contains(&name.as_str()))
			.map(|(name, field)| (name.clone(), normalise(field, (path.1, name.as_str()))))
			.collect::<Map<String, Value>>()
			.into(),
		| Value::Array(items) => {
			let mut items: Vec<Value> = items
				.iter()
				.map(|item| normalise(item, path))
				.collect();

			if path != ("timeline", "events") {
				items.sort_by_key(ToString::to_string);
			}

			items.into()
		},
		| other => other.clone(),
	}
}

/// Checks which rooms a round carries: every one of `present`, none of
/// `absent`.
fn expect(response: &Value, round: &str, present: &[&RoomId], absent: &[&RoomId]) -> Result {
	let joined = joined(response);

	for room in present {
		if !joined.contains(room.as_str()) {
			return Err(err!("the {round} round left out {room}: {response}"));
		}
	}

	for room in absent {
		if joined.contains(room.as_str()) {
			return Err(err!(
				"the {round} round carries {room}, where nothing happened: {response}"
			));
		}
	}

	Ok(())
}

/// `rooms` without `touched`.
fn except<'a>(rooms: &[&'a RoomId], touched: &[&RoomId]) -> Vec<&'a RoomId> {
	rooms
		.iter()
		.copied()
		.filter(|room| !touched.contains(room))
		.collect()
}

fn joined(response: &Value) -> BTreeSet<&str> {
	response["rooms"]["join"]
		.as_object()
		.into_iter()
		.flat_map(Map::keys)
		.map(String::as_str)
		.collect()
}

/// The events of one section of a joined room.
fn events<'a>(response: &'a Value, room_id: &RoomId, section: &str) -> &'a [Value] {
	response["rooms"]["join"][room_id.as_str()][section]["events"]
		.as_array()
		.map(Vec::as_slice)
		.unwrap_or_default()
}

fn has_type(response: &Value, room_id: &RoomId, section: &str, event_type: &str) -> bool {
	events(response, room_id, section)
		.iter()
		.any(|event| event["type"].as_str() == Some(event_type))
}

/// Who the room's typing event names, or nobody if it carries none.
fn typists<'a>(response: &'a Value, room_id: &RoomId) -> Vec<&'a str> {
	events(response, room_id, "ephemeral")
		.iter()
		.filter(|event| event["type"].as_str() == Some("m.typing"))
		.filter_map(|event| event["content"]["user_ids"].as_array())
		.flatten()
		.filter_map(Value::as_str)
		.collect()
}

fn notification_count(response: &Value, room_id: &RoomId) -> Option<u64> {
	response["rooms"]["join"][room_id.as_str()]["unread_notifications"]["notification_count"]
		.as_u64()
}

fn device_list_changed(response: &Value, user_id: &UserId) -> bool {
	response["device_lists"]["changed"]
		.as_array()
		.into_iter()
		.flatten()
		.filter_map(Value::as_str)
		.any(|user| user == user_id.as_str())
}

fn next_batch(response: &Value) -> Result<&str> { field(response, "next_batch") }

/// Whether the room's notification count settles on `want` before the
/// deadline.
///
/// Push evaluation trails the send that triggers it, so neither the count a
/// message raises nor the zero a receipt writes is readable from one sample.
async fn count_settles(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	want: u64,
) -> bool {
	poll_until(COUNT_DEADLINE, async || {
		services
			.pusher
			.notification_count(user_id, room_id)
			.await
			.eq(&want)
	})
	.await
}

#[implement(Client, params = "<'_>")]
async fn join(&self, room_id: &RoomId) -> Result {
	self.post(&format!("rooms/{room_id}/join"), &json!({}))
		.await
		.map(drop)
}

/// One sync, optionally resuming from a token and optionally filtered.
#[implement(Client, params = "<'_>")]
async fn sync(&self, since: Option<&str>, filter: Option<&str>, timeout: &str) -> Result<Value> {
	let since = since.map(|since| ("since", since));
	let filter = filter.map(|filter| ("filter", filter));

	self.services
		.client
		.clients
		.default
		.get(self.url("sync"))
		.bearer_auth(self.token)
		.query(&[("timeout", timeout)])
		.query(since.as_slice())
		.query(filter.as_slice())
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}

/// Send a text message and return its event id.
#[implement(Client, params = "<'_>")]
async fn send(&self, room_id: &RoomId, body: &str) -> Result<String> {
	let path = format!("rooms/{room_id}/send/m.room.message/sync-skip-unchanged-{body}");
	let response = self
		.put(&path, &json!({ "msgtype": "m.text", "body": body }))
		.await?;

	field(&response, "event_id").map(ToOwned::to_owned)
}

/// Put a JSON body to one endpoint path as this user and parse the reply.
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

/// The device this user's token belongs to.
#[implement(Client, params = "<'_>")]
async fn device_id(&self) -> Result<String> {
	let response: Value = self
		.services
		.client
		.clients
		.default
		.get(self.url("account/whoami"))
		.bearer_auth(self.token)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	field(&response, "device_id").map(ToOwned::to_owned)
}
