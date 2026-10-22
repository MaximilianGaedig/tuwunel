#![cfg(test)]

//! Push delivery shaping: `push_burst_window_s` and `push_quiet_badge_only`.
//!
//! Drives event notices straight through the pusher service against a stub
//! gateway and asserts which go out in full (with an `event_id`, which starts
//! Element X's notification extension) and which only as a counts-only badge
//! update, and that the badge carries the account's unread total.

use std::{collections::HashMap, fs::remove_dir_all, str::from_utf8, time::Duration};

use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	spawn,
	sync::mpsc::{UnboundedReceiver, UnboundedSender, error::TryRecvError, unbounded_channel},
	task::JoinHandle,
	time::{sleep, timeout},
};
use tuwunel::{Args, Runtime, Server, async_run, async_stop};
use tuwunel_core::{
	Err, Result, err,
	matrix::Pdu,
	ruma::{
		RoomId, UserId,
		api::client::push::{
			Pusher, PusherIds, PusherInit, PusherKind, set_pusher::v3::Request as SetPusherRequest,
		},
		device_id,
		push::{HttpPusherData, PushFormat, Ruleset},
	},
};
use tuwunel_service::Services;

type Captured = (u16, Value);
type CaptureRx = UnboundedReceiver<Captured>;

const APP_ID: &str = "app.tuwunel.test";
const SENDER: &str = "@alice:remote.example";
const NOTIFY_PATH: &str = "/_matrix/push/v1/notify";
const WINDOW_S: u64 = 3;

#[test]
fn pusher_burst_throttle() -> Result {
	let db_path = Args::test_database_path("test-pusher-burst-throttle");

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option
		.push(format!("database_path={db_path:?}"));
	args.option
		.push("ip_range_denylist=[]".to_owned());
	args.option
		.push(format!("push_burst_window_s={WINDOW_S}"));
	args.option
		.push("push_quiet_badge_only=true".to_owned());

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result: Result = runtime.block_on(async {
		let services = Services::build(server.server.clone()).await?;
		let services = services.start().await?;
		_ = server
			.services
			.lock()
			.await
			.insert(services.clone());

		let outcome = run_cases(&services).await;

		server.server.shutdown()?;
		drop(services);

		async_run(&server).await?;
		async_stop(&server).await?;

		outcome
	});

	drop(runtime);
	remove_dir_all(&db_path).ok();

	result
}

struct Fixture<'a> {
	services: &'a Services,
	user: &'a UserId,
	ruleset: Ruleset,
	pusher: Pusher,
	rx: CaptureRx,
	unread: u64,
	room_unread: HashMap<String, u64>,
}

async fn run_cases(services: &Services) -> Result {
	let server_name = services.globals.server_name();
	let user = UserId::parse_with_server_name("bursttest", server_name)?;

	services
		.users
		.create(&user, Some("password"), None)
		.await?;

	let rooms: Vec<_> = ["a", "b", "c", "d"]
		.iter()
		.map(|name| RoomId::parse(format!("!burst-{name}:{server_name}")))
		.collect::<Result<_, _>>()?;

	let joined = services.db.get("userroomid_joined")?;
	for room_id in &rooms {
		joined.put((&user, room_id), 1_u64);
	}

	let listener = TcpListener::bind("127.0.0.1:0").await?;
	let url = format!("http://{}{NOTIFY_PATH}", listener.local_addr()?);
	let (tx, rx) = unbounded_channel();
	let (fail_tx, fail_rx) = unbounded_channel();
	let _stub = AbortOnDrop(spawn(stub_gateway(listener, tx, fail_rx)));

	let pusher = register_pusher(services, &user, "pk-burst", url).await?;
	let mut fx = Fixture {
		services,
		user: &user,
		ruleset: ruleset()?,
		pusher,
		rx,
		unread: 0,
		room_unread: HashMap::new(),
	};

	let [a, b, c, d] = [&rooms[0], &rooms[1], &rooms[2], &rooms[3]];

	// The first event of a burst goes out in full.
	expect_full(&mut fx, a, "m.room.message", "hello", "$a1:x").await?;

	// A second event in the same room within the window: badge only, and the
	// badge carries the unread total that includes it.
	expect_badge(&mut fx, a, "m.room.message", "again", "$a2:x", "burst").await?;
	expect_badge(&mut fx, a, "m.room.message", "and again", "$a3:x", "burst").await?;

	// Another room has a window of its own.
	expect_full(&mut fx, b, "m.room.message", "hello", "$b1:x").await?;

	// A mention always pushes in full, inside the window.
	expect_full(&mut fx, a, "m.room.message", "mention", "$a4:x").await?;

	// A call invite always pushes in full, inside the window, though its rule
	// sets no sound.
	expect_full(&mut fx, a, "m.call.invite", "", "$a5:x").await?;

	// Quiet (no sound) notifications are badge only, even as the first event.
	expect_badge(&mut fx, c, "m.room.message", "quiet", "$c1:x", "quiet").await?;
	expect_badge(&mut fx, c, "m.room.message", "quiet", "$c2:x", "quiet").await?;

	// The quiet ones did not open a window: a sounding event there is full.
	expect_full(&mut fx, c, "m.room.message", "hello", "$c3:x").await?;

	// The mention restarted room a's window: after the window has passed since
	// the last full push the next event is full again, and the one after it
	// is a badge.
	sleep(Duration::from_millis(WINDOW_S * 1000 + 300)).await;
	expect_full(&mut fx, a, "m.room.message", "later", "$a6:x").await?;
	expect_badge(&mut fx, a, "m.room.message", "later again", "$a7:x", "burst").await?;

	// A full push the gateway fails does not open the window, so its retry is
	// full rather than a badge.
	fail_tx
		.send(())
		.map_err(|_| err!("stub gateway gone"))?;

	bump_unread(&mut fx, d)?;
	let pdu = event(d, "m.room.message", "hello", "$d1:x")?;
	let failed = fx
		.services
		.pusher
		.send_push_notice(fx.user, &fx.pusher, &fx.ruleset, &pdu)
		.await;

	recv(&mut fx.rx).await?;
	if failed.is_ok() {
		return Err!("a push the gateway answered 502 reported success");
	}

	send(&fx, &pdu).await?;
	let (_, body) = recv(&mut fx.rx).await?;
	expect_event_id(&body, "$d1:x", "retry of a failed full push")?;

	expect_quiescent(&mut fx.rx)
}

/// Count one more unread notification in `room_id`, as the server's append
/// does before it queues the push, and in the account-wide total.
fn bump_unread(fx: &mut Fixture<'_>, room_id: &RoomId) -> Result {
	let count = fx
		.room_unread
		.entry(room_id.to_string())
		.or_insert(0);

	*count = count.saturating_add(1);
	fx.unread = fx.unread.saturating_add(1);

	fx.services
		.db
		.get("userroomid_notificationcount")?
		.put((fx.user, room_id), *count);

	Ok(())
}

async fn send(fx: &Fixture<'_>, pdu: &Pdu) -> Result {
	fx.services
		.pusher
		.send_push_notice(fx.user, &fx.pusher, &fx.ruleset, pdu)
		.await
}

async fn expect_full(
	fx: &mut Fixture<'_>,
	room_id: &RoomId,
	kind: &str,
	body: &str,
	event_id: &str,
) -> Result {
	bump_unread(fx, room_id)?;

	let pdu = event(room_id, kind, body, event_id)?;
	send(fx, &pdu).await?;

	let (status, body) = recv(&mut fx.rx).await?;
	if status != 200 {
		return Err!("stub answered {status} for {event_id}");
	}

	expect_event_id(&body, event_id, "full push")?;

	let notification = notification(&body)?;
	if notification.get("counts") != Some(&json!({"unread": fx.unread})) {
		return Err!(
			"full push for {event_id} carried counts {:?}, expected unread {}",
			notification.get("counts"),
			fx.unread
		);
	}

	expect_quiescent(&mut fx.rx)
}

async fn expect_badge(
	fx: &mut Fixture<'_>,
	room_id: &RoomId,
	kind: &str,
	body: &str,
	event_id: &str,
	why: &str,
) -> Result {
	bump_unread(fx, room_id)?;

	let pdu = event(room_id, kind, body, event_id)?;
	send(fx, &pdu).await?;

	let (_, body) = recv(&mut fx.rx).await?;
	let notification = notification(&body)?;

	for field in ["event_id", "room_id", "sender", "type", "content"] {
		if let Some(found) = notification.get(field) {
			return Err!("{why} event {event_id} went out as a full push ({field}: {found})");
		}
	}

	if notification.get("counts") != Some(&json!({"unread": fx.unread})) {
		return Err!(
			"{why} badge for {event_id} carried counts {:?}, expected unread {}",
			notification.get("counts"),
			fx.unread
		);
	}

	expect_quiescent(&mut fx.rx)
}

fn expect_event_id(body: &Value, want: &str, case: &str) -> Result {
	let got = notification(body)?
		.get("event_id")
		.and_then(Value::as_str);

	if got != Some(want) {
		return Err!("{case}: expected a full push for {want}, got event_id {got:?}: {body}");
	}

	Ok(())
}

/// Messages notify with a sound; a body of "mention" also highlights; a body
/// of "quiet" notifies without a sound; a call invite notifies without one.
fn ruleset() -> Result<Ruleset> {
	let body_rule = |id: &str, pattern: &str, actions: Value| {
		json!({
			"rule_id": id,
			"default": false,
			"enabled": true,
			"conditions": [{"kind": "event_match", "key": "content.body", "pattern": pattern}],
			"actions": actions,
		})
	};

	serde_json::from_value(json!({
		"override": [
			body_rule("test.mention", "mention", json!([
				"notify",
				{"set_tweak": "sound", "value": "default"},
				{"set_tweak": "highlight"},
			])),
			body_rule("test.quiet", "quiet", json!(["notify"])),
			{
				"rule_id": "test.call",
				"default": false,
				"enabled": true,
				"conditions": [{"kind": "event_match", "key": "type", "pattern": "m.call.invite"}],
				"actions": ["notify"],
			},
		],
		"underride": [{
			"rule_id": "test.message",
			"default": false,
			"enabled": true,
			"conditions": [{"kind": "event_match", "key": "type", "pattern": "m.room.message"}],
			"actions": ["notify", {"set_tweak": "sound", "value": "default"}],
		}],
	}))
	.map_err(|e| err!("invalid test ruleset: {e}"))
}

fn event(room_id: &RoomId, kind: &str, body: &str, event_id: &str) -> Result<Pdu> {
	let content = match kind {
		| "m.call.invite" => json!({
			"call_id": "c1",
			"version": "1",
			"lifetime": 60000,
			"offer": {"type": "offer", "sdp": "v=0"},
		}),
		| _ => json!({"msgtype": "m.text", "body": body}),
	};

	serde_json::from_value(json!({
		"type": kind,
		"content": content,
		"event_id": event_id,
		"room_id": room_id,
		"sender": SENDER,
		"prev_events": ["$prev:remote.example"],
		"auth_events": ["$auth:remote.example"],
		"origin_server_ts": 1_838_188_000,
		"depth": 12,
		"hashes": { "sha256": "thishashcoversallfieldsincasethisisredacted" },
	}))
	.map_err(|e| err!("invalid test pdu: {e}"))
}

async fn register_pusher(
	services: &Services,
	user: &UserId,
	pushkey: &str,
	url: String,
) -> Result<Pusher> {
	let mut data = HttpPusherData::new(url);
	data.format = Some(PushFormat::EventIdOnly);

	let pusher: Pusher = PusherInit {
		ids: PusherIds::new(pushkey.to_owned(), APP_ID.to_owned()),
		kind: PusherKind::Http(data),
		app_display_name: "Tuwunel Test".into(),
		device_display_name: "Test Device".into(),
		profile_tag: None,
		lang: "en".into(),
	}
	.into();

	let action = SetPusherRequest::post(pusher).action;

	services
		.pusher
		.set_pusher(user, device_id!("BURSTDEV"), &action)
		.await?;

	services.pusher.get_pusher(user, pushkey).await
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
	fn drop(&mut self) { self.0.abort(); }
}

/// Answers 200 with no rejected keys, or 502 once per message on `fail`.
async fn stub_gateway(
	listener: TcpListener,
	tx: UnboundedSender<Captured>,
	mut fail: UnboundedReceiver<()>,
) {
	while let Ok((mut socket, _)) = listener.accept().await {
		let Some(body) = read_request(&mut socket).await else {
			continue;
		};

		let (status, reply) = match fail.try_recv() {
			| Ok(()) => (502_u16, "502 Bad Gateway"),
			| Err(_) => (200_u16, "200 OK"),
		};

		let body = serde_json::from_slice(&body).unwrap_or(Value::Null);
		if tx.send((status, body)).is_err() {
			return;
		}

		let payload = if status == 200 { r#"{"rejected":[]}"# } else { "{}" };
		let response = format!(
			"HTTP/1.1 {reply}\r\nContent-Type: application/json\r\nContent-Length: \
			 {}\r\nConnection: close\r\n\r\n{payload}",
			payload.len(),
		);

		socket.write_all(response.as_bytes()).await.ok();
		socket.flush().await.ok();
	}
}

async fn read_request(socket: &mut TcpStream) -> Option<Vec<u8>> {
	let mut buf = Vec::new();
	let mut chunk = [0_u8; 4096];
	loop {
		if let Some(head_end) = buf
			.windows(4)
			.position(|window| window == b"\r\n\r\n")
		{
			let content_length: usize = from_utf8(&buf[..head_end])
				.ok()?
				.lines()
				.find_map(|line| {
					line.split_once(':')
						.filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
				})
				.and_then(|(_, value)| value.trim().parse().ok())?;

			let body_start = head_end.checked_add(4)?;
			let body_end = body_start.checked_add(content_length)?;
			if buf.len() >= body_end {
				return Some(buf[body_start..body_end].to_vec());
			}
		}

		let read = socket.read(&mut chunk).await.ok()?;
		if read == 0 {
			return None;
		}

		buf.extend_from_slice(&chunk[..read]);
	}
}

async fn recv(rx: &mut CaptureRx) -> Result<Captured> {
	timeout(Duration::from_secs(10), rx.recv())
		.await
		.map_err(|_| err!("timed out waiting for a push notification"))?
		.ok_or_else(|| err!("stub gateway channel closed"))
}

fn expect_quiescent(rx: &mut CaptureRx) -> Result {
	match rx.try_recv() {
		| Err(TryRecvError::Empty) => Ok(()),
		| Err(TryRecvError::Disconnected) => Err!("stub gateway channel closed"),
		| Ok((_, body)) => Err!("unexpected extra notification: {body}"),
	}
}

fn notification(body: &Value) -> Result<&Value> {
	body.get("notification")
		.ok_or_else(|| err!("push body had no notification object: {body}"))
}
