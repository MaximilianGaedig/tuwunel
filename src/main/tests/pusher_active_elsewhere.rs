#![cfg(test)]

//! Push delivery while the user is active in a room on another device:
//! `push_active_elsewhere_s`.
//!
//! One user has two devices, a phone with an `event_id_only` pusher on a stub
//! gateway and a desktop. Another user sends into rooms they share. Through the
//! real client API and the sending queue, the test asserts which messages
//! reach the phone's gateway in full (with an `event_id`, which starts Element
//! X's notification extension) and which only as a counts-only badge update.

use std::{
	fs::remove_dir_all,
	net::TcpListener as StdTcpListener,
	path::PathBuf,
	str::from_utf8,
	time::Duration,
};

use futures::future::join;
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	spawn,
	sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
	task::JoinHandle,
	time::{Instant, sleep, timeout},
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err, implement,
	ruma::{EventId, OwnedEventId, RoomId, UserId},
};
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

#[expect(dead_code)]
mod client;

const PHONE_TOKEN: &str = "active-elsewhere-phone-token-padded-to-32-chars";
const DESKTOP_TOKEN: &str = "active-elsewhere-desktop-token-padded-to-32-chars";
const SENDER_TOKEN: &str = "active-elsewhere-sender-token-padded-to-32-chars";
const NOTIFY_PATH: &str = "/_matrix/push/v1/notify";
const WINDOW_S: u64 = 4;

/// How long a push may take to reach the gateway.
const PUSH_DEADLINE: Duration = Duration::from_secs(10);

/// No further notification for this long counts as the gateway gone quiet.
const SETTLE: Duration = Duration::from_millis(600);

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn pusher_active_elsewhere() -> Result {
	let listener = StdTcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = DatabasePath(Args::test_database_path("pusher-active-elsewhere"));

	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option(format!("database_path={:?}", db_path.0))
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("ip_range_denylist=[]")
		.with_option(format!("push_active_elsewhere_s={WINDOW_S}"));

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

		let (run_result, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run_result?;

		outcome
	});

	drop(runtime);

	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let reader_id = register(services, "activereader", PHONE_TOKEN).await?;
	services
		.users
		.create_device(&reader_id, None, (Some(DESKTOP_TOKEN), None), None, None, None)
		.await?;

	register(services, "activesender", SENDER_TOKEN).await?;

	let phone = Client { services, base, token: PHONE_TOKEN };
	let desktop = Client { services, base, token: DESKTOP_TOKEN };
	let sender = Client { services, base, token: SENDER_TOKEN };

	let mut rooms = Vec::new();
	for _ in 0..3 {
		let room = sender
			.create_room(&json!({ "preset": "private_chat", "invite": [reader_id] }))
			.await?;

		phone.join(&room).await?;
		rooms.push(room);
	}

	let [a, b, c] = [&rooms[0], &rooms[1], &rooms[2]];

	let listener = TcpListener::bind("127.0.0.1:0").await?;
	let url = format!("http://{}{NOTIFY_PATH}", listener.local_addr()?);
	let (tx, rx) = unbounded_channel();
	let _stub = AbortOnDrop(spawn(stub_gateway(listener, tx)));
	let mut gw = Gateway { rx };

	phone
		.post(
			"pushers/set",
			&json!({
				"kind": "http",
				"app_id": "app.tuwunel.test",
				"pushkey": "pk-phone",
				"app_display_name": "Tuwunel Test",
				"device_display_name": "Phone",
				"lang": "en",
				"data": { "url": url, "format": "event_id_only" },
			}),
		)
		.await?;

	// No activity yet: a message pushes in full.
	let first = sender.message(a, "a1", "hello", None).await?;
	gw.expect_full(&first, "a message before any activity")
		.await?;

	// The desktop reads room a: the next message there is a badge on the phone.
	desktop.receipt(a, &first).await?;
	gw.settle().await;

	let second = sender.message(a, "a2", "again", None).await?;
	gw.expect_badge(&second, "a message while the desktop reads the room")
		.await?;

	// A mention and a call invite still push in full.
	let mention = sender
		.message(a, "a3", "hey you", Some(&reader_id))
		.await?;
	gw.expect_full(&mention, "a mention while active elsewhere")
		.await?;

	let call = sender.call_invite(a, "a4").await?;
	gw.expect_full(&call, "a call invite while active elsewhere")
		.await?;

	// An encrypted event is held to the same rule: the user is in the room.
	let encrypted = sender.encrypted(a, "a5").await?;
	gw.expect_badge(&encrypted, "an encrypted event while active elsewhere")
		.await?;

	// Activity on the pusher's own device does not count.
	let b1 = sender.message(b, "b1", "hello", None).await?;
	gw.expect_full(&b1, "the first message in room b").await?;
	phone.read_markers(b, &b1).await?;
	gw.settle().await;

	let b2 = sender.message(b, "b2", "again", None).await?;
	gw.expect_full(&b2, "a message after a receipt from the phone itself")
		.await?;

	// Sending a message from the desktop counts as being active.
	desktop.message(c, "c0", "i'm here", None).await?;
	gw.settle().await;

	let c1 = sender.message(c, "c1", "hello", None).await?;
	gw.expect_badge(&c1, "a message after the desktop sent one")
		.await?;

	// Once the window has passed since the desktop's activity, full again.
	sleep(Duration::from_secs(WINDOW_S) + Duration::from_millis(500)).await;

	let later = sender.message(a, "a6", "later", None).await?;
	gw.expect_full(&later, "a message after the window").await
}

struct Gateway {
	rx: UnboundedReceiver<Value>,
}

impl Gateway {
	/// Wait for the push for `event_id`: either a full one naming it or a
	/// counts-only badge; then require the gateway to go quiet.
	async fn outcome(&mut self, event_id: &EventId) -> Result<&'static str> {
		let deadline = Instant::now() + PUSH_DEADLINE;
		loop {
			let left = deadline.saturating_duration_since(Instant::now());
			let body = timeout(left, self.rx.recv())
				.await
				.map_err(|_| err!("no push for {event_id} reached the gateway"))?
				.ok_or_else(|| err!("stub gateway channel closed"))?;

			let notification = body
				.get("notification")
				.ok_or_else(|| err!("push body had no notification: {body}"))?;

			match notification.get("event_id").and_then(Value::as_str) {
				| Some(id) if id == event_id.as_str() => {
					self.settle().await;
					return Ok("full");
				},
				| Some(other) => return Err!("unexpected full push for {other}: {body}"),
				| None if notification.get("counts").is_some() => {
					// A late full push for this event would arrive now.
					if let Ok(Some(next)) = timeout(SETTLE, self.rx.recv()).await {
						let id = next
							.pointer("/notification/event_id")
							.and_then(Value::as_str);
						if id == Some(event_id.as_str()) {
							return Ok("full");
						}
					}
					self.settle().await;
					return Ok("badge");
				},
				| None => return Err!("push with neither event nor counts: {body}"),
			}
		}
	}

	async fn expect_full(&mut self, event_id: &EventId, case: &str) -> Result {
		match self.outcome(event_id).await? {
			| "full" => Ok(()),
			| got => Err!("{case}: expected a full push for {event_id}, got a {got}"),
		}
	}

	async fn expect_badge(&mut self, event_id: &EventId, case: &str) -> Result {
		match self.outcome(event_id).await? {
			| "badge" => Ok(()),
			| got => Err!("{case}: expected only a badge for {event_id}, got a {got} push"),
		}
	}

	/// Drain whatever arrives until the gateway has been quiet for a while,
	/// such as the badge refresh a receipt sends.
	async fn settle(&mut self) {
		while let Ok(Some(_)) = timeout(SETTLE, self.rx.recv()).await {}
	}
}

#[implement(Client, params = "<'_>")]
async fn join(&self, room_id: &RoomId) -> Result {
	self.post(&format!("rooms/{room_id}/join"), &json!({}))
		.await
		.map(drop)
}

#[implement(Client, params = "<'_>")]
async fn receipt(&self, room_id: &RoomId, event_id: &EventId) -> Result {
	self.post(&format!("rooms/{room_id}/receipt/m.read/{event_id}"), &json!({}))
		.await
		.map(drop)
}

#[implement(Client, params = "<'_>")]
async fn read_markers(&self, room_id: &RoomId, event_id: &EventId) -> Result {
	let body = json!({ "m.fully_read": event_id, "m.read": event_id });

	self.post(&format!("rooms/{room_id}/read_markers"), &body)
		.await
		.map(drop)
}

#[implement(Client, params = "<'_>")]
async fn send(
	&self,
	room_id: &RoomId,
	kind: &str,
	txn: &str,
	content: &Value,
) -> Result<OwnedEventId> {
	let response: Value = self
		.services
		.client
		.clients
		.default
		.put(self.url(&format!("rooms/{room_id}/send/{kind}/{txn}")))
		.bearer_auth(self.token)
		.json(content)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	Ok(field(&response, "event_id")?.try_into()?)
}

#[implement(Client, params = "<'_>")]
async fn message(
	&self,
	room_id: &RoomId,
	txn: &str,
	body: &str,
	mention: Option<&UserId>,
) -> Result<OwnedEventId> {
	let mentions: Vec<_> = mention.into_iter().collect();
	let content = json!({
		"msgtype": "m.text",
		"body": body,
		"m.mentions": { "user_ids": mentions },
	});

	self.send(room_id, "m.room.message", txn, &content)
		.await
}

#[implement(Client, params = "<'_>")]
async fn call_invite(&self, room_id: &RoomId, txn: &str) -> Result<OwnedEventId> {
	let content = json!({
		"call_id": txn,
		"version": "1",
		"lifetime": 60000,
		"offer": { "type": "offer", "sdp": "v=0" },
	});

	self.send(room_id, "m.call.invite", txn, &content)
		.await
}

#[implement(Client, params = "<'_>")]
async fn encrypted(&self, room_id: &RoomId, txn: &str) -> Result<OwnedEventId> {
	let content = json!({
		"algorithm": "m.megolm.v1.aes-sha2",
		"ciphertext": "AwgAEnAcdoe",
		"device_id": "SENDERDEV",
		"sender_key": "senderkey",
		"session_id": "sessionid",
	});

	self.send(room_id, "m.room.encrypted", txn, &content)
		.await
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
	fn drop(&mut self) { self.0.abort(); }
}

/// Answers every notify with 200 and no rejected keys.
async fn stub_gateway(listener: TcpListener, tx: UnboundedSender<Value>) {
	while let Ok((mut socket, _)) = listener.accept().await {
		let Some(body) = read_request(&mut socket).await else {
			continue;
		};

		let body = serde_json::from_slice(&body).unwrap_or(Value::Null);
		if tx.send(body).is_err() {
			return;
		}

		let payload = r#"{"rejected":[]}"#;
		let response = format!(
			"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
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
