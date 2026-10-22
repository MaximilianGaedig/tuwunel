#![cfg(test)]
#![cfg(feature = "media_thumbnail")]

//! Media downloads by a push notification extension: `nse_media_user_agents`.
//!
//! One user has a phone with an `event_id_only` pusher on a stub gateway and
//! a desktop. Another user sends media into their room. After each full push,
//! the test downloads the media the way Element X's notification extension
//! does (the phone's token, the extension's user agent) and the way the app
//! and other devices do, and asserts that only the extension's downloads of
//! audio, video and large images are reduced.

use std::{
	fs::remove_dir_all,
	io::Cursor,
	net::TcpListener as StdTcpListener,
	path::PathBuf,
	str::from_utf8,
	time::Duration,
};

use futures::future::join;
use image::{ImageFormat, RgbImage};
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
	ruma::{EventId, OwnedEventId, RoomId},
};
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

#[expect(dead_code)]
mod client;

const PHONE_TOKEN: &str = "nse-media-phone-token-padded-to-32-chars";
const DESKTOP_TOKEN: &str = "nse-media-desktop-token-padded-to-32-chars";
const SENDER_TOKEN: &str = "nse-media-sender-token-padded-to-32-chars";
const NOTIFY_PATH: &str = "/_matrix/push/v1/notify";

const NSE_AGENT: &str = "Element X/26.10.4 (iPhone 15 Pro; iOS 27.2.0; Scale/2.00)";
const APP_AGENT: &str = "Element X/26.10.4 (iPhone 15 Pro; iOS 27.2.0; Scale/1.00)";
const MAX_BYTES: usize = 50_000;

const PUSH_DEADLINE: Duration = Duration::from_secs(10);

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn pusher_nse_media() -> Result {
	let listener = StdTcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = DatabasePath(Args::test_database_path("pusher-nse-media"));

	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option(format!("database_path={:?}", db_path.0))
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("ip_range_denylist=[]")
		.with_option("nse_media_user_agents=[\"^Element X/.*; Scale/2[.]00[)]$\"]")
		.with_option(format!("nse_media_max_bytes={MAX_BYTES}"))
		.with_option("nse_media_thumbnail=true");

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

	let reader_id = register(services, "nsereader", PHONE_TOKEN).await?;
	services
		.users
		.create_device(&reader_id, None, (Some(DESKTOP_TOKEN), None), None, None, None)
		.await?;

	register(services, "nsesender", SENDER_TOKEN).await?;

	let phone = Client { services, base, token: PHONE_TOKEN };
	let desktop = Client { services, base, token: DESKTOP_TOKEN };
	let sender = Client { services, base, token: SENDER_TOKEN };

	let room = sender
		.create_room(&json!({ "preset": "private_chat", "invite": [reader_id] }))
		.await?;

	phone
		.post(&format!("rooms/{room}/join"), &json!({}))
		.await?;

	let listener = TcpListener::bind("127.0.0.1:0").await?;
	let url = format!("http://{}{NOTIFY_PATH}", listener.local_addr()?);
	let (tx, rx) = unbounded_channel();
	let _stub = AbortOnDrop(spawn(stub_gateway(listener, tx)));
	let mut gateway = rx;

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

	let big = noise_png(1200, 900)?;
	let small = noise_png(16, 16)?;
	let audio: Vec<u8> = (0..4000_u32).map(|i| (i % 251) as u8).collect();
	if big.len() <= MAX_BYTES || small.len() > MAX_BYTES {
		return Err!("fixture sizes are wrong: big {} small {}", big.len(), small.len());
	}

	let big_mxc = sender.upload(&big, "image/png").await?;
	let small_mxc = sender.upload(&small, "image/png").await?;
	let audio_mxc = sender.upload(&audio, "audio/ogg").await?;
	let unsent_mxc = sender.upload(&big, "image/png").await?;

	// Before any push, the extension's agent gets the original.
	expect_original(&phone, &big_mxc, NSE_AGENT, &big, "a big image before its push")
		.await?;

	let event = sender
		.media(&room, "t1", "m.image", &big_mxc, "image/png")
		.await?;
	wait_push(&mut gateway, &event).await?;

	// The extension (phone token, extension agent) gets a thumbnail.
	let (status, kind, body) = phone.download(&big_mxc, NSE_AGENT).await?;
	if status != 200 || !kind.starts_with("image/") {
		return Err!("the extension's big image download answered {status} {kind}");
	}
	if body.len() >= big.len() {
		return Err!(
			"the extension got {} bytes for a {} byte image: expected a thumbnail",
			body.len(),
			big.len()
		);
	}
	let thumb = image::load_from_memory(&body).map_err(|e| err!("thumbnail: {e}"))?;
	if thumb.width() > 800 || thumb.height() > 600 {
		return Err!("thumbnail is {}x{}", thumb.width(), thumb.height());
	}

	// The app on the same phone, and the desktop, still get the original.
	expect_original(&phone, &big_mxc, APP_AGENT, &big, "the app's download").await?;
	expect_original(&desktop, &big_mxc, NSE_AGENT, &big, "another device's download")
		.await?;

	// Media that was not pushed is the original even to the extension.
	expect_original(&phone, &unsent_mxc, NSE_AGENT, &big, "media never pushed").await?;

	// A small image is the original.
	let event = sender
		.media(&room, "t2", "m.image", &small_mxc, "image/png")
		.await?;
	wait_push(&mut gateway, &event).await?;
	expect_original(&phone, &small_mxc, NSE_AGENT, &small, "a small image").await?;

	// Audio is not sent to the extension at all: 404, whatever its size.
	let event = sender
		.media(&room, "t3", "m.audio", &audio_mxc, "audio/ogg")
		.await?;
	wait_push(&mut gateway, &event).await?;

	let (status, ..) = phone.download(&audio_mxc, NSE_AGENT).await?;
	if status != 404 {
		return Err!("the extension's audio download answered {status}, expected 404");
	}
	expect_original(&phone, &audio_mxc, APP_AGENT, &audio, "the app's audio download").await
}

async fn expect_original(
	client: &Client<'_>,
	mxc: &str,
	agent: &str,
	original: &[u8],
	case: &str,
) -> Result {
	let (status, _, body) = client.download(mxc, agent).await?;
	if status != 200 || body != original {
		return Err!(
			"{case}: answered {status} with {} bytes, expected the {} byte original",
			body.len(),
			original.len()
		);
	}

	Ok(())
}

/// Wait for the full push of `event_id`, then for its delivery to be recorded.
async fn wait_push(gateway: &mut UnboundedReceiver<Value>, event_id: &EventId) -> Result {
	let deadline = Instant::now() + PUSH_DEADLINE;
	loop {
		let left = deadline.saturating_duration_since(Instant::now());
		let body = timeout(left, gateway.recv())
			.await
			.map_err(|_| err!("no full push for {event_id}"))?
			.ok_or_else(|| err!("stub gateway channel closed"))?;

		if body
			.pointer("/notification/event_id")
			.and_then(Value::as_str)
			== Some(event_id.as_str())
		{
			sleep(Duration::from_millis(300)).await;
			return Ok(());
		}
	}
}

fn noise_png(width: u32, height: u32) -> Result<Vec<u8>> {
	let mut seed: u32 = 0x9E37_79B9;
	let image = RgbImage::from_fn(width, height, |_, _| {
		seed ^= seed << 13;
		seed ^= seed >> 17;
		seed ^= seed << 5;
		let [r, g, b, _] = seed.to_le_bytes();
		image::Rgb([r, g, b])
	});

	let mut out = Cursor::new(Vec::new());
	image
		.write_to(&mut out, ImageFormat::Png)
		.map_err(|e| err!("png: {e}"))?;

	Ok(out.into_inner())
}

#[implement(Client, params = "<'_>")]
async fn upload(&self, data: &[u8], content_type: &str) -> Result<String> {
	let response: Value = self
		.services
		.client
		.clients
		.default
		.post(format!("{}/_matrix/media/v3/upload", self.base))
		.bearer_auth(self.token)
		.header("content-type", content_type)
		.body(data.to_vec())
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	Ok(field(&response, "content_uri")?.to_owned())
}

/// Download as `agent`: status, content type and body.
#[implement(Client, params = "<'_>")]
async fn download(&self, mxc: &str, agent: &str) -> Result<(u16, String, Vec<u8>)> {
	let path = mxc
		.strip_prefix("mxc://")
		.ok_or_else(|| err!("not an mxc uri: {mxc}"))?;

	let response = self
		.services
		.client
		.clients
		.default
		.get(format!("{}/_matrix/client/v1/media/download/{path}", self.base))
		.bearer_auth(self.token)
		.header("user-agent", agent)
		.send()
		.await?;

	let status = response.status().as_u16();
	let kind = response
		.headers()
		.get("content-type")
		.and_then(|value| value.to_str().ok())
		.unwrap_or_default()
		.to_owned();

	Ok((status, kind, response.bytes().await?.to_vec()))
}

#[implement(Client, params = "<'_>")]
async fn media(
	&self,
	room_id: &RoomId,
	txn: &str,
	msgtype: &str,
	mxc: &str,
	mimetype: &str,
) -> Result<OwnedEventId> {
	let content = json!({
		"msgtype": msgtype,
		"body": "attachment",
		"url": mxc,
		"info": { "mimetype": mimetype },
	});

	let response: Value = self
		.services
		.client
		.clients
		.default
		.put(self.url(&format!("rooms/{room_id}/send/m.room.message/{txn}")))
		.bearer_auth(self.token)
		.json(&content)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	Ok(field(&response, "event_id")?.try_into()?)
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
