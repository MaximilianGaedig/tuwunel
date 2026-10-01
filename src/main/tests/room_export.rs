#![cfg(test)]

//! A room export end to end: the events come back in order and in pages that
//! join up, the manifest lists the room's files and says which are stored
//! here, and someone outside the room gets neither.
//!
//! The service's own tests cover how a request is read and how a manifest row
//! is built. Nothing there would notice a page that repeated or dropped the
//! event at its edge, or an endpoint that answered for a room the asker
//! cannot see.

mod client;

use std::net::TcpListener;

use futures::future::join;
use reqwest::RequestBuilder;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{OwnedEventId, RoomId},
};
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

const ALICE: &str = "room-export-test-access-token-alice";
const BOB: &str = "room-export-test-access-token-bob";

#[test]
fn a_room_exports_in_order_with_a_manifest_of_its_files() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let args = Args::default_test(&["fresh", "cleanup"])
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
			let outcome = export(&services, &base).await;
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

async fn export(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let alice_id = register(services, "exportalice", ALICE).await?;
	register(services, "exportbob", BOB).await?;
	let alice = Client { services, base, token: ALICE };
	let bob = Client { services, base, token: BOB };
	let server_name = services.globals.server_name().as_str();

	// A private room of Alice's: Bob is not in it.
	let room = alice.create_room(&json!({})).await?;

	// Three messages, a picture stored here, and a file a bridge would serve from its network.
	let mut sent: Vec<String> = Vec::new();
	for txn in ["one", "two", "three"] {
		let content = json!({"msgtype": "m.text", "body": txn});
		sent.push(send(&alice, &room, txn, &content).await?.to_string());
	}
	let picture = json!({
		"msgtype": "m.image",
		"body": "A day out",
		"filename": "cat.png",
		"url": format!("mxc://{server_name}/roomexportpicture"),
		"info": {"mimetype": "image/png", "size": 1234},
	});
	let picture_id = send(&alice, &room, "picture", &picture)
		.await?
		.to_string();
	sent.push(picture_id.clone());
	let document = json!({
		"msgtype": "m.file",
		"body": "report.pdf",
		"url": "mxc://bridge.example.org/roomexportdocument",
		"info": {"mimetype": "application/pdf", "size": 9000},
	});
	let document_id = send(&alice, &room, "document", &document)
		.await?
		.to_string();
	sent.push(document_id.clone());

	// The whole room in one page: it starts where the room does, and holds the messages in the
	// order they were sent.
	let (status, all) = get(&alice, events_url(base, &room, "")).await?;
	assert_eq!(status, 200, "export: {all}");
	let events = chunk(&all)?;
	assert_eq!(events.first().map(kind), Some("m.room.create"), "oldest first: {all}");
	let messages: Vec<&Value> = events
		.iter()
		.filter(|event| kind(event) == "m.room.message")
		.collect();
	assert_eq!(ids(&messages), sent, "{all}");
	assert!(all.get("end").is_none(), "a room read to its end has nowhere to continue: {all}");
	// The event as `/messages` returns it.
	let first = messages
		.first()
		.ok_or_else(|| err!("no messages in {all}"))?;
	assert_eq!(first["sender"].as_str(), Some(alice_id.as_str()), "{all}");
	assert_eq!(first["content"]["body"].as_str(), Some("one"), "{all}");
	assert!(first["origin_server_ts"].is_u64(), "{all}");

	// Pages of two, followed by their `end`: every message once, in the same order.
	let mut paged: Vec<String> = Vec::new();
	let mut from: Option<String> = None;
	let mut requests = 0_usize;
	loop {
		requests = requests.saturating_add(1);
		assert!(requests <= sent.len().saturating_add(1), "paging does not end: {paged:?}");
		let query = match &from {
			| Some(from) => format!("?types=m.room.message&limit=2&from={from}"),
			| None => "?types=m.room.message&limit=2".to_owned(),
		};
		let (status, page) = get(&alice, events_url(base, &room, &query)).await?;
		assert_eq!(status, 200, "page: {page}");
		let events = chunk(&page)?;
		assert!(events.len() <= 2, "a page holds no more than was asked for: {page}");
		assert!(events.iter().all(|event| kind(event) == "m.room.message"), "{page}");
		paged.extend(events.iter().map(id));
		match page.get("end").and_then(Value::as_str) {
			| Some(end) => from = Some(end.to_owned()),
			| None => break,
		}
	}
	assert_eq!(paged, sent, "pages repeat or drop an event where they join");

	// Newest first is the same events the other way round.
	let query = "?types=m.room.message&dir=b";
	let (status, backwards) = get(&alice, events_url(base, &room, query)).await?;
	assert_eq!(status, 200, "{backwards}");
	let mut reversed: Vec<String> = chunk(&backwards)?.iter().map(id).collect();
	reversed.reverse();
	assert_eq!(reversed, sent, "{backwards}");

	// A stretch of time before anything was sent holds nothing, and says it is done.
	let (status, early) = get(&alice, events_url(base, &room, "?until_ts=1")).await?;
	assert_eq!(status, 200, "{early}");
	assert_eq!(chunk(&early)?.len(), 0, "{early}");
	assert!(early.get("end").is_none(), "{early}");

	// Requests that cannot mean anything are refused, not answered with an empty export.
	for query in ["?since_ts=2000&until_ts=1000", "?dir=sideways", "?from=soon"] {
		let (status, refused) = get(&alice, events_url(base, &room, query)).await?;
		assert_eq!(
			(status, refused["errcode"].as_str()),
			(400, Some("M_INVALID_PARAM")),
			"{query}: {refused}"
		);
	}

	// The manifest: the picture, then the file, each with what a client needs to fetch it.
	let (status, manifest) = get(&alice, media_url(base, &room, "")).await?;
	assert_eq!(status, 200, "manifest: {manifest}");
	let rows = chunk(&manifest)?;
	assert_eq!(rows.len(), 2, "{manifest}");
	assert_eq!(manifest["encrypted"], false, "{manifest}");
	assert!(manifest.get("end").is_none(), "{manifest}");

	let row = &rows[0];
	assert_eq!(row["event_id"].as_str(), Some(picture_id.as_str()), "{manifest}");
	assert_eq!(row["sender"].as_str(), Some(alice_id.as_str()), "{manifest}");
	assert_eq!(row["kind"], "media", "{manifest}");
	assert_eq!(row["msgtype"], "m.image", "{manifest}");
	assert_eq!(row["filename"], "cat.png", "{manifest}");
	assert_eq!(row["mimetype"], "image/png", "{manifest}");
	assert_eq!(row["size"], 1234, "{manifest}");
	assert_eq!(row["url"].as_str(), picture["url"].as_str(), "{manifest}");
	assert_eq!(row["encrypted"], false, "{manifest}");
	assert_eq!(row["stored"], true, "the picture's mxc names this server: {manifest}");
	assert!(row["ts"].is_u64(), "{manifest}");

	let row = &rows[1];
	assert_eq!(row["event_id"].as_str(), Some(document_id.as_str()), "{manifest}");
	assert_eq!(row["kind"], "files", "{manifest}");
	assert_eq!(row["filename"], "report.pdf", "{manifest}");
	assert_eq!(row["size"], 9000, "{manifest}");
	assert_eq!(row["stored"], false, "the file's mxc names a bridge: {manifest}");

	// A page of one stops between the two kinds, and the next page picks the second up.
	let (status, page) = get(&alice, media_url(base, &room, "?limit=1")).await?;
	assert_eq!(status, 200, "{page}");
	assert_eq!(ids(&chunk(&page)?.iter().collect::<Vec<_>>()), [picture_id.clone()], "{page}");
	let end = field(&page, "end")?;
	let (status, rest) = get(&alice, media_url(base, &room, &format!("?limit=1&from={end}"))).await?;
	assert_eq!(status, 200, "{rest}");
	assert_eq!(ids(&chunk(&rest)?.iter().collect::<Vec<_>>()), [document_id.clone()], "{rest}");
	assert!(rest.get("end").is_none(), "{rest}");

	let (status, refused) = get(&alice, media_url(base, &room, "?from=42")).await?;
	assert_eq!((status, refused["errcode"].as_str()), (400, Some("M_INVALID_PARAM")), "{refused}");

	// Bob is not in the room: neither its events nor its files are his to export.
	for url in [events_url(base, &room, ""), media_url(base, &room, "")] {
		let (status, refused) = get(&bob, url).await?;
		assert_eq!((status, refused["errcode"].as_str()), (403, Some("M_FORBIDDEN")), "{refused}");
	}

	Ok(())
}

fn events_url(base: &str, room: &RoomId, query: &str) -> String {
	format!("{base}/_matrix/client/unstable/im.mxg.export/rooms/{room}/events{query}")
}

fn media_url(base: &str, room: &RoomId, query: &str) -> String {
	format!("{base}/_matrix/client/unstable/im.mxg.export/rooms/{room}/media{query}")
}

fn chunk(answer: &Value) -> Result<&Vec<Value>> {
	answer
		.get("chunk")
		.and_then(Value::as_array)
		.ok_or_else(|| err!("no chunk in {answer}"))
}

fn kind(event: &Value) -> &str { event["type"].as_str().unwrap_or("(no type)") }

fn id(event: &Value) -> String {
	event["event_id"]
		.as_str()
		.unwrap_or("(no event id)")
		.to_owned()
}

fn ids(events: &[&Value]) -> Vec<String> { events.iter().map(|event| id(event)).collect() }

async fn send(
	client: &Client<'_>,
	room: &RoomId,
	txn: &str,
	content: &Value,
) -> Result<OwnedEventId> {
	let request = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/{txn}")))
		.json(content);
	let (status, response) = answer(client, request).await?;
	assert_eq!(status, 200, "send: {response}");

	Ok(field(&response, "event_id")?.try_into()?)
}

async fn get(client: &Client<'_>, url: String) -> Result<(u16, Value)> {
	answer(client, client.services.client.clients.default.get(url)).await
}

/// The status and body of a request made as this user, whatever the status: refusals are half of
/// what is checked here.
async fn answer(client: &Client<'_>, request: RequestBuilder) -> Result<(u16, Value)> {
	let response = request.bearer_auth(client.token).send().await?;
	let status = response.status().as_u16();

	Ok((status, response.json().await?))
}
