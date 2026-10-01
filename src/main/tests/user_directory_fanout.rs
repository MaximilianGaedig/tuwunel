#![cfg(test)]

//! A directory search end to end, with appservices on the other end of it.
//!
//! `user_directory.rs` registers an appservice with no URL, so the search there never leaves the
//! server. Here each appservice is a small listener that answers the search the way a bridge does -
//! or does not answer, or answers what it has no business saying - and what is checked is what a
//! client is told: who is listed, in which order, under whose name, with which line under it, and
//! how long it took.
//!
//! The service's own tests cover the merging rule case by case. Nothing there would notice a search
//! that asked the bridges one after another, waited for the slowest, or asked the wrong ones.

#[expect(
	dead_code,
	reason = "the shared client harness exposes helpers used by sibling integration tests"
)]
mod client;

use std::{
	collections::BTreeMap,
	net::TcpListener as StdTcpListener,
	str::from_utf8,
	time::{Duration, Instant},
};

use futures::future::join;
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	spawn,
	sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Result, err, ruma::UserId};
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

const SEARCHER: &str = "directory-fanout-test-token-searcher-0123456789";
const HUMAN: &str = "directory-fanout-test-token-human-0123456789";

const SEARCH_PATH: &str = "/_matrix/app/unstable/im.mxg.user_directory_search";

/// A search has to be back well inside this. The server gives the appservices two and a half
/// seconds; its HTTP client would wait 35 for the one that never answers, and for each in turn if
/// they were asked one after another. The margin is for a slow CI machine, not for the server.
const PATIENCE: Duration = Duration::from_secs(12);

/// One appservice the server has registered, and how its listener behaves.
#[derive(Clone)]
struct Bridge {
	id: &'static str,
	sender: &'static str,

	/// Its `users` namespace, and whether it owns those users or only listens to them.
	users: Option<(&'static str, bool)>,

	/// Whom it says it found, whatever was searched for.
	results: Value,

	/// The one search term it says it had more matches for than it sent.
	limited_for: &'static str,

	/// Reads the question and then never answers it.
	stalls: bool,
}

/// A search one of the listeners received.
struct Asked {
	bridge: &'static str,
	authorization: String,
	body: Value,
}

#[test]
fn a_search_asks_the_bridges_and_merges_what_they_say() -> Result {
	let listener = StdTcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	// Everybody local is listed, puppets included: a puppet the server already knows has to be
	// found by both the server and its bridge for the two to be merged.
	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("show_all_local_users_in_user_directory=true")
		.with_option("show_appservice_users_in_user_directory=true");

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let exercise = async {
			let outcome = fan_out(&services, &base).await;
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

/// The appservices of this test. The IDs are in the order the server holds them, so the one that
/// never answers comes first: asked in turn, it would use up the whole deadline before anybody
/// else was asked.
fn bridges(server_name: &str) -> Vec<Bridge> {
	let user = |localpart: &str| format!("@{localpart}:{server_name}");

	vec![
		Bridge {
			id: "a-slow",
			sender: "slowbot",
			users: Some(("@slow_.*", true)),
			results: json!([{"user_id": user("slow_fanout"), "display_name": "Fanout Late"}]),
			limited_for: "",
			stalls: true,
		},
		Bridge {
			id: "b-network",
			sender: "networkbot",
			users: Some(("@fanout_.*", true)),
			results: json!([
				// Somebody this server has never heard of.
				{
					"user_id": user("fanout_new"),
					"display_name": "Fanout Stranger",
					"im.mxg.context": "12 mutual friends",
				},
				// A puppet it already has, under another name than the server knows them by.
				{
					"user_id": user("fanout_known"),
					"display_name": "Not Their Name",
					"im.mxg.context": "Lives in Warsaw",
				},
				// The stranger again, as a second login would find them.
				{
					"user_id": user("fanout_new"),
					"display_name": "Fanout Stranger Again",
					"im.mxg.context": "3 mutual friends",
				},
				// A real person on this server, who is not this bridge's to describe.
				{
					"user_id": user("human_fanout"),
					"display_name": "Fanout Impostor",
					"im.mxg.context": "Trust me",
				},
				// The right shape of ID on a server it has no users on.
				{"user_id": "@fanout_far:elsewhere.example", "display_name": "Fanout Elsewhere"},
			]),
			limited_for: "",
			stalls: false,
		},
		Bridge {
			id: "c-other",
			sender: "otherbot",
			users: Some(("@other_.*", true)),
			results: json!([
				{"user_id": user("other_fanout"), "display_name": "Other Fanout"},
				// The first bridge's namespace.
				{"user_id": user("fanout_theirs"), "display_name": "Fanout Poached"},
			]),
			limited_for: "fanout",
			stalls: false,
		},
		// Listens to everybody and owns nobody, as a double-puppeting registration does.
		Bridge {
			id: "d-listener",
			sender: "listenerbot",
			users: Some(("@.*", false)),
			results: json!([{"user_id": user("fanout_overheard"), "display_name": "Fanout Overheard"}]),
			limited_for: "",
			stalls: false,
		},
		// No users at all: a bot.
		Bridge {
			id: "e-plain",
			sender: "plainbot",
			users: None,
			results: json!([{"user_id": user("fanout_plain"), "display_name": "Fanout Plain"}]),
			limited_for: "",
			stalls: false,
		},
	]
}

async fn fan_out(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let server_name = services.globals.server_name().as_str();
	let user = |localpart: &str| format!("@{localpart}:{server_name}");
	let searcher = register(services, "seeker", SEARCHER).await?;
	register(services, "human_fanout", HUMAN).await?;

	let (tx, mut rx) = unbounded_channel();
	let bridges = bridges(server_name);
	for bridge in &bridges {
		let listener = TcpListener::bind("127.0.0.1:0").await?;
		let url = format!("http://{}", listener.local_addr()?);
		let users: Vec<Value> = bridge
			.users
			.iter()
			.map(|(regex, exclusive)| json!({"exclusive": exclusive, "regex": regex}))
			.collect();

		let registration = json!({
			"id": bridge.id,
			"url": url,
			"as_token": format!("{}-appservice-token-0123456789abcdef", bridge.id),
			"hs_token": hs_token(bridge.id),
			"sender_localpart": bridge.sender,
			"namespaces": {"users": users, "aliases": [], "rooms": []},
		});

		spawn(play_appservice(listener, bridge.clone(), tx.clone()));
		services
			.appservice
			.register_appservice(serde_json::from_value(registration)?)
			.await?;
	}

	// The puppet the server already has, under the name the server knows.
	let known = user("fanout_known");
	let known_id: &UserId = known.as_str().try_into()?;
	services
		.users
		.create(known_id, None, None)
		.await?;
	services
		.profile
		.set_displayname(known_id, Some("Fanout Known"), None)
		.await?;

	let client = Client { services, base, token: SEARCHER };

	// A client is told the server does this, so it can stop asking the bridges itself.
	let versions: Value = services
		.client
		.clients
		.default
		.get(format!("{base}/_matrix/client/versions"))
		.send()
		.await?
		.json()
		.await?;
	assert_eq!(
		versions["unstable_features"]["im.mxg.user_directory_search"],
		true,
		"the fan-out is not advertised: {versions}"
	);

	/*
	 * Everybody whose name starts a word with "fanout", from the server and two bridges, as one
	 * ranked list: a first word beats a later one whoever found it, and of two equals the one the
	 * server knows comes first.
	 */
	let (answer, took) = search(&client, "fanout", 100).await?;
	assert!(took < PATIENCE, "the search waited {took:?} for a bridge that never answers");

	let found = results(&answer)?;
	assert_eq!(
		ids(found)?,
		[user("fanout_known"), user("fanout_new"), user("human_fanout"), user("other_fanout")],
		"{answer}"
	);

	// Listed by both: the server's name for them, the bridge's line about them.
	assert_eq!(found[0]["display_name"], "Fanout Known", "{answer}");
	assert_eq!(found[0]["im.mxg.context"], "Lives in Warsaw", "{answer}");

	// Named twice by one bridge: listed once, as it was first described.
	assert_eq!(found[1]["display_name"], "Fanout Stranger", "{answer}");
	assert_eq!(found[1]["im.mxg.context"], "12 mutual friends", "{answer}");

	// A local person a bridge tried to describe: found by the server alone, and untouched.
	assert_ne!(found[2]["display_name"], "Fanout Impostor", "{answer}");
	assert!(found[2].get("im.mxg.context").is_none(), "{answer}");

	// No line was sent, so there is no key - not an empty one.
	assert_eq!(found[3]["display_name"], "Other Fanout", "{answer}");
	assert!(found[3].get("im.mxg.context").is_none(), "{answer}");

	assert_eq!(answer["limited"], true, "a bridge said it had more for this term: {answer}");

	/*
	 * A term only the server's rule can place: the puppet it knows matches, and the people the
	 * bridges found - by rules of their own, as far as the server can tell - are kept, last.
	 */
	let (answer, took) = search(&client, "known", 100).await?;
	assert!(took < PATIENCE, "the search waited {took:?} for a bridge that never answers");
	assert_eq!(
		ids(results(&answer)?)?,
		[user("fanout_known"), user("fanout_new"), user("other_fanout")],
		"{answer}"
	);
	assert_eq!(answer["limited"], false, "nobody had more and everybody fits: {answer}");

	// More than fit is said, and the best one is the one kept.
	let (answer, _) = search(&client, "known", 1).await?;
	assert_eq!(ids(results(&answer)?)?, [user("fanout_known")], "{answer}");
	assert_eq!(answer["limited"], true, "three people were cut to one: {answer}");

	/*
	 * What the listeners saw. The three that own users were asked every time, the slow one
	 * included; the two that own nobody were never told what was typed.
	 */
	let asked = drain(&mut rx);
	let terms: BTreeMap<&str, Vec<(&str, u64)>> = asked
		.iter()
		.fold(BTreeMap::new(), |mut terms, asked| {
			let term = asked.body["search_term"].as_str().unwrap_or("(none)");
			let limit = asked.body["limit"].as_u64().unwrap_or(0);
			let of_bridge: &mut Vec<_> = terms.entry(asked.bridge).or_default();
			of_bridge.push((term, limit));
			of_bridge.sort_unstable();

			terms
		});

	let every_search = vec![("fanout", 100), ("known", 1), ("known", 100)];
	let expected: BTreeMap<&str, Vec<(&str, u64)>> = ["a-slow", "b-network", "c-other"]
		.into_iter()
		.map(|bridge| (bridge, every_search.clone()))
		.collect();
	assert_eq!(terms, expected);

	for asked in &asked {
		// Each is asked with its own token, as whoever is searching.
		assert_eq!(asked.authorization, format!("Bearer {}", hs_token(asked.bridge)));
		assert_eq!(asked.body["user_id"], searcher.as_str(), "{}", asked.body);
	}

	Ok(())
}

fn hs_token(bridge: &str) -> String { format!("{bridge}-homeserver-token-0123456789abcdef") }

/// One search as the client makes it: the answer, and how long the client waited for it.
async fn search(client: &Client<'_>, term: &str, limit: u64) -> Result<(Value, Duration)> {
	let started = Instant::now();
	let answer = client
		.post("user_directory/search", &json!({"search_term": term, "limit": limit}))
		.await?;

	Ok((answer, started.elapsed()))
}

fn results(answer: &Value) -> Result<&Vec<Value>> {
	answer
		.get("results")
		.and_then(Value::as_array)
		.ok_or_else(|| err!("no results in {answer}"))
}

/// Who is listed, in the order they are listed.
fn ids(results: &[Value]) -> Result<Vec<&str>> {
	results
		.iter()
		.map(|result| field(result, "user_id"))
		.collect()
}

fn drain(rx: &mut UnboundedReceiver<Asked>) -> Vec<Asked> {
	let mut asked = Vec::new();
	while let Ok(one) = rx.try_recv() {
		asked.push(one);
	}

	asked
}

/// An appservice, as far as the server can tell: it takes each connection as it comes, so one
/// that is kept waiting does not keep the next from being answered.
async fn play_appservice(listener: TcpListener, bridge: Bridge, tx: UnboundedSender<Asked>) {
	while let Ok((socket, _)) = listener.accept().await {
		spawn(answer_one(socket, bridge.clone(), tx.clone()));
	}
}

/// Answers one request: a search with what this bridge has to say, anything else (the server
/// sends an appservice transactions as well) with an empty success.
async fn answer_one(mut socket: TcpStream, bridge: Bridge, tx: UnboundedSender<Asked>) {
	let Some((path, authorization, body)) = read_request(&mut socket).await else {
		return;
	};

	if path != SEARCH_PATH {
		respond(&mut socket, &json!({})).await;
		return;
	}

	let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
	let limited = body["search_term"] == bridge.limited_for;
	tx.send(Asked { bridge: bridge.id, authorization, body })
		.ok();

	if bridge.stalls {
		// Holds the line until the server gives up and hangs up.
		let mut rest = [0_u8; 64];
		while matches!(socket.read(&mut rest).await, Ok(read) if read > 0) {}

		return;
	}

	respond(&mut socket, &json!({"results": bridge.results, "limited": limited})).await;
}

async fn respond(socket: &mut TcpStream, body: &Value) {
	let body = body.to_string();
	let response = format!(
		"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: \
		 close\r\n\r\n{body}",
		body.len()
	);

	socket.write_all(response.as_bytes()).await.ok();
	socket.flush().await.ok();
}

/// The path, the `Authorization` header and the body of one HTTP request.
async fn read_request(socket: &mut TcpStream) -> Option<(String, String, Vec<u8>)> {
	let mut buf = Vec::new();
	let mut chunk = [0_u8; 4096];
	loop {
		if let Some(head_end) = find(&buf, b"\r\n\r\n") {
			let head = from_utf8(buf.get(..head_end)?).ok()?;
			let body_start = head_end.checked_add(4)?;
			let body_end = body_start.checked_add(content_length(head))?;
			if buf.len() >= body_end {
				let path = head
					.lines()
					.next()?
					.split(' ')
					.nth(1)?
					.to_owned();

				let authorization = header(head, "authorization")
					.unwrap_or_default()
					.to_owned();

				let body = buf.get(body_start..body_end)?.to_vec();

				return Some((path, authorization, body));
			}
		}

		let read = socket.read(&mut chunk).await.ok()?;
		if read == 0 {
			return None;
		}

		buf.extend_from_slice(chunk.get(..read)?);
	}
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
	haystack
		.windows(needle.len())
		.position(|window| window == needle)
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
	head.lines()
		.filter_map(|line| line.split_once(':'))
		.find(|(key, _)| key.eq_ignore_ascii_case(name))
		.map(|(_, value)| value.trim())
}

fn content_length(head: &str) -> usize {
	header(head, "content-length")
		.and_then(|value| value.parse().ok())
		.unwrap_or(0)
}
