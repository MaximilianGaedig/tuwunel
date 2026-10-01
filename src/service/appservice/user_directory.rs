use std::{collections::HashMap, time::Duration};

use futures::future::join_all;
use ruma::{
	OwnedMxcUri, OwnedUserId, UserId,
	api::{appservice::Registration, client::user_directory::search_users::v3::User},
};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tuwunel_core::{debug, implement, warn};

use super::RegistrationInfo;
use crate::rooms::search::matcher::{Matcher, Score};

/// The endpoint an appservice answers to be searched.
///
/// Unstable and ours for now: the spec's third-party lookup
/// (`/_matrix/app/v1/thirdparty/user/{protocol}`) matches on protocol-defined fields, which answers
/// "who is this exact handle?" rather than "who is called Anna?", and the user directory is the
/// second question. Prefixed so that a spec'd version can take over without a flag day.
const PATH: &str = "_matrix/app/unstable/im.mxg.user_directory_search";

/// How long a search waits for the appservices, all of them together.
///
/// The search box is typed into and watched, and the endpoint is one response: nothing can be shown
/// until the slowest source has answered or been given up on. The appservice HTTP client's own
/// timeout is `appservice_timeout`, 35 seconds by default, which is right for delivering a
/// transaction and useless here. A bridge that needs longer than this to search its network
/// contributes nothing to this search and is not waited for.
const DEADLINE: Duration = Duration::from_millis(2500);

/// The longest context line passed on, in characters. It is one line under a name, and it comes
/// from outside; a bridge that sends a page of text gets the start of it shown.
const CONTEXT_MAX_CHARS: usize = 256;

/// What the homeserver asks.
///
/// The searching user is named because a bridge searches a network as somebody: it looks through
/// that user's own account on the network and nobody else's.
#[derive(Serialize)]
struct Query<'a> {
	search_term: &'a str,
	limit: usize,
	user_id: &'a UserId,
}

#[derive(Deserialize)]
struct Found {
	user_id: OwnedUserId,
	#[serde(default)]
	display_name: Option<String>,
	#[serde(default)]
	avatar_url: Option<OwnedMxcUri>,
	/// The line the network itself shows to tell this person apart from others of the same name
	/// ("12 mutual friends", "Lives in Warsaw"). Free text from outside: never interpreted here.
	#[serde(default, rename = "im.mxg.context")]
	context: Option<String>,
}

/// One person an appservice found, with the line that says which person it is, if it sent one.
///
/// A pair rather than a type of its own because ruma's `User` has no room for anything but the three
/// spec'd fields; the client API puts the two back together in its own response type.
type Match = (User, Option<String>);

#[derive(Deserialize)]
struct Answer {
	#[serde(default)]
	results: Vec<Found>,
	/// Whether the appservice had more matches than it was allowed to send.
	#[serde(default)]
	limited: bool,
}

/// Everybody the appservices say matches, for a search this server cannot answer on its own, and
/// whether any of them had more to give than the limit allowed.
///
/// A bridged person is unknown to this server until they have a puppet, which happens the first
/// time somebody talks to them - so searching only what is already here answers "who have I already
/// spoken to?" and a contact on a bridged network is missing until it is too late to be useful. The
/// bridges can answer it about their own networks, and answering creates the puppet, so what comes
/// back is an ordinary user any client can open a chat with.
///
/// The appservices are asked at the same time and share one deadline, so the wait is at most
/// `DEADLINE` however many there are. One that does not implement this, is too slow, fails or
/// answers rubbish contributes nothing rather than failing the search.
///
/// Not every appservice is asked; see `is_asked`. What one answers is only believed about its own
/// users; see `vet`.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn search_users(
	&self,
	sender_user: &UserId,
	search_term: &str,
	limit: usize,
) -> (Vec<(User, Option<String>)>, bool) {
	let asked: Vec<RegistrationInfo> = self
		.read()
		.await
		.values()
		.filter(|info| is_asked(info))
		.cloned()
		.collect();

	// Every request is started by the same first poll, so each one's own timer is the shared
	// deadline to within the time it takes to build the requests.
	let answers = join_all(
		asked
			.iter()
			.map(|info| self.ask_until_deadline(info, sender_user, search_term, limit)),
	)
	.await;

	let mut found = Vec::new();
	let mut limited = false;
	for (matches, more) in answers.into_iter().flatten() {
		found.extend(matches);
		limited |= more;
	}

	(found, limited)
}

/// Whether an appservice is asked to search at all.
///
/// Only one that could give an answer worth keeping: it has somewhere to send the question, and it
/// owns users - an exclusive `users` namespace - which is what makes it a bridge as far as a
/// registration can say. Anything else (a bot, a double-puppeting registration with a catch-all
/// non-exclusive namespace, a moderation tool) would be handed what somebody is typing for nothing,
/// because `vet` would discard whatever it sent back.
///
/// Remembering which appservices answered "no such endpoint" would spare the remaining bridges that
/// do not implement this a request each, but it needs state that outlives the search and a rule for
/// forgetting it when a bridge is upgraded. With the requests concurrent and under a deadline, one
/// such request costs the search nothing it would notice.
fn is_asked(info: &RegistrationInfo) -> bool {
	let has_url = info
		.registration
		.url
		.as_deref()
		.is_some_and(|url| !url.is_empty() && url != "null");

	has_url && info.users.exclusive.is_some()
}

/// Asks one appservice and stops waiting for it at the deadline.
#[implement(super::Service)]
async fn ask_until_deadline(
	&self,
	info: &RegistrationInfo,
	sender_user: &UserId,
	search_term: &str,
	limit: usize,
) -> Option<(Vec<Match>, bool)> {
	let asking = self.ask_appservice(&info.registration, sender_user, search_term, limit);
	let Ok(answer) = timeout(DEADLINE, asking).await else {
		debug!(
			appservice = %info.registration.id,
			"The appservice did not answer the search in time"
		);

		return None;
	};

	answer.map(|answer| vet(info, sender_user, answer))
}

/// Asks one appservice, and treats every way it can let us down as "nothing to add".
#[implement(super::Service)]
async fn ask_appservice(
	&self,
	registration: &Registration,
	sender_user: &UserId,
	search_term: &str,
	limit: usize,
) -> Option<Answer> {
	let dest = registration.url.as_deref()?.trim_end_matches('/');
	let response = self
		.services
		.client
		.appservice
		.post(format!("{dest}/{PATH}"))
		.bearer_auth(registration.hs_token.as_str())
		.json(&Query { search_term, limit, user_id: sender_user })
		.send()
		.await
		.ok()?;

	// Not implemented is the ordinary case for a bridge that has not learned this yet.
	if !response.status().is_success() {
		return None;
	}

	response
		.json::<Answer>()
		.await
		.inspect_err(|e| {
			warn!(
				appservice = %registration.id,
				"Could not read the appservice's search results: {e}"
			);
		})
		.ok()
}

/// Keeps what an appservice is entitled to say, and whether it had more.
///
/// An appservice vouches for its own users and for nobody else. Without this check it could put any
/// ID into somebody's results - a real local user, another bridge's puppet, a user of another
/// server - under a name and avatar of its choosing, and the client would show it as that person.
/// "Its own" means its exclusive namespace or its own sender: a non-exclusive namespace says an
/// appservice wants to hear about those users, not that they are its to describe.
///
/// The searching user is dropped as it is from the local results, and the context line is trimmed,
/// cut to a line's length and dropped when nothing is left of it.
fn vet(info: &RegistrationInfo, sender_user: &UserId, answer: Answer) -> (Vec<Match>, bool) {
	let limited = answer.limited;
	let matches = answer
		.results
		.into_iter()
		.filter(|found| found.user_id != sender_user)
		.filter(|found| info.is_exclusive_user_match(&found.user_id))
		.map(|found| {
			let context = found
				.context
				.as_deref()
				.map(str::trim)
				.filter(|context| !context.is_empty())
				.map(|context| {
					context
						.chars()
						.take(CONTEXT_MAX_CHARS)
						.collect::<String>()
				});

			let user = User {
				user_id: found.user_id,
				display_name: found.display_name,
				avatar_url: found.avatar_url,
			};

			(user, context)
		})
		.collect();

	(matches, limited)
}

/// One list out of what this server found and what the appservices found, best first, and whether
/// there was more than fits.
///
/// - Nobody is listed twice. Somebody this server already lists keeps the profile it has for them -
///   that is the one every other part of the client shows - and gains the appservice's context
///   line, which only the appservice can know. Somebody two appservices found, or one found through
///   two logins, is listed as the first one said, with the first context line any of them sent.
/// - The two are ranked together by the one matcher. A bridge matched by its own network's rules,
///   which may know something we cannot see - a handle, a phone number - so a person it found whom
///   our rule does not match is ranked last rather than thrown away.
/// - Among equals, somebody this server already knows comes first: that is someone with a history,
///   worth putting above a stranger of equal standing. Then by ID, so the same search twice gives
///   the same answer.
/// - There was more than fits if the local scan stopped early, if any appservice said so, or if the
///   merged list had to be cut.
///
/// No `self`: this is the whole rule and depends on nothing but what it is given, which is what
/// lets it be tested without a server.
#[implement(super::Service)]
#[must_use]
pub fn merge_user_directory(
	matcher: &Matcher,
	local: Vec<(Score, User)>,
	local_limited: bool,
	remote: Vec<(User, Option<String>)>,
	remote_limited: bool,
	limit: usize,
) -> (Vec<(User, Option<String>)>, bool) {
	let mut merged: Vec<(Score, bool, Match)> = local
		.into_iter()
		.map(|(score, user)| (score, false, (user, None)))
		.collect();

	let mut places: HashMap<OwnedUserId, usize> = merged
		.iter()
		.enumerate()
		.map(|(place, (_, _, (user, _)))| (user.user_id.clone(), place))
		.collect();

	for (user, context) in remote {
		let known = places
			.get(&user.user_id)
			.and_then(|&place| merged.get_mut(place));

		if let Some((_, _, (_, kept))) = known {
			*kept = kept.take().or(context);
			continue;
		}

		let score = [user.display_name.as_deref(), Some(user.user_id.localpart())]
			.into_iter()
			.flatten()
			.filter_map(|text| matcher.score(text))
			.max()
			.unwrap_or(0);

		places.insert(user.user_id.clone(), merged.len());
		merged.push((score, true, (user, context)));
	}

	let limited = local_limited || remote_limited || merged.len() > limit;

	merged.sort_by(
		|(left, left_bridged, (left_user, _)), (right, right_bridged, (right_user, _))| {
			right
				.cmp(left)
				.then_with(|| left_bridged.cmp(right_bridged))
				.then_with(|| left_user.user_id.cmp(&right_user.user_id))
		},
	);

	let results = merged
		.into_iter()
		.take(limit)
		.map(|(_, _, found)| found)
		.collect();

	(results, limited)
}

#[cfg(test)]
mod tests {
	use ruma::{
		OwnedUserId, UserId, api::client::user_directory::search_users::v3::User,
		owned_server_name,
	};
	use serde_json::{Value, json};

	use super::{Answer, CONTEXT_MAX_CHARS, Match, is_asked, vet};
	use crate::{
		appservice::{RegistrationInfo, Service},
		rooms::search::matcher::{Matcher, Score},
	};

	const SEARCHER: &str = "@alice:example.org";

	fn id(user_id: &str) -> OwnedUserId { UserId::parse(user_id).expect("a valid user ID") }

	/// A registration as an administrator would write it, compiled the way the server does.
	fn registration(url: Value, users: Value) -> RegistrationInfo {
		let registration = json!({
			"id": "telegram",
			"url": url,
			"as_token": "as-token-of-the-test-bridge",
			"hs_token": "hs-token-of-the-test-bridge",
			"sender_localpart": "telegrambot",
			"namespaces": {"users": users, "aliases": [], "rooms": []},
		});

		let registration =
			serde_json::from_value(registration).expect("a registration that parses");

		RegistrationInfo::new(registration, &owned_server_name!("example.org"))
			.expect("namespaces that compile")
	}

	/// An ordinary bridge: it has a URL and owns `@telegram_*`.
	fn bridge() -> RegistrationInfo {
		registration(
			json!("http://127.0.0.1:29317"),
			json!([{"exclusive": true, "regex": "@telegram_.*"}]),
		)
	}

	/// What an appservice sent, read the way the server reads it off the wire.
	fn answer(body: Value) -> Answer { serde_json::from_value(body).expect("an answer that parses") }

	fn user(user_id: &str, display_name: Option<&str>) -> User {
		User {
			user_id: id(user_id),
			display_name: display_name.map(ToOwned::to_owned),
			avatar_url: None,
		}
	}

	fn found(user_id: &str, display_name: Option<&str>, context: Option<&str>) -> Match {
		(user(user_id, display_name), context.map(ToOwned::to_owned))
	}

	fn matcher(term: &str) -> Matcher { Matcher::new(term).expect("a term with a word in it") }

	fn ids(results: &[Match]) -> Vec<&str> {
		results
			.iter()
			.map(|(user, _)| user.user_id.as_str())
			.collect()
	}

	fn contexts(results: &[Match]) -> Vec<Option<&str>> {
		results
			.iter()
			.map(|(_, context)| context.as_deref())
			.collect()
	}

	fn merge(
		term: &str,
		local: Vec<(Score, User)>,
		remote: Vec<Match>,
		limit: usize,
	) -> (Vec<Match>, bool) {
		Service::merge_user_directory(&matcher(term), local, false, remote, false, limit)
	}

	#[test]
	fn only_an_appservice_that_has_a_url_and_owns_users_is_asked() {
		let exclusive = json!([{"exclusive": true, "regex": "@telegram_.*"}]);
		let shared = json!([{"exclusive": false, "regex": "@.*"}]);
		let url = json!("http://127.0.0.1:29317");

		assert!(is_asked(&bridge()));
		assert!(is_asked(&registration(
			url.clone(),
			json!([{"exclusive": false, "regex": "@.*"}, {"exclusive": true, "regex": "@tg_.*"}]),
		)));

		// Nowhere to send the question.
		assert!(!is_asked(&registration(Value::Null, exclusive.clone())));
		assert!(!is_asked(&registration(json!(""), exclusive.clone())));
		assert!(!is_asked(&registration(json!("null"), exclusive)));

		// Nobody it could answer about: no users at all, or only ones it listens to.
		assert!(!is_asked(&registration(url.clone(), json!([]))));
		assert!(!is_asked(&registration(url, shared)));
	}

	#[test]
	fn an_appservice_is_believed_about_its_own_users_only() {
		let answer = answer(json!({
			"results": [
				{"user_id": "@telegram_1:example.org", "display_name": "Max"},
				// A real person on this server, under a name of the appservice's choosing.
				{"user_id": "@bob:example.org", "display_name": "Bob (click me)"},
				// Another bridge's puppet.
				{"user_id": "@whatsapp_1:example.org"},
				// The right shape of ID, on a server this appservice has no users on.
				{"user_id": "@telegram_1:elsewhere.example"},
				// Its own bot is its own.
				{"user_id": "@telegrambot:example.org"},
				{"user_id": "@telegram_2:example.org"},
			],
		}));

		let (matches, limited) = vet(&bridge(), &id(SEARCHER), answer);

		assert_eq!(ids(&matches), [
			"@telegram_1:example.org",
			"@telegrambot:example.org",
			"@telegram_2:example.org"
		]);
		assert_eq!(matches[0].0.display_name.as_deref(), Some("Max"));
		assert!(!limited);
	}

	#[test]
	fn a_shared_namespace_is_not_ownership() {
		let info = registration(
			json!("http://127.0.0.1:29317"),
			json!([
				{"exclusive": true, "regex": "@telegram_.*"},
				{"exclusive": false, "regex": "@.*"},
			]),
		);
		let answer = answer(json!({
			"results": [{"user_id": "@bob:example.org"}, {"user_id": "@telegram_1:example.org"}],
		}));

		let (matches, _) = vet(&info, &id(SEARCHER), answer);

		assert_eq!(ids(&matches), ["@telegram_1:example.org"]);
	}

	#[test]
	fn the_searcher_is_never_a_result() {
		let searcher = id("@telegram_me:example.org");
		let answer = answer(json!({
			"results": [{"user_id": "@telegram_me:example.org"}, {"user_id": "@telegram_1:example.org"}],
		}));

		let (matches, _) = vet(&bridge(), &searcher, answer);

		assert_eq!(ids(&matches), ["@telegram_1:example.org"]);
	}

	#[test]
	fn the_appservice_says_whether_it_had_more() {
		let more = answer(json!({"results": [], "limited": true}));
		let all = answer(json!({"results": [], "limited": false}));
		let silent = answer(json!({"results": []}));
		let empty = answer(json!({}));

		assert!(vet(&bridge(), &id(SEARCHER), more).1);
		assert!(!vet(&bridge(), &id(SEARCHER), all).1);
		assert!(!vet(&bridge(), &id(SEARCHER), silent).1);
		assert!(!vet(&bridge(), &id(SEARCHER), empty).1);
	}

	#[test]
	fn the_context_line_is_read_trimmed_and_cut() {
		// Two bytes each, so cutting by bytes would either split one or keep half as many.
		let long = "ż".repeat(CONTEXT_MAX_CHARS.saturating_mul(2));
		let answer = answer(json!({
			"results": [
				{"user_id": "@telegram_1:example.org", "im.mxg.context": "12 mutual friends"},
				{"user_id": "@telegram_2:example.org", "im.mxg.context": "  Lives in Warsaw \n"},
				{"user_id": "@telegram_3:example.org", "im.mxg.context": " \t "},
				{"user_id": "@telegram_4:example.org", "im.mxg.context": ""},
				{"user_id": "@telegram_5:example.org"},
				{"user_id": "@telegram_6:example.org", "im.mxg.context": long},
				// The stable name is not ours to read yet.
				{"user_id": "@telegram_7:example.org", "context": "ignored"},
			],
		}));

		let (matches, _) = vet(&bridge(), &id(SEARCHER), answer);
		let cut = "ż".repeat(CONTEXT_MAX_CHARS);

		assert_eq!(contexts(&matches), [
			Some("12 mutual friends"),
			Some("Lives in Warsaw"),
			None,
			None,
			None,
			Some(cut.as_str()),
			None
		]);
	}

	#[test]
	fn somebody_found_twice_is_listed_once() {
		let remote = vec![
			found("@telegram_1:example.org", Some("Max Mueller"), None),
			found("@telegram_2:example.org", Some("Max Schmidt"), Some("Lives in Warsaw")),
			// The same person through a second login, which knows a line the first did not.
			found("@telegram_1:example.org", Some("Maximilian"), Some("12 mutual friends")),
			// And a third time: the first line sent stays.
			found("@telegram_1:example.org", None, Some("3 mutual friends")),
			found("@telegram_2:example.org", None, Some("Lives in Berlin")),
		];

		let (results, limited) = merge("max", Vec::new(), remote, 10);

		assert_eq!(ids(&results), ["@telegram_1:example.org", "@telegram_2:example.org"]);
		assert_eq!(results[0].0.display_name.as_deref(), Some("Max Mueller"));
		assert_eq!(contexts(&results), [Some("12 mutual friends"), Some("Lives in Warsaw")]);
		assert!(!limited, "two people fit in ten, however often they were named");
	}

	#[test]
	fn somebody_already_listed_keeps_their_profile_and_gains_the_context() {
		let local = vec![
			(4, user("@telegram_1:example.org", Some("Max Mueller"))),
			(4, user("@max:example.org", Some("Max"))),
		];
		let remote = vec![found(
			"@telegram_1:example.org",
			Some("Somebody Else Entirely"),
			Some("12 mutual friends"),
		)];

		let (results, limited) = merge("max", local, remote, 10);

		assert_eq!(ids(&results), ["@max:example.org", "@telegram_1:example.org"]);
		assert_eq!(results[1].0.display_name.as_deref(), Some("Max Mueller"));
		assert_eq!(contexts(&results), [None, Some("12 mutual friends")]);
		assert!(!limited);
	}

	#[test]
	fn everybody_is_ranked_together() {
		let local = vec![
			(3, user("@anna:example.org", Some("Anna Max"))),
			(4, user("@max:example.org", Some("Max"))),
		];
		let remote = vec![
			// Found by something only the network can see: no word here starts with "max".
			found("@telegram_9:example.org", Some("Unrelated Name"), None),
			// A later word matches: as good as Anna, and a stranger.
			found("@telegram_3:example.org", Some("Eva Max"), None),
			// The first word matches: as good as the local Max, and a stranger.
			found("@telegram_2:example.org", Some("Max Schmidt"), None),
			found("@telegram_1:example.org", Some("Max Mueller"), None),
		];

		let (results, limited) = merge("max", local, remote, 10);

		assert_eq!(ids(&results), [
			"@max:example.org",
			"@telegram_1:example.org",
			"@telegram_2:example.org",
			"@anna:example.org",
			"@telegram_3:example.org",
			"@telegram_9:example.org"
		]);
		assert!(!limited);
	}

	#[test]
	fn a_bridged_person_is_matched_by_their_id_as_well() {
		let remote = vec![
			found("@telegram_1:example.org", Some("Unrelated Name"), None),
			found("@max_telegram:example.org", None, None),
		];

		let (results, _) = merge("max", Vec::new(), remote, 10);

		assert_eq!(ids(&results), ["@max_telegram:example.org", "@telegram_1:example.org"]);
	}

	#[test]
	fn the_list_is_cut_to_the_limit_and_says_so() {
		let local = vec![(4, user("@max:example.org", Some("Max")))];
		let remote = vec![
			found("@telegram_1:example.org", Some("Max Mueller"), None),
			found("@telegram_2:example.org", Some("Max Schmidt"), None),
		];

		let (results, limited) = merge("max", local.clone(), remote.clone(), 2);
		assert_eq!(ids(&results), ["@max:example.org", "@telegram_1:example.org"]);
		assert!(limited);

		let (results, limited) = merge("max", local.clone(), remote.clone(), 3);
		assert_eq!(results.len(), 3);
		assert!(!limited, "exactly as many as were asked for is all of them");

		let (results, limited) = merge("max", local, remote, 0);
		assert!(results.is_empty());
		assert!(limited);
	}

	#[test]
	fn more_at_any_source_is_more() {
		let local = || vec![(4, user("@max:example.org", Some("Max")))];
		let remote = || vec![found("@telegram_1:example.org", Some("Max Mueller"), None)];
		let merge = |local_limited, remote_limited| {
			Service::merge_user_directory(
				&matcher("max"),
				local(),
				local_limited,
				remote(),
				remote_limited,
				10,
			)
			.1
		};

		assert!(!merge(false, false));
		assert!(merge(true, false), "the local scan stopped early");
		assert!(merge(false, true), "an appservice had more than it could send");
		assert!(merge(true, true));
	}

	#[test]
	fn what_an_appservice_may_not_say_does_not_reach_the_list() {
		let answer = answer(json!({
			"results": [
				{"user_id": "@telegram_1:example.org", "display_name": "Max Mueller", "im.mxg.context": "12 mutual friends"},
				// A local person the server lists itself: neither the name nor the line is taken.
				{"user_id": "@max:example.org", "display_name": "Max (verified)", "im.mxg.context": "Trust me"},
			],
			"limited": true,
		}));
		let local = vec![(4, user("@max:example.org", Some("Max")))];

		let (remote, remote_limited) = vet(&bridge(), &id(SEARCHER), answer);
		let (results, limited) = Service::merge_user_directory(
			&matcher("max"),
			local,
			false,
			remote,
			remote_limited,
			10,
		);

		assert_eq!(ids(&results), ["@max:example.org", "@telegram_1:example.org"]);
		assert_eq!(results[0].0.display_name.as_deref(), Some("Max"));
		assert_eq!(contexts(&results), [None, Some("12 mutual friends")]);
		assert!(limited);
	}
}
