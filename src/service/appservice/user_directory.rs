use futures::StreamExt;
use ruma::{
	OwnedMxcUri, OwnedUserId, UserId,
	api::{appservice::Registration, client::user_directory::search_users::v3::User},
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{implement, utils::stream::IterStream, warn};

/// The endpoint an appservice answers to be searched.
///
/// Unstable and ours for now: the spec's third-party lookup
/// (`/_matrix/app/v1/thirdparty/user/{protocol}`) matches on protocol-defined fields, which answers
/// "who is this exact handle?" rather than "who is called Anna?", and the user directory is the
/// second question. Prefixed so that a spec'd version can take over without a flag day.
const PATH: &str = "_matrix/app/unstable/im.mxg.user_directory_search";

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
}

#[derive(Deserialize)]
struct Answer {
	#[serde(default)]
	results: Vec<Found>,
}

/// Everybody the appservices say matches, for a search this server cannot answer on its own.
///
/// A bridged person is unknown to this server until they have a puppet, which happens the first
/// time somebody talks to them - so searching only what is already here answers "who have I already
/// spoken to?" and a contact on a bridged network is missing until it is too late to be useful. The
/// bridges can answer it about their own networks, and answering creates the puppet, so what comes
/// back is an ordinary user any client can open a chat with.
///
/// Every appservice is asked at once, and one that does not implement this, has no URL, fails or
/// answers rubbish contributes nothing rather than failing the search.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn search_users(&self, sender_user: &UserId, search_term: &str, limit: usize) -> Vec<User> {
	let registrations: Vec<Registration> = self
		.read()
		.await
		.values()
		.map(|info| info.registration.clone())
		.filter(|registration| {
			registration
				.url
				.as_deref()
				.is_some_and(|url| !url.is_empty() && url != "null")
		})
		.collect();

	registrations
		.into_iter()
		.stream()
		.filter_map(async |registration| {
			self.ask_appservice(&registration, sender_user, search_term, limit)
				.await
		})
		.flat_map(IterStream::stream)
		.collect()
		.await
}

/// Asks one appservice, and treats every way it can let us down as "nothing to add".
#[implement(super::Service)]
async fn ask_appservice(
	&self,
	registration: &Registration,
	sender_user: &UserId,
	search_term: &str,
	limit: usize,
) -> Option<Vec<User>> {
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

	// Not implemented is the ordinary case for any appservice that is not a bridge.
	if !response.status().is_success() {
		return None;
	}

	let answer = response
		.json::<Answer>()
		.await
		.inspect_err(|e| {
			warn!(
				appservice = %registration.id,
				"Could not read the appservice's search results: {e}"
			);
		})
		.ok()?;

	Some(
		answer
			.results
			.into_iter()
			.filter(|found| found.user_id != sender_user)
			.map(|found| User {
				user_id: found.user_id,
				display_name: found.display_name,
				avatar_url: found.avatar_url,
			})
			.collect(),
	)
}
