use std::collections::HashSet;

use axum::extract::State;
use futures::{FutureExt, StreamExt, pin_mut};
use ruma::{
	OwnedUserId, UserId,
	api::client::user_directory::search_users::v3::{Request, Response, User},
	events::room::join_rules::JoinRule,
};
use tuwunel_core::{
	Result,
	utils::{
		BoolExt, FutureBoolExt,
		math::usize_from_ruma_bounded,
		stream::{BroadbandExt, ReadyExt, WidebandExt},
	},
};
use tuwunel_service::{Services, rooms::search::matcher::Matcher};

use crate::Ruma;

// Tuwunel can handle a lot more results than synapse
const LIMIT_MAX: usize = 500;
const LIMIT_DEFAULT: usize = 10;

/// How many candidates to consider per result asked for, so that ranking has something to rank.
const RANK_WINDOW: usize = 20;

/// # `POST /_matrix/client/r0/user_directory/search`
///
/// Searches all known users for a match.
///
/// - Hides any local users that aren't in any public rooms (i.e. those that
///   have the join rule set to public) and don't share a room with the sender
///   unless `show_all_local_users_in_user_directory` is enabled
/// - Hides appservice senders and users in exclusive appservice user namespaces
///   unless `show_appservice_users_in_user_directory` is enabled
/// - Asks the appservices about their own networks as well, so a bridged person who has no puppet
///   here yet can still be found (see appservice::Service::search_users). Answering creates the
///   puppet, so the result is an ordinary user and every client benefits without knowing any of
///   this.
pub(crate) async fn search_users_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();
	let limit = usize_from_ruma_bounded(body.limit, LIMIT_DEFAULT, LIMIT_MAX);

	let search_term = body.search_term.as_str();
	/*
	 * One compiled matcher, then every candidate scored against it.
	 *
	 * Matching used to be `contains` on the lowercased ID and display name, which meant `krzys`
	 * could not find `Krzyś`, `carter bob` could not find `Bob Carter`, and nothing was ranked - so
	 * with more matches than fit in the page, which ones came back was decided by the order the
	 * user table happened to be in. The rule now is the one in rooms::search::matcher, which is the
	 * rule the client and the message index also use.
	 */
	let Some(matcher) = Matcher::new(search_term) else {
		return Ok(Response { results: Vec::new(), limited: false });
	};

	let users = services
		.users
		.stream()
		.ready_filter(|&user_id| user_id != sender_user)
		.map(ToOwned::to_owned)
		.wide_filter_map(async |user_id| {
			let display_name = services.profile.displayname(&user_id).await.ok();

			// A Matrix ID is worth matching on as well as the name: it is what somebody types when
			// they know the handle rather than the person.
			let score = [display_name.as_deref(), Some(user_id.localpart())]
				.into_iter()
				.flatten()
				.filter_map(|text| matcher.score(text))
				.max()?;

			Some((score, user_id, display_name))
		})
		.wide_filter_map(async |(score, user_id, display_name)| {
			should_show_user(&services, sender_user, &user_id)
				.await
				.then_async(async move || {
					let avatar_url = services.profile.avatar_url(&user_id).await.ok();

					(score, User { user_id, display_name, avatar_url })
				})
				.await
		});

	/*
	 * Ranking needs more than a page of candidates to rank, so this takes a bounded multiple of
	 * what was asked for and sorts that. Unbounded would mean a one-letter search reading every
	 * profile on the server; a single page would mean the best match is missing whenever it happens
	 * to sit late in the user table, which is the bug being fixed.
	 */
	let ceiling = limit.saturating_mul(RANK_WINDOW).min(LIMIT_MAX);
	pin_mut!(users);
	let mut scored: Vec<(u32, User)> = users.by_ref().take(ceiling).collect().await;
	let mut limited = users.next().await.is_some() || scored.len() > limit;

	// Best first, and a stable tie-break so the same search twice gives the same answer.
	scored.sort_by(|(left, left_user), (right, right_user)| {
		right
			.cmp(left)
			.then_with(|| left_user.user_id.cmp(&right_user.user_id))
	});

	let mut results: Vec<User> = scored
		.into_iter()
		.take(limit)
		.map(|(_, user)| user)
		.collect();

	/*
	 * What is already here first, then what the networks say.
	 *
	 * Someone this server knows about is someone with a history worth putting at the top, and the
	 * bridges are asked for the rest. A person who is both - already bridged - comes back from both
	 * and is kept once.
	 *
	 * The networks are asked whichever way the local search went. Asking only when the local
	 * results came up short, as this did at first, means the person you are looking for is missing
	 * precisely when the server happens to know a handful of others whose names also match - and
	 * the searcher has no way to tell that happened.
	 */
	let known: HashSet<OwnedUserId> = results
		.iter()
		.map(|user| user.user_id.clone())
		.collect();

	let bridged = services
		.appservice
		.search_users(sender_user, &search_term, limit)
		.await;

	for user in bridged {
		if results.len() >= limit {
			limited = true;
			break;
		}
		if !known.contains(&user.user_id) {
			results.push(user);
		}
	}

	Ok(Response { results, limited })
}

async fn should_show_user(
	services: &Services,
	sender_user: &UserId,
	target_user: &UserId,
) -> bool {
	let config = &services.server.config;

	if !config.show_appservice_users_in_user_directory
		&& services
			.appservice
			.is_exclusive_user_id(target_user)
			.await
	{
		return false;
	}

	if config.show_all_local_users_in_user_directory {
		return true;
	}

	let user_in_public_room = services
		.state_cache
		.rooms_joined(target_user)
		.map(ToOwned::to_owned)
		.broad_any(async |room_id| {
			services
				.state_accessor
				.get_join_rules(&room_id)
				.map(|rule| matches!(rule, JoinRule::Public))
				.await
		});

	let user_sees_user = services
		.state_cache
		.user_sees_user(sender_user, target_user);

	pin_mut!(user_in_public_room, user_sees_user);
	user_in_public_room.or(user_sees_user).await
}
