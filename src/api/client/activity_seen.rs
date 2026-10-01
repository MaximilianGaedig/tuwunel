//! Lets a bridge say when a network held one of its users as last active
//! (`activity_log`): Matrix presence has "online" and "offline" and no place
//! for "was here at 14:05", which is what a network tells a bridge about
//! someone who has come and gone.

use axum::extract::State;
use ruma::{
	OwnedUserId, UInt,
	api::{auth_scheme::AccessToken, request, response},
	metadata,
};
use tuwunel_core::{Err, Result};

use crate::Ruma;

metadata! {
	method: PUT,
	rate_limited: false,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/im.mxg.activity/users/{user_id}/seen",
	}
}

#[request]
pub struct Request {
	/// Who was seen: the sender themselves, which for a bridge is the ghost it speaks as.
	#[ruma_api(path)]
	pub user_id: OwnedUserId,

	/// When, in milliseconds since the epoch.
	pub ts: UInt,
}

#[response]
pub struct Response {}

/// # `PUT /_matrix/client/unstable/im.mxg.activity/users/{userId}/seen`
pub(crate) async fn put_user_seen_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	if body.sender_user() != body.user_id {
		return Err!(Request(Forbidden("Only the user themselves can say when they were seen.")));
	}

	if !services
		.activity_log
		.log_seen(&body.user_id, body.ts.into())
	{
		return Err!(Request(InvalidParam("`ts` is not a time in the past.")));
	}

	Ok(Response {})
}
