use futures::{FutureExt, TryFutureExt};
use ruma::{
	OwnedRoomAliasId, RoomId, UserId,
	events::{
		AnyMessageLikeEventContent, AnyStateEventContent, MessageLikeEventType, StateEventType,
		reaction::ReactionEventContent,
		room::{
			canonical_alias::RoomCanonicalAliasEventContent,
			history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
			join_rules::{JoinRule, RoomJoinRulesEventContent},
			member::{MembershipState, RoomMemberEventContent},
			server_acl::RoomServerAclEventContent,
		},
	},
	serde::Raw,
};
use tuwunel_core::{Err, Result, err, is_false, utils::BoolExt};

use crate::Services;

/// Validates whether a message-like event can be sent according to server
/// policy. This includes checks for disabled redactions, disabled encryption,
/// duplicate reactions, and public room call invites.
///
/// Used by both normal /send and delayed events (at send time).
pub async fn validate_message_event_policy(
	services: &Services,
	sender_user: &UserId,
	event_type: &MessageLikeEventType,
	content: &Raw<AnyMessageLikeEventContent>,
	room_id: &RoomId,
) -> Result {
	if *event_type == MessageLikeEventType::RoomRedaction
		&& services.config.disable_local_redactions
		&& !services.admin.user_is_admin(sender_user).await
	{
		return Err!(Request(Forbidden("Redactions are disabled on this server.")));
	}

	if *event_type == MessageLikeEventType::RoomEncrypted && !services.config.allow_encryption {
		return Err!(Request(Forbidden("Encryption has been disabled")));
	}

	check_duplicate_reaction(services, event_type, sender_user, content).await?;
	check_public_call_invite(services, event_type, room_id).await?;

	Ok(())
}

async fn check_public_call_invite(
	services: &Services,
	event_type: &MessageLikeEventType,
	room_id: &RoomId,
) -> Result {
	if *event_type != MessageLikeEventType::CallInvite {
		return Ok(());
	}

	if !services.directory.is_public_room(room_id).await {
		return Ok(());
	}

	Err!(Request(Forbidden("Room call invites are not allowed in public rooms")))
}

async fn check_duplicate_reaction(
	services: &Services,
	event_type: &MessageLikeEventType,
	sender_user: &UserId,
	body: &Raw<AnyMessageLikeEventContent>,
) -> Result {
	if *event_type != MessageLikeEventType::Reaction {
		return Ok(());
	}

	let Ok(content) = body.deserialize_as_unchecked::<ReactionEventContent>() else {
		return Ok(());
	};

	if !services
		.pdu_metadata
		.event_has_relation(
			&content.relates_to.event_id,
			Some(sender_user),
			None,
			Some(&content.relates_to.key),
		)
		.await
	{
		return Ok(());
	}

	Err!(Request(DuplicateAnnotation("Duplicate reactions are not allowed.")))
}

/// Validates whether a state event can be sent according to server policy.
/// Used by both normal /state and delayed events (at send time).
pub async fn allowed_to_send_state_event(
	services: &Services,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
	json: &Raw<AnyStateEventContent>,
) -> Result {
	match event_type {
		| StateEventType::RoomCreate => Err!(Request(BadJson(
			"You cannot update m.room.create after a room has been created."
		))),
		| StateEventType::RoomServerAcl => validate_server_acl(services, room_id, json),
		| StateEventType::RoomEncryption => validate_encryption(services),
		| StateEventType::RoomJoinRules => validate_join_rules(services, room_id, json).await,
		| StateEventType::RoomHistoryVisibility =>
			validate_history_visibility(services, room_id, json).await,
		| StateEventType::RoomCanonicalAlias =>
			validate_canonical_alias(services, room_id, json).await,
		| StateEventType::RoomMember => validate_member(services, room_id, state_key, json).await,
		| _ => Ok(()),
	}
}

fn validate_encryption(services: &Services) -> Result {
	services
		.config
		.allow_encryption
		.then_some(())
		.ok_or_else(|| err!(Request(Forbidden("Encryption is disabled on this homeserver."))))
}

fn validate_server_acl(
	services: &Services,
	_room_id: &RoomId,
	json: &Raw<AnyStateEventContent>,
) -> Result {
	let acl_content = json
		.deserialize_as_unchecked::<RoomServerAclEventContent>()
		.map_err(|e| err!(Request(BadJson("Room server ACL event is invalid: {e}"))))?;

	if acl_content.allow_is_empty() {
		return Err!(Request(BadJson(
			"Sending an ACL event with an empty allow key will permanently brick the room for \
			 non-tuwunel's as this equates to no servers being allowed to participate in this \
			 room."
		)));
	}

	if acl_content.deny_contains("*") && acl_content.allow_contains("*") {
		return Err!(Request(BadJson(
			"Sending an ACL event with a deny and allow key value of \"*\" will permanently \
			 brick the room for non-tuwunel's as this equates to no servers being allowed to \
			 participate in this room."
		)));
	}

	let server_name = services.globals.server_name();
	let self_allowed =
		acl_content.is_allowed(server_name) || acl_content.allow_contains(server_name.as_str());

	if acl_content.deny_contains("*") && !self_allowed {
		return Err!(Request(BadJson(
			"Sending an ACL event with a deny key value of \"*\" and without your own server \
			 name in the allow key will result in you being unable to participate in this room."
		)));
	}

	if !acl_content.allow_contains("*") && !self_allowed {
		return Err!(Request(BadJson(
			"Sending an ACL event for an allow key without \"*\" and without your own server \
			 name in the allow key will result in you being unable to participate in this room."
		)));
	}

	Ok(())
}

async fn validate_join_rules(
	services: &Services,
	room_id: &RoomId,
	json: &Raw<AnyStateEventContent>,
) -> Result {
	let Ok(admin_room_id) = services.admin.get_admin_room().await else {
		return Ok(());
	};

	if admin_room_id != room_id {
		return Ok(());
	}

	let join_rule = json
		.deserialize_as_unchecked::<RoomJoinRulesEventContent>()
		.map_err(|e| err!(Request(BadJson("Room join rules event is invalid: {e}"))))?;

	if join_rule.join_rule == JoinRule::Public {
		return Err!(Request(Forbidden(
			"Admin room is a sensitive room, it cannot be made public"
		)));
	}

	Ok(())
}

async fn validate_history_visibility(
	services: &Services,
	room_id: &RoomId,
	json: &Raw<AnyStateEventContent>,
) -> Result {
	let Ok(admin_room_id) = services.admin.get_admin_room().await else {
		return Ok(());
	};

	let visibility_content = json
		.deserialize_as_unchecked::<RoomHistoryVisibilityEventContent>()
		.map_err(|e| err!(Request(BadJson("Room history visibility event is invalid: {e}"))))?;

	if admin_room_id == room_id
		&& visibility_content.history_visibility == HistoryVisibility::WorldReadable
	{
		return Err!(Request(Forbidden(
			"Admin room is a sensitive room, it cannot be made world readable (public room \
			 history)."
		)));
	}

	Ok(())
}

async fn validate_canonical_alias(
	services: &Services,
	room_id: &RoomId,
	json: &Raw<AnyStateEventContent>,
) -> Result {
	let canonical_alias_content = json
		.deserialize_as_unchecked::<RoomCanonicalAliasEventContent>()
		.map_err(|e| err!(Request(InvalidParam("Room canonical alias event is invalid: {e}"))))?;

	let current_aliases: Vec<OwnedRoomAliasId> = services
		.state_accessor
		.room_state_get_content::<RoomCanonicalAliasEventContent>(
			room_id,
			&StateEventType::RoomCanonicalAlias,
			"",
		)
		.await
		.ok()
		.map(|content| content.aliases().cloned().collect())
		.unwrap_or_default();

	let new_aliases = canonical_alias_content
		.aliases()
		.filter(|alias| !current_aliases.contains(alias));

	for alias in new_aliases {
		let (alias_room_id, _servers) = services
			.alias
			.resolve_alias(alias)
			.await
			.map_err(|e| err!(Request(BadAlias("Failed resolving alias \"{alias}\": {e}"))))?;

		if alias_room_id != room_id {
			return Err!(Request(BadAlias(
				"Room alias {alias} does not belong to room {room_id}"
			)));
		}
	}

	Ok(())
}

async fn validate_member(
	services: &Services,
	room_id: &RoomId,
	state_key: &str,
	json: &Raw<AnyStateEventContent>,
) -> Result {
	let membership_content = json
		.deserialize_as_unchecked::<RoomMemberEventContent>()
		.map_err(|e| {
			err!(Request(BadJson(
				"Membership content must have a valid JSON body with at least a valid \
				 membership state: {e}"
			)))
		})?;

	let Ok(target_user) = UserId::parse(state_key) else {
		return Err!(Request(BadJson("Membership event has invalid or non-existent state key")));
	};

	if membership_content.membership == MembershipState::Invite
		&& services.globals.user_is_local(&target_user)
		&& services.users.invites_blocked(&target_user).await
	{
		return Err!(Request(InviteBlocked("{target_user} has blocked invites.")));
	}

	let Some(authorising_user) = membership_content.join_authorized_via_users_server else {
		return Ok(());
	};

	if membership_content.membership != MembershipState::Join {
		return Err!(Request(BadJson(
			"join_authorised_via_users_server is only for member joins"
		)));
	}

	if !services.globals.user_is_local(&authorising_user) {
		return Err!(Request(InvalidParam(
			"Authorising user {authorising_user} does not belong to this homeserver"
		)));
	}

	services
		.state_cache
		.is_joined(&authorising_user, room_id)
		.map(is_false!())
		.map(BoolExt::into_result)
		.map_err(|()| {
			err!(Request(InvalidParam(
				"Authorising user {authorising_user} is not in the room. They cannot authorise \
				 the join."
			)))
		})
		.await
}
