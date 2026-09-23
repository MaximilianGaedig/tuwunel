mod profiles;
#[cfg(test)]
mod tests;
mod v3;
mod v5;

use std::collections::HashSet;

use futures::{StreamExt, pin_mut};
use ruma::{
	OwnedUserId, RoomId, UserId,
	events::{
		AnyStrippedStateEvent, StateEventType,
		TimelineEventType::{RoomCreate, RoomMember},
	},
	serde::Raw,
};
use serde::Deserialize;
use tuwunel_core::{
	Error, PduCount, Result, debug_warn, is_equal_to,
	matrix::{Event, pdu::PduEvent},
	utils::{ReadyExt, result::LogErr, stream::BroadbandExt},
};
use tuwunel_service::{Services, users::InviteFilter};

pub(crate) use self::{
	v3::{calculate_heroes, sync_events_route},
	v5::sync_events_v5_route,
};

/// MSC4171: the members a room has declared to be services rather than people.
///
/// A bridged direct message holds more than two members: the bridge's bot, and the puppet of the
/// reader's own account on the other network. Left in the heroes, they become the room's name for
/// every client that has no name to show - "Signal bridge bot and 2 others" for a conversation with
/// one person - and they make a chat of two look like a group of four.
///
/// The room says which of its members are these, in `io.element.functional_members`. Honouring it
/// here rather than in a client fixes it once for every client, which is the whole point: the
/// bridge bot is not a person in the conversation whichever app you read it in.
const FUNCTIONAL_MEMBERS: &str = "io.element.functional_members";

#[derive(Deserialize)]
struct FunctionalMembers {
	#[serde(default)]
	service_members: Vec<String>,
}

/// The bot a bridge declares as its own, for the rooms that declare no service members at all.
///
/// A bridge writes `m.bridge` (MSC2346) naming the bot it speaks through. That is the same fact
/// MSC4171 carries, from the same source, and it is already in the room - so a bridge that never got
/// round to writing the newer event still tells us which of its members is machinery.
///
/// This is not a guess about who looks like a bot: it is the bridge's own statement about itself. It
/// matters because the bridges that most need it are the ones least likely to be updated - the legacy
/// ones - and because a room whose state was written before a bridge learned to declare its services
/// is never revisited.
async fn bridge_bots(services: &Services, room_id: &RoomId) -> HashSet<OwnedUserId> {
	#[derive(Deserialize)]
	struct BridgeInfo {
		bridgebot: Option<OwnedUserId>,
	}

	let mut bots = HashSet::new();
	for event_type in [StateEventType::from("m.bridge"), StateEventType::from("uk.half-shot.bridge")] {
		let mut pdus = services
			.state_accessor
			.room_state_type_pdus(room_id, &event_type)
			.ready_filter_map(Result::ok)
			.boxed();

		while let Some(pdu) = pdus.next().await {
			// One per network: a room can be bridged to more than one at a time.
			if let Ok(info) = pdu.get_content::<BridgeInfo>() {
				bots.extend(info.bridgebot);
			}
		}
	}
	bots
}

/// The room's service members: what it declares, and the bridge bots it names.
///
/// Read as strings and parsed one by one: this is unvalidated content from whoever set the state,
/// and one malformed id in the list must not throw away the rest of it.
pub(crate) async fn service_members(services: &Services, room_id: &RoomId) -> HashSet<OwnedUserId> {
	let declared: HashSet<OwnedUserId> = services
		.state_accessor
		.room_state_get_content::<FunctionalMembers>(
			room_id,
			&StateEventType::from(FUNCTIONAL_MEMBERS),
			"",
		)
		.await
		.map(|content| {
			content
				.service_members
				.into_iter()
				.filter_map(|id| OwnedUserId::try_from(id).ok())
				.collect()
		})
		.unwrap_or_default();

	// Both, always: a room may declare its own puppet as a service member while the bridge that
	// wrote it predates declaring the bot, and neither list is authoritative over the other.
	let mut all = declared;
	all.extend(bridge_bots(services, room_id).await);
	all
}

#[derive(Clone, Copy)]
enum TimelineErrors {
	Ignore,
	Propagate,
}

async fn load_timeline(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	roomsincecount: PduCount,
	next_batch: Option<PduCount>,
	limit: usize,
) -> Result<(Vec<(PduCount, PduEvent)>, bool, PduCount), Error> {
	load_timeline_with_errors(
		services,
		sender_user,
		room_id,
		roomsincecount,
		next_batch,
		limit,
		TimelineErrors::Ignore,
	)
	.await
}

async fn load_timeline_fallible(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	roomsincecount: PduCount,
	next_batch: Option<PduCount>,
	limit: usize,
) -> Result<(Vec<(PduCount, PduEvent)>, bool, PduCount), Error> {
	load_timeline_with_errors(
		services,
		sender_user,
		room_id,
		roomsincecount,
		next_batch,
		limit,
		TimelineErrors::Propagate,
	)
	.await
}

async fn load_timeline_with_errors(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	roomsincecount: PduCount,
	next_batch: Option<PduCount>,
	limit: usize,
	errors: TimelineErrors,
) -> Result<(Vec<(PduCount, PduEvent)>, bool, PduCount), Error> {
	let until = next_batch.map(|count| count.saturating_add(1));
	let pdus = services
		.timeline
		.pdus_rev(Some(sender_user), room_id, until);

	// Take the last events for the timeline.
	pin_mut!(pdus);
	let mut timeline_pdus = Vec::new();
	let mut last_timeline_count = PduCount::max();
	let mut first = true;
	let mut limited = false;

	while let Some(pdu) = pdus.next().await {
		let (pducount, pdu) = match pdu {
			| Ok(pdu) => pdu,
			| Err(error) if first || matches!(errors, TimelineErrors::Propagate) => {
				return Err(error);
			},
			| Err(_) => continue,
		};

		if first {
			first = false;
			last_timeline_count = matches!(pducount, PduCount::Normal(_))
				.then_some(pducount)
				.unwrap_or_else(PduCount::max);
		}

		if pducount <= roomsincecount {
			break;
		}

		if timeline_pdus.len() == limit {
			limited = true;
			break;
		}

		timeline_pdus.push((pducount, pdu));
	}

	timeline_pdus.reverse();

	Ok((timeline_pdus, limited, last_timeline_count))
}

/// Returns the backward pagination token for a timeline slice.
///
/// A slice beginning at room creation has nothing before it, so its window's
/// end lets a members query describe the room as received.
/// Backward pagination from that token re-reads the slice once before ending.
fn timeline_prev_batch(
	timeline_pdus: &[(PduCount, PduEvent)],
	window_end: PduCount,
) -> Option<PduCount> {
	timeline_pdus
		.first()
		.map(|(count, pdu)| match pdu.kind() {
			| RoomCreate => window_end,
			| _ => *count,
		})
}

async fn share_encrypted_room(
	services: &Services,
	sender_user: &UserId,
	user_id: &UserId,
	ignore_room: Option<&RoomId>,
) -> bool {
	services
		.state_cache
		.get_shared_rooms(sender_user, user_id)
		.ready_filter(|&room_id| Some(room_id) != ignore_room)
		.map(ToOwned::to_owned)
		.broad_any(async |other_room_id| {
			services
				.state_accessor
				.is_encrypted_room(&other_room_id)
				.await
		})
		.await
}

/// MSC4155: whether a stored invite may be served to the invitee.
///
/// The verdict is the recipient's own, judged against the sender of the
/// stripped invite membership event. Unreadable invite state takes the same
/// verdict as the sender-less case below, since neither can name a sender.
async fn invite_permitted_room(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	filter: &InviteFilter,
) -> bool {
	filter.is_permissive()
		|| services
			.state_cache
			.invite_state(user_id, room_id)
			.await
			.map_or_else(
				|error| {
					debug_warn!(%user_id, %room_id, ?error, "invite state is unreadable; skipping the sender rules");
					filter.permits(None)
				},
				|invite_state| invite_permitted(user_id, room_id, filter, &invite_state),
			)
}

/// [`invite_permitted_room`] for a room whose stripped state is in hand.
///
/// Callers walking stored invites already hold the state and take this form,
/// which spares them the load the room-keyed form pays per room. `room_id`
/// names the affected row in the sender-less diagnostic and does not enter the
/// verdict. That diagnostic stays at debug on a release build deliberately,
/// since the condition repeats for every sync an affected user makes.
fn invite_permitted(
	user_id: &UserId,
	room_id: &RoomId,
	filter: &InviteFilter,
	invite_state: &[Raw<AnyStrippedStateEvent>],
) -> bool {
	// Load-bearing: keeps a permissive user off the sender derivation below.
	if filter.is_permissive() {
		return true;
	}

	let sender = invite_sender(user_id, invite_state);

	if sender.is_none() {
		debug_warn!(%user_id, %room_id, "invite state names no sender; skipping the sender rules");
	}

	filter.permits(sender.as_deref())
}

/// The sender of the stripped membership event inviting `user_id`.
///
/// The last matching entry wins. An invite this server recorded after the
/// federation route began sanitising stripped state holds one entry for this
/// cell, our own copy of the signed membership PDU, whose sender the origin
/// check authenticated. An invite stored before that still carries whatever
/// the inviting server sent ahead of our copy, and the array has no ordering
/// semantics in the spec, so reading the last entry is what keeps those
/// answering with the authenticated sender too.
fn invite_sender(
	user_id: &UserId,
	invite_state: &[Raw<AnyStrippedStateEvent>],
) -> Option<OwnedUserId> {
	invite_state
		.iter()
		.rev()
		.filter(|event| {
			event
				.get_field::<&str>("state_key")
				.is_ok_and(|state_key| state_key.is_some_and(is_equal_to!(user_id.as_str())))
		})
		.filter_map(|event| event.deserialize().ok())
		.find_map(|event| match event {
			| AnyStrippedStateEvent::RoomMember(member) if member.state_key == user_id =>
				Some(member.sender),
			| _ => None,
		})
}

/// State sections strip the stored `prev_content`/`prev_sender` pair
/// (Synapse injects the pair on timeline fetches only). The requester's own
/// membership and events duplicated from the returned timeline (MSC4222,
/// full_state) keep it: clients read membership transitions from those
/// copies.
fn strip_prev_state(
	mut pdu: PduEvent,
	sender_user: &UserId,
	in_timeline: impl Fn(&PduEvent) -> bool,
) -> PduEvent {
	let own_membership =
		*pdu.kind() == RoomMember && pdu.state_key() == Some(sender_user.as_str());

	if !own_membership && !in_timeline(&pdu) {
		pdu.remove_prev_state().log_err().ok();
	}

	pdu
}
