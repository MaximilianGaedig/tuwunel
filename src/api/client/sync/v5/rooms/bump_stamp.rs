use ruma::{RoomId, UInt, UserId};
use tuwunel_core::{Result, matrix::pdu::PduCount};
use tuwunel_service::Services;

use super::super::activity::newest_said;

/// When the room's newest message since `roomsince` was sent (MSC4186 leaves what a
/// bump_stamp is to the server; see `activity`), or nothing when none has come.
pub(super) async fn room_bump_stamp(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	roomsince: PduCount,
	next_batch: PduCount,
	last_timeline_count: PduCount,
) -> Result<Option<UInt>> {
	if last_timeline_count <= roomsince {
		return Ok(None);
	}

	newest_said(services, sender_user, room_id, next_batch, roomsince).await
}
