use futures::FutureExt;
use ruma::{
	CanonicalJsonObject, OwnedEventId, OwnedServerName,
	api::federation::event::get_event::v1::Request as EventRequest,
};
use tuwunel_core::{Err, Result, err, info, trace, warn};

use crate::admin_command;

#[admin_command]
pub(super) async fn get_remote_pdu(
	&self,
	event_id: OwnedEventId,
	server: OwnedServerName,
) -> Result {
	if !self.services.server.config.allow_federation {
		return Err!("Federation is disabled on this homeserver.");
	}

	if server == self.services.globals.server_name() {
		return Err!(
			"Not allowed to send federation requests to ourselves. Please use `get-pdu` for \
			 fetching local PDUs.",
		);
	}

	// Direct fetch, not the fetcher service: Opts needs a room_id this command
	// only learns after parsing the response.
	let response = self
		.services
		.federation
		.execute(&server, EventRequest { event_id: event_id.clone() })
		.await
		.map_err(|e| {
			err!("Remote server did not have PDU or failed sending request to remote server: {e}")
		})?;

	let json: CanonicalJsonObject = serde_json::from_str(response.pdu.get()).map_err(|e| {
		warn!(
			"Requested event ID {event_id} from server but failed to convert from RawValue to \
			 CanonicalJsonObject (malformed event/response?): {e}"
		);
		err!(Request(Unknown("Received response from server but failed to parse PDU")))
	})?;

	trace!("Attempting to parse PDU: {:?}", &response.pdu);
	let (room_id, ..) = self
		.services
		.event_handler
		.parse_incoming_pdu(&response.pdu)
		.boxed()
		.await
		.map_err(|e| {
			warn!("Failed to parse PDU: {e}");
			info!("Full PDU: {:?}", &response.pdu);
			err!("Failed to parse PDU remote server {server} sent us: {e}")
		})?;

	info!("Attempting to handle event ID {event_id} as backfilled PDU");
	self.services
		.timeline
		.backfill_pdu(&room_id, &server, response.pdu)
		.await?;

	let text = serde_json::to_string_pretty(&json)?;
	let msg = "Got PDU from specified server and handled as backfilled";
	write!(self, "{msg}. Event body:\n```json\n{text}\n```").await
}
