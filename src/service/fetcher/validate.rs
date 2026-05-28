use ruma::{CanonicalJsonObject, RoomVersionId};
use serde::de::IgnoredAny;
use tuwunel_core::{Err, Result, err, implement, matrix::event::gen_event_id};

use super::{Op, Opts};

/// Poison detection applied before a response is accepted, so a hostile server
/// answering with garbage transparently rolls onto the next candidate. Full
/// auth resolution stays outside; this only rejects responses we can prove bad
/// from the bytes alone.
#[implement(super::Service)]
#[tracing::instrument(name = "validate", level = "trace", skip_all)]
pub(super) async fn validate(&self, opts: &Opts, bytes: &[u8]) -> Result {
	if opts.check_conforms {
		serde_json::from_slice::<IgnoredAny>(bytes)
			.map_err(|e| err!(BadServerResponse("malformed federation response: {e}")))?;
	}

	let deep = opts.check_event_id || opts.check_hashes || opts.check_signature;
	if matches!(opts.op, Op::Event) && deep {
		self.verify_pdu(opts, bytes).await?;
	}

	Ok(())
}

#[implement(super::Service)]
async fn verify_pdu(&self, opts: &Opts, bytes: &[u8]) -> Result {
	let value: CanonicalJsonObject = serde_json::from_slice(bytes)
		.map_err(|e| err!(BadServerResponse("PDU is not a canonical JSON object: {e}")))?;

	// Room version is pinned to V11 here, so a valid event from a non-V11 room
	// (v12 redaction, v1/v2 explicit ids) is wrongly rejected and rolls onward.
	if opts.check_event_id
		&& let Some(expected) = opts.event_id.as_ref()
	{
		let calculated = gen_event_id(&value, &RoomVersionId::V11)?;
		if calculated != *expected {
			return Err!(BadServerResponse("server returned the wrong event id"));
		}
	}

	if opts.check_signature || opts.check_hashes {
		self.services
			.server_keys
			.verify_event(&value, None)
			.await?;
	}

	Ok(())
}
