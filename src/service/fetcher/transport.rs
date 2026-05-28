use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use ruma::{
	OwnedEventId, ServerName, UInt,
	api::federation::{
		authorization::get_event_authorization::v1::Request as EventAuthRequest,
		backfill::get_backfill::v1::Request as BackfillRequest,
		event::{
			get_event::v1::Request as EventRequest,
			get_missing_events::v1::Request as MissingEventsRequest,
			get_room_state_ids::v1::Request as StateIdsRequest,
		},
	},
};
use tuwunel_core::{Result, err};

use super::{Op, Opts};
use crate::services::OnceServices;

/// Injection seam between the fetcher and the network. The production impl
/// routes through `federation::execute`; tests substitute a scripted mock.
#[async_trait]
pub(super) trait Transport: Send + Sync {
	async fn fetch_raw(&self, op: Op, server: &ServerName, opts: &Opts) -> Result<Bytes>;
}

pub(super) struct FederationTransport {
	pub(super) services: Arc<OnceServices>,
}

#[async_trait]
impl Transport for FederationTransport {
	async fn fetch_raw(&self, op: Op, server: &ServerName, opts: &Opts) -> Result<Bytes> {
		let federation = &self.services.federation;
		let room_id = opts.room_id.clone();
		let event_id = require_event_id(opts)?;

		match op {
			| Op::Event => {
				let res = federation
					.execute(server, EventRequest { event_id })
					.await?;

				Ok(Bytes::copy_from_slice(res.pdu.get().as_bytes()))
			},
			| Op::EventAuth => {
				let res = federation
					.execute(server, EventAuthRequest { room_id, event_id })
					.await?;

				to_bytes(&res.auth_chain)
			},
			| Op::Backfill => {
				let limit = opts.backfill_limit.map_or_else(
					|| UInt::from(10_u8),
					|n| UInt::new_saturating(u64::try_from(n.get()).unwrap_or(u64::MAX)),
				);

				let res = federation
					.execute(server, BackfillRequest { room_id, v: vec![event_id], limit })
					.await?;

				to_bytes(&res.pdus)
			},
			| Op::StateIds => {
				let res = federation
					.execute(server, StateIdsRequest { room_id, event_id })
					.await?;

				to_bytes(&serde_json::json!({
					"auth_chain_ids": res.auth_chain_ids,
					"pdu_ids": res.pdu_ids,
				}))
			},
			| Op::MissingEvents => {
				let req = MissingEventsRequest {
					room_id,
					earliest_events: Vec::new(),
					latest_events: vec![event_id],
					limit: UInt::from(10_u8),
					min_depth: UInt::default(),
				};

				let res = federation.execute(server, req).await?;

				to_bytes(&res.events)
			},
		}
	}
}

fn require_event_id(opts: &Opts) -> Result<OwnedEventId> {
	opts.event_id
		.clone()
		.ok_or_else(|| err!(Request(InvalidParam("event_id is required for op {:?}", opts.op))))
}

fn to_bytes<T: serde::Serialize>(value: &T) -> Result<Bytes> {
	serde_json::to_vec(value)
		.map(Bytes::from)
		.map_err(|e| err!(BadServerResponse("failed to re-encode federation response: {e}")))
}
