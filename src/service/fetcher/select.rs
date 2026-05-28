use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use ruma::{EventId, OwnedServerName, ServerName};

use super::Opts;
use crate::services::OnceServices;

/// Candidate enumeration seam. The production impl derives the server pool from
/// room state; tests substitute a fixed list.
#[async_trait]
pub(super) trait Select: Send + Sync {
	async fn candidates(&self, opts: &Opts) -> Vec<OwnedServerName>;
}

pub(super) struct RoomCandidates {
	pub(super) services: Arc<OnceServices>,
}

#[async_trait]
impl Select for RoomCandidates {
	async fn candidates(&self, opts: &Opts) -> Vec<OwnedServerName> {
		let route_via = self
			.services
			.state_cache
			.servers_route_via(&opts.room_id)
			.await
			.unwrap_or_default();

		let room_servers = if route_via.is_empty() {
			self.services
				.state_cache
				.room_servers(&opts.room_id)
				.map(ToOwned::to_owned)
				.collect::<Vec<_>>()
				.await
		} else {
			route_via
		};

		let mxid_hosts = [
			opts.event_id
				.as_deref()
				.and_then(EventId::server_name),
			opts.room_id.server_name(),
		]
		.into_iter()
		.flatten()
		.map(ToOwned::to_owned);

		let mut seen = BTreeSet::new();
		opts.hint
			.clone()
			.into_iter()
			.chain(room_servers)
			.chain(mxid_hosts)
			.filter(|server| self.is_eligible(server))
			.filter(|server| seen.insert(server.clone()))
			.collect()
	}
}

impl RoomCandidates {
	fn is_eligible(&self, server: &ServerName) -> bool {
		!self.services.globals.server_is_ours(server)
			&& !self
				.services
				.server
				.config
				.is_forbidden_remote_server_name(server)
	}
}
