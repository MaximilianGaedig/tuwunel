mod record_failure;
mod record_success;
mod should_attempt;
mod snapshot;

use clap::Subcommand;
use ruma::OwnedServerName;
use tuwunel_core::Result;

use crate::admin_command_dispatch;

/// Per-server reachability store backed by the `servername_status` CF and
/// exposed through `tuwunel_service::federation::Service`.
#[admin_command_dispatch(handler_prefix = "peer_status")]
#[derive(Debug, Subcommand)]
pub(crate) enum PeerStatusCommand {
	/// List populated buckets, optionally filtered to one server.
	Snapshot {
		server_name: Option<OwnedServerName>,
	},

	/// Resolve the verdict the sender would observe right now for
	/// `server_name`.
	ShouldAttempt {
		server_name: OwnedServerName,
	},

	/// Diagnostic: clear the current-window bucket for `server_name` as if a
	/// transaction had just succeeded. Does not touch prior buckets, so the
	/// walk-back streak still reflects the existing history.
	RecordSuccess {
		server_name: OwnedServerName,
	},

	/// Diagnostic: write a synthetic failure into the current-window bucket
	/// for `server_name`. `--permanent` records a permanent verdict; otherwise
	/// the value is transient.
	RecordFailure {
		server_name: OwnedServerName,
		#[arg(long)]
		permanent: bool,
	},
}
