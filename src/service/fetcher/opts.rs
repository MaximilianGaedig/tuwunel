use std::{fmt, num::NonZeroUsize};

use bytes::Bytes;
use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName};
use tuwunel_core::err;

/// Federation endpoint a fetch targets. The dedup key folds this in, so two
/// callers asking for the same event over different endpoints do not coalesce.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Op {
	/// `GET /_matrix/federation/v1/event/{eventId}`
	Event,

	/// `GET /_matrix/federation/v1/event_auth/{roomId}/{eventId}`
	EventAuth,

	/// `GET /_matrix/federation/v1/backfill/{roomId}`
	Backfill,

	/// `GET /_matrix/federation/v1/state_ids/{roomId}?event_id=`
	StateIds,

	/// `POST /_matrix/federation/v1/get_missing_events/{roomId}`
	MissingEvents,
}

/// Caller contract. `event_id` is the sought datum for [`Op::Event`] /
/// [`Op::EventAuth`] / [`Op::StateIds`] and a reference point for the others.
#[derive(Clone, Debug)]
pub struct Opts {
	pub op: Op,
	pub room_id: OwnedRoomId,
	pub event_id: Option<OwnedEventId>,
	pub hint: Option<OwnedServerName>,
	pub attempt_limit: Option<NonZeroUsize>,
	pub backfill_limit: Option<NonZeroUsize>,
	pub check_event_id: bool,
	pub check_conforms: bool,
	pub check_hashes: bool,
	// Accepted but not yet consulted; redaction-aware hash verification is unimplemented.
	pub authoritative_redaction: bool,
	pub check_signature: bool,
}

impl Opts {
	/// All validation toggles default on; the caller relaxes them per request.
	#[must_use]
	pub fn new(op: Op, room_id: OwnedRoomId) -> Self {
		Self {
			op,
			room_id,
			event_id: None,
			hint: None,
			attempt_limit: None,
			backfill_limit: None,
			check_event_id: true,
			check_conforms: true,
			check_hashes: true,
			authoritative_redaction: true,
			check_signature: true,
		}
	}

	#[must_use]
	pub fn event_id(self, event_id: OwnedEventId) -> Self {
		Self { event_id: Some(event_id), ..self }
	}

	#[must_use]
	pub fn hint(self, hint: OwnedServerName) -> Self { Self { hint: Some(hint), ..self } }

	/// Toggle every validation gate at once. Callers that re-validate
	/// downstream pass `false` to fetch raw bytes without rejecting non-V11
	/// events.
	#[must_use]
	pub fn checks(self, enabled: bool) -> Self {
		Self {
			check_event_id: enabled,
			check_conforms: enabled,
			check_hashes: enabled,
			check_signature: enabled,
			..self
		}
	}
}

/// Raw response body plus the server that answered. `bytes` is ref-counted so
/// concurrent callers coalesced onto one fetch share a single buffer.
#[derive(Debug)]
pub struct Outcome {
	pub bytes: Bytes,
	pub origin: OwnedServerName,
}

/// Internal failure shape. Kept `Clone` so it can ride the shared-result
/// channel to every coalesced caller; converted to [`tuwunel_core::Error`] at
/// the public boundary. Carries the servers tried for the operator-facing
/// message.
#[derive(Clone, Debug)]
pub(super) enum Failure {
	/// Every candidate was tried and none returned a valid response.
	NotFound {
		attempted: Vec<OwnedServerName>,
	},

	/// No candidate servers were available to try.
	NoCandidates,

	/// All callers dropped the future before a server answered.
	Cancelled,
}

impl fmt::Display for Failure {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			| Self::NotFound { attempted } => {
				write!(f, "event not found on any of {} servers", attempted.len())
			},
			| Self::NoCandidates => write!(f, "no candidate servers available"),
			| Self::Cancelled => write!(f, "fetch cancelled"),
		}
	}
}

impl From<Failure> for tuwunel_core::Error {
	fn from(failure: Failure) -> Self { err!(Request(NotFound("{failure}"))) }
}

/// Single-flight dedup key. `MissingEvents` does not coalesce on a single
/// event_id (OQ6); a body hash will fold in when that op gains a multi-id
/// request body in a later phase.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct Key {
	pub(super) op: Op,
	pub(super) room_id: OwnedRoomId,
	pub(super) event_id: Option<OwnedEventId>,
}

impl Key {
	pub(super) fn new(opts: &Opts) -> Self {
		Self {
			op: opts.op,
			room_id: opts.room_id.clone(),
			event_id: opts.event_id.clone(),
		}
	}
}
