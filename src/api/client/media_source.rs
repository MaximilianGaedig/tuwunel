//! Lets a bridge give storage back: for media it uploaded, it names where the
//! same bytes can be fetched again (its own direct-media mxc). The server keeps
//! answering the original mxc after its file is deleted, by fetching from that
//! source and serving the bytes itself, so events never have to be edited and
//! clients never contact the network.

use std::collections::BTreeMap;

use axum::extract::State;
use ruma::{
	Mxc, OwnedMxcUri,
	api::{auth_scheme::AccessToken, request, response},
	metadata,
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Result};

use crate::Ruma;

metadata! {
	method: POST,
	rate_limited: false,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/im.mxg.media_source",
	}
}

/// Bounded so one request holds the database for a bounded time.
const SOURCES_MAX: usize = 1000;

#[request]
pub struct Request {
	/// Local mxc -> the mxc its original is fetched from; `null` forgets it.
	pub sources: BTreeMap<OwnedMxcUri, Option<OwnedMxcUri>>,
}

/// What became of one entry.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Outcome {
	/// The original's object name in the media directory, for whoever drops it.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub file: Option<String>,

	/// Whether that object is still stored.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub present: Option<bool>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub error: Option<String>,
}

#[response]
pub struct Response {
	pub results: BTreeMap<OwnedMxcUri, Outcome>,
}

/// # `POST /_matrix/client/unstable/im.mxg.media_source`
///
/// Only an application service may call it, and only for media one of its
/// own users uploaded.
pub(crate) async fn set_media_source_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let Some(appservice) = body.appservice_info.as_ref() else {
		return Err!(Request(Forbidden("Only an application service can name media sources.")));
	};

	if body.sources.len() > SOURCES_MAX {
		return Err!(Request(TooLarge("Too many sources in one request.")));
	}

	let mut results = BTreeMap::new();
	for (mxc, source) in &body.sources {
		let outcome = async {
			let mxc: Mxc<'_> = mxc.as_str().try_into()?;
			let source: Option<Mxc<'_>> = source
				.as_ref()
				.map(|source| source.as_str().try_into())
				.transpose()?;

			let uploader = services
				.media
				.media_entry(&mxc)
				.await
				.and_then(|entry| entry.user_id);

			if !uploader.is_some_and(|user| appservice.is_user_match(&user)) {
				return Err!(Request(Forbidden("This media was not uploaded by this bridge.")));
			}

			services
				.media
				.set_source(&mxc, source.as_ref())
				.await
		}
		.await;

		let outcome = match outcome {
			| Ok(file) => Outcome {
				file: Some(file.file),
				present: Some(file.present),
				error: None,
			},
			| Err(error) => Outcome {
				error: Some(error.to_string()),
				..Outcome::default()
			},
		};

		results.insert(mxc.clone(), outcome);
	}

	Ok(Response { results })
}
