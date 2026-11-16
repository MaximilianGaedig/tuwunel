//! Local media whose file was dropped, fetched again from where it came from.
//!
//! A bridge that copied a network's attachments into this server can hand the
//! storage back: it names, for each of its uploads, an mxc:// URI on its own
//! direct-media server (`http_federation_peers`) that resolves to the same
//! bytes on the network. Once that is recorded the local file may be deleted;
//! the event keeps its mxc and every download and thumbnail of it is answered
//! from the source, through this server, so no client ever contacts the
//! network itself. Thumbnails derived from the fetched original are stored as
//! usual, so a timeline of dropped pictures costs one fetch per picture.

use bytes::Bytes;
use ruma::{
	Mxc, OwnedMxcUri,
	api::federation::authenticated_media::{FileOrLocation, get_content},
};
use tuwunel_core::{Err, Result, debug_warn, err, implement};

use super::{Animate, Dim, Metadata, remote::Fetch};

/// Where a local mxc's original file lives in storage, and whether it is there.
#[derive(Debug)]
pub struct SourceFile {
	/// The object's name in the media directory and every storage provider.
	pub file: String,
	pub present: bool,
}

/// Records (or with `None` forgets) where the original of a local mxc is
/// fetched from once its file is gone.
///
/// The mxc must be one of this server's with an original on record, and the
/// source must be another server's. Answers where the original is stored, so
/// whoever drops the file deletes exactly that one.
#[implement(super::Service)]
pub async fn set_source(&self, mxc: &Mxc<'_>, source: Option<&Mxc<'_>>) -> Result<SourceFile> {
	if !self
		.services
		.globals
		.server_is_ours(mxc.server_name)
	{
		return Err!(Request(InvalidParam("Only local media can be given a source.")));
	}

	if let Some(source) = source
		&& self
			.services
			.globals
			.server_is_ours(source.server_name)
	{
		return Err!(Request(InvalidParam("A source must be media of another server.")));
	}

	let Ok(Metadata { key, .. }) = self
		.db
		.search_file_metadata(mxc, &Dim::default(), Animate::Allowed)
		.await
	else {
		return Err!(Request(NotFound("No original is on record for this media.")));
	};

	let source = source.map(ToString::to_string);
	self.db
		.set_source(&mxc.to_string(), source.as_deref());

	Ok(SourceFile {
		file: self.get_media_name_sha256(&key),
		present: self.head_meta(&key).await.is_some(),
	})
}

/// The source recorded for a local mxc, if any.
#[implement(super::Service)]
pub async fn source_of(&self, mxc: &Mxc<'_>) -> Option<OwnedMxcUri> {
	self.db
		.get_source(&mxc.to_string())
		.await
		.ok()
		.map(Into::into)
}

/// The stored bytes for a metadata key, from the first provider holding them,
/// or from the original's recorded source once the file is gone.
#[implement(super::Service)]
pub(super) async fn read_stored(&self, key: &[u8]) -> Result<Bytes> {
	use futures::{StreamExt, pin_mut};
	use tuwunel_core::utils::{result::LogDebugErr, stream::IterStream};

	let path = self.get_media_name_sha256(key);
	let fetch = self
		.storage_providers()
		.stream()
		.filter_map(async |provider| {
			provider
				.get(path.as_str())
				.await
				.log_debug_err()
				.ok()
		});

	pin_mut!(fetch);
	if let Some(bytes) = fetch.next().await {
		return Ok(bytes);
	}

	self.fetch_source(key).await
}

/// Fetches the original named by `key` from its recorded source.
///
/// Only an original has a source: a thumbnail row whose file is missing is
/// regenerated from its original by the caller's ordinary miss path, not
/// fetched. Concurrent requests for one mxc share a lock so a picture opened
/// by several clients at once is fetched once at a time, not once per client.
#[implement(super::Service)]
async fn fetch_source(&self, key: &[u8]) -> Result<Bytes> {
	let not_found = || err!(Request(NotFound("Media not found.")));

	let mxc = original_mxc(key).ok_or_else(not_found)?;
	let source = self
		.db
		.get_source(mxc)
		.await
		.map_err(|_| not_found())?;

	let source: OwnedMxcUri = source.into();
	let source: Mxc<'_> = source
		.as_str()
		.try_into()
		.map_err(|e| err!(Database("Media source of {mxc} is not an mxc URI: {e}")))?;

	let _lock = self.federation_mutex.lock(mxc).await;

	let request = get_content::v1::Request {
		media_id: source.media_id.into(),
		timeout_ms: super::SOURCE_TIMEOUT,
	};

	let response = self
		.services
		.federation
		.execute(source.server_name, request)
		.await
		.map_err(|error| {
			err!(Request(NotFound(
				debug_warn!(%mxc, %source, ?error, "Fetching media from its source failed")
			)))
		})?;

	match response.content {
		| FileOrLocation::File(content) => Ok(content.file.into()),
		| FileOrLocation::Location(location) => {
			let limit = self.services.server.config.max_response_size;

			self.location_request(Fetch::Extern, &location, limit)
				.await
				.map(|media| media.content.into())
				.map_err(|error| {
					err!(Request(NotFound(debug_warn!(
						%mxc, %source, ?error,
						"Fetching media from its source's location failed"
					))))
				})
		},
	}
}

/// The mxc an original's metadata key belongs to.
///
/// A key is the mxc, `0xFF`, width and height as two big-endian `u32`s, then
/// `0xFF` and the disposition and type. The mxc holds no `0xFF` (it is UTF-8),
/// so the first one ends it; an original is the row at zero by zero.
pub(super) fn original_mxc(key: &[u8]) -> Option<&str> {
	let end = key.iter().position(|&b| b == 0xFF)?;
	let dim = key.get(end.saturating_add(1)..end.saturating_add(9))?;

	if dim.iter().any(|&b| b != 0) {
		return None;
	}

	std::str::from_utf8(&key[..end]).ok()
}

#[cfg(test)]
mod tests {
	use super::original_mxc;

	#[test]
	fn original_mxc_reads_the_original_row_only() {
		const MXC: &[u8] = b"mxc://example.com/abc";

		let original: [&[u8]; 5] = [MXC, &[0xFF], &[0; 8], &[0xFF], b"x"];
		assert_eq!(original_mxc(&original.concat()), Some("mxc://example.com/abc"));

		let thumbnail: [&[u8]; 4] = [MXC, &[0xFF], &[0, 0, 0, 32, 0, 0, 0, 32], &[0xFF]];
		assert_eq!(original_mxc(&thumbnail.concat()), None);

		// a width whose bytes include 0xFF is still a size, not a separator
		let wide: [&[u8]; 3] = [MXC, &[0xFF], &[0, 0, 0, 0xFF, 0, 0, 0, 1]];
		assert_eq!(original_mxc(&wide.concat()), None);
	}
}
