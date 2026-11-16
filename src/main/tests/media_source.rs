#![cfg(test)]
#![cfg(feature = "media_thumbnail")]

//! Local media whose file was dropped, answered from its recorded source.
//!
//! A bridge records for an upload of its own where the same bytes can be
//! fetched again (its direct-media mxc on a plain-HTTP peer); the local file is
//! then deleted. Downloads and thumbnails of the original mxc keep working:
//! the server fetches from the source, through itself, and stores nothing but
//! the thumbnails it derives. A dropped file without a source stays gone,
//! which is what shows the source is what answers.
//!
//! The peer is a stub on loopback named in `http_federation_peers`. It answers
//! the federation download endpoint in `multipart/mixed`, once with the file
//! itself and once with a location (what a bridge's direct media answers with
//! a refreshed CDN URL), which it then also serves.

use std::{
	fs::{remove_dir_all, remove_file},
	io::Cursor,
	net::TcpListener,
	path::PathBuf,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
};

use axum::{
	Router,
	extract::State,
	http::{Uri, header::CONTENT_TYPE},
	response::IntoResponse,
	routing::any,
};
use image::{ImageFormat, RgbImage};
use tokio::spawn;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	ruma::{Mxc, ServerName, UserId},
	utils::content_disposition::make_content_disposition,
};
use tuwunel_service::{
	Services,
	media::{Animate, Dim},
};

/// The made-up server name the peer answers for, as a bridge's would be.
const PEER: &str = "source-media.test";

const BOUNDARY: &str = "sourcestubmultipartboundary";

/// The peer's media answered with the file itself.
const FILE_SOURCE_ID: &str = "sourcefilemediaid";

/// The peer's media answered with a location the peer also serves.
const LOCATION_SOURCE_ID: &str = "sourcelocationmediaid";

const CDN_PATH: &str = "/cdn/picture.png";

static FEDERATION_ASKED: AtomicUsize = AtomicUsize::new(0);

static CDN_ASKED: AtomicUsize = AtomicUsize::new(0);

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn dropped_media_is_answered_from_its_source() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let address = format!("127.0.0.1:{}", listener.local_addr()?.port());
	listener.set_nonblocking(true)?;

	let db_path = DatabasePath(Args::test_database_path("media-source"));

	let args = [
		format!("database_path={:?}", db_path.0),
		"ip_range_denylist=[]".to_owned(),
		format!("http_federation_peers={{\"{PEER}\"=\"{address}\"}}"),
		"log=\"error\"".to_owned(),
	]
	.into_iter()
	.fold(Args::default_test(&["fresh", "cleanup"]), Args::with_option);

	let args = Args { maintenance: true, ..args };

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let stub = spawn(serve_peer(listener, address));
		let outcome = exercise(&services).await;

		let outcome = match stub.is_finished() {
			| true => stub
				.await
				.unwrap_or_else(|error| Err(err!("the peer stub panicked: {error}")))
				.and(outcome),
			| false => {
				stub.abort();
				outcome
			},
		};

		let shutdown = server.server.shutdown();

		drop(services);

		let run = async_run(&server).await;
		let stop = async_stop(&server).await;

		outcome.and(shutdown).and(run).and(stop)
	});

	drop(runtime);

	result
}

fn picture() -> Vec<u8> {
	let image = RgbImage::from_fn(64, 48, |x, y| image::Rgb([(x * 4) as u8, (y * 5) as u8, 90]));
	let mut bytes = Cursor::new(Vec::new());
	image
		.write_to(&mut bytes, ImageFormat::Png)
		.expect("encodes a png");

	bytes.into_inner()
}

async fn serve_peer(listener: TcpListener, address: String) -> Result {
	let app = Router::new()
		.route("/_matrix/federation/{*rest}", any(federation))
		.route(CDN_PATH, any(cdn))
		.with_state(Arc::<str>::from(address));

	axum_server::from_tcp(listener)?
		.serve(app.into_make_service())
		.await?;

	Ok(())
}

async fn federation(uri: Uri, State(address): State<Arc<str>>) -> impl IntoResponse {
	FEDERATION_ASKED.fetch_add(1, Ordering::Relaxed);

	let body = match uri.path().ends_with(LOCATION_SOURCE_ID) {
		| true => format!(
			"\r\n--{BOUNDARY}\r\ncontent-type: \
			 application/json\r\n\r\n{{}}\r\n--{BOUNDARY}\r\nlocation: \
			 http://{address}{CDN_PATH}\r\n\r\n\r\n--{BOUNDARY}--"
		)
		.into_bytes(),
		| false => {
			let head = format!(
				"\r\n--{BOUNDARY}\r\ncontent-type: \
				 application/json\r\n\r\n{{}}\r\n--{BOUNDARY}\r\ncontent-type: image/png\r\n\r\n"
			);
			let tail = format!("\r\n--{BOUNDARY}--");
			let picture = picture();
			let parts: [&[u8]; 3] = [head.as_bytes(), &picture, tail.as_bytes()];

			parts.concat()
		},
	};

	([(CONTENT_TYPE, format!("multipart/mixed; boundary={BOUNDARY}"))], body)
}

async fn cdn() -> impl IntoResponse {
	CDN_ASKED.fetch_add(1, Ordering::Relaxed);

	([(CONTENT_TYPE, "image/png")], picture())
}

/// Uploads the picture under a local mxc and returns where its file lies.
async fn upload(services: &Services, media_id: &str) -> Result<String> {
	let server_name = services.globals.server_name();
	let mxc = Mxc { server_name, media_id };
	let uploader = UserId::parse_with_server_name("bridgebot", server_name)?;
	let disposition = make_content_disposition(None, Some("image/png"), Some("picture.png"));

	services
		.media
		.create(&mxc, Some(&*uploader), Some(&disposition), Some("image/png"), &picture())
		.await?;

	let file = services.media.set_source(&mxc, None).await?;
	if !file.present {
		return Err!("the uploaded file is not where set_source says it is");
	}

	Ok(file.file)
}

fn drop_file(services: &Services, file: &str) -> Result {
	remove_file(services.media.get_media_dir().join(file))?;

	Ok(())
}

async fn exercise(services: &Services) -> Result {
	let server_name = services.globals.server_name();
	let peer = ServerName::parse(PEER).map_err(|e| err!("peer server name: {e}"))?;
	let peer = &*peer;
	let original = picture();

	// a source on this very server would loop back to the file it replaces
	let own = Mxc { server_name, media_id: "own" };
	upload(services, "withownsource").await?;
	let mxc = Mxc { server_name, media_id: "withownsource" };
	if services.media.set_source(&mxc, Some(&own)).await.is_ok() {
		return Err!("a source on this server was accepted");
	}

	// without a source a dropped file stays gone
	let file = upload(services, "nosource").await?;
	drop_file(services, &file)?;
	let mxc = Mxc { server_name, media_id: "nosource" };
	if services.media.get(&mxc, None).await.is_ok() {
		return Err!("a dropped file without a source was still served");
	}

	// the source answers with the file itself
	let file = upload(services, "filesource").await?;
	let mxc = Mxc { server_name, media_id: "filesource" };
	let source = Mxc { server_name: peer, media_id: FILE_SOURCE_ID };
	services.media.set_source(&mxc, Some(&source)).await?;
	drop_file(services, &file)?;

	let state = services.media.set_source(&mxc, Some(&source)).await?;
	if state.present {
		return Err!("the dropped file is reported as stored");
	}

	let before = FEDERATION_ASKED.load(Ordering::Relaxed);
	let media = services.media.get(&mxc, None).await?;
	if media.content != original {
		return Err!("the source's bytes differ from the original");
	}
	if media.content_type.as_deref() != Some("image/png") {
		return Err!("the download lost its type: {:?}", media.content_type);
	}
	if FEDERATION_ASKED.load(Ordering::Relaxed) != before + 1 {
		return Err!("the download did not ask the source exactly once");
	}
	if services.media.get_media_dir().join(&file).exists() {
		return Err!("the fetched original was stored again");
	}

	// a thumbnail is derived from the fetched original and kept
	let dim = Dim::new(32, 32, None);
	let thumbnail = services
		.media
		.get_stored_thumbnail(&mxc, &dim, Animate::Allowed)
		.await?;
	let decoded = image::load_from_memory(&thumbnail.content)
		.map_err(|e| err!("the thumbnail does not decode: {e}"))?;
	if decoded.width() > 32 || decoded.height() > 32 {
		return Err!("the thumbnail is {}x{}", decoded.width(), decoded.height());
	}

	let asked = FEDERATION_ASKED.load(Ordering::Relaxed);
	services
		.media
		.get_stored_thumbnail(&mxc, &dim, Animate::Allowed)
		.await?;
	if FEDERATION_ASKED.load(Ordering::Relaxed) != asked {
		return Err!("a stored thumbnail went back to the source");
	}

	// the source answers with a location, which this server fetches itself
	let file = upload(services, "locationsource").await?;
	let mxc = Mxc { server_name, media_id: "locationsource" };
	let source = Mxc { server_name: peer, media_id: LOCATION_SOURCE_ID };
	services.media.set_source(&mxc, Some(&source)).await?;
	drop_file(services, &file)?;

	let cdn_before = CDN_ASKED.load(Ordering::Relaxed);
	let media = services.media.get(&mxc, None).await?;
	if media.content != original {
		return Err!("the location's bytes differ from the original");
	}
	if CDN_ASKED.load(Ordering::Relaxed) != cdn_before + 1 {
		return Err!("the location was not fetched by the server");
	}

	// forgetting the source leaves the dropped file gone
	services.media.set_source(&mxc, None).await?;
	if services.media.get(&mxc, None).await.is_ok() {
		return Err!("a forgotten source still answered");
	}

	Ok(())
}
