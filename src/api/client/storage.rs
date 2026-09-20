//! What the account's chats take up on the server: per chat (largest first), for the whole account,
//! and, for a server admin, for the server (database and media directory).

use axum::extract::State;
use ruma::{
	UInt,
	api::{auth_scheme::AccessToken, request, response},
	metadata,
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Result, utils::math::usize_from_ruma_bounded};

use super::room_stats::Storage;
use crate::Ruma;


const ROOMS_DEFAULT: usize = 50;
const ROOMS_MAX: usize = 500;

metadata! {
	method: GET,
	rate_limited: true,
	authentication: AccessToken,
	history: {
		unstable => "/_matrix/client/unstable/im.mxg.stats/storage",
	}
}

#[request]
pub struct Request {
	/// How many chats to list, the ones taking the most first.
	#[ruma_api(query)]
	pub rooms: Option<UInt>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RoomStorage {
	pub room_id: String,
	pub name: Option<String>,
	pub messages: u64,
	pub storage: Storage,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ServerStorage {
	/// The database: every room's events, state and indexes.
	pub database: u64,
	/// The media directory.
	pub media: u64,
}

#[response]
pub struct Response {
	/// The account's chats, largest first.
	pub rooms: Vec<RoomStorage>,
	/// How many chats there are, and what all of them add up to.
	pub room_count: u64,
	pub account: Storage,
	/// Only for a server admin.
	pub server: Option<ServerStorage>,
}

/// The directory sizes are a walk of the disk, so they are measured at most this often.
const SERVER_SIZE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

fn dir_size(path: &std::path::Path, skip: Option<&std::path::Path>) -> u64 {
	let mut total = 0_u64;
	let mut stack = vec![path.to_path_buf()];
	while let Some(dir) = stack.pop() {
		let Ok(entries) = std::fs::read_dir(&dir) else {
			continue;
		};
		for entry in entries.flatten() {
			let path = entry.path();
			if skip.is_some_and(|skip| path == skip) {
				continue;
			}
			match entry.metadata() {
				| Ok(meta) if meta.is_dir() => stack.push(path),
				| Ok(meta) => total = total.saturating_add(meta.len()),
				| Err(_) => {},
			}
		}
	}
	total
}

static SERVER_SIZE: std::sync::Mutex<Option<(std::time::Instant, ServerStorage)>> =
	std::sync::Mutex::new(None);

/// # `GET /_matrix/client/unstable/im.mxg.stats/storage`
pub(crate) async fn get_storage_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	use futures::StreamExt;

	let sender_user = body.sender_user();
	let limit = body.rooms.map_or(ROOMS_DEFAULT, |limit| {
		usize_from_ruma_bounded(limit, ROOMS_DEFAULT, ROOMS_MAX)
	});

	let room_ids: Vec<_> = services
		.state_cache
		.rooms_joined(sender_user)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let mut account = Storage::default();
	let mut rooms = Vec::with_capacity(room_ids.len());
	for room_id in &room_ids {
		let Ok(shortroomid) = services.short.get_shortroomid(room_id).await else {
			continue;
		};
		let stats = services.room_stats.stats(shortroomid).await;
		let messages: u64 = stats.counts.iter().map(|c| c.count).sum();
		let storage = Storage {
			events: stats.bytes.event,
			media_stored: stats.bytes.media_stored,
			media_on_demand: stats.bytes.media_on_demand,
		};
		account.events = account.events.saturating_add(storage.events);
		account.media_stored = account.media_stored.saturating_add(storage.media_stored);
		account.media_on_demand = account.media_on_demand.saturating_add(storage.media_on_demand);
		if messages > 0 {
			rooms.push((room_id.clone(), messages, storage));
		}
	}

	rooms.sort_by(|a, b| {
		let (sa, sb) = (
			a.2.events.saturating_add(a.2.media_stored),
			b.2.events.saturating_add(b.2.media_stored),
		);
		sb.cmp(&sa).then_with(|| b.1.cmp(&a.1))
	});
	let room_count = rooms.len() as u64;
	rooms.truncate(limit);

	let mut listed = Vec::with_capacity(rooms.len());
	for (room_id, messages, storage) in rooms {
		let name = services.state_accessor.get_name(&room_id).await.ok();
		listed.push(RoomStorage { room_id: room_id.to_string(), name, messages, storage });
	}

	let server = if services.admin.user_is_admin(sender_user).await {
		let cached = SERVER_SIZE
			.lock()
			.expect("locked")
			.as_ref()
			.filter(|(at, _)| at.elapsed() < SERVER_SIZE_TTL)
			.map(|(_, size)| size.clone());
		if let Some(cached) = cached {
			Some(cached)
		} else {
			let database_path = services.server.config.database_path.clone();
			let media_dir = services.media.get_media_dir();
			let measured = tokio::task::spawn_blocking(move || {
				let media = dir_size(&media_dir, None);
				let all = dir_size(&database_path, None);
				ServerStorage { database: all.saturating_sub(media), media }
			})
			.await
			.ok();
			if let Some(measured) = &measured {
				*SERVER_SIZE.lock().expect("locked") = Some((std::time::Instant::now(), measured.clone()));
			}
			measured
		}
	} else {
		None
	};

	Ok(Response { rooms: listed, room_count, account, server })
}
