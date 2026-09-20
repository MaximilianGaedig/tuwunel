use tuwunel_core::{Result, error, info};

use crate::admin_command;

// The rebuilds walk every room's history, which takes minutes. They run on their own task and the
// command returns at once: nothing an admin types may keep the server from serving.

#[admin_command]
pub(super) async fn rebuild_media_index(&self) -> Result {
	let service = self.services.media_index.clone();
	self.services
		.server
		.runtime()
		.spawn(async move {
			match service.rebuild().await {
				| Ok(indexed) => info!("Indexed the media of {indexed} messages."),
				| Err(e) => error!("Rebuilding the media index failed: {e}"),
			}
		});

	self.write_str("Rebuilding the media index in the background; the result goes to the log.")
		.await
}

#[admin_command]
pub(super) async fn rebuild_search_words(&self) -> Result {
	let service = self.services.search.clone();
	self.services
		.server
		.runtime()
		.spawn(async move {
			match service.rebuild_words().await {
				| Ok(indexed) =>
					info!("Indexed the words of {indexed} messages for typo-tolerant search."),
				| Err(e) => error!("Rebuilding the search words failed: {e}"),
			}
		});

	self.write_str("Rebuilding the search words in the background; the result goes to the log.")
		.await
}

#[admin_command]
pub(super) async fn rebuild_room_stats(&self) -> Result {
	let service = self.services.room_stats.clone();
	self.services
		.server
		.runtime()
		.spawn(async move {
			match service.rebuild().await {
				| Ok(counted) => info!("Counted {counted} messages."),
				| Err(e) => error!("Rebuilding the room statistics failed: {e}"),
			}
		});

	self.write_str("Counting the rooms' messages in the background; the result goes to the log.")
		.await
}
