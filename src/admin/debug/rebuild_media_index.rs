use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn rebuild_media_index(&self) -> Result {
	let indexed = self
		.services
		.media_index
		.rebuild()
		.await?;

	self.write_str(&format!("Indexed the media of {indexed} messages."))
		.await
}

#[admin_command]
pub(super) async fn rebuild_search_words(&self) -> Result {
	let indexed = self.services.search.rebuild_words().await?;

	self.write_str(&format!("Indexed the words of {indexed} messages for typo-tolerant search."))
		.await
}

#[admin_command]
pub(super) async fn rebuild_room_stats(&self) -> Result {
	let counted = self.services.room_stats.rebuild().await?;

	self.write_str(&format!("Counted {counted} messages."))
		.await
}
