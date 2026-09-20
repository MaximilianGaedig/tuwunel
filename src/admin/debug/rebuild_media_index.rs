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
