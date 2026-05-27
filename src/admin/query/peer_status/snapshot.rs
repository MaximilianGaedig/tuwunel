use futures::StreamExt;
use ruma::OwnedServerName;
use tokio::time::Instant;
use tuwunel_core::{Result, utils::time};

use crate::admin_command;

#[admin_command]
pub(super) async fn peer_status_snapshot(&self, server_name: Option<OwnedServerName>) -> Result {
	writeln!(self, "| Server Name | Bucket Start | Classification |").await?;
	writeln!(self, "| ----------- | ------------ | -------------- |").await?;

	let timer = Instant::now();
	let mut snapshot = self.services.federation.peer_snapshot().boxed();
	while let Some((server, bucket_start, classification)) = snapshot.next().await {
		if let Some(filter) = server_name.as_deref()
			&& server != filter
		{
			continue;
		}

		let start = time::format(bucket_start, "%+");
		write!(self, "| {server} | {start} | {classification:?} |\n").await?;
	}

	let query_time = timer.elapsed();
	write!(self, "\nQuery completed in {query_time:?}.").await
}
