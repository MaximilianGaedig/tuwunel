use ruma::OwnedServerName;
use tuwunel_core::{Err, Result};
use tuwunel_service::client::read_response_capped;

use crate::admin_command;

#[admin_command]
pub(super) async fn fetch_support_well_known(&self, server_name: OwnedServerName) -> Result {
	let response = self
		.services
		.client
		.default
		.get(format!("https://{server_name}/.well-known/matrix/support"))
		.send()
		.await?;

	let body = read_response_capped(response, 1500).await?;
	let text = String::from_utf8_lossy(&body);

	if text.is_empty() {
		return Err!("Response text/body is empty.");
	}

	let json: serde_json::Value = match serde_json::from_str(&text) {
		| Ok(json) => json,
		| Err(_) => {
			return Err!("Response text/body is not valid JSON.",);
		},
	};

	let pretty_json: String = match serde_json::to_string_pretty(&json) {
		| Ok(json) => json,
		| Err(_) => {
			return Err!("Response text/body is not valid JSON.",);
		},
	};

	write!(self, "Got JSON response:\n\n```json\n{pretty_json}\n```").await
}
