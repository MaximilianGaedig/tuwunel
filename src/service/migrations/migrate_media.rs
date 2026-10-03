use tuwunel_core::Result;

use super::{conduit::migrate_conduit_media, marker_present};
use crate::{
	Services,
	media::migrations::{checkup_sha256_media, migrate_sha256_media},
};

/// What the media migration does on a database, from what is in it.
#[derive(Debug, PartialEq, Eq)]
enum MediaPlan {
	/// A Conduit database seen for the first time: its media is imported, and
	/// that is all the key-addressed store needs.
	ImportConduit,
	/// Conduit media that was never imported, in a database tuwunel has already
	/// run on: imported, and then the usual checkup.
	ImportConduitThenCheckup,
	MigrateSha256,
	Checkup,
	Nothing,
}

/// The Conduit import used to be latched by `feat_sha256_media` alone. A
/// database tuwunel had already started on before the import existed had that
/// marker set by tuwunel's own migration, so its Conduit media (the tables and
/// the content-addressed files are still there) was never imported, and every
/// file from before the switch answered "not found". The import now has a
/// marker of its own.
fn plan(
	conduit_media: bool,
	conduit_imported: bool,
	sha256_done: bool,
	startup_check: bool,
) -> MediaPlan {
	match (conduit_media && !conduit_imported, sha256_done) {
		| (true, false) => MediaPlan::ImportConduit,
		| (true, true) => MediaPlan::ImportConduitThenCheckup,
		| (false, false) => MediaPlan::MigrateSha256,
		| (false, true) if startup_check => MediaPlan::Checkup,
		| (false, true) => MediaPlan::Nothing,
	}
}

/// Imports a Conduit database's content-addressed media into tuwunel's
/// key-addressed store when it is present and not yet imported.
///
/// Otherwise runs the key-addressed media migrations.
pub(super) async fn migrate_media(services: &Services) -> Result {
	services.server.check_running()?;

	let db = &services.db;
	let config = &services.server.config;
	let progress = &services.server.progress;

	let sha256_done = marker_present(services, "feat_sha256_media").await?;
	let conduit_imported = marker_present(services, "import_conduit_media").await?;
	// The foreign CF persists, so the markers (not its presence) are the latch.
	let conduit_media = db
		.open_cf("servernamemediaid_metadata")?
		.is_some();

	match plan(conduit_media, conduit_imported, sha256_done, config.media_startup_check) {
		| plan @ (MediaPlan::ImportConduit | MediaPlan::ImportConduitThenCheckup) => {
			progress.begin("migrate_conduit_media");
			migrate_conduit_media(services).await?;
			db["global"].insert("import_conduit_media", []);
			db["global"].insert("feat_sha256_media", []);
			if plan == MediaPlan::ImportConduitThenCheckup && config.media_startup_check {
				progress.begin("checkup_sha256_media");
				checkup_sha256_media(services).await?;
			}
		},
		| MediaPlan::MigrateSha256 => {
			progress.begin("migrate_sha256_media");
			migrate_sha256_media(services).await?;
		},
		| MediaPlan::Checkup => {
			progress.begin("checkup_sha256_media");
			checkup_sha256_media(services).await?;
		},
		| MediaPlan::Nothing => {},
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::{MediaPlan, plan};

	#[test]
	fn a_conduit_database_seen_for_the_first_time_is_imported() {
		assert_eq!(plan(true, false, false, true), MediaPlan::ImportConduit);
	}

	// The case that left 13,871 files unreachable: tuwunel had already run on the
	// database (feat_sha256_media set) before the import existed.
	#[test]
	fn conduit_media_never_imported_is_imported_on_a_database_tuwunel_ran_on() {
		assert_eq!(plan(true, false, true, true), MediaPlan::ImportConduitThenCheckup);
		assert_eq!(plan(true, false, true, false), MediaPlan::ImportConduitThenCheckup);
	}

	#[test]
	fn conduit_media_is_imported_once() {
		assert_eq!(plan(true, true, true, true), MediaPlan::Checkup);
		assert_eq!(plan(true, true, true, false), MediaPlan::Nothing);
	}

	#[test]
	fn a_database_without_conduit_media_is_migrated_as_before() {
		assert_eq!(plan(false, false, false, true), MediaPlan::MigrateSha256);
		assert_eq!(plan(false, false, true, true), MediaPlan::Checkup);
		assert_eq!(plan(false, false, true, false), MediaPlan::Nothing);
	}
}
