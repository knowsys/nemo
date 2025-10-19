use cargo_metadata::MetadataCommand;
use cargo_util_schemas::manifest::PackageName;

pub(crate) const NEMO_CLI_CRATE: &str = "nemo-cli";
pub(crate) const NEMO_CLI_DEFAULT_BIN_NAME: &str = "nmo";

pub(crate) const NEMO_FMT_CRATE: &str = "nemo-fmt";
pub(crate) const NEMO_FMT_DEFAULT_BIN_NAME: &str = "nmo-fmt";

pub(crate) fn bin_name_from_manifest(crate_name: &str) -> Option<String> {
    let manifest = option_env!("CARGO_MANIFEST_FILE")?;
    let metadata = MetadataCommand::new().manifest_path(manifest).exec().ok()?;

    let name = PackageName::new(crate_name.to_string()).ok()?;
    let packages = metadata.workspace_packages();
    let package = packages.iter().find(|package| package.name == name)?;

    package
        .targets
        .iter()
        .find(|target| target.is_bin())
        .map(|bin| bin.name.clone())
}

pub(crate) fn nemo_cli_bin_name() -> String {
    bin_name_from_manifest(NEMO_CLI_CRATE).unwrap_or(NEMO_CLI_DEFAULT_BIN_NAME.to_string())
}

pub(crate) fn nemo_fmt_bin_name() -> String {
    bin_name_from_manifest(NEMO_FMT_CRATE).unwrap_or(NEMO_FMT_DEFAULT_BIN_NAME.to_string())
}
