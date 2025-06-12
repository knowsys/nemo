use cargo_metadata::MetadataCommand;

pub(crate) const NEMO_CLI_CRATE: &str = "nemo-cli";
pub(crate) const NEMO_CLI_DEFAULT_BIN_NAME: &str = "nmo";

pub(crate) fn bin_name_from_manifest(crate_name: &str) -> Option<String> {
    let manifest = option_env!("CARGO_MANIFEST_FILE")?;
    let metadata = MetadataCommand::new().manifest_path(manifest).exec().ok()?;

    let packages = metadata.workspace_packages();
    let package = packages.iter().find(|package| package.name == crate_name)?;

    package
        .targets
        .iter()
        .find(|target| target.is_bin())
        .map(|bin| bin.name.clone())
}

pub(crate) fn nemo_cli_bin_name() -> String {
    bin_name_from_manifest(NEMO_CLI_CRATE).unwrap_or(NEMO_CLI_DEFAULT_BIN_NAME.to_string())
}
