use std::io::Result;
use std::path::PathBuf;

use clap::{CommandFactory, Subcommand};

use crate::bin_name::{nemo_cli_bin_name, nemo_fmt_bin_name};
use clap_mangen::generate_to;
use nemo_cli::cli::CliApp as NemoCli;
use nemo_fmt::cli::CliApp as NemoFmt;

use super::Task;

#[derive(Debug, Subcommand)]
pub(crate) enum Manpages {
    #[command(about = "Generate a man page")]
    GenerateManpages { out_dir: PathBuf },
}

impl Task for Manpages {
    fn handle(self) -> Result<()> {
        match self {
            Self::GenerateManpages { out_dir } => {
                let nemo_command = NemoCli::command().name(nemo_cli_bin_name());
                generate_to(nemo_command, &out_dir)?;
                let fmt_command = NemoFmt::command().name(nemo_fmt_bin_name());
                generate_to(fmt_command, &out_dir)
            }
        }
    }
}
