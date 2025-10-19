use std::io::Result;
use std::path::PathBuf;

use clap::{CommandFactory, Subcommand, ValueEnum};

use clap_complete::{generate_to, Shell};
use nemo_cli::cli::CliApp as NemoCli;
use nemo_fmt::cli::CliApp as NemoFmt;

use super::Task;
use crate::bin_name::{nemo_cli_bin_name, nemo_fmt_bin_name};

#[derive(Debug, Subcommand)]
pub(crate) enum ShellCompletions {
    #[command(about = "Generate completion data for shells")]
    GenerateShellCompletions { out_dir: PathBuf },
}

impl Task for ShellCompletions {
    fn handle(self) -> Result<()> {
        match self {
            Self::GenerateShellCompletions { out_dir } => {
                let mut nemo_command = NemoCli::command();
                let mut fmt_command = NemoFmt::command();
                Shell::value_variants().iter().try_for_each(|&shell| {
                    generate_to(
                        shell,
                        &mut nemo_command,
                        nemo_cli_bin_name(),
                        out_dir.clone(),
                    )?;
                    generate_to(
                        shell,
                        &mut fmt_command,
                        nemo_fmt_bin_name(),
                        out_dir.clone(),
                    )
                    .and(Ok(()))
                })
            }
        }
    }
}
