use std::io::Result;

use clap::{Parser, Subcommand};

mod completions;
mod manpages;

pub(crate) trait Task {
    fn handle(self) -> Result<()>;
}

#[derive(Subcommand, Debug)]
enum Tasks {
    #[command(flatten)]
    Manpages(manpages::Manpages),
    #[command(flatten)]
    ShellCompletions(completions::ShellCompletions),
}

impl Task for Tasks {
    fn handle(self) -> Result<()> {
        match self {
            Self::Manpages(manpages) => manpages.handle(),
            Self::ShellCompletions(completions) => completions.handle(),
        }
    }
}

#[derive(Parser, Debug)]
pub(crate) struct XtaskCli {
    #[command(subcommand)]
    task: Tasks,
}

impl Task for XtaskCli {
    fn handle(self) -> Result<()> {
        self.task.handle()
    }
}
