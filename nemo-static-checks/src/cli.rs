use clap::ArgAction;

use std::path::PathBuf;

/// Nemo CLI
#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
pub struct CliApp {
    /// One or more rule program files
    #[arg(value_parser, required = true)]
    pub(crate) rules: Vec<PathBuf>,

    /// One or more static checks
    #[arg(short = 'c', long = "checks", value_parser = CHECK_NAMES, action = ArgAction::Append, default_values = CHECK_NAMES)]
    pub(crate) checks: Vec<String>,
}

const CHECK_NAMES: [&str; 27] = [
    "joinless",
    "linear",
    "guarded",
    "sticky",
    "domain-restricted",
    "frontier-one",
    "datalog",
    "monadic",
    "frontier-guarded",
    "weakly-guarded",
    "weakly-frontier-guarded",
    "jointly-guarded",
    "jointly-frontier-guarded",
    "weakly-acyclic",
    "jointly-acyclic",
    "weakly-sticky",
    "glut-guarded",
    "glut-frontier-guarded",
    "shy",
    "mfa",
    "msa",
    "dmfa",
    "rmfa",
    "mfc",
    "dmfc",
    "drpc",
    "rpc",
];
