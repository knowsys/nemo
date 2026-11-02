use clap::{ArgAction, ValueEnum};

use std::path::PathBuf;

/// Nemo CLI
#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
pub struct CliApp {
    /// One or more rule program files
    #[arg(value_parser, required = true)]
    pub(crate) rules: Vec<PathBuf>,

    /// One or more static checks
    #[arg(short = 'c', long = "checks", value_enum, action = ArgAction::Append, default_values_t = Check::value_variants().to_vec())]
    pub(crate) checks: Vec<Check>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum Check {
    Joinless,
    Linear,
    Guarded,
    Sticky,
    DomainRestricted,
    FrontierOne,
    Datalog,
    Monadic,
    FrontierGuarded,
    WeaklyGuarded,
    WeaklyFrontierGuarded,
    JointlyGuarded,
    JointlyFrontierGuarded,
    WeaklyAcyclic,
    JointlyAcyclic,
    WeaklySticky,
    GlutGuarded,
    GlutFrontierGuarded,
    Shy,
    Mfa,
    Msa,
    Dmfa,
    Rmfa,
    Mfc,
    Dmfc,
    Drpc,
    Rpc,
}
