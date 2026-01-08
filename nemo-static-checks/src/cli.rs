#![feature(const_slice_make_iter)]
use clap::ArgAction;

/// Nemo CLI
#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
pub struct CliApp {
    #[arg(short = 'c', long = "checks", value_parser = CHECK_NAMES, action = ArgAction::Append, default_values = CHECK_NAMES)]
    pub(crate) checks: Vec<String>,
}

const CHECK_NAMES: [&'static str; 27] = [
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

fn parse_checks(s: &str) -> Result<String, String> {
    if !CHECK_NAMES.contains(&s) {
        return Err(format!("Unknown check: {}", s));
    }
    Ok(s.to_string())
}
