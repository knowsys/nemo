pub mod cli;
pub mod static_checks;

use crate::static_checks::file_properties::FileProperties;
use clap::Parser;
use cli::CliApp;
use std::fmt::{Debug, Error, Formatter};
use std::path::PathBuf;

#[derive(Clone)]
struct Check<'a> {
    name: String,
    fun: &'a dyn Fn(&PathBuf) -> bool,
}

impl Debug for Check<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        self.name.fmt(f)
    }
}

fn match_name(s: &str) -> &dyn Fn(&PathBuf) -> bool {
    match s {
        "joinless" => &FileProperties::is_joinless,
        "linear" => &FileProperties::is_linear,
        "guarded" => &FileProperties::is_guarded,
        "sticky" => &FileProperties::is_sticky,
        "domain-restricted" => &FileProperties::is_domain_restricted,
        "frontier-one" => &FileProperties::is_frontier_one,
        "datalog" => &FileProperties::is_datalog,
        "monadic" => &FileProperties::is_monadic,
        "frontier-guarded" => &FileProperties::is_frontier_guarded,
        "weakly-guarded" => &FileProperties::is_weakly_guarded,
        "weakly-frontier-guarded" => &FileProperties::is_weakly_frontier_guarded,
        "jointly-guarded" => &FileProperties::is_jointly_guarded,
        "jointly-frontier-guarded" => &FileProperties::is_jointly_frontier_guarded,
        "weakly-acyclic" => &FileProperties::is_weakly_acyclic,
        "jointly-acyclic" => &FileProperties::is_jointly_acyclic,
        "weakly-sticky" => &FileProperties::is_weakly_sticky,
        "glut-guarded" => &FileProperties::is_glut_guarded,
        "glut-frontier-guarded" => &FileProperties::is_glut_frontier_guarded,
        "shy" => &FileProperties::is_shy,
        "mfa" => &FileProperties::is_mfa,
        "msa" => &FileProperties::is_msa,
        "dmfa" => &FileProperties::is_dmfa,
        "rmfa" => &FileProperties::is_rmfa,
        "mfc" => &FileProperties::is_mfc,
        "dmfc" => &FileProperties::is_dmfc,
        "drpc" => &FileProperties::is_drpc,
        "rpc" => &FileProperties::is_rpc,
        _ => unreachable!(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = CliApp::parse();
    let checks: Vec<Check> = cli
        .checks
        .iter()
        .map(|c_name| Check {
            name: c_name.clone(),
            fun: match_name(&c_name),
        })
        .collect();
}
