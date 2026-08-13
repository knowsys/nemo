pub mod cli;
pub mod static_checks;

use nemo::{
    error::{Error, report::ProgramReport},
    execution::execution_parameters::ExecutionParameters,
    rule_file::RuleFile,
    rule_model::{
        pipeline::transformations::default::TransformationDefault, programs::handle::ProgramHandle,
    },
};

use crate::cli::Check;
use crate::static_checks::{rule_set::RuleSet, rules_properties::RulesProperties};

use colored::Colorize;

use nemo_cli::error::CliError;

use clap::Parser;
use cli::CliApp;

fn get_handle_from_rule_file(program_file: RuleFile) -> Result<ProgramHandle, nemo::error::Error> {
    let parameters: ExecutionParameters = ExecutionParameters::default();
    let handle = ProgramHandle::from_file(&program_file);
    let report = ProgramReport::new(program_file);

    let (handle, report) = report.merge_program_parser_report(handle)?;
    let (handle, _) = report.merge_validation_report(
        &handle,
        handle.transform(TransformationDefault::new(&parameters)),
    )?;

    Ok(handle)
}

async fn run(mut cli: CliApp) -> Result<(), CliError> {
    if cli.rules.len() > 1 {
        return Err(CliError::MultipleFilesNotImplemented);
    }

    let program_path = cli.rules.pop().ok_or(CliError::NoInput)?;
    let program_file = RuleFile::load(program_path)?;
    let handle: ProgramHandle = get_handle_from_rule_file(program_file)?;
    let rule_set: RuleSet = RuleSet(handle.materialize().all_rules());
    for check in cli.checks.into_iter() {
        // let check_str: &str = &check;
        match check {
            Check::Joinless => println!("joinless: {}", RulesProperties::is_joinless(&rule_set)),
            Check::Linear => println!("linear: {}", RulesProperties::is_linear(&rule_set)),
            Check::Guarded => println!("guarded: {}", RulesProperties::is_guarded(&rule_set)),
            Check::Sticky => println!("sticky: {}", RulesProperties::is_sticky(&rule_set)),
            Check::DomainRestricted => println!(
                "domain-restricted: {}",
                RulesProperties::is_domain_restricted(&rule_set)
            ),
            Check::FrontierOne => println!(
                "frontier-one: {}",
                RulesProperties::is_frontier_one(&rule_set)
            ),
            Check::Datalog => println!("datalog: {}", RulesProperties::is_datalog(&rule_set)),
            Check::Monadic => println!("monadic: {}", RulesProperties::is_monadic(&rule_set)),
            Check::FrontierGuarded => println!(
                "frontier-guarded: {}",
                RulesProperties::is_frontier_guarded(&rule_set)
            ),
            Check::WeaklyGuarded => println!(
                "weakly-guarded: {}",
                RulesProperties::is_weakly_guarded(&rule_set)
            ),
            Check::WeaklyFrontierGuarded => println!(
                "weakly-frontier-guarded: {}",
                RulesProperties::is_weakly_frontier_guarded(&rule_set)
            ),
            Check::JointlyGuarded => println!(
                "jointly-guarded: {}",
                RulesProperties::is_jointly_guarded(&rule_set)
            ),
            Check::JointlyFrontierGuarded => println!(
                "jointly-frontier-guarded: {}",
                RulesProperties::is_jointly_frontier_guarded(&rule_set)
            ),
            Check::WeaklyAcyclic => println!(
                "weakly-acyclic: {}",
                RulesProperties::is_weakly_acyclic(&rule_set)
            ),
            Check::JointlyAcyclic => println!(
                "jointly-acyclic: {}",
                RulesProperties::is_jointly_acyclic(&rule_set)
            ),
            Check::WeaklySticky => println!(
                "weakly-sticky: {}",
                RulesProperties::is_weakly_sticky(&rule_set)
            ),
            Check::GlutGuarded => println!(
                "glut-guarded: {}",
                RulesProperties::is_glut_guarded(&rule_set)
            ),
            Check::GlutFrontierGuarded => println!(
                "glut-frontier-guarded: {}",
                RulesProperties::is_glut_frontier_guarded(&rule_set)
            ),
            Check::Shy => println!("shy: {}", RulesProperties::is_shy(&rule_set)),
            Check::Mfa => println!("mfa: {}", RulesProperties::is_mfa(&handle).await),
            Check::Msa => println!("msa: {}", RulesProperties::is_msa(&handle).await),
            Check::Dmfa => println!(
                "dmfa: not yet implemented",
                // RulesProperties::is_dmfa(&rule_set)
            ),
            Check::Rmfa => println!("rmfa: {}", RulesProperties::is_rmfa(&handle).await),
            Check::Mfc => println!("mfc: {}", RulesProperties::is_mfc(&handle).await),
            Check::Dmfc => println!(
                "dmfc: not yet implemented",
                // RulesProperties::is_dmfc(&rule_set)
            ),
            Check::Drpc => println!("drpc: {}", RulesProperties::is_drpc(&handle).await),
            Check::Rpc => println!(
                "rpc: not yet implemented",
                // RulesProperties::is_rpc(&rule_set)
            ),
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = CliApp::parse();

    if let Err(error) = run(cli).await {
        if let CliError::NemoError(Error::ProgramReport(report)) = error {
            // let _ = report.eprint(disable_warnings);

            if report.contains_errors() {
                std::process::exit(1);
            }
        } else {
            log::error!("{} {error}", "error:".red().bold());
            std::process::exit(1);
        }
    }
}
