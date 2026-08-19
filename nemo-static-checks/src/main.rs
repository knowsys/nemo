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
use crate::static_checks::rules_properties::RulesProperties;

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
    for check in cli.checks.into_iter() {
        match check {
            Check::Joinless => println!("joinless: {}", RulesProperties::is_joinless(&handle)),
            Check::Linear => println!("linear: {}", RulesProperties::is_linear(&handle)),
            Check::Guarded => println!("guarded: {}", RulesProperties::is_guarded(&handle)),
            Check::Sticky => println!("sticky: {}", RulesProperties::is_sticky(&handle)),
            Check::DomainRestricted => println!(
                "domain-restricted: {}",
                RulesProperties::is_domain_restricted(&handle)
            ),
            Check::FrontierOne => println!(
                "frontier-one: {}",
                RulesProperties::is_frontier_one(&handle)
            ),
            Check::Datalog => println!("datalog: {}", RulesProperties::is_datalog(&handle)),
            Check::Monadic => println!("monadic: {}", RulesProperties::is_monadic(&handle)),
            Check::FrontierGuarded => println!(
                "frontier-guarded: {}",
                RulesProperties::is_frontier_guarded(&handle)
            ),
            Check::WeaklyGuarded => println!(
                "weakly-guarded: {}",
                RulesProperties::is_weakly_guarded(&handle)
            ),
            Check::WeaklyFrontierGuarded => println!(
                "weakly-frontier-guarded: {}",
                RulesProperties::is_weakly_frontier_guarded(&handle)
            ),
            Check::JointlyGuarded => println!(
                "jointly-guarded: {}",
                RulesProperties::is_jointly_guarded(&handle)
            ),
            Check::JointlyFrontierGuarded => println!(
                "jointly-frontier-guarded: {}",
                RulesProperties::is_jointly_frontier_guarded(&handle)
            ),
            Check::WeaklyAcyclic => println!(
                "weakly-acyclic: {}",
                RulesProperties::is_weakly_acyclic(&handle)
            ),
            Check::JointlyAcyclic => println!(
                "jointly-acyclic: {}",
                RulesProperties::is_jointly_acyclic(&handle)
            ),
            Check::WeaklySticky => println!(
                "weakly-sticky: {}",
                RulesProperties::is_weakly_sticky(&handle)
            ),
            Check::GlutGuarded => println!(
                "glut-guarded: {}",
                RulesProperties::is_glut_guarded(&handle)
            ),
            Check::GlutFrontierGuarded => println!(
                "glut-frontier-guarded: {}",
                RulesProperties::is_glut_frontier_guarded(&handle)
            ),
            Check::Shy => println!("shy: {}", RulesProperties::is_shy(&handle)),
            Check::Mfa => println!("mfa: {}", RulesProperties::is_mfa(&handle).await),
            Check::Msa => println!("msa: {}", RulesProperties::is_msa(&handle).await),
            Check::Dmfa => println!(
                "dmfa: not yet implemented",
                // RulesProperties::is_dmfa(&handle)
            ),
            Check::Rmfa => println!("rmfa: {}", RulesProperties::is_rmfa(&handle).await),
            Check::Mfc => println!("mfc: {}", RulesProperties::is_mfc(&handle).await),
            Check::Dmfc => println!(
                "dmfc: not yet implemented",
                // RulesProperties::is_dmfc(&handle)
            ),
            Check::Drpc => println!("drpc: {}", RulesProperties::is_drpc(&handle).await),
            Check::Rpc => println!(
                "rpc: not yet implemented",
                // RulesProperties::is_rpc(&handle)
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
