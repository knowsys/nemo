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
        let check_str: &str = &check;
        match check_str {
            "joinless" => println!("{check_str}: {}", RulesProperties::is_joinless(&rule_set)),
            "linear" => println!("{check_str}: {}", RulesProperties::is_linear(&rule_set)),
            "guarded" => println!("{check_str}: {}", RulesProperties::is_guarded(&rule_set)),
            "sticky" => println!("{check_str}: {}", RulesProperties::is_sticky(&rule_set)),
            "domain-restricted" => println!(
                "{check_str}: {}",
                RulesProperties::is_domain_restricted(&rule_set)
            ),
            "frontier-one" => println!(
                "{check_str}: {}",
                RulesProperties::is_frontier_one(&rule_set)
            ),
            "datalog" => println!("{check_str}: {}", RulesProperties::is_datalog(&rule_set)),
            "monadic" => println!("{check_str}: {}", RulesProperties::is_monadic(&rule_set)),
            "frontier-guarded" => println!(
                "{check_str}: {}",
                RulesProperties::is_frontier_guarded(&rule_set)
            ),
            "weakly-guarded" => println!(
                "{check_str}: {}",
                RulesProperties::is_weakly_guarded(&rule_set)
            ),
            "weakly-frontier-guarded" => println!(
                "{check_str}: {}",
                RulesProperties::is_weakly_frontier_guarded(&rule_set)
            ),
            "jointly-guarded" => println!(
                "{check_str}: {}",
                RulesProperties::is_jointly_guarded(&rule_set)
            ),
            "jointly-frontier-guarded" => println!(
                "{check_str}: {}",
                RulesProperties::is_jointly_frontier_guarded(&rule_set)
            ),
            "weakly-acyclic" => println!(
                "{check_str}: {}",
                RulesProperties::is_weakly_acyclic(&rule_set)
            ),
            "jointly-acyclic" => println!(
                "{check_str}: {}",
                RulesProperties::is_jointly_acyclic(&rule_set)
            ),
            "weakly-sticky" => println!(
                "{check_str}: {}",
                RulesProperties::is_weakly_sticky(&rule_set)
            ),
            "glut-guarded" => println!(
                "{check_str}: {}",
                RulesProperties::is_glut_guarded(&rule_set)
            ),
            "glut-frontier-guarded" => println!(
                "{check_str}: {}",
                RulesProperties::is_glut_frontier_guarded(&rule_set)
            ),
            "shy" => println!("{check_str}: {}", RulesProperties::is_shy(&rule_set)),
            "mfa" => println!(
                "{check_str}: not yet implemented", /*RulesProperties::is_mfa(&rule_set)*/
            ),
            "msa" => println!("{check_str}: {}", RulesProperties::is_msa(&handle).await),
            "dmfa" => println!(
                "{check_str}: not yet implemented",
                // RulesProperties::is_dmfa(&rule_set)
            ),
            "rmfa" => println!(
                "{check_str}: not yet implemented",
                // RulesProperties::is_rmfa(&rule_set)
            ),
            "mfc" => println!(
                "{check_str}: not yet implemented",
                // RulesProperties::is_mfc(&rule_set)
            ),
            "dmfc" => println!(
                "{check_str}: not yet implemented",
                // RulesProperties::is_dmfc(&rule_set)
            ),
            "drpc" => println!(
                "{check_str}: not yet implemented",
                // RulesProperties::is_drpc(&rule_set)
            ),
            "rpc" => println!(
                "{check_str}: not yet implemented",
                // RulesProperties::is_rpc(&rule_set)
            ),
            _ => unreachable!(),
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
