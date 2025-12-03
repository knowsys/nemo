use std::path::PathBuf;

use nemo::{
    error::{report::ProgramReport, warned::Warned, Error},
    rule_file::RuleFile,
    rule_model::{
        error::TranslationReport,
        programs::{handle::ProgramHandle, program::Program},
        translation::ProgramParseReport,
    },
};

pub trait FileProperties {
    /// Determines if the ruleset in this file is joinless.
    fn is_joinless(&self) -> bool;
    /// Determines if the ruleset in this file is linear.
    fn is_linear(&self) -> bool;
    /// Determines if the ruleset in this file is guarded.
    fn is_guarded(&self) -> bool;
    /// Determines if the ruleset in this file is sticky.
    fn is_sticky(&self) -> bool;
    /// Determines if the ruleset in this file is domain restricted.
    fn is_domain_restricted(&self) -> bool;
    /// Determines if the ruleset in this file is frontier one.
    fn is_frontier_one(&self) -> bool;
    /// Determines if the ruleset in this file is datalog.
    fn is_datalog(&self) -> bool;
    /// Determines if the ruleset in this file is monadic.
    fn is_monadic(&self) -> bool;
    /// Determines if the ruleset in this file is frontier guarded.
    fn is_frontier_guarded(&self) -> bool;
    /// Determines if the ruleset in this file is weakly guarded.
    fn is_weakly_guarded(&self) -> bool;
    /// Determines if the ruleset in this file is weakly fronier guarded.
    fn is_weakly_frontier_guarded(&self) -> bool;
    /// Determines if the ruleset in this file is jointly guarded.
    fn is_jointly_guarded(&self) -> bool;
    /// Determines if the ruleset in this file is jointly frontier guarded.
    fn is_jointly_frontier_guarded(&self) -> bool;
    /// Determines if the ruleset in this file is weakly acyclic.
    fn is_weakly_acyclic(&self) -> bool;
    /// Determines if the ruleset in this file is jointly acyclic.
    fn is_jointly_acyclic(&self) -> bool;
    /// Determines if the ruleset in this file is weakly sticky.
    fn is_weakly_sticky(&self) -> bool;
    /// Determines if the ruleset in this file is glut guarded.
    fn is_glut_guarded(&self) -> bool;
    /// Determines if the ruleset in this file is glut frontier guarded.
    fn is_glut_frontier_guarded(&self) -> bool;
    /// Determines if the ruleset in this file is shy.
    fn is_shy(&self) -> bool;
    /// Determines if the ruleset in this file is mfa.
    fn is_mfa(&self) -> bool;
    /// Determines if the ruleset in this file is msa.
    fn is_msa(&self) -> bool;
    /// Determines if the ruleset in this file is dmfa.
    fn is_dmfa(&self) -> bool;
    /// Determines if the ruleset in this file is rmfa.
    fn is_rmfa(&self) -> bool;
    /// Determines if the ruleset in this file is mfc.
    fn is_mfc(&self) -> bool;
    /// Determines if the ruleset in this file is dmfc.
    fn is_dmfc(&self) -> bool;
    /// Determines if the ruleset in this file is drpc.
    fn is_drpc(&self) -> bool;
    /// Determines if the ruleset in this file is rpc.
    fn is_rpc(&self) -> bool;
}

impl FileProperties for PathBuf {
    fn is_joinless(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }

    fn is_linear(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }

    fn is_guarded(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }

    fn is_sticky(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }

    fn is_domain_restricted(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }

    fn is_frontier_one(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }

    fn is_datalog(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }

    fn is_monadic(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }

    fn is_frontier_guarded(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is weakly guarded.
    fn is_weakly_guarded(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is weakly fronier guarded.
    fn is_weakly_frontier_guarded(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is jointly guarded.
    fn is_jointly_guarded(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is jointly frontier guarded.
    fn is_jointly_frontier_guarded(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is weakly acyclic.
    fn is_weakly_acyclic(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is jointly acyclic.
    fn is_jointly_acyclic(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is weakly sticky.
    fn is_weakly_sticky(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is glut guarded.
    fn is_glut_guarded(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is glut frontier guarded.
    fn is_glut_frontier_guarded(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is shy.
    fn is_shy(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is mfa.
    fn is_mfa(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is msa.
    fn is_msa(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is dmfa.
    fn is_dmfa(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is rmfa.
    fn is_rmfa(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is mfc.
    fn is_mfc(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is dmfc.
    fn is_dmfc(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is drpc.
    fn is_drpc(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
    /// Determines if the ruleset in this file is rpc.
    fn is_rpc(&self) -> bool {
        // TODO: IMPLEMENT
        todo!("IMPLEMENT");
    }
}

fn create_rules_from_path(path: PathBuf) {
    let rule_file: Result<RuleFile, Error> = RuleFile::load(path);
}

// fn nemo_program_from_rule_file(rule_file: RuleFile) -> Program {
//     let r_init_phandle: Result<Warned<ProgramHandle, TranslationReport>, ProgramParseReport> =
//         ProgramHandle::from_file(&rule_file);
//     let preport: ProgramReport = ProgramReport::new(rule_file);
//
//     let (r_phandle, preport): (ProgramHandle, ProgramReport) =
//         preport.merge_program_parser_report(r_init_phandle)?;
// }
