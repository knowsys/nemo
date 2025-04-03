use nemo::{
    io::{resource_providers::ResourceProviders, ImportManager},
    rule_model::{components::fact::Fact, program::Program},
};

use crate::static_checks::rule_set::RuleSet;

use std::collections::HashSet;

pub enum ChaseVariant {
    SkolemMFA,
    SkolemDMFA,
    SkolemRestricted,
}

impl RuleSet {
    fn critical_instance(&self) -> HashSet<Fact> {
        todo!("IMPLEMENT");
    }

    fn initialize_dummy_import_manager(&self) -> ImportManager {
        ImportManager::new(ResourceProviders::empty())
    }

    fn build_datalog_program(&self) -> Program {
        todo!("IMPLEMENT");
    }

    pub fn check_acyclicity(&self, variant: ChaseVariant) -> bool {
        let cur_facts: HashSet<Fact> = self.critical_instance();
        let dummy_import_manager: ImportManager = self.initialize_dummy_import_manager();
        let mut datalog_program: Program = self.build_datalog_program();
        todo!("IMPLEMENT");
    }
}
