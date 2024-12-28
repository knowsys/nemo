use crate::static_checks::rule_set::RuleSet;

use self::rules_properties_internal::RulesPropertiesInternal;

mod rules_properties_internal;

#[allow(dead_code)]
pub trait RulesProperties: RulesPropertiesInternal {
    fn is_joinless(&self) -> bool;
    fn is_linear(&self) -> bool;
    fn is_guarded(&self) -> bool;
    fn is_sticky(&self) -> bool;
    fn is_domain_restricted(&self) -> bool;
    fn is_frontier_one(&self) -> bool;
    fn is_datalog(&self) -> bool;
    fn is_monadic(&self) -> bool;
    fn is_frontier_guarded(&self) -> bool;
    fn is_weakly_guarded(&self) -> bool;
    fn is_weakly_frontier_guarded(&self) -> bool;
    fn is_jointly_guarded(&self) -> bool;
    fn is_jointly_frontier_guarded(&self) -> bool;
    fn is_weakly_acyclic(&self) -> bool;
    fn is_jointly_acyclic(&self) -> bool;
    fn is_weakly_sticky(&self) -> bool;
    fn is_glut_guarded(&self) -> bool;
    fn is_glut_frontier_guarded(&self) -> bool;
    fn is_shy(&self) -> bool;
    fn is_mfa(&self) -> bool;
    fn is_dmfa(&self) -> bool;
    fn is_rmfa(&self) -> bool;
    fn is_mfc(&self) -> bool;
    fn is_dmfc(&self) -> bool;
    fn is_drpc(&self) -> bool;
    fn is_rpc(&self) -> bool;
}

impl RulesProperties for RuleSet {
    fn is_joinless(&self) -> bool {
        self.is_joinless_internal()
    }

    fn is_linear(&self) -> bool {
        self.is_linear_internal()
    }

    fn is_guarded(&self) -> bool {
        self.is_guarded_internal()
    }

    fn is_sticky(&self) -> bool {
        self.is_sticky_internal()
    }

    fn is_weakly_sticky(&self) -> bool {
        self.is_weakly_sticky_internal()
    }

    fn is_domain_restricted(&self) -> bool {
        self.is_domain_restricted_internal()
    }

    fn is_frontier_one(&self) -> bool {
        self.is_frontier_one_internal()
    }

    fn is_datalog(&self) -> bool {
        self.is_datalog_internal()
    }

    fn is_monadic(&self) -> bool {
        self.is_monadic_internal()
    }

    fn is_frontier_guarded(&self) -> bool {
        self.is_frontier_guarded_internal()
    }

    fn is_weakly_guarded(&self) -> bool {
        self.is_weakly_guarded_internal()
    }

    fn is_weakly_frontier_guarded(&self) -> bool {
        self.is_weakly_frontier_guarded_internal()
    }

    fn is_jointly_guarded(&self) -> bool {
        self.is_jointly_guarded_internal()
    }

    fn is_jointly_frontier_guarded(&self) -> bool {
        self.is_jointly_frontier_guarded_internal()
    }

    fn is_weakly_acyclic(&self) -> bool {
        self.is_weakly_acyclic_internal()
    }

    fn is_jointly_acyclic(&self) -> bool {
        self.is_jointly_acyclic_internal()
    }

    fn is_glut_guarded(&self) -> bool {
        self.is_glut_guarded_internal()
    }

    fn is_glut_frontier_guarded(&self) -> bool {
        self.is_glut_frontier_guarded_internal()
    }

    fn is_shy(&self) -> bool {
        self.is_shy_internal()
    }

    fn is_mfa(&self) -> bool {
        self.is_mfa_internal()
    }

    fn is_dmfa(&self) -> bool {
        self.is_dmfa_internal()
    }

    fn is_rmfa(&self) -> bool {
        self.is_rmfa_internal()
    }

    fn is_mfc(&self) -> bool {
        self.is_mfc_internal()
    }

    fn is_dmfc(&self) -> bool {
        self.is_dmfc_internal()
    }

    fn is_drpc(&self) -> bool {
        self.is_drpc_internal()
    }

    fn is_rpc(&self) -> bool {
        self.is_rpc_internal()
    }
}
