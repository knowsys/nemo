use crate::rule_model::components::{rule::Rule, term::primitive::variable::Variable};
use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::positions::{
    AffectedPositions, AttackedPositions, AttackedPositionsBuilder, AttackingVariables,
    ExtendedPositions, FromPositions, MarkedPositions, Positions, PositionsByVariables,
    SpecialPositionsBuilder,
};

use std::collections::HashSet;

pub type RuleSet = Vec<Rule>;

pub trait SpecialPositionsConstructor {
    fn affected_positions(&self) -> AffectedPositions;
    fn attacked_positions_by_cycle_variables(&self) -> AttackedPositions;
    fn attacked_positions_by_existential_variables(&self) -> AttackedPositions;
    fn build_and_check_marking(&self) -> MarkedPositions;
}

impl SpecialPositionsConstructor for RuleSet {
    fn affected_positions(&self) -> AffectedPositions {
        AffectedPositions::build_positions(self)
    }

    fn attacked_positions_by_cycle_variables(&self) -> AttackedPositions {
        AttackedPositions::build_positions(AttackingVariables::Cycle, self)
    }

    fn attacked_positions_by_existential_variables(&self) -> AttackedPositions {
        AttackedPositions::build_positions(AttackingVariables::Existential, self)
    }

    fn build_and_check_marking(&self) -> MarkedPositions {
        MarkedPositions::build_positions(self)
    }
}

pub trait ExistentialVariables {
    fn existential_variables(&self) -> HashSet<&Variable>;
}

impl ExistentialVariables for RuleSet {
    fn existential_variables(&self) -> HashSet<&Variable> {
        self.iter()
            .fold(HashSet::<&Variable>::new(), |ex_vars, rule| {
                let ex_vars_of_rule: HashSet<&Variable> = rule.existential_variables();
                ex_vars.union(&ex_vars_of_rule).copied().collect()
            })
    }
}

pub trait AllPositivePositions<'a> {
    fn all_positive_extended_positions(&'a self) -> ExtendedPositions<'a>;
    fn all_positive_positions(&'a self) -> Positions<'a>;
}

impl<'a> AllPositivePositions<'a> for RuleSet {
    fn all_positive_extended_positions(&'a self) -> ExtendedPositions<'a> {
        let all_positive_positions: Positions = self.all_positive_positions();
        ExtendedPositions::from_positions(all_positive_positions)
    }

    fn all_positive_positions(&'a self) -> Positions<'a> {
        self.iter().fold(Positions::new(), |all_pos, rule| {
            let all_pos_of_rule: Positions = rule.all_positive_positions();
            all_pos.insert_all_take_ret(all_pos_of_rule)
        })
    }
}
