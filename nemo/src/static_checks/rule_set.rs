use crate::rule_model::components::{rule::Rule, term::primitive::variable::Variable};
use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::positions::{
    ExtendedPositions, FromPositions, Positions, PositionsByVariables,
};
use crate::static_checks::rule_set::rule_set_internal::SpecialPositionsInternal;

use std::collections::HashSet;

mod rule_set_internal;

pub type RuleSet = Vec<Rule>;

pub trait SpecialPositions<'a>: SpecialPositionsInternal<'a> {
    fn affected_positions(&'a self) -> Positions<'a>;
    fn attacked_positions_by_cycle_variables(&'a self) -> PositionsByVariables<'a, 'a>;
    fn attacked_positions_by_existential_variables(&'a self) -> PositionsByVariables<'a, 'a>;
    fn build_and_check_marking(&'a self) -> Option<Positions<'a>>;
}

impl<'a> SpecialPositions<'a> for RuleSet {
    fn affected_positions(&'a self) -> Positions<'a> {
        self.affected_positions_internal()
    }

    fn attacked_positions_by_cycle_variables(&'a self) -> PositionsByVariables<'a, 'a> {
        self.attacked_positions_by_cycle_variables_internal()
    }

    fn attacked_positions_by_existential_variables(&'a self) -> PositionsByVariables<'a, 'a> {
        self.attacked_positions_by_existential_variables_internal()
    }

    fn build_and_check_marking(&'a self) -> Option<Positions<'a>> {
        self.build_and_check_marking_internal()
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
