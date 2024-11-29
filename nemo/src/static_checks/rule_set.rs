use crate::rule_model::components::{rule::Rule, term::primitive::variable::Variable};
use crate::static_checks::{acyclicity_graphs::ExtendedPositions, positions::Positions};
use core::hash;
use std::collections::{HashMap, HashSet};

pub struct RuleSet(Vec<Rule>);

impl<'a> RuleSet {
    // TODO: SELF SHOULD NOT HAVE LIFETIME 'A
    pub fn affected_positions(&'a self) -> Positions<'a> {
        let mut affected_positions: Positions = self.initial_affected_positions();
        let mut new_found_affected_positions: Positions = affected_positions.clone();
        while !new_found_affected_positions.is_empty() {
            new_found_affected_positions =
                self.new_affected_positions(new_found_affected_positions, &affected_positions);
            affected_positions = affected_positions.union(new_found_affected_positions.clone());
        }
        affected_positions
    }

    pub fn all_positive_extended_positions(&self) -> ExtendedPositions {
        let all_positive_positions: Positions = self.all_positive_positions();
        ExtendedPositions::from(all_positive_positions)
    }

    fn all_positive_positions(&self) -> Positions {
        self.iter().fold(Positions::new(), |all_pos, rule| {
            let all_pos_of_rule: Positions = rule.all_positive_positions();
            all_pos.union(all_pos_of_rule)
        })
    }

    // TODO: SELF AND RULE SHOULD NOT HAVE LIFETIME 'A
    fn attacked_positions_by_var_in_rule(
        &'a self,
        variable: &Variable,
        rule: &'a Rule,
    ) -> Positions<'a> {
        let mut attacked_positions: Positions = self.initial_attacked_positions(variable, rule);
        let mut new_found_attacked_positions: Positions = attacked_positions.clone();
        while !new_found_attacked_positions.is_empty() {
            new_found_attacked_positions = self.new_attacked_positions(&attacked_positions);
            attacked_positions = attacked_positions.union(new_found_attacked_positions.clone());
        }
        attacked_positions
    }

    pub fn attacked_positions_by_variables(&self) -> HashMap<&Variable, Positions> {
        self.iter()
            .flat_map(|rule| {
                rule.existential_variables()
                    .iter()
                    .map(|var| (*var, self.attacked_positions_by_var_in_rule(var, rule)))
                    .collect::<HashMap<&Variable, Positions>>()
            })
            .collect()
    }

    pub fn build_and_check_marking(&self) -> Option<Positions> {
        let mut marking: Positions = self.initial_marked_positions();
        let mut new_found_marked_positions: Positions = marking.clone();
        while !new_found_marked_positions.is_empty() {
            new_found_marked_positions =
                self.new_marked_positions(new_found_marked_positions, &marking)?;
            marking = marking.union(new_found_marked_positions.clone());
        }
        Some(marking)
    }

    fn conclude_affected_positions(&self, last_iteration_positions: Positions) -> Positions {
        self.iter()
            .fold(Positions::new(), |new_found_affected_positions, rule| {
                let new_aff_pos_in_rule: Positions =
                    rule.conclude_affected_positions(&last_iteration_positions);
                new_found_affected_positions.union(new_aff_pos_in_rule)
            })
    }

    fn conclude_attacked_positions(&self, currently_attacked_positions: &Positions) -> Positions {
        self.iter()
            .fold(Positions::new(), |new_found_attacked_positions, rule| {
                let new_att_pos_in_rule: Positions =
                    rule.conclude_attacked_positions(currently_attacked_positions);
                new_found_attacked_positions.union(new_att_pos_in_rule)
            })
    }

    fn conclude_marked_positions(&self, last_iteration_positions: Positions) -> Option<Positions> {
        self.iter()
            .try_fold(Positions::new(), |new_found_marked_positions, rule| {
                let new_mar_pos_in_rule: Positions =
                    rule.conclude_marked_positions(&last_iteration_positions)?;
                Some(new_found_marked_positions.union(new_mar_pos_in_rule))
            })
    }

    pub fn existential_variables(&self) -> HashSet<&Variable> {
        self.iter()
            .fold(HashSet::<&Variable>::new(), |ex_vars, rule| {
                let ex_vars_of_rule: HashSet<&Variable> = rule.existential_variables();
                ex_vars.union(&ex_vars_of_rule).copied().collect()
            })
    }

    fn initial_marked_positions(&self) -> Positions {
        self.iter()
            .fold(Positions::new(), |initial_marked_positions, rule| {
                let pos_of_join_vars: Positions = rule.positions_of_join_variables();
                initial_marked_positions.union(pos_of_join_vars)
            })
    }

    fn initial_affected_positions(&self) -> Positions {
        self.iter()
            .fold(Positions::new(), |initial_affected_positions, rule| {
                let pos_of_ex_vars: Positions = rule.positions_of_existential_variables();
                initial_affected_positions.union(pos_of_ex_vars)
            })
    }

    fn initial_attacked_positions(&self, variable: &Variable, rule: &'a Rule) -> Positions<'a> {
        variable.get_positions_in_head(rule)
    }

    pub fn iter(&self) -> std::slice::Iter<Rule> {
        self.0.iter()
    }

    // TODO: SELF SHOULD NOT HAVE LIFETIME 'A
    fn new_affected_positions(
        &'a self,
        last_iteration_positions: Positions<'a>,
        currently_affected_positions: &Positions<'a>,
    ) -> Positions<'a> {
        let new_found_affected_positions: Positions<'a> =
            self.conclude_affected_positions(last_iteration_positions);
        new_found_affected_positions.difference(currently_affected_positions)
    }

    // TODO: SELF SHOULD NOT HAVE LIFETIME 'A
    fn new_attacked_positions(
        &'a self,
        currently_attacked_postions: &Positions<'a>,
    ) -> Positions<'a> {
        let new_found_attacked_positions: Positions =
            self.conclude_attacked_positions(currently_attacked_postions);
        new_found_attacked_positions.difference(currently_attacked_postions)
    }

    // TODO: SELF SHOULD NOT HAVE LIFETIME 'A
    fn new_marked_positions(
        &'a self,
        last_iteration_positions: Positions,
        current_marking: &Positions<'a>,
    ) -> Option<Positions<'a>> {
        let new_found_marked_positions: Positions =
            self.conclude_marked_positions(last_iteration_positions)?;
        Some(new_found_marked_positions.difference(current_marking))
    }
}
