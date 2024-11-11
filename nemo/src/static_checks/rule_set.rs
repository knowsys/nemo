use crate::rule_model::components::{rule::Rule, term::primitive::variable::Variable};
use crate::static_checks::positions::Positions;
use std::collections::HashMap;
pub struct RuleSet(Vec<Rule>);

impl RuleSet {
    pub fn affected_positions(&self) -> Positions {
        let mut affected_positions: Positions = self.initial_affected_positions();
        let mut new_found_affected_positions: Positions = affected_positions.clone();
        while !new_found_affected_positions.is_empty() {
            new_found_affected_positions =
                self.new_affected_positions(&new_found_affected_positions, &affected_positions);
            affected_positions = affected_positions.union(&new_found_affected_positions);
        }
        affected_positions
    }

    pub fn all_positive_positions(&self) -> Positions {
        self.iter().fold(Positions::new(), |all_pos, rule| {
            all_pos.union(&rule.all_positive_positions())
        })
    }

    fn attacked_positions(&self, variable: &Variable, rule: &Rule) -> Positions {
        let mut attacked_positions: Positions = self.initial_attacked_positions(variable, rule);
        let mut new_found_attacked_positions: Positions = attacked_positions.clone();
        while !new_found_attacked_positions.is_empty() {
            new_found_attacked_positions = self.new_attacked_positions(&attacked_positions);
            attacked_positions = attacked_positions.union(&new_found_attacked_positions);
        }
        attacked_positions
    }

    pub fn attacked_positions_by_variables(&self) -> HashMap<&Variable, Positions> {
        self.iter()
            .flat_map(|rule| {
                rule.existential_variables()
                    .iter()
                    .map(|var| (*var, self.attacked_positions(var, rule)))
                    .collect::<HashMap<&Variable, Positions>>()
            })
            .collect()
    }

    pub fn build_and_check_marking(&self) -> Option<Positions> {
        let mut marking: Positions = self.initial_marked_positions();
        let mut new_found_marked_positions: Positions = marking.clone();
        while !new_found_marked_positions.is_empty() {
            new_found_marked_positions =
                self.new_marked_positions(&new_found_marked_positions, &marking)?;
            marking = marking.union(&new_found_marked_positions);
        }
        Some(marking)
    }

    fn conclude_affected_positions(&self, last_iteration_positions: &Positions) -> Positions {
        self.iter()
            .fold(Positions::new(), |new_found_affected_positions, rule| {
                let new_aff_pos_in_rule: Positions =
                    rule.conclude_affected_positions(last_iteration_positions);
                new_found_affected_positions.union(&new_aff_pos_in_rule)
            })
    }

    fn conclude_attacked_positions(&self, currently_attacked_positions: &Positions) -> Positions {
        self.iter()
            .fold(Positions::new(), |new_found_attacked_positions, rule| {
                let new_att_pos_in_rule: Positions =
                    rule.conclude_attacked_positions(currently_attacked_positions);
                new_found_attacked_positions.union(&new_att_pos_in_rule)
            })
    }

    fn conclude_marked_positions(&self, last_iteration_positions: &Positions) -> Option<Positions> {
        self.iter()
            .try_fold(Positions::new(), |new_found_marked_positions, rule| {
                let new_mar_pos_in_rule: Positions =
                    rule.conclude_marked_positions(last_iteration_positions)?;
                Some(new_found_marked_positions.union(&new_mar_pos_in_rule))
            })
    }

    fn initial_marked_positions(&self) -> Positions {
        self.iter()
            .fold(Positions::new(), |initial_marked_positions, rule| {
                initial_marked_positions.union(&rule.positions_of_join_variables())
            })
    }

    fn initial_affected_positions(&self) -> Positions {
        self.iter()
            .fold(Positions::new(), |initial_affected_positions, rule| {
                initial_affected_positions.union(&rule.positions_of_existential_variables())
            })
    }

    fn initial_attacked_positions(&self, variable: &Variable, rule: &Rule) -> Positions {
        variable.get_positions_in_atoms(rule.head())
    }

    pub fn iter(&self) -> std::slice::Iter<Rule> {
        self.0.iter()
    }

    fn new_affected_positions(
        &self,
        last_iteration_positions: &Positions,
        currently_affected_positions: &Positions,
    ) -> Positions {
        let new_found_affected_positions: Positions =
            self.conclude_affected_positions(last_iteration_positions);
        new_found_affected_positions.difference(currently_affected_positions)
    }

    fn new_attacked_positions(&self, currently_attacked_postions: &Positions) -> Positions {
        let new_found_attacked_positions: Positions =
            self.conclude_attacked_positions(currently_attacked_postions);
        new_found_attacked_positions.difference(currently_attacked_postions)
    }

    fn new_marked_positions(
        &self,
        last_iteration_positions: &Positions,
        current_marking: &Positions,
    ) -> Option<Positions> {
        let new_found_marked_positions: Positions =
            self.conclude_marked_positions(last_iteration_positions)?;
        Some(new_found_marked_positions.difference(current_marking))
    }
}
