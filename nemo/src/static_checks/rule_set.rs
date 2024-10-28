use crate::rule_model::components::rule::Rule;
use crate::rule_model::components::IterableVariables;
use crate::static_checks::positions::Positions;

pub struct RuleSet(Vec<Rule>);

impl RuleSet {
    pub fn affected_positions(&self) -> Positions {
        let mut affected_positions: Positions = self.initial_affected_positions();
        let mut new_found_affected_positions: Positions = affected_positions.clone();
        while !new_found_affected_positions.is_empty() {
            new_found_affected_positions =
                self.new_affected_positions(new_found_affected_positions, &affected_positions);
            affected_positions = affected_positions.union(&new_found_affected_positions);
        }
        affected_positions
    }

    // TODO: SHORTEN FUNCTION
    pub fn conclude_affected_positions(&self, last_iteration_positions: Positions) -> Positions {
        self.iter()
            .fold(Positions::new(), |new_found_affected_positions, rule| {
                let new_aff_pos_in_rule: Positions = rule.positive_variables().iter().fold(
                    Positions::new(),
                    |mut new_aff_pos_in_rule, var| {
                        if var.appears_at_positions_in_atoms(
                            &last_iteration_positions,
                            rule.body_positive().collect(),
                        ) {
                            new_aff_pos_in_rule =
                                new_aff_pos_in_rule.union(&var.get_positions_in_atoms(rule.head()));
                        }
                        new_aff_pos_in_rule
                    },
                );
                new_found_affected_positions.union(&new_aff_pos_in_rule)
            })
    }

    fn initial_affected_positions(&self) -> Positions {
        self.iter()
            .fold(Positions::new(), |initial_affected_positions, rule| {
                initial_affected_positions.union(&rule.initial_affected_positions())
            })
    }

    pub fn iter(&self) -> std::slice::Iter<Rule> {
        self.0.iter()
    }

    pub fn marking(&self) -> Positions {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    pub fn new_affected_positions(
        &self,
        last_iteration_positions: Positions,
        currently_affected_positions: &Positions,
    ) -> Positions {
        let new_found_affected_positions =
            self.conclude_affected_positions(last_iteration_positions);
        new_found_affected_positions.difference(currently_affected_positions)
    }
}
