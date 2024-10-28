use crate::rule_model::components::rule::Rule;
use crate::static_checks::positions::Positions;

pub struct RuleSet(Vec<Rule>);

impl RuleSet {
    pub fn affected_positions(&self) -> Positions {
        let mut affected_positions: Positions = self.initial_affected_positions();
        let mut new_in_last_iteration: Positions = affected_positions.clone();
        while !new_in_last_iteration.is_empty() {
            let mut new_found_affected_positions: Positions =
                new_in_last_iteration.conclude_affected_positions();
            new_found_affected_positions.subtract(&affected_positions);
            affected_positions.union(&new_found_affected_positions);
            new_in_last_iteration = new_found_affected_positions;
        }
        affected_positions
    }

    fn initial_affected_positions(&self) -> Positions {
        let mut initial_affected_positions: Positions = Positions::new();
        for rule in self.iter() {
            initial_affected_positions.union(&rule.initial_affected_positions());
        }
        initial_affected_positions
    }

    pub fn iter(&self) -> std::slice::Iter<Rule> {
        self.0.iter()
    }

    pub fn marking(&self) -> Positions {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
}
