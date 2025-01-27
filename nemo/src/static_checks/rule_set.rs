use crate::rule_model::components::term::primitive::variable::VariableName;
use crate::rule_model::components::{
    atom::Atom, rule::Rule, term::primitive::variable::Variable, IterableVariables,
};
use crate::static_checks::collection_traits::{Disjoint, InsertAll, Superset};
use crate::static_checks::positions::{
    AffectedPositionsBuilder, AttackedPositionsBuilder, AttackingType, ExtendedPositions,
    FromPositions, MarkedPositionsBuilder, MarkingType, Positions, PositionsByVariables,
};

use std::collections::{HashMap, HashSet};

type RuleIndex = usize;

pub type RuleSet = Vec<Rule>;

type RuleIndexVariable<'a> = (RuleIndex, &'a Variable);

type RuleIndexVariables<'a> = HashSet<RuleIndexVariable<'a>>;

pub type Variables<'a> = HashSet<&'a Variable>;

pub type VariablePair<'a> = [&'a Variable; 2];

pub type VariablePairs<'a> = HashSet<VariablePair<'a>>;

pub trait Affected {
    fn is_affected(&self, rule: &Rule, affected_positions: &Positions) -> bool;
}

impl Affected for Variable {
    fn is_affected(&self, rule: &Rule, affected_positions: &Positions) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body(rule);
        affected_positions.is_superset(&positions_of_variable_in_body)
    }
}

pub trait AffectedVariables<'a> {
    fn affected_frontier_variables(&self, affected_positions: &Positions) -> Variables;
    fn affected_universal_variables(&self, affected_positions: &Positions) -> Variables;
    fn affected_variables(
        &self,
        variables: Variables<'a>,
        affected_positions: &Positions,
    ) -> Variables<'a>;
}

impl<'a> AffectedVariables<'a> for Rule {
    fn affected_frontier_variables(&self, affected_positions: &Positions) -> Variables {
        self.affected_variables(self.frontier_variables(), affected_positions)
    }

    fn affected_universal_variables(&self, affected_positions: &Positions) -> Variables {
        self.affected_variables(self.positive_variables(), affected_positions)
    }

    fn affected_variables(
        &self,
        variables: Variables<'a>,
        affected_positions: &Positions,
    ) -> Variables<'a> {
        variables
            .iter()
            .filter(|var| var.is_affected(self, affected_positions))
            .copied()
            .collect()
    }
}

pub trait AtomPositionsAppearance {
    fn appears_at_some_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool;
    fn appears_at_some_positions_in_atoms(&self, positions: &Positions, atoms: &[&Atom]) -> bool;
    fn appears_only_at_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool;
    fn appears_only_at_positions_in_atoms(&self, positions: &Positions, atoms: &[&Atom]) -> bool;
}

impl AtomPositionsAppearance for Variable {
    fn appears_at_some_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool {
        let positions_in_atom: Positions = self.positions_in_atom(atom);
        !positions_in_atom.is_disjoint(positions)
    }

    fn appears_at_some_positions_in_atoms(&self, positions: &Positions, atoms: &[&Atom]) -> bool {
        atoms
            .iter()
            .any(|atom| self.appears_at_some_positions_in_atom(positions, atom))
    }

    fn appears_only_at_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool {
        let positions_in_atom: Positions = self.positions_in_atom(atom);
        positions.is_superset(&positions_in_atom)
    }

    fn appears_only_at_positions_in_atoms(&self, positions: &Positions, atoms: &[&Atom]) -> bool {
        atoms
            .iter()
            .all(|atom| self.appears_only_at_positions_in_atom(positions, atom))
    }
}

pub trait AtomPositions<'a> {
    fn positions_in_atom(&self, atom: &'a Atom) -> Positions<'a>;
    fn positions_in_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a>;
}

impl<'a> AtomPositions<'a> for Variable {
    fn positions_in_atom(&self, atom: &'a Atom) -> Positions<'a> {
        atom.variables()
            .enumerate()
            .filter(|(_, var)| self == *var)
            .fold(Positions::new(), |mut positions_in_atom, (index, _)| {
                positions_in_atom
                    .entry(atom.predicate_ref())
                    .and_modify(|indeces| {
                        indeces.insert(index);
                    })
                    .or_insert(HashSet::from([index]));
                positions_in_atom
            })
    }

    fn positions_in_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a> {
        atoms
            .iter()
            .fold(Positions::new(), |positions_in_atoms, atom| {
                let positions_in_atom: Positions = self.positions_in_atom(atom);
                positions_in_atoms.insert_all_take_ret(positions_in_atom)
            })
    }
}

pub trait AtomRefs {
    fn variables_refs(&self) -> Variables;
}

impl AtomRefs for Atom {
    fn variables_refs(&self) -> Variables {
        self.variables().collect()
    }
}

pub trait Attacked {
    fn is_attacked(&self, rule: &Rule, attacked_pos_by_vars: &PositionsByVariables) -> bool;
    fn is_attacked_by_positions_in_rule(
        &self,
        rule: &Rule,
        attacked_pos_of_var: &Positions,
    ) -> bool;
    fn is_attacked_by_variable(
        &self,
        attacking_var: &Variable,
        rule: &Rule,
        attacked_pos_by_vars: &PositionsByVariables,
    ) -> bool;
}

impl Attacked for Variable {
    fn is_attacked(&self, rule: &Rule, attacked_pos_by_vars: &PositionsByVariables) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body(rule);
        attacked_pos_by_vars
            .values()
            .any(|att_pos| att_pos.is_superset(&positions_of_variable_in_body))
    }

    fn is_attacked_by_positions_in_rule(
        &self,
        rule: &Rule,
        attacked_pos_of_var: &Positions,
    ) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body(rule);
        attacked_pos_of_var.is_superset(&positions_of_variable_in_body)
    }

    fn is_attacked_by_variable(
        &self,
        attacking_var: &Variable,
        rule: &Rule,
        attacked_pos_by_vars: &PositionsByVariables,
    ) -> bool {
        let attacked_pos_by_var: &Positions = attacked_pos_by_vars.get(attacking_var).unwrap();
        self.is_attacked_by_positions_in_rule(rule, attacked_pos_by_var)
    }
}

pub trait AttackedGlutVariables<'a>: AttackedVariables<'a> {
    fn attacked_frontier_glut_variables(
        &self,
        attacked_pos_by_cycle_vars: &PositionsByVariables,
    ) -> Variables;
    fn attacked_universal_glut_variables(
        &self,
        attacked_pos_by_cycle_vars: &PositionsByVariables,
    ) -> Variables;
}

impl AttackedGlutVariables<'_> for Rule {
    fn attacked_frontier_glut_variables(
        &self,
        attacked_pos_by_cycle_vars: &PositionsByVariables,
    ) -> Variables {
        let frontier_variables: Variables = self.frontier_variables();
        self.attacked_variables(frontier_variables, attacked_pos_by_cycle_vars)
    }

    fn attacked_universal_glut_variables(
        &self,
        attacked_pos_by_cycle_vars: &PositionsByVariables,
    ) -> Variables {
        let universal_variables: Variables = self.positive_variables();
        self.attacked_variables(universal_variables, attacked_pos_by_cycle_vars)
    }
}

pub trait AttackedVariables<'a> {
    fn attacked_frontier_variables(&self, attacked_pos_by_vars: &PositionsByVariables)
        -> Variables;
    fn attacked_universal_variables(
        &self,
        attacked_pos_by_vars: &PositionsByVariables,
    ) -> Variables;
    fn attacked_variables(
        &self,
        variables: Variables<'a>,
        attacked_pos_by_vars: &PositionsByVariables,
    ) -> Variables<'a>;
}

impl<'a> AttackedVariables<'a> for Rule {
    fn attacked_frontier_variables(
        &self,
        attacked_pos_by_vars: &PositionsByVariables,
    ) -> Variables {
        let frontier_variables: Variables = self.frontier_variables();
        self.attacked_variables(frontier_variables, attacked_pos_by_vars)
    }

    /// Returns the attacked universal variables of the rule.
    fn attacked_universal_variables(
        &self,
        attacked_pos_by_vars: &PositionsByVariables,
    ) -> Variables {
        let universal_variables: Variables = self.positive_variables();
        self.attacked_variables(universal_variables, attacked_pos_by_vars)
    }

    fn attacked_variables(
        &self,
        variables: Variables<'a>,
        attacked_pos_by_vars: &PositionsByVariables,
    ) -> Variables<'a> {
        variables
            .iter()
            .filter(|var| var.is_attacked(self, attacked_pos_by_vars))
            .copied()
            .collect()
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

impl<'a> AllPositivePositions<'a> for Rule {
    fn all_positive_extended_positions(&'a self) -> ExtendedPositions<'a> {
        let all_positive_positions: Positions = self.all_positive_positions();
        ExtendedPositions::from_positions(all_positive_positions)
    }

    fn all_positive_positions(&'a self) -> Positions<'a> {
        let all_positive_body_positions: Positions = self.all_positions_of_positive_body();
        let all_head_positions: Positions = self.all_positions_of_head();
        all_positive_body_positions.insert_all_take_ret(all_head_positions)
    }
}

trait AllPositivePositionsRulePrivate<'a> {
    fn all_positions_of_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a>;
    fn all_positions_of_head(&'a self) -> Positions<'a>;
    fn all_positions_of_positive_body(&'a self) -> Positions<'a>;
}

impl<'a> AllPositivePositionsRulePrivate<'a> for Rule {
    fn all_positions_of_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a> {
        atoms.iter().fold(Positions::new(), |all_pos, atom| {
            let positions: Positions =
                HashMap::from([(atom.predicate_ref(), (0..atom.len()).collect())]);
            all_pos.insert_all_take_ret(positions)
        })
    }

    fn all_positions_of_head(&'a self) -> Positions<'a> {
        let head_atoms: Vec<&Atom> = self.head_refs();
        self.all_positions_of_atoms(&head_atoms)
    }

    fn all_positions_of_positive_body(&'a self) -> Positions<'a> {
        let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
        self.all_positions_of_atoms(&positive_body_atoms)
    }
}

pub trait ExistentialRuleIndexVariables {
    fn existential_rule_index_variables(&self) -> RuleIndexVariables;
}

impl ExistentialRuleIndexVariables for RuleSet {
    fn existential_rule_index_variables(&self) -> RuleIndexVariables {
        self.iter()
            .enumerate()
            .fold(RuleIndexVariables::new(), |ex_vars, (index, rule)| {
                let ex_vars_of_rule: Variables = rule.existential_variables();
                let idx_ex_vars: RuleIndexVariables = ex_vars_of_rule
                    .into_iter()
                    .map(|var| (index, var))
                    .collect();
                ex_vars.union(&idx_ex_vars).copied().collect()
            })
    }
}

pub trait ExistentialVariables {
    fn existential_variables(&self) -> Variables;
}

impl ExistentialVariables for RuleSet {
    fn existential_variables(&self) -> Variables {
        self.iter().fold(Variables::new(), |ex_vars, rule| {
            let ex_vars_of_rule: Variables = rule.existential_variables();
            ex_vars.union(&ex_vars_of_rule).copied().collect()
        })
    }
}

impl ExistentialVariables for Rule {
    fn existential_variables(&self) -> Variables {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }
}

impl ExistentialVariables for Atom {
    fn existential_variables(&self) -> Variables {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }
}

pub trait ExistentialVariablesPositions {
    fn extended_positions_of_existential_variables(&self) -> ExtendedPositions;
    fn positions_of_existential_variables(&self) -> Positions;
}

impl ExistentialVariablesPositions for Rule {
    fn extended_positions_of_existential_variables(&self) -> ExtendedPositions {
        let pos_of_ex_vars: Positions = self.positions_of_existential_variables();
        ExtendedPositions::from_positions(pos_of_ex_vars)
    }

    fn positions_of_existential_variables(&self) -> Positions {
        self.existential_variables()
            .iter()
            .fold(Positions::new(), |pos_of_ex_vars, var| {
                let pos_of_var_in_head: Positions = var.positions_in_head(self);
                pos_of_ex_vars.insert_all_take_ret(pos_of_var_in_head)
            })
    }
}

pub trait FrontierVariables {
    fn frontier_variables(&self) -> Variables;
}

impl FrontierVariables for Rule {
    fn frontier_variables(&self) -> Variables {
        let positive_body_variables: Variables = self.positive_variables();
        let universal_head_variables: Variables = self.universal_head_variables();
        positive_body_variables
            .intersection(&universal_head_variables)
            .copied()
            .collect()
    }
}

trait FrontierVariablesPrivate {
    fn universal_head_variables(&self) -> Variables;
}

impl FrontierVariablesPrivate for Rule {
    fn universal_head_variables(&self) -> Variables {
        self.head().iter().fold(
            Variables::new(),
            |universal_head_variables: Variables, atom| {
                let un_vars_of_atom: Variables = atom.universal_variables();
                universal_head_variables.insert_all_take_ret(un_vars_of_atom)
            },
        )
    }
}

pub trait GuardedForVariables {
    fn is_guarded_for_variables(&self, variables: Variables) -> bool;
}

impl GuardedForVariables for Rule {
    fn is_guarded_for_variables(&self, variables: Variables) -> bool {
        if variables.is_empty() {
            return true;
        }
        self.body_positive_refs().iter().any(|atom| {
            let vars_of_atom: Variables = atom.variables_refs();
            vars_of_atom.is_superset(&variables)
        })
    }
}

pub trait JoinVariables {
    fn join_variables(&self) -> Variables;
}

impl JoinVariables for Rule {
    fn join_variables(&self) -> Variables {
        self.positive_variables()
            .iter()
            .filter(|var| var.is_join_variable(self))
            .copied()
            .collect()
    }
}

trait JoinVariablePrivate {
    fn is_join_variable(&self, rule: &Rule) -> bool;
}

impl JoinVariablePrivate for Variable {
    fn is_join_variable(&self, rule: &Rule) -> bool {
        let mut count: usize = 0;
        rule.positive_variables_as_vec().iter().any(|var| {
            if *var == self {
                count += 1;
            }
            if 2 == count {
                return true;
            }
            false
        })
    }
}

#[allow(dead_code)]
pub trait JoinVariablesPositions {
    fn extended_positions_of_join_variables(&self) -> ExtendedPositions;
    fn positions_of_join_variables(&self) -> Positions;
}

// impl JoinVariablesPositions for Rule {
//     fn extended_positions_of_join_variables(&self) -> ExtendedPositions {
//         let pos_of_join_vars: Positions = self.positions_of_join_variables();
//         ExtendedPositions::from_positions(pos_of_join_vars)
//     }
//
//     fn positions_of_join_variables(&self) -> Positions {
//         self.join_variables()
//             .iter()
//             .fold(Positions::new(), |pos_of_join_vars, var| {
//                 let pos_of_var_in_body: Positions = var.positions_in_positive_body(self);
//                 pos_of_join_vars.insert_all_take_ret(pos_of_var_in_body)
//             })
//     }
// }

pub trait FrontierVariablePairs {
    fn frontier_variable_pairs(&self) -> VariablePairs;
}

impl FrontierVariablePairs for Rule {
    fn frontier_variable_pairs(&self) -> VariablePairs {
        self.frontier_variables()
            .iter()
            .fold(VariablePairs::new(), |mut pairs, var1| {
                self.frontier_variables()
                    .iter()
                    .filter(|var2| var1 != *var2)
                    .for_each(|var2| {
                        pairs.insert([var1, var2]);
                    });
                pairs
            })
    }
}

pub trait RulePositions<'a> {
    fn extended_positions_in_head(&self, rule: &'a Rule) -> ExtendedPositions<'a>;
    fn extended_positions_in_positive_body(&self, rule: &'a Rule) -> ExtendedPositions<'a>;
    fn positions_in_head(&self, rule: &'a Rule) -> Positions<'a>;
    fn positions_in_positive_body(&self, rule: &'a Rule) -> Positions<'a>;
}

impl<'a> RulePositions<'a> for Variable {
    fn extended_positions_in_positive_body(&self, rule: &'a Rule) -> ExtendedPositions<'a> {
        let body_pos_of_var: Positions = self.positions_in_positive_body(rule);
        ExtendedPositions::from_positions(body_pos_of_var)
    }

    fn extended_positions_in_head(&self, rule: &'a Rule) -> ExtendedPositions<'a> {
        let head_pos_of_var: Positions = self.positions_in_head(rule);
        ExtendedPositions::from_positions(head_pos_of_var)
    }

    fn positions_in_positive_body(&self, rule: &'a Rule) -> Positions<'a> {
        let positive_body_atoms: Vec<&Atom> = rule.body_positive_refs();
        self.positions_in_atoms(&positive_body_atoms)
    }

    fn positions_in_head(&self, rule: &'a Rule) -> Positions<'a> {
        let head_atoms: Vec<&Atom> = rule.head_refs();
        self.positions_in_atoms(&head_atoms)
    }
}

pub trait RuleRefs {
    fn body_positive_refs(&self) -> Vec<&Atom>;
    fn head_refs(&self) -> Vec<&Atom>;
}

impl RuleRefs for Rule {
    fn body_positive_refs(&self) -> Vec<&Atom> {
        self.body_positive().collect()
    }

    fn head_refs(&self) -> Vec<&Atom> {
        self.head().iter().collect()
    }
}

pub trait SpecialPositionsConstructor {
    fn affected_positions(&self) -> Positions;
    fn attacked_positions_by_cycle_variables(&self) -> PositionsByVariables;
    fn attacked_positions_by_existential_variables(&self) -> PositionsByVariables;
    fn build_and_check_marking(&self) -> Option<Positions>;
    fn build_and_check_weakly_marking(&self) -> Option<Positions>;
}

impl SpecialPositionsConstructor for RuleSet {
    fn affected_positions(&self) -> Positions {
        Positions::build_positions(self)
    }

    fn attacked_positions_by_cycle_variables(&self) -> PositionsByVariables {
        PositionsByVariables::build_positions(AttackingType::Cycle, self)
    }

    fn attacked_positions_by_existential_variables(&self) -> PositionsByVariables {
        PositionsByVariables::build_positions(AttackingType::Existential, self)
    }

    fn build_and_check_marking(&self) -> Option<Positions> {
        Option::<Positions>::build_positions(MarkingType::Common, self)
    }

    fn build_and_check_weakly_marking(&self) -> Option<Positions> {
        Option::<Positions>::build_positions(MarkingType::Weakly, self)
    }
}

pub trait UniversalVariables {
    fn universal_variables(&self) -> Variables;
}

impl UniversalVariables for Atom {
    fn universal_variables(&self) -> Variables {
        self.variables().filter(|var| var.is_universal()).collect()
    }
}
