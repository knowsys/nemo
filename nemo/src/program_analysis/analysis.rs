use std::collections::{HashMap, HashSet};

use nemo_physical::management::execution_plan::ColumnOrder;

use crate::{
    error::Error,
    model::chase_model::{ChaseProgram, ChaseRule},
    model::{
        chase_model::{ChaseAtom, PrimitiveAtom, VariableAtom},
        Constraint, Identifier, PrimitiveTerm, Term, Variable,
    },
};

use super::variable_order::{
    build_preferable_variable_orders, BuilderResultVariants, VariableOrder,
};

use thiserror::Error;

/// Contains useful information for a (existential) rule
#[derive(Debug, Clone)]
pub struct RuleAnalysis {
    /// Whether it uses an existential variable in its head.
    pub is_existential: bool,
    /// Whether an atom in the head also occurs in the body.
    pub is_recursive: bool,
    /// Whether the rule has positive constraints that need to be applied.
    pub has_positive_constraints: bool,
    /// Whether the rule has at least one aggregate term in the head.
    pub has_aggregates: bool,

    /// Predicates appearing in the positive part of the body.
    pub positive_body_predicates: HashSet<Identifier>,
    /// Predicates appearing in the negative part of the body.
    pub negative_body_predicates: HashSet<Identifier>,
    /// Predicates appearing in the head.
    pub head_predicates: HashSet<Identifier>,

    /// Variables occurring in the positive part of the body.
    pub positive_body_variables: HashSet<Variable>,
    /// Variables occurring in the positive part of the body.
    pub negative_body_variables: HashSet<Variable>,
    /// Variables occurring in the head.
    pub head_variables: HashSet<Variable>,
    /// Number of existential variables.
    pub num_existential: usize,

    /// Rule that represents the calculation of the satisfied matches for an existential rule.
    pub existential_aux_rule: ChaseRule,
    /// The associated variable order for the join of the head atoms
    pub existential_aux_order: VariableOrder,

    /// Variable orders that are worth considering.
    pub promising_variable_orders: Vec<VariableOrder>,
}

/// Errors than can occur during rule analysis
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum RuleAnalysisError {
    /// Unsupported feature: Overloading of predicate names by arity/type
    #[error(
        "predicate \"{predicate}\" required to have conflicting arities {arity1} and {arity2}"
    )]
    UnsupportedFeaturePredicateOverloading {
        predicate: Identifier,
        arity1: usize,
        arity2: usize,
    },
    /// There is a predicate whose arity could not be determined  
    #[error("arity of predicate \"{predicate}\" could not be derived")]
    UnspecifiedPredicateArity { predicate: Identifier },
}

/// Return true if there is a predicate in the positive part of the rule that also appears in the head of the rule.
fn is_recursive(rule: &ChaseRule) -> bool {
    rule.head().iter().any(|h| {
        rule.positive_body()
            .iter()
            .any(|b| h.predicate() == b.predicate())
    })
}

fn count_distinct_existential_variables(rule: &ChaseRule) -> usize {
    let mut existentials = HashSet::<Variable>::new();

    for head_atom in rule.head() {
        for term in head_atom.terms() {
            if let PrimitiveTerm::Variable(Variable::Existential(id)) = term {
                existentials.insert(Variable::Existential(id.clone()));
            }
        }
    }

    existentials.len()
}

fn get_variables<Atom: ChaseAtom>(atoms: &[Atom]) -> HashSet<Variable> {
    let mut result = HashSet::new();
    for atom in atoms {
        for variable in atom.get_variables() {
            result.insert(variable);
        }
    }
    result
}

fn get_predicates<Atom: ChaseAtom>(atoms: &[Atom]) -> HashSet<Identifier> {
    atoms.iter().map(|a| a.predicate()).collect()
}

pub(super) fn get_fresh_rule_predicate(rule_index: usize) -> Identifier {
    Identifier(format!(
        "FRESH_HEAD_MATCHES_IDENTIFIER_FOR_RULE_{rule_index}"
    ))
}

fn construct_existential_aux_rule(
    rule_index: usize,
    head_atoms: Vec<PrimitiveAtom>,
    column_orders: &HashMap<Identifier, HashSet<ColumnOrder>>,
) -> (ChaseRule, VariableOrder) {
    let mut new_body = Vec::new();
    let mut constraints = Vec::new();

    let mut variable_index = 0;
    let mut generate_variable = move || {
        variable_index += 1;
        let name = format!("__GENERATED_HEAD_AUX_VARIABLE_{}", variable_index);
        Variable::Universal(Identifier(name))
    };

    let mut used_variables = HashSet::new();
    let mut aux_predicate_terms = Vec::new();
    for atom in &head_atoms {
        let mut new_terms = Vec::new();

        for term in atom.terms() {
            match term {
                PrimitiveTerm::Variable(variable) => {
                    if variable.is_universal() && used_variables.insert(variable.clone()) {
                        aux_predicate_terms.push(PrimitiveTerm::Variable(variable.clone()));
                    }

                    new_terms.push(variable.clone());
                }
                PrimitiveTerm::Constant(_) => {
                    let generated_variable =
                        Term::Primitive(PrimitiveTerm::Variable(generate_variable()));
                    let new_constraint =
                        Constraint::Equals(generated_variable, Term::Primitive(term.clone()));

                    constraints.push(new_constraint);
                }
            }
        }

        new_body.push(VariableAtom::new(atom.predicate(), new_terms));
    }

    let temp_rule = {
        let temp_head_identifier = get_fresh_rule_predicate(rule_index);

        let temp_head_atom = PrimitiveAtom::new(temp_head_identifier, aux_predicate_terms);
        ChaseRule::new(
            vec![temp_head_atom],
            vec![],
            vec![],
            new_body,
            constraints,
            vec![],
            vec![],
        )
    };

    let variable_order = build_preferable_variable_orders(
        &ChaseProgram::builder().rule(temp_rule.clone()).build(),
        Some(column_orders.clone()),
    )
    .all_variable_orders
    .pop()
    .and_then(|mut v| v.pop())
    .expect("This functions provides at least one variable order");

    (temp_rule, variable_order)
}

fn analyze_rule(
    rule: &ChaseRule,
    promising_variable_orders: Vec<VariableOrder>,
    promising_column_orders: &[HashMap<Identifier, HashSet<ColumnOrder>>],
    rule_index: usize,
) -> RuleAnalysis {
    let num_existential = count_distinct_existential_variables(rule);

    let (existential_aux_rule, existential_aux_order) = if num_existential > 0 {
        // TODO: We only consider the first variable order
        construct_existential_aux_rule(rule_index, rule.head().clone(), &promising_column_orders[0])
    } else {
        (ChaseRule::default(), VariableOrder::new())
    };

    RuleAnalysis {
        is_existential: num_existential > 0,
        is_recursive: is_recursive(rule),
        has_positive_constraints: !rule.positive_constraints().is_empty(),
        has_aggregates: !rule.aggregates().is_empty(),
        positive_body_predicates: get_predicates(rule.positive_body()),
        negative_body_predicates: get_predicates(rule.negative_body()),
        head_predicates: get_predicates(rule.head()),
        positive_body_variables: get_variables(rule.positive_body()),
        negative_body_variables: get_variables(rule.negative_body()),
        head_variables: get_variables(rule.head()),
        num_existential,
        existential_aux_rule,
        existential_aux_order,
        promising_variable_orders,
    }
}

/// Contains useful information about the
#[derive(Debug)]
pub struct ProgramAnalysis {
    /// Analysis result for each rule.
    pub rule_analysis: Vec<RuleAnalysis>,
    /// Set of all the predicates that are derived in the chase.
    pub derived_predicates: HashSet<Identifier>,
    /// Set of all predicates and their arity.
    pub all_predicates: HashMap<Identifier, usize>,
}

impl ChaseProgram {
    /// Collect all predicates that appear in a head atom into a [`HashSet`]
    fn get_head_predicates(&self) -> HashSet<Identifier> {
        let mut result = HashSet::<Identifier>::new();

        for rule in self.rules() {
            for head_atom in rule.head() {
                result.insert(head_atom.predicate());
            }
        }

        result
    }

    /// Collect all predicates in the program, and determine their arity.
    /// An error is returned if arities required for a predicate are not unique.
    pub(super) fn get_all_predicates(
        &self,
    ) -> Result<HashMap<Identifier, usize>, RuleAnalysisError> {
        let mut result = HashMap::<Identifier, usize>::new();
        let mut missing = HashSet::<Identifier>::new();

        fn add_arity(
            predicate: Identifier,
            arity: usize,
            arities: &mut HashMap<Identifier, usize>,
            missing: &mut HashSet<Identifier>,
        ) -> Result<(), RuleAnalysisError> {
            if let Some(current) = arities.get(&predicate) {
                if *current != arity {
                    return Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading {
                        predicate: predicate,
                        arity1: *current,
                        arity2: arity,
                    });
                }
            } else {
                missing.remove(&predicate);
                arities.insert(predicate, arity);
            }
            Ok(())
        }
        fn add_missing(
            predicate: Identifier,
            arities: &HashMap<Identifier, usize>,
            missing: &mut HashSet<Identifier>,
        ) {
            if let None = arities.get(&predicate) {
                missing.insert(predicate);
            }
        }

        // Predicates in import statements
        for (pred, handler) in self.imports() {
            if let Some(arity) = handler.predicate_arity() {
                add_arity(pred.clone(), arity, &mut result, &mut missing)?;
            } else {
                add_missing(pred.clone(), &result, &mut missing);
            }
        }

        // Predicates in export statements
        for (pred, handler) in self.exports() {
            if let Some(arity) = handler.predicate_arity() {
                add_arity(pred.clone(), arity, &mut result, &mut missing)?;
            } else {
                add_missing(pred.clone(), &result, &mut missing);
            }
        }

        // Predicates in rules
        for rule in self.rules() {
            for atom in rule.head() {
                add_arity(
                    atom.predicate(),
                    atom.terms().len(),
                    &mut result,
                    &mut missing,
                )?;
            }
            for atom in rule.all_body() {
                add_arity(
                    atom.predicate(),
                    atom.terms().len(),
                    &mut result,
                    &mut missing,
                )?;
            }
        }

        // Predicates in facts
        for fact in self.facts() {
            add_arity(
                fact.predicate(),
                fact.terms().len(),
                &mut result,
                &mut missing,
            )?;
        }

        // Additional predicates for existential rules
        for (rule_index, rule) in self.rules().iter().enumerate() {
            let is_existential = count_distinct_existential_variables(rule) > 0;
            if !is_existential {
                continue;
            }

            let body_variables = get_variables(rule.positive_body());
            let head_variables = get_variables(rule.head());

            let predicate = get_fresh_rule_predicate(rule_index);
            let arity = head_variables.difference(&body_variables).count();

            add_arity(predicate, arity, &mut result, &mut missing)?;
        }

        if !missing.is_empty() {
            return Err(RuleAnalysisError::UnspecifiedPredicateArity {
                predicate: missing.iter().next().expect("not empty").clone(),
            });
        }

        Ok(result)
    }

    /// Check if the program contains rules with unsupported features.
    /// This is always performed as part of [ChaseProgram::analyze].
    fn check_for_unsupported_features(&self) -> Result<(), RuleAnalysisError> {
        // Currently no interesting checks here. Uniqueness of arities is already checked in the analysis phase.
        // In general, should we maybe just do all checks in the analysis?
        Ok(())
    }

    /// Analyze the program and return a struct containing the results.
    /// This method also checks for structural problems that are not detected
    /// in parsing.
    pub fn analyze(&self) -> Result<ProgramAnalysis, Error> {
        let BuilderResultVariants {
            all_variable_orders,
            all_column_orders,
        } = build_preferable_variable_orders(self, None);

        let all_predicates = self.get_all_predicates()?;
        let derived_predicates = self.get_head_predicates();

        self.check_for_unsupported_features()?;

        let rule_analysis: Vec<RuleAnalysis> = self
            .rules()
            .iter()
            .enumerate()
            .map(|(idx, rule)| {
                analyze_rule(
                    rule,
                    all_variable_orders[idx].clone(),
                    &all_column_orders,
                    idx,
                )
            })
            .collect();

        Ok(ProgramAnalysis {
            rule_analysis,
            derived_predicates,
            all_predicates,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        error::Error, io::parser::parse_program, model::chase_model::ChaseProgram,
        program_analysis::analysis::RuleAnalysisError,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn no_arity_overloading() {
        let program = ChaseProgram::try_from(
            parse_program(
                r#"
                           @import q :- turtle{resource="dummy.nt"} .
                           p(?x, ?y) :- q(?x), q(?y) .
                         "#,
            )
            .unwrap(),
        )
        .unwrap();

        assert!(matches!(
            program.analyze(),
            Err(Error::RuleAnalysisError(
                RuleAnalysisError::UnsupportedFeaturePredicateOverloading { .. }
            ))
        ));

        let program =
            ChaseProgram::try_from(parse_program(r#"q(?x, ?y) :- q(?x), q(?y) ."#).unwrap())
                .unwrap();

        assert!(matches!(
            program.analyze(),
            Err(Error::RuleAnalysisError(
                RuleAnalysisError::UnsupportedFeaturePredicateOverloading { .. }
            ))
        ));

        let program = ChaseProgram::try_from(
            parse_program(
                r#"
                           p(?x, ?y) :- q(?x), q(?y) .
                           q(23, 42) .
                         "#,
            )
            .unwrap(),
        )
        .unwrap();

        assert!(matches!(
            program.analyze(),
            Err(Error::RuleAnalysisError(
                RuleAnalysisError::UnsupportedFeaturePredicateOverloading { .. }
            ))
        ));
    }
}
