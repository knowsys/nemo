use std::collections::{HashMap, HashSet};

use nemo_physical::management::database::ColumnOrder;

use crate::{
    error::Error,
    model::chase_model::{ChaseProgram, ChaseRule},
    model::{
        chase_model::{ChaseAtom, PrimitiveAtom, VariableAtom},
        Constraint, DataSource, Identifier, PrimitiveTerm, PrimitiveType, Term, Variable,
    },
};

use super::{
    type_inference::infer_types,
    variable_order::{build_preferable_variable_orders, BuilderResultVariants, VariableOrder},
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
    /// The types associated with the auxillary rule
    pub existential_aux_types: HashMap<Variable, PrimitiveType>,

    /// Variable orders that are worth considering.
    pub promising_variable_orders: Vec<VariableOrder>,

    /// Logical Type of each Variable
    pub variable_types: HashMap<Variable, PrimitiveType>,
    /// Logical Type of predicates in Rule
    pub predicate_types: HashMap<Identifier, Vec<PrimitiveType>>,
}

/// Errors than can occur during rule analysis
#[derive(Error, Debug, Copy, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum RuleAnalysisError {
    /// Unsupported feature: Overloading of predicate names by arity/type
    #[error("Overloading of predicate names by arity is currently not supported.")]
    UnsupportedFeaturePredicateOverloading,
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
    predicate_types: &HashMap<Identifier, Vec<PrimitiveType>>,
    column_orders: &HashMap<Identifier, HashSet<ColumnOrder>>,
) -> (ChaseRule, VariableOrder, HashMap<Variable, PrimitiveType>) {
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

    let mut variable_types = HashMap::<Variable, PrimitiveType>::new();
    for atom in &head_atoms {
        let types = predicate_types
            .get(&atom.predicate())
            .expect("Every predicate should have type information at this point");

        for (term_index, term) in atom.terms().iter().enumerate() {
            if let PrimitiveTerm::Variable(variable) = term {
                variable_types.insert(variable.clone(), types[term_index]);
            }
        }
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

    (temp_rule, variable_order, variable_types)
}

fn analyze_rule(
    rule: &ChaseRule,
    promising_variable_orders: Vec<VariableOrder>,
    promising_column_orders: &[HashMap<Identifier, HashSet<ColumnOrder>>],
    rule_index: usize,
    variable_types: HashMap<Variable, PrimitiveType>,
    type_declarations: &HashMap<Identifier, Vec<PrimitiveType>>,
) -> RuleAnalysis {
    let num_existential = count_distinct_existential_variables(rule);

    let rule_all_predicates: Vec<_> = rule
        .all_body()
        .cloned()
        .map(Into::into)
        .chain(rule.head().iter().cloned())
        .map(|a| a.predicate())
        .collect();

    let (existential_aux_rule, existential_aux_order, existential_aux_types) =
        if num_existential > 0 {
            // TODO: We only consider the first variable order
            construct_existential_aux_rule(
                rule_index,
                rule.head().clone(),
                type_declarations,
                &promising_column_orders[0],
            )
        } else {
            (ChaseRule::default(), VariableOrder::new(), HashMap::new())
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
        existential_aux_types,
        promising_variable_orders,
        variable_types,
        predicate_types: type_declarations
            .iter()
            .filter(|(k, _v)| rule_all_predicates.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

/// Contains useful information about the
#[derive(Debug)]
pub struct ProgramAnalysis {
    /// Analysis result for each rule.
    pub rule_analysis: Vec<RuleAnalysis>,
    /// Set of all the predicates that are derived in the chase along with their arity.
    pub derived_predicates: HashSet<Identifier>,
    /// Set of all predicates and their arity.
    pub all_predicates: HashSet<(Identifier, usize)>,
    /// Logical Type Declarations for Predicates
    pub predicate_types: HashMap<Identifier, Vec<PrimitiveType>>,
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

    /// Collect all predicates occurring in the program.
    pub(super) fn get_all_predicates(&self) -> HashSet<(Identifier, usize)> {
        let mut result = HashSet::<(Identifier, usize)>::new();

        // Predicates in source statements
        for source in self.sources() {
            result.insert((source.predicate.clone(), source.input_types().arity()));
        }

        // Predicates in rules
        for rule in self.rules() {
            for atom in rule.head() {
                result.insert((atom.predicate(), atom.terms().len()));
            }

            for atom in rule.all_body() {
                result.insert((atom.predicate(), atom.terms().len()));
            }
        }

        // Predicates in facts
        for fact in self.facts() {
            result.insert((fact.predicate(), fact.terms().len()));
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

            result.insert((predicate, arity));
        }

        result
    }

    /// Check if the program contains rules with unsupported features
    pub fn check_for_unsupported_features(&self) -> Result<(), RuleAnalysisError> {
        let mut arities = self
            .parsed_predicate_declarations()
            .iter()
            .map(|(predicate, types)| (predicate.clone(), types.len()))
            .collect::<HashMap<_, _>>();

        for source in self.sources() {
            match arities.entry(source.predicate.clone()) {
                std::collections::hash_map::Entry::Occupied(slot) => {
                    // both declared and in a source
                    let arity = slot.get();

                    if *arity != source.input_types().arity() {
                        return Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading);
                    }
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(source.input_types().arity());
                }
            }
        }

        for rule in self.rules() {
            for atom in rule.head() {
                // check for consistent predicate arities
                let arity = atom.terms().len();
                if arity != *arities.entry(atom.predicate()).or_insert(arity) {
                    return Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading);
                }
            }

            for atom in rule.all_body() {
                // check for consistent predicate arities
                let arity = atom.terms().len();
                if arity != *arities.entry(atom.predicate()).or_insert(arity) {
                    return Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading);
                }
            }
        }

        for fact in self.facts() {
            let arity = fact.terms().len();
            if arity != *arities.entry(fact.predicate()).or_insert(arity) {
                return Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading);
            }
        }

        Ok(())
    }

    /// Analyze itself and return a struct containing the results.
    pub fn analyze(&self) -> Result<ProgramAnalysis, Error> {
        let BuilderResultVariants {
            all_variable_orders,
            all_column_orders,
        } = build_preferable_variable_orders(self, None);

        let all_predicates = self.get_all_predicates();
        let derived_predicates = self.get_head_predicates();

        let (predicate_types, rule_var_types) = infer_types(self)?;

        let rule_analysis: Vec<RuleAnalysis> = self
            .rules()
            .iter()
            .zip(rule_var_types)
            .enumerate()
            .map(|(idx, (rule, variable_types))| {
                analyze_rule(
                    rule,
                    all_variable_orders[idx].clone(),
                    &all_column_orders,
                    idx,
                    variable_types,
                    &predicate_types,
                )
            })
            .collect();

        Ok(ProgramAnalysis {
            rule_analysis,
            derived_predicates,
            all_predicates,
            predicate_types,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        io::parser::parse_program, model::chase_model::ChaseProgram,
        program_analysis::analysis::RuleAnalysisError,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn overloading_is_unsupported() {
        let program = ChaseProgram::try_from(
            parse_program(
                r#"
                           @source q[3]: load-rdf("dummy.nt") .
                           p(?x, ?y) :- q(?x), q(?y) .
                         "#,
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            program.check_for_unsupported_features(),
            Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading)
        );

        let program = ChaseProgram::try_from(
            parse_program(
                r#"
                           @source q[3]: load-rdf("dummy.nt") .
                           @declare q(integer, integer) .
                         "#,
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            program.check_for_unsupported_features(),
            Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading)
        );

        let program =
            ChaseProgram::try_from(parse_program(r#"q(?x, ?y) :- q(?x), q(?y) ."#).unwrap())
                .unwrap();

        assert_eq!(
            program.check_for_unsupported_features(),
            Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading)
        );

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

        assert_eq!(
            program.check_for_unsupported_features(),
            Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading)
        );

        let program = ChaseProgram::try_from(
            parse_program(
                r#"
                           @declare q(integer, integer) .
                           p(?x, ?y) :- q(?x), q(?y) .
                           q(23) .
                         "#,
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            program.check_for_unsupported_features(),
            Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading)
        );
    }
}
