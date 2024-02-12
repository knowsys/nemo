//! This module contains basic data structures for tracing the origins of derived facts.

use std::collections::{HashMap, HashSet};

use ascii_tree::write_tree;
use serde::Serialize;

use crate::model::{
    chase_model::{ChaseAtom, ChaseFact},
    Constant, Identifier, PrimitiveTerm, PrimitiveType, Program, Rule, Term, Variable,
};

/// Index of a rule within a [`Program`]
type RuleIndex = usize;

/// Represents the application of a rule to derive a specific fact
#[derive(Debug)]
pub(crate) struct TraceRuleApplication {
    /// Index of the rule that was applied
    rule_index: RuleIndex,
    /// Variable assignment used during the rule application
    assignment: HashMap<Variable, Constant>,
    /// Index of the head atom which produced the fact under consideration
    _position: usize,
}

impl TraceRuleApplication {
    /// Create new [`TraceRuleApplication`].
    pub fn new(
        rule_index: RuleIndex,
        assignment: HashMap<Variable, Constant>,
        _position: usize,
    ) -> Self {
        Self {
            rule_index,
            assignment,
            _position,
        }
    }
}

/// Handle to a traced fact within an [`ExecutionTrace`].
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct TraceFactHandle(usize);

/// Encodes the origin of a fact
#[derive(Debug)]
pub(crate) enum TraceDerivation {
    /// Fact was part of the input to the chase
    Input,
    /// Fact was derived during the chase
    Derived(TraceRuleApplication, Vec<TraceFactHandle>),
}

/// Encodes current status of the derivation of a given fact
#[derive(Debug)]
pub(crate) enum TraceStatus {
    /// It is not yet known whether this fact derived during chase
    Unknown,
    /// Fact was derived during the chase with the given [`Derivation`]
    Success(TraceDerivation),
    /// Fact was not derived during the chase
    Fail,
}

impl TraceStatus {
    /// Return `true` when fact was successfully derived
    /// and `false` otherwise.
    pub fn is_success(&self) -> bool {
        matches!(self, TraceStatus::Success(_))
    }

    /// Return `true` if it has already been decided whether
    /// a given fact has been derived and `false` otherwise.
    pub fn is_known(&self) -> bool {
        !matches!(self, TraceStatus::Unknown)
    }
}

/// Fact which was considered during the construction of an [`ExecutionTrace`]
#[derive(Debug)]
struct TracedFact {
    /// The considered fact
    fact: ChaseFact,
    /// Its current status with resepect to its derivablity in the chase
    status: TraceStatus,
}

/// Graph structure that encodes how certain facts were derived during the chase.
#[derive(Debug)]
pub struct ExecutionTrace {
    /// Input program
    program: Program,
    /// Logical types associated with each predicate
    predicate_types: HashMap<Identifier, Vec<PrimitiveType>>,

    /// All the facts considered during tracing
    facts: Vec<TracedFact>,
}

impl ExecutionTrace {
    /// Create an empty [`ExecutionTrace`].
    pub(crate) fn new(
        program: Program,
        predicate_types: HashMap<Identifier, Vec<PrimitiveType>>,
    ) -> Self {
        Self {
            program,
            predicate_types,
            facts: Vec::new(),
        }
    }

    fn get_fact(&self, handle: TraceFactHandle) -> &TracedFact {
        &self.facts[handle.0]
    }

    fn get_fact_mut(&mut self, handle: TraceFactHandle) -> &mut TracedFact {
        &mut self.facts[handle.0]
    }

    /// Search a given [`ChaseFact`] in `self.facts`.
    /// Also takes into account that the interpretation of a constant depends on its type.
    fn find_fact(&self, fact: &ChaseFact) -> Option<TraceFactHandle> {
        for (fact_index, traced_fact) in self.facts.iter().enumerate() {
            if traced_fact.fact.predicate() != fact.predicate()
                || traced_fact.fact.arity() != fact.arity()
            {
                continue;
            }

            let types = self
                .predicate_types
                .get(&fact.predicate())
                .expect("Every predicate must have received type information");

            let mut identical = true;
            for (ty, (term_fact, term_traced_fact)) in types
                .iter()
                .zip(fact.terms().iter().zip(traced_fact.fact.terms()))
            {
                if ty.ground_term_to_data_value_t(term_fact.clone())
                    != ty.ground_term_to_data_value_t(term_traced_fact.clone())
                {
                    identical = false;
                    break;
                }
            }

            if identical {
                return Some(TraceFactHandle(fact_index));
            }
        }

        None
    }

    /// Registers a new [`ChaseFact`].
    ///
    /// If the fact was not already known then it will return a fresh handle
    /// with the status `TraceStatus::Known`.
    /// Otherwise a handle to the existing fact will be returned.
    pub fn register_fact(&mut self, fact: ChaseFact) -> TraceFactHandle {
        if let Some(handle) = self.find_fact(&fact) {
            handle
        } else {
            let handle = TraceFactHandle(self.facts.len());
            self.facts.push(TracedFact {
                fact,
                status: TraceStatus::Unknown,
            });

            handle
        }
    }

    /// Return the [`TraceStatus`] of a given fact identified by its [`TraceFactHandle`].
    pub(crate) fn status(&self, handle: TraceFactHandle) -> &TraceStatus {
        &self.get_fact(handle).status
    }

    /// Update the [`TraceStatus`] of a given fact identified by its [`TraceFactHandle`].
    pub(crate) fn update_status(&mut self, handle: TraceFactHandle, status: TraceStatus) {
        self.get_fact_mut(handle).status = status;
    }
}

/// Represents the application of a rule to derive a specific fact
#[derive(Debug, Clone)]
pub struct TraceTreeRuleApplication {
    /// Rule that was applied
    pub rule: Rule,
    /// Variable assignment used during the rule application
    pub assignment: HashMap<Variable, Constant>,
    /// Index of the head atom which produced the fact under consideration
    _position: usize,
}

/// Tree representation of an [`ExecutionTrace`] from a given start node
#[derive(Debug, Clone)]
pub enum ExecutionTraceTree {
    /// Node represent a fact in the initial data base
    Fact(ChaseFact),
    /// Node represents a derived fact
    Rule(TraceTreeRuleApplication, Vec<ExecutionTraceTree>),
}

impl ExecutionTrace {
    /// Return a [`ExecutionTraceTree`] representation of an [`ExecutionTrace`]
    /// starting from a given fact.
    pub fn tree(&self, fact_handle: TraceFactHandle) -> Option<ExecutionTraceTree> {
        let traced_fact = self.get_fact(fact_handle);

        if let TraceStatus::Success(derivation) = &traced_fact.status {
            match derivation {
                TraceDerivation::Input => Some(ExecutionTraceTree::Fact(traced_fact.fact.clone())),
                TraceDerivation::Derived(application, subderivations) => {
                    let mut subtrees = Vec::new();
                    for &derivation in subderivations {
                        if let Some(tree) = self.tree(derivation) {
                            subtrees.push(tree);
                        } else {
                            return None;
                        }
                    }

                    let tree_application = TraceTreeRuleApplication {
                        rule: self.program.rules()[application.rule_index].clone(),
                        assignment: application.assignment.clone(),
                        _position: application._position,
                    };

                    Some(ExecutionTraceTree::Rule(tree_application, subtrees))
                }
            }
        } else {
            None
        }
    }
}

impl ExecutionTrace {
    /// Converts a rule to a string representation
    /// so it can appear as a node in the ascii representation of the [`ExecutionTrace`].
    fn ascii_format_rule(&self, application: &TraceRuleApplication) -> String {
        let mut rule_applied = self.program.rules()[application.rule_index].clone();
        rule_applied.apply_assignment(
            &application
                .assignment
                .iter()
                .map(|(variable, constant)| {
                    (
                        variable.clone(),
                        Term::Primitive(PrimitiveTerm::Constant(constant.clone())),
                    )
                })
                .collect(),
        );

        rule_applied.to_string()
    }

    /// Create an ascii tree representation starting form a particular node.
    ///
    /// Returns `None` if no successful derivation can be given.
    pub fn ascii_tree(&self, handle: TraceFactHandle) -> Option<ascii_tree::Tree> {
        let traced_fact = self.get_fact(handle);

        if let TraceStatus::Success(derivation) = &traced_fact.status {
            match derivation {
                TraceDerivation::Input => {
                    Some(ascii_tree::Tree::Leaf(vec![traced_fact.fact.to_string()]))
                }
                TraceDerivation::Derived(application, subderivations) => {
                    let mut subtrees = Vec::new();
                    for &derivation in subderivations {
                        if let Some(tree) = self.ascii_tree(derivation) {
                            subtrees.push(tree);
                        } else {
                            return None;
                        }
                    }

                    Some(ascii_tree::Tree::Node(
                        self.ascii_format_rule(application),
                        subtrees,
                    ))
                }
            }
        } else {
            None
        }
    }

    /// Create an ascii tree representation starting form a particular node.
    ///
    /// Returns `None` if no successful derivation can be given.
    pub fn ascii_tree_string(&self, handle: TraceFactHandle) -> Option<String> {
        let mut result = String::new();
        write_tree(&mut result, &self.ascii_tree(handle)?).ok()?;

        Some(result)
    }
}

/// Represents an inference in an [`ExecutionTraceJson`]
#[derive(Debug, Serialize)]
struct ExecutionTraceJsonInference {
    #[serde(rename = "ruleName")]
    rule_name: String,

    conclusion: String,
    premises: Vec<String>,
}

impl ExecutionTraceJsonInference {
    /// Create a new [`ExecutionTraceJsonInference`]
    pub fn new(rule_name: String, conclusion: String, premises: Vec<String>) -> Self {
        Self {
            rule_name,
            conclusion,
            premises,
        }
    }
}

/// Object representing an [`ExecutionTrace`] that can be sertialized into a json format
#[derive(Debug, Serialize, Default)]
pub struct ExecutionTraceJson {
    #[serde(rename = "finalConclusion")]
    final_conclusions: Vec<String>,

    inferences: Vec<ExecutionTraceJsonInference>,
}

impl ExecutionTrace {
    /// Translate an [`TraceDerivation`] into an [`ExecutionTraceJsonInference`].
    fn json_inference(
        &self,
        derivation: &TraceDerivation,
        conclusion: &ChaseFact,
    ) -> ExecutionTraceJsonInference {
        const RULE_NAME_FACT: &str = "Asserted";

        match derivation {
            TraceDerivation::Input => ExecutionTraceJsonInference::new(
                String::from(RULE_NAME_FACT),
                conclusion.to_string(),
                vec![],
            ),
            TraceDerivation::Derived(application, premises_handles) => {
                let rule = &self.program.rules()[application.rule_index];

                let premises = premises_handles
                    .iter()
                    .map(|&handle| self.get_fact(handle).fact.to_string())
                    .collect();

                ExecutionTraceJsonInference::new(rule.to_string(), conclusion.to_string(), premises)
            }
        }
    }

    /// Create a json representation of the trace.
    pub fn json(&self, fact_handles: &[TraceFactHandle]) -> ExecutionTraceJson {
        let mut final_conclusions = Vec::<String>::new();
        let mut fact_stack = fact_handles.to_vec();
        let mut inferences = Vec::<ExecutionTraceJsonInference>::new();
        let mut inferred_conclusions = HashSet::<TraceFactHandle>::new();

        for &final_handle in fact_handles {
            let mut successful_derivation = true;
            fact_stack.push(final_handle);

            while let Some(current_fact) = fact_stack.pop() {
                if inferred_conclusions.contains(&current_fact) {
                    continue;
                }
                inferred_conclusions.insert(current_fact);

                let traced_fact = self.get_fact(current_fact);

                if let TraceStatus::Success(derivation) = &traced_fact.status {
                    let inference = self.json_inference(derivation, &traced_fact.fact);
                    inferences.push(inference);

                    if let TraceDerivation::Derived(_, handles_derived) = derivation {
                        fact_stack.extend(handles_derived);
                    }
                } else {
                    successful_derivation = false;
                    break;
                }
            }

            if successful_derivation {
                final_conclusions.push(self.get_fact(final_handle).fact.to_string());
            }
        }

        ExecutionTraceJson {
            final_conclusions,
            inferences,
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        execution::tracing::trace::{TraceDerivation, TraceStatus},
        model::{
            chase_model::ChaseFact, Atom, Constant, Identifier, Literal, PrimitiveTerm,
            PrimitiveType, Program, Rule, Term, Variable,
        },
    };

    use super::{ExecutionTrace, TraceRuleApplication};

    macro_rules! variable_assignment {
        ($($k:expr => $v:expr),*) => {
            [$(($k, $v)),*]
                .into_iter()
                .map(|(k, v)| {
                    (
                        Variable::Universal(k.to_string()),
                        Constant::Abstract(Identifier(v.to_string())),
                    )
                })
                .collect()
        };
    }

    macro_rules! atom_term {
        (? $var:expr ) => {
            Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(
                $var.to_string(),
            )))
        };
    }

    macro_rules! atom {
        ( $pred:expr; $( $marker:tt $t:tt ),* ) => {
            Atom::new(Identifier($pred.to_string()), vec![ $( atom_term!( $marker $t ) ),* ])
        };
    }

    fn test_trace() -> ExecutionTrace {
        // P(?x, ?y) :- Q(?y, ?x) .
        let rule_1 = Rule::new(
            vec![atom!("P"; ?"x", ?"y")],
            vec![Literal::Positive(atom!("Q"; ?"y", ?"x"))],
            vec![],
        );
        let rule_1_assignment = variable_assignment!("x" => "b", "y" => "a");

        // S(?x) :- T(?x) .
        let rule_2 = Rule::new(
            vec![atom!("S"; ?"x")],
            vec![Literal::Positive(atom!("T"; ?"x"))],
            vec![],
        );

        let rule_2_assignment = variable_assignment!("x" => "a");

        // R(?x, ?y) :- P(?x, ?y), S(?y) .
        let rule_3 = Rule::new(
            vec![atom!("R"; ?"x", ?"y")],
            vec![
                Literal::Positive(atom!("P"; ?"x", ?"y")),
                Literal::Positive(atom!("S"; ?"y")),
            ],
            vec![],
        );
        let rule_3_assignment: HashMap<_, _> = variable_assignment!("x" => "b", "y" => "a");

        let q_ab = ChaseFact::new(
            Identifier("Q".to_string()),
            vec![
                Constant::Abstract(Identifier("a".to_string())),
                Constant::Abstract(Identifier("b".to_string())),
            ],
        );

        let p_ba = ChaseFact::new(
            Identifier("P".to_string()),
            vec![
                Constant::Abstract(Identifier("b".to_string())),
                Constant::Abstract(Identifier("a".to_string())),
            ],
        );

        let r_ba = ChaseFact::new(
            Identifier("R".to_string()),
            vec![
                Constant::Abstract(Identifier("b".to_string())),
                Constant::Abstract(Identifier("a".to_string())),
            ],
        );

        let s_a = ChaseFact::new(
            Identifier("S".to_string()),
            vec![Constant::Abstract(Identifier("a".to_string()))],
        );

        let t_a = ChaseFact::new(
            Identifier("T".to_string()),
            vec![Constant::Abstract(Identifier("a".to_string()))],
        );

        let rules = vec![rule_1, rule_2, rule_3];
        let rule_1_index = 0;
        let rule_2_index = 1;
        let rule_3_index = 2;

        let program = Program::builder().rules(rules).build();

        let mut predicate_types = HashMap::new();
        predicate_types.insert(Identifier(String::from("S")), vec![PrimitiveType::Any]);
        predicate_types.insert(Identifier(String::from("T")), vec![PrimitiveType::Any]);
        predicate_types.insert(
            Identifier(String::from("Q")),
            vec![PrimitiveType::Any, PrimitiveType::Any],
        );
        predicate_types.insert(
            Identifier(String::from("P")),
            vec![PrimitiveType::Any, PrimitiveType::Any],
        );
        predicate_types.insert(
            Identifier(String::from("R")),
            vec![PrimitiveType::Any, PrimitiveType::Any],
        );

        let mut trace = ExecutionTrace::new(program, predicate_types);

        let trace_s_a = trace.register_fact(s_a);
        let trace_t_a = trace.register_fact(t_a);
        let trace_q_ab = trace.register_fact(q_ab);
        let trace_p_ba = trace.register_fact(p_ba);
        let trace_r_ba = trace.register_fact(r_ba);

        trace.update_status(trace_t_a, TraceStatus::Success(TraceDerivation::Input));
        trace.update_status(
            trace_s_a,
            TraceStatus::Success(TraceDerivation::Derived(
                TraceRuleApplication::new(rule_2_index, rule_2_assignment, 0),
                vec![trace_t_a],
            )),
        );
        trace.update_status(trace_q_ab, TraceStatus::Success(TraceDerivation::Input));
        trace.update_status(
            trace_p_ba,
            TraceStatus::Success(TraceDerivation::Derived(
                TraceRuleApplication::new(rule_1_index, rule_1_assignment, 0),
                vec![trace_q_ab],
            )),
        );
        trace.update_status(
            trace_r_ba,
            TraceStatus::Success(TraceDerivation::Derived(
                TraceRuleApplication::new(rule_3_index, rule_3_assignment, 0),
                vec![trace_p_ba, trace_s_a],
            )),
        );

        trace
    }

    #[test]
    fn trace_ascii() {
        let trace = test_trace();
        let r_ba = ChaseFact::new(
            Identifier("R".to_string()),
            vec![
                Constant::Abstract(Identifier("b".to_string())),
                Constant::Abstract(Identifier("a".to_string())),
            ],
        );
        let trace_r_ba = trace.find_fact(&r_ba).unwrap();

        let trace_string = r#" R(b, a) :- P(b, a), S(a) .
 ├─ P(b, a) :- Q(a, b) .
 │  └─ Q(a, b)
 └─ S(a) :- T(a) .
    └─ T(a)
"#;

        assert_eq!(
            trace.ascii_tree_string(trace_r_ba).unwrap(),
            trace_string.to_string()
        )
    }

    #[test]
    fn trace_json() {
        let trace = test_trace();

        let r_ba = ChaseFact::new(
            Identifier("R".to_string()),
            vec![
                Constant::Abstract(Identifier("b".to_string())),
                Constant::Abstract(Identifier("a".to_string())),
            ],
        );
        let p_ba = ChaseFact::new(
            Identifier("P".to_string()),
            vec![
                Constant::Abstract(Identifier("b".to_string())),
                Constant::Abstract(Identifier("a".to_string())),
            ],
        );

        let trace_r_ba = trace.find_fact(&r_ba).unwrap();
        let trace_p_ba = trace.find_fact(&p_ba).unwrap();
        let trace_handles = vec![trace_r_ba, trace_p_ba];

        let expected_json = r#"{"finalConclusion":["R(b, a)","P(b, a)"],"inferences":[{"ruleName":"R(?x, ?y) :- P(?x, ?y), S(?y) .","conclusion":"R(b, a)","premises":["P(b, a)","S(a)"]},{"ruleName":"S(?x) :- T(?x) .","conclusion":"S(a)","premises":["T(a)"]},{"ruleName":"Asserted","conclusion":"T(a)","premises":[]},{"ruleName":"P(?x, ?y) :- Q(?y, ?x) .","conclusion":"P(b, a)","premises":["Q(a, b)"]},{"ruleName":"Asserted","conclusion":"Q(a, b)","premises":[]}]}"#.to_string();
        let computed_json = serde_json::to_string(&trace.json(&trace_handles)).unwrap();

        assert_eq!(expected_json, computed_json);
    }
}
