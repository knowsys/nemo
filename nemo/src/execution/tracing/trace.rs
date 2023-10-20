//! This module contains basic data structures for tracing the origins of derived facts.

use std::collections::HashMap;

use ascii_tree::write_tree;

use crate::model::{
    chase_model::{ChaseAtom, ChaseFact},
    Constant, Identifier, PrimitiveTerm, PrimitiveType, Program, Term, Variable,
};

/// Index of a rule within a [`Program`]
type RuleIndex = usize;

/// Represents the application of a rule to derive a specific fact
#[derive(Debug)]
pub struct RuleApplication {
    /// Index of the rule that was applied
    rule_index: RuleIndex,
    /// Variable assignment used during the rule application
    assignment: HashMap<Variable, Constant>,
    /// Index of the head atom which produced the fact under consideration
    _position: usize,
}

impl RuleApplication {
    /// Create new [`RuleApplication`].
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
#[derive(Debug, Clone, Copy)]
pub struct FactTraceHandle(usize);

/// Encodes the origin of a fact
#[derive(Debug)]
pub enum TraceDerivation {
    /// Fact was part of the input to the chase
    Input,
    /// Fact was derived during the chase
    Derived(RuleApplication, Vec<FactTraceHandle>),
}

/// Encodes current status of the derivation of a given fact
#[derive(Debug)]
pub enum TraceStatus {
    /// It is not yet known whether this fact derived during chase
    Unknown,
    /// Fact was derived during the chase with the given [`Derivation`]
    Success(TraceDerivation),
    /// Fact was not derived during the chase
    Fail,
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
    pub fn new(program: Program, predicate_types: HashMap<Identifier, Vec<PrimitiveType>>) -> Self {
        Self {
            program,
            predicate_types,
            facts: Vec::new(),
        }
    }

    fn get_fact(&self, handle: FactTraceHandle) -> &TracedFact {
        &self.facts[handle.0]
    }

    fn get_fact_mut(&mut self, handle: FactTraceHandle) -> &mut TracedFact {
        &mut self.facts[handle.0]
    }

    /// Search a given [`ChaseFact`] in `self.facts`.
    /// Also takes into account that the interpretation of a constant depends on its type.
    fn find_fact(&self, fact: &ChaseFact) -> Option<FactTraceHandle> {
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
                return Some(FactTraceHandle(fact_index));
            }
        }

        None
    }

    /// Registers a new [`ChaseFact`].
    pub fn register_fact(&mut self, fact: ChaseFact) -> FactTraceHandle {
        if let Some(handle) = self.find_fact(&fact) {
            handle
        } else {
            let handle = FactTraceHandle(self.facts.len());
            self.facts.push(TracedFact {
                fact,
                status: TraceStatus::Unknown,
            });

            handle
        }
    }

    /// Return the [`TraceStatus`] of a given fact identified by its [`FactTraceHandle`].
    pub fn status(&self, handle: FactTraceHandle) -> &TraceStatus {
        &self.get_fact(handle).status
    }

    /// Update the [`TraceStatus`] of a given fact identified by its [`FactTraceHandle`].
    pub fn update_status(&mut self, handle: FactTraceHandle, status: TraceStatus) {
        self.get_fact_mut(handle).status = status;
    }
}

impl ExecutionTrace {
    fn ascii_format_rule(&self, application: &RuleApplication) -> String {
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
    pub fn ascii_tree(&self, handle: FactTraceHandle) -> Option<ascii_tree::Tree> {
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
    pub fn ascii_tree_string(&self, handle: FactTraceHandle) -> Option<String> {
        let mut result = String::new();
        write_tree(&mut result, &self.ascii_tree(handle)?).ok()?;

        Some(result)
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

    use super::{ExecutionTrace, RuleApplication};

    macro_rules! variable_assignment {
        ($($k:expr => $v:expr),*) => {
            [$(($k, $v)),*]
                .into_iter()
                .map(|(k, v)| {
                    (
                        Variable::Universal(Identifier(k.to_string())),
                        Constant::Abstract(Identifier(v.to_string())),
                    )
                })
                .collect()
        };
    }

    macro_rules! atom_term {
        (? $var:expr ) => {
            Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                $var.to_string(),
            ))))
        };
    }

    macro_rules! atom {
        ( $pred:expr; $( $marker:tt $t:tt ),* ) => {
            Atom::new(Identifier($pred.to_string()), vec![ $( atom_term!( $marker $t ) ),* ])
        };
    }

    #[test]
    fn print_trace() {
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

        let program = Program::from(rules);

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
                RuleApplication::new(rule_2_index, rule_2_assignment, 0),
                vec![trace_t_a],
            )),
        );
        trace.update_status(trace_q_ab, TraceStatus::Success(TraceDerivation::Input));
        trace.update_status(
            trace_p_ba,
            TraceStatus::Success(TraceDerivation::Derived(
                RuleApplication::new(rule_1_index, rule_1_assignment, 0),
                vec![trace_q_ab],
            )),
        );
        trace.update_status(
            trace_r_ba,
            TraceStatus::Success(TraceDerivation::Derived(
                RuleApplication::new(rule_3_index, rule_3_assignment, 0),
                vec![trace_p_ba, trace_s_a],
            )),
        );

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
}
