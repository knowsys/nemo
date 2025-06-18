//! This module contains basic data structures for tracing the origins of derived facts.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use ascii_tree::write_tree;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph_graphml::GraphMl;
use serde::Serialize;

use crate::{
    chase_model::components::atom::{ground_atom::GroundAtom, ChaseAtom},
    rule_model::{
        components::{fact::Fact, rule::Rule},
        programs::program::Program,
        substitution::Substitution,
    },
};

/// Index of a rule within a [Program]
type RuleIndex = usize;

/// Represents the application of a rule to derive a specific fact
#[derive(Debug)]
pub(crate) struct TraceRuleApplication {
    /// Index of the rule that was applied
    rule_index: RuleIndex,
    /// Variable assignment used during the rule application
    assignment: Substitution,
    /// Index of the head atom which produced the fact under consideration
    _position: usize,
}

impl TraceRuleApplication {
    /// Create new [TraceRuleApplication].
    pub fn new(rule_index: RuleIndex, assignment: Substitution, _position: usize) -> Self {
        Self {
            rule_index,
            assignment,
            _position,
        }
    }
}

/// Handle to a traced fact within an [ExecutionTrace].
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
    /// Fact was derived during the chase with the given [TraceDerivation]
    Success(TraceDerivation),
    /// Fact was not derived during the chase
    Fail,
}

impl TraceStatus {
    /// Return true when fact was successfully derived
    /// and false otherwise.
    pub fn is_success(&self) -> bool {
        matches!(self, TraceStatus::Success(_))
    }

    /// Return true if it has already been decided whether
    /// a given fact has been derived and false otherwise.
    pub fn is_known(&self) -> bool {
        !matches!(self, TraceStatus::Unknown)
    }
}

/// Fact which was considered during the construction of an [ExecutionTrace]
#[derive(Debug)]
struct TracedFact {
    /// The considered fact
    fact: GroundAtom,
    /// Its current status with respect to its derivability in the chase
    status: TraceStatus,
}

/// Structure for recording execution traces
/// while not recomputing traces for the same facts occurring multiple times
#[derive(Debug)]
pub struct ExecutionTrace {
    /// Input program
    program: Program,

    /// All the facts considered during tracing
    facts: Vec<TracedFact>,
}

impl ExecutionTrace {
    /// Create an empty [ExecutionTrace].
    pub(crate) fn new(program: Program) -> Self {
        Self {
            program,
            facts: Vec::new(),
        }
    }

    /// Given a [TraceFactHandle] return a reference to the corresponding [TracedFact].
    fn get_fact(&self, handle: TraceFactHandle) -> &TracedFact {
        &self.facts[handle.0]
    }

    /// Given a [TraceFactHandle] return a mutable reference to the corresponding [TracedFact].
    fn get_fact_mut(&mut self, handle: TraceFactHandle) -> &mut TracedFact {
        &mut self.facts[handle.0]
    }

    /// Search a given [GroundAtom] in self.facts.
    /// Also takes into account that the interpretation of a constant depends on its type.
    fn find_fact(&self, fact: &GroundAtom) -> Option<TraceFactHandle> {
        for (fact_index, traced_fact) in self.facts.iter().enumerate() {
            if traced_fact.fact.predicate() != fact.predicate()
                || traced_fact.fact.arity() != fact.arity()
            {
                continue;
            }

            let mut identical = true;
            for (term_fact, term_traced_fact) in fact.terms().zip(traced_fact.fact.terms()) {
                if term_fact != term_traced_fact {
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

    /// Registers a new [GroundAtom].
    ///
    /// If the fact was not already known then it will return a fresh handle
    /// with the status TraceStatus::Known.
    /// Otherwise a handle to the existing fact will be returned.
    pub fn register_fact(&mut self, fact: GroundAtom) -> TraceFactHandle {
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

    /// Return the [TraceStatus] of a given fact identified by its [TraceFactHandle].
    pub(crate) fn status(&self, handle: TraceFactHandle) -> &TraceStatus {
        &self.get_fact(handle).status
    }

    /// Update the [TraceStatus] of a given fact identified by its [TraceFactHandle].
    pub(crate) fn update_status(&mut self, handle: TraceFactHandle, status: TraceStatus) {
        self.get_fact_mut(handle).status = status;
    }
}

/// Trait implemented by tree structure of traces
/// that can be converted into the GraphML format
trait ToGraphMl {
    /// Return graphml representation of the content.
    fn to_graphml(&self) -> String;
}

impl ToGraphMl for DiGraph<TracePetGraphNodeLabel, ()> {
    fn to_graphml(&self) -> String {
        GraphMl::new(&self)
            .export_node_weights(Box::new(|node_label| match node_label {
                TracePetGraphNodeLabel::Fact(chase_fact) => vec![
                    (Cow::from("type"), Cow::from("axiom")),
                    (Cow::from("element"), Cow::from(chase_fact.to_string())),
                ],
                TracePetGraphNodeLabel::Rule(rule) => vec![
                    (Cow::from("type"), Cow::from("DLRule")),
                    (Cow::from("element"), Cow::from(rule.to_string())),
                ],
            }))
            .to_string()
    }
}

/// Represents the application of a rule to derive a specific fact
#[derive(Debug, Clone)]
pub struct TraceTreeRuleApplication {
    /// Rule that was applied
    pub rule: Rule,
    /// Variable assignment used during the rule application
    pub assignment: Substitution,
    /// Index of the head atom which produced the fact under consideration
    _position: usize,
}

impl TraceTreeRuleApplication {
    /// Get the [Fact] that was produced by this rule application.
    fn to_derived_atom(&self) -> Fact {
        let mut fact = self.rule.head()[self._position].clone();
        self.assignment.apply(&mut fact);

        Fact::from(fact)
    }

    /// Get a string representation of the Instantiated rule.
    fn to_instantiated_string(&self) -> String {
        if let Some(display_string) = self.rule.instantiated_display(&self.assignment) {
            return display_string;
        }

        let mut rule_name_prefix = String::from("");
        if let Some(name) = &self.rule.name() {
            rule_name_prefix = format!("{}: ", name.clone());
        }

        let mut rule_instantiated = self.rule.clone();
        self.assignment.apply(&mut rule_instantiated);

        format!("{rule_name_prefix}{rule_instantiated}")
    }
}

/// Tree representation of an [ExecutionTrace] from a given start node
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum ExecutionTraceTree {
    /// Node represent a fact in the initial data base
    Fact(GroundAtom),
    /// Node represents a derived fact
    Rule(TraceTreeRuleApplication, Vec<ExecutionTraceTree>),
}

/// Type of labels for [DiGraph] representation of [ExecutionTrace]
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
enum TracePetGraphNodeLabel {
    /// Fact
    Fact(Fact),
    /// Application of a rule
    Rule(Rule),
}

impl ExecutionTraceTree {
    /// Create an ascii tree representation.
    fn to_ascii_tree(&self) -> ascii_tree::Tree {
        match self {
            Self::Fact(chase_fact) => ascii_tree::Tree::Leaf(vec![chase_fact.to_string()]),
            Self::Rule(trace_tree_rule_application, subtrees) => ascii_tree::Tree::Node(
                trace_tree_rule_application.to_instantiated_string(),
                subtrees
                    .iter()
                    .map(ExecutionTraceTree::to_ascii_tree)
                    .collect(),
            ),
        }
    }

    /// Create an ascii tree representation.
    pub fn to_ascii_art(&self) -> String {
        let mut result = String::new();
        let _ = write_tree(&mut result, &self.to_ascii_tree());

        result
    }

    fn to_petgraph(&self) -> DiGraph<TracePetGraphNodeLabel, ()> {
        let mut graph = DiGraph::new();

        let mut node_stack: Vec<(Option<NodeIndex>, Self)> = vec![(None, self.clone())];
        while let Some((parent_node_index_opt, next_node)) = node_stack.pop() {
            let next_node_index = match next_node {
                Self::Fact(ref chase_fact) => {
                    let next_node_index = graph
                        .add_node(TracePetGraphNodeLabel::Fact(Fact::from(chase_fact.clone())));
                    if let Some(parent_node_index) = parent_node_index_opt {
                        graph.add_edge(next_node_index, parent_node_index, ());
                    }
                    next_node_index
                }
                Self::Rule(ref trace_tree_rule_application, _) => {
                    let fact = trace_tree_rule_application.to_derived_atom();
                    let rule = trace_tree_rule_application.rule.clone();

                    let fact_node_index = graph.add_node(TracePetGraphNodeLabel::Fact(fact));
                    let next_node_index = graph.add_node(TracePetGraphNodeLabel::Rule(rule));
                    graph.add_edge(next_node_index, fact_node_index, ());

                    if let Some(parent_node_index) = parent_node_index_opt {
                        graph.add_edge(fact_node_index, parent_node_index, ());
                    }

                    next_node_index
                }
            };

            if let Self::Rule(_, subtrees) = next_node {
                let new_parent_node_index_opt = Some(next_node_index);
                let mut entries_to_append: Vec<(Option<NodeIndex>, Self)> = subtrees
                    .iter()
                    .cloned()
                    .map(|st| (new_parent_node_index_opt, st))
                    .collect();
                node_stack.append(&mut entries_to_append)
            }
        }

        graph
    }

    /// Return [ExecutionTraceTree] in [GraphML](http://graphml.graphdrawing.org/) format (for [Evonne](https://github.com/imldresden/evonne) integration)
    pub fn to_graphml(&self) -> String {
        self.to_petgraph().to_graphml()
    }
}

impl ExecutionTrace {
    /// Return a [ExecutionTraceTree] representation of an [ExecutionTrace]
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
                        rule: self.program.rule(application.rule_index).clone(),
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

/// Inference in [ExecutionTraceListOfInferences]
#[derive(Debug)]
struct ExecutionTraceInference {
    /// Pair of [Rule] and [Substitution]
    trigger: Option<(Rule, Substitution)>,
    /// Result of the rule application
    conclusion: GroundAtom,
    /// Input facts to the rule
    premises: Vec<GroundAtom>,
}

/// Serializable version of [ExecutionTraceInference]
#[derive(Debug, Serialize)]
struct ExecutionTraceInferenceJSON {
    /// String representation of the logical rule
    #[serde(rename = "rule")]
    rule_string: String,
    /// Optionally provided name of the rule
    #[serde(rename = "ruleName", skip_serializing_if = "Option::is_none")]
    rule_name: Option<String>,
    /// Optional human readable string representation of a rule instance
    #[serde(rename = "ruleDisplay", skip_serializing_if = "Option::is_none")]
    rule_display: Option<String>,
    /// Instantiated result of the rule application
    conclusion: String,
    /// Premises enabling the rule application
    premises: Vec<String>,
}

const DEFAULT_DERIVATION_ANNOTATION: &str = "Asserted";

impl From<ExecutionTraceInference> for ExecutionTraceInferenceJSON {
    fn from(value: ExecutionTraceInference) -> Self {
        Self {
            rule_string: value
                .trigger
                .as_ref()
                .map(|(rule, _)| rule.to_string())
                .unwrap_or(DEFAULT_DERIVATION_ANNOTATION.to_string()),
            rule_name: value.trigger.as_ref().and_then(|(rule, _)| rule.name()),
            rule_display: value
                .trigger
                .as_ref()
                .and_then(|(rule, substitution)| rule.instantiated_display(substitution)),
            conclusion: value.conclusion.to_string(),
            premises: value
                .premises
                .into_iter()
                .map(|premise| premise.to_string())
                .collect(),
        }
    }
}

impl ExecutionTraceInference {
    /// Create a new [ExecutionTraceInference]
    pub fn new(
        trigger: Option<(Rule, Substitution)>,
        conclusion: GroundAtom,
        premises: Vec<GroundAtom>,
    ) -> Self {
        Self {
            trigger,
            conclusion,
            premises,
        }
    }
}

/// Representation of an [ExecutionTrace] as a list of inferences
#[derive(Debug)]
struct ExecutionTraceListOfInferences {
    /// The list of conclusions for which the trace is computed
    final_conclusions: Vec<GroundAtom>,

    /// List of inferences which result in the `final_conclusions`
    inferences: Vec<ExecutionTraceInference>,
}

/// Serializable representation of an [ExecutionTrace] as a list of inferences
#[derive(Debug, Serialize, Default)]
pub struct ExecutionTraceListOfInferencesJSON {
    /// The list of conclusions for which the trace is computed
    #[serde(rename = "finalConclusion")]
    final_conclusions: Vec<String>,

    /// List of inferences which result in the `final_conclusions`
    inferences: Vec<ExecutionTraceInferenceJSON>,
}

impl From<ExecutionTraceListOfInferences> for ExecutionTraceListOfInferencesJSON {
    fn from(value: ExecutionTraceListOfInferences) -> Self {
        Self {
            final_conclusions: value
                .final_conclusions
                .into_iter()
                .map(|concl| concl.to_string())
                .collect(),
            inferences: value
                .inferences
                .into_iter()
                .map(ExecutionTraceInferenceJSON::from)
                .collect(),
        }
    }
}

impl ExecutionTrace {
    /// Translate an [TraceDerivation] into an [ExecutionTraceInference].
    fn inference_from_derivation(
        &self,
        derivation: &TraceDerivation,
        conclusion: &GroundAtom,
    ) -> ExecutionTraceInference {
        match derivation {
            TraceDerivation::Input => {
                ExecutionTraceInference::new(None, conclusion.clone(), vec![])
            }
            TraceDerivation::Derived(application, premises_handles) => {
                let rule = self.program.rule(application.rule_index);

                let premises = premises_handles
                    .iter()
                    .map(|&handle| self.get_fact(handle).fact.clone())
                    .collect();

                ExecutionTraceInference::new(
                    Some((rule.clone(), application.assignment.clone())),
                    conclusion.clone(),
                    premises,
                )
            }
        }
    }

    /// Represent the [ExecutionTrace] as a list of inferences.
    fn list_of_inferences(
        &self,
        fact_handles: &[TraceFactHandle],
    ) -> ExecutionTraceListOfInferences {
        let mut final_conclusions = Vec::<GroundAtom>::new();
        let mut fact_stack = fact_handles.to_vec();
        let mut inferences = Vec::<ExecutionTraceInference>::new();
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
                    let inference = self.inference_from_derivation(derivation, &traced_fact.fact);
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
                final_conclusions.push(self.get_fact(final_handle).fact.clone());
            }
        }

        ExecutionTraceListOfInferences {
            final_conclusions,
            inferences,
        }
    }

    /// Create a json representation of the trace.
    pub fn json(&self, fact_handles: &[TraceFactHandle]) -> ExecutionTraceListOfInferencesJSON {
        ExecutionTraceListOfInferencesJSON::from(self.list_of_inferences(fact_handles))
    }

    /// Return [`ExecutionTrace`] in [GraphML](http://graphml.graphdrawing.org/) format as DAG (for [Evonne](https://github.com/imldresden/evonne) integration)
    pub fn graphml(&self, fact_handles: &[TraceFactHandle]) -> String {
        self.list_of_inferences(fact_handles).to_graphml()
    }
}

impl ExecutionTraceListOfInferences {
    fn to_petgraph(&self) -> DiGraph<TracePetGraphNodeLabel, ()> {
        let mut graph = DiGraph::new();
        let mut label_to_node_index: HashMap<TracePetGraphNodeLabel, NodeIndex> = HashMap::new();

        self.inferences.iter().for_each(|inference| {
            let conclusion = &inference.conclusion;
            let trigger_opt = &inference.trigger;
            let premises = &inference.premises;

            let conclusion_label = TracePetGraphNodeLabel::Fact(Fact::from(conclusion.clone()));
            let conclusion_node_index = label_to_node_index
                .entry(conclusion_label.clone())
                .or_insert_with(|| graph.add_node(conclusion_label));

            if let Some((rule, _substitution)) = trigger_opt {
                let rule_node_index = graph.add_node(TracePetGraphNodeLabel::Rule(rule.clone()));
                graph.add_edge(rule_node_index, *conclusion_node_index, ());

                premises.iter().for_each(|premise| {
                    let premise_label = TracePetGraphNodeLabel::Fact(Fact::from(premise.clone()));
                    let premise_node_index = label_to_node_index
                        .entry(premise_label.clone())
                        .or_insert_with(|| graph.add_node(premise_label));

                    graph.add_edge(*premise_node_index, rule_node_index, ());
                })
            }
        });

        graph
    }

    fn to_graphml(&self) -> String {
        self.to_petgraph().to_graphml()
    }
}

#[cfg(test)]
mod test {
    use nemo_physical::datavalues::AnyDataValue;

    use crate::{
        chase_model::components::atom::ground_atom::GroundAtom,
        execution::tracing::trace::{TraceDerivation, TraceStatus},
        rule_model::{
            components::{
                atom::Atom,
                rule::Rule,
                term::primitive::{variable::Variable, Primitive},
            },
            programs::{program::Program, ProgramWrite},
            substitution::Substitution,
            translation::TranslationComponent,
        },
    };

    use super::{ExecutionTrace, TraceRuleApplication};

    macro_rules! variable_assignment {
        ($($k:expr => $v:expr),*) => {{
            let terms = [$(($k, $v)),*]
            .into_iter()
            .map(|(k, v)| {
                (
                    Primitive::from(Variable::universal(k)),
                    Primitive::from(AnyDataValue::new_iri(v.to_string())),
                )
            });

            Substitution::new(terms)
        }};
    }

    fn test_trace() -> ExecutionTrace {
        let rule_1 = Rule::parse("P(?x, ?y) :- Q(?y, ?x)").unwrap();
        let rule_1_assignment = variable_assignment!("x" => "b", "y" => "a");

        let rule_2 = Rule::parse("S(?x) :- T(?x)").unwrap();
        let rule_2_assignment = variable_assignment!("x" => "a");

        let rule_3 = Rule::parse("R(?x, ?y) :- P(?x, ?y), S(?y)").unwrap();
        let rule_3_assignment = variable_assignment!("x" => "b", "y" => "a");

        let q_ab = GroundAtom::try_from(Atom::parse("Q(a,b)").unwrap()).unwrap();
        let p_ba = GroundAtom::try_from(Atom::parse("P(b,a)").unwrap()).unwrap();
        let r_ba = GroundAtom::try_from(Atom::parse("R(b,a)").unwrap()).unwrap();
        let s_a = GroundAtom::try_from(Atom::parse("S(a)").unwrap()).unwrap();
        let t_a = GroundAtom::try_from(Atom::parse("T(a)").unwrap()).unwrap();

        let rule_1_index = 0;
        let rule_2_index = 1;
        let rule_3_index = 2;

        let mut program = Program::default();
        program.add_rule(rule_1);
        program.add_rule(rule_2);
        program.add_rule(rule_3);

        let mut trace = ExecutionTrace::new(program);

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
    #[cfg_attr(miri, ignore)]
    fn trace_ascii() {
        let trace = test_trace();
        let r_ba = GroundAtom::try_from(Atom::parse("R(b,a)").unwrap()).unwrap();

        let trace_r_ba = trace.find_fact(&r_ba).unwrap();

        let trace_string = r#" R(b, a) :- P(b, a), S(a) .
 ├─ P(b, a) :- Q(a, b) .
 │  └─ Q(a, b)
 └─ S(a) :- T(a) .
    └─ T(a)
"#;

        assert_eq!(
            trace.tree(trace_r_ba).unwrap().to_ascii_art(),
            trace_string.to_string()
        )
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn trace_json() {
        let trace = test_trace();

        let r_ba = GroundAtom::try_from(Atom::parse("R(b,a)").unwrap()).unwrap();
        let p_ba = GroundAtom::try_from(Atom::parse("P(b,a)").unwrap()).unwrap();

        let trace_r_ba = trace.find_fact(&r_ba).unwrap();
        let trace_p_ba = trace.find_fact(&p_ba).unwrap();
        let trace_handles = vec![trace_r_ba, trace_p_ba];

        let expected_json = r#"{"finalConclusion":["R(b, a)","P(b, a)"],"inferences":[{"rule":"R(?x, ?y) :- P(?x, ?y), S(?y) .","conclusion":"R(b, a)","premises":["P(b, a)","S(a)"]},{"rule":"S(?x) :- T(?x) .","conclusion":"S(a)","premises":["T(a)"]},{"rule":"Asserted","conclusion":"T(a)","premises":[]},{"rule":"P(?x, ?y) :- Q(?y, ?x) .","conclusion":"P(b, a)","premises":["Q(a, b)"]},{"rule":"Asserted","conclusion":"Q(a, b)","premises":[]}]}"#.to_string();
        let computed_json = serde_json::to_string(&trace.json(&trace_handles)).unwrap();

        assert_eq!(expected_json, computed_json);
    }
}
