use std::collections::{hash_map::Entry, HashMap, HashSet};

use petgraph::{visit::Dfs, Undirected};

use crate::{
    logical::{
        model::{Atom, Identifier, Program, Rule, Term, Variable},
        types::LogicalTypeEnum,
    },
    physical::util::labeled_graph::NodeLabeledGraph,
};

use super::variable_order::{build_preferable_variable_orders, VariableOrder};

/// Contains useful information for a (existential) rule
#[derive(Debug, Clone)]
pub struct RuleAnalysis {
    /// Whether it uses an existential variable in its head.
    pub is_existential: bool,
    /// Whether an atom in the head also occurs in the body.
    pub is_recursive: bool,
    /// Whether the rule has filter that need to be applied.
    pub has_filters: bool,

    /// Predicates appearing in the body.
    pub body_predicates: HashSet<Identifier>,
    /// Predicates appearing in the head.
    pub head_predicates: HashSet<Identifier>,

    /// Variables occuring in the body.
    pub body_variables: HashSet<Variable>,
    /// Variables occuring in the head.
    pub head_variables: HashSet<Variable>,
    /// Number of existential variables.
    pub num_existential: usize,

    /// Table identifier for storing head matches for the restricted chase
    pub head_matches_identifier: Identifier,

    /// Variable orders that are worth considering.
    pub promising_variable_orders: Vec<VariableOrder>,

    /// Logical Type of each Variable
    pub variable_types: HashMap<Variable, LogicalTypeEnum>,
    /// Logical Type of predicates in Rule
    pub predicate_types: HashMap<Identifier, Vec<LogicalTypeEnum>>,
}

fn is_recursive(rule: &Rule) -> bool {
    rule.head().iter().any(|h| {
        rule.body()
            .iter()
            .filter(|b| b.is_positive())
            .any(|b| h.predicate() == b.predicate())
    })
}

fn count_distinct_existential_variables(rule: &Rule) -> usize {
    let mut existentials = HashSet::<Variable>::new();

    for head_atom in rule.head() {
        for term in head_atom.terms() {
            if let Term::Variable(Variable::Existential(id)) = term {
                existentials.insert(Variable::Existential(id.clone()));
            }
        }
    }

    existentials.len()
}

fn get_variables(atoms: &[&Atom]) -> HashSet<Variable> {
    let mut result = HashSet::new();
    for atom in atoms {
        for term in atom.terms() {
            if let Term::Variable(v) = term {
                result.insert(v.clone());
            }
        }
    }
    result
}

fn get_predicates(atoms: &[&Atom]) -> HashSet<Identifier> {
    atoms.iter().map(|a| a.predicate()).collect()
}

fn get_fresh_rule_predicate(rule_index: usize) -> Identifier {
    Identifier(format!(
        "FRESH_HEAD_MATCHES_IDENTIFIER_FOR_RULE_{rule_index}"
    ))
}

fn analyze_rule(
    rule: &Rule,
    promising_variable_orders: Vec<VariableOrder>,
    rule_index: usize,
    type_declarations: &HashMap<Identifier, Vec<LogicalTypeEnum>>,
) -> RuleAnalysis {
    let body_atoms: Vec<&Atom> = rule.body().iter().map(|l| l.atom()).collect();
    let head_atoms: Vec<&Atom> = rule.head().iter().collect();

    let num_existential = count_distinct_existential_variables(rule);

    // Check if type declarations are violated; add them if they do not exist
    let mut variable_types: HashMap<Variable, LogicalTypeEnum> = HashMap::new();
    for atom in body_atoms.iter().chain(head_atoms.iter()) {
        for (term_position, term) in atom.terms().iter().enumerate() {
            if let Term::Variable(variable) = term {
                if let Entry::Vacant(entry) = variable_types.entry(variable.clone()) {
                    let assigned_type = type_declarations
                        .get(&atom.predicate())
                        .expect("Every predicate should have recived type information.")
                        [term_position];

                    entry.insert(assigned_type);
                }
            }
        }
    }

    let rule_preds: Vec<Identifier> = body_atoms
        .iter()
        .chain(head_atoms.iter())
        .map(|a| a.predicate())
        .collect();

    RuleAnalysis {
        is_existential: num_existential > 0,
        is_recursive: is_recursive(rule),
        has_filters: !rule.filters().is_empty(),
        body_predicates: get_predicates(&body_atoms),
        head_predicates: get_predicates(&head_atoms),
        body_variables: get_variables(&body_atoms),
        head_variables: get_variables(&head_atoms),
        num_existential,
        // TODO: not sure if this is a good way to introduce a fresh head identifier; I'm not quite sure why it is even required
        head_matches_identifier: get_fresh_rule_predicate(rule_index),
        promising_variable_orders,
        variable_types,
        predicate_types: type_declarations
            .iter()
            .filter_map(|(k, v)| rule_preds.contains(k).then(|| (k.clone(), v.clone())))
            .collect(),
    }
}

/// Identifies a position within a predicate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PredicatePosition {
    predicate: Identifier,
    position: usize,
}

impl PredicatePosition {
    /// Create new [`PredicatePosition`].
    pub fn new(predicate: Identifier, position: usize) -> Self {
        Self {
            predicate,
            position,
        }
    }
}

/// Graph that represents a prioritization between rules.
pub type PositionGraph = NodeLabeledGraph<PredicatePosition, Undirected>;

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
    pub predicate_types: HashMap<Identifier, Vec<LogicalTypeEnum>>,
    /// Graph representing the information flow between predicates
    pub position_graph: PositionGraph,
}

impl Program {
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
    fn get_all_predicates(&self) -> HashSet<(Identifier, usize)> {
        let mut result = HashSet::<(Identifier, usize)>::new();

        // Predicates in source statments
        for ((predicate, arity), _) in self.sources() {
            result.insert((predicate.clone(), arity));
        }

        // Predicates in rules
        for rule in self.rules() {
            for body_atom in rule.body() {
                result.insert((body_atom.predicate(), body_atom.terms().len()));
            }

            for head_atom in rule.head() {
                result.insert((head_atom.predicate(), head_atom.terms().len()));
            }
        }

        // Predicates in facts
        for fact in self.facts() {
            result.insert((fact.0.predicate(), fact.0.terms().len()));
        }

        // Additional predicates for existential rules
        for (rule_index, rule) in self.rules().iter().enumerate() {
            let is_existential = count_distinct_existential_variables(rule) > 0;
            if !is_existential {
                continue;
            }

            let body_atoms: Vec<&Atom> = rule.body().iter().map(|l| l.atom()).collect();
            let head_atoms: Vec<&Atom> = rule.head().iter().collect();

            let body_variables = get_variables(&body_atoms);
            let head_variables = get_variables(&head_atoms);

            let predicate = get_fresh_rule_predicate(rule_index);
            let arity = head_variables.difference(&body_variables).count();

            result.insert((predicate, arity));
        }

        result
    }

    fn build_position_graph(&self) -> PositionGraph {
        let mut graph = PositionGraph::default();

        for rule in self.rules() {
            let body_atoms: Vec<&Atom> = rule.body().iter().map(|l| l.atom()).collect();
            let head_atoms: Vec<&Atom> = rule.head().iter().collect();

            let mut variable_to_last_node = HashMap::<Variable, PredicatePosition>::new();

            for atom in body_atoms.iter().chain(head_atoms.iter()) {
                for (term_position, term) in atom.terms().iter().enumerate() {
                    if let Term::Variable(variable) = term {
                        let predicate_position =
                            PredicatePosition::new(atom.predicate(), term_position);

                        match variable_to_last_node.entry(variable.clone()) {
                            Entry::Occupied(mut entry) => {
                                let last_position = entry.insert(predicate_position.clone());
                                graph.add_edge(last_position, predicate_position);
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(predicate_position);
                            }
                        }
                    }
                }
            }
        }

        graph
    }

    fn infer_predicate_types(
        &self,
        position_graph: &PositionGraph,
        all_predicates: &HashSet<(Identifier, usize)>,
    ) -> HashMap<Identifier, Vec<LogicalTypeEnum>> {
        // Set all predicate types to unknown
        let mut predicate_types: HashMap<Identifier, Vec<Option<LogicalTypeEnum>>> = all_predicates
            .iter()
            .map(|(predicate, arity)| (predicate.clone(), vec![None; *arity]))
            .collect();

        // For now we set the type of each variable that occurrs together with an existential variable in the head
        // to `LogicalTypeEnum::RdfsResource`
        // TODO: Only set this type for the positions where there is an existential variables
        for rule in self.rules() {
            if count_distinct_existential_variables(rule) > 0 {
                for atom in rule.head() {
                    predicate_types.insert(
                        atom.predicate(),
                        vec![Some(LogicalTypeEnum::RdfsResource); atom.terms().len()],
                    );
                }
            }
        }

        // Propagate each type from its declaration
        for (predicate, types) in self.parsed_predicate_declarations() {
            for (position, logical_type) in types.into_iter().enumerate() {
                let predicate_position = PredicatePosition::new(predicate.clone(), position);

                if let Some(start_node) = position_graph.get_node(&predicate_position) {
                    let mut dfs = Dfs::new(position_graph.graph(), start_node);

                    while let Some(next_node) = dfs.next(position_graph.graph()) {
                        let next_position = position_graph
                            .graph()
                            .node_weight(next_node)
                            .expect("The DFS iterator guarantees that every node exists.");

                        let current_type_opt = &mut predicate_types
                            .get_mut(&next_position.predicate)
                            .expect("The initialization step inserted every known predicate")
                            [next_position.position];

                        if let Some(current_type) = current_type_opt {
                            if *current_type != logical_type {
                                // TODO: Throw an error
                                panic!("Incompatible type declarations.");
                            }
                        } else {
                            *current_type_opt = Some(logical_type);
                        }
                    }
                }
            }
        }

        // All the types that are not set will be mapped to a default type
        predicate_types
            .into_iter()
            .map(|(predicate, types)| {
                (
                    predicate,
                    types.into_iter().map(|t| t.unwrap_or_default()).collect(),
                )
            })
            .collect()
    }

    /// Analyze itself and return a struct containing the results.
    pub fn analyze(&self) -> ProgramAnalysis {
        let variable_orders = build_preferable_variable_orders(self, None);

        let all_predicates = self.get_all_predicates();
        let derived_predicates = self.get_head_predicates();

        let position_graph = self.build_position_graph();
        let predicate_types = self.infer_predicate_types(&position_graph, &all_predicates);

        let rule_analysis: Vec<RuleAnalysis> = self
            .rules()
            .iter()
            .enumerate()
            .map(|(i, r)| analyze_rule(r, variable_orders[i].clone(), i, &predicate_types))
            .collect();

        ProgramAnalysis {
            rule_analysis,
            derived_predicates,
            all_predicates,
            predicate_types,
            position_graph,
        }
    }
}
