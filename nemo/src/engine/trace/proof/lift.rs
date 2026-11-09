//! Lifting a proof onto the written program.
//!
//! The search finds proofs over the statements of the compiled program.
//! A node derived by a statement that a pipeline stage created is *lifted*:
//! it is mapped back onto the input rule that the statement replaces, one stage at a time,
//! until its statement belongs to the written program.
//!
//! - Its premises follow the literal correspondence that the stage recorded.
//!   A premise over a predicate that the stage introduced is replaced by the premises of its
//!   definition.
//!   A premise over a predicate that an import evaluates at its endpoint is replaced by
//!   *remote* leaves, which are facts queried at the endpoint instead of stored.
//! - Its assignment becomes the input rule's assignment.
//!   Each input variable gets the value of the term it became, if the evaluation computed one.
//!
//! A column whose value the evaluation never computed becomes the written term at its position,
//! and so does a constant of a written atom, which the [responder](super::Responder) evaluates.
//!
//! The lifted proofs of one response form a [graph](LiftedGraph) with one node per fact.

use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
};

use nemo_physical::{
    dictionary::Dictionary,
    management::{TableId, TableManager},
    storage::StorageValue,
    value::DataValue,
};

use crate::{
    engine::{
        Engine,
        compile::{compiled::CompiledProgram, translation::TranslationState},
        evaluate::body::combined_positions,
        trace::proof::{Explanation, ProofNode},
    },
    model::{
        Atom, Directive, ImportDirective, Literal, Rule, Statement, Substitution, Term, TermKind,
        Variable, VariableKind, Walk,
    },
    pipeline::{Correspondence, Pipeline, RewriteDeclaration, StatementId},
};

/// One column of a lifted fact.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum LiftedCell {
    /// A value the evaluation computed.
    Value(DataValue),
    /// A written term: a constant, still to be evaluated, or a variable
    /// whose value the evaluation never computed.
    Term(Term),
}

/// A fact of a proof over the written program, and how it holds.
#[derive(Debug)]
pub(crate) struct LiftedNode {
    /// The fact's predicate, by its resolved name.
    pub(crate) predicate: String,
    /// The fact's columns.
    pub(crate) terms: Vec<LiftedCell>,
    /// How the fact holds.
    pub(crate) proof: LiftedProof,
}

/// How a lifted fact holds.
#[derive(Debug)]
pub(crate) enum LiftedProof {
    /// The fact was loaded, or queried at an import's endpoint instead of stored.
    Leaf,
    /// The fact follows from the premises by an instance of a written
    /// statement.
    Derived {
        /// The written statement.
        statement: StatementId,
        /// The engine step of the application that derived the fact.
        step: u64,
        /// The values that the evaluation computed for the statement's variables,
        /// in the order in which the statement mentions them.
        assignment: Vec<(Variable, LiftedCell)>,
        /// One premise per positive literal, in order, by its id in the graph,
        /// or none for a summarized aggregate.
        premises: Vec<usize>,
        /// The predicate and columns of each negated literal.
        absences: Vec<(String, Vec<LiftedCell>)>,
        /// For an aggregate, the number of distinct contributions
        /// the group's first aggregate ranged over.
        matches: Option<usize>,
        /// Whether a head column calls a nondeterministic function, such as `RAND()`,
        /// whose value the search cannot reproduce.
        nondeterministic: bool,
    },
}

/// The key of a lifted stored fact, by which the graph lifts each fact once.
#[derive(Debug, PartialEq, Eq, Hash)]
struct LiftedKey {
    /// The fact's table.
    table: TableId,
    /// The fact's row.
    row: Vec<StorageValue>,
    /// The terms of the written literal that the fact was widened to,
    /// if its import drops columns.
    widened: Option<Vec<Term>>,
}

/// The key of a fact queried at an endpoint, by which the graph lists each such fact once.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RemoteKey {
    /// The fact's predicate.
    predicate: String,
    /// The fact's columns.
    terms: Vec<LiftedCell>,
}

/// The lifted proofs of one response, with one node per fact.
///
/// Each node comes after the nodes of its premises, which its derivation names by id.
/// The nodes are lifted in batches and taken out after each,
/// and the ids continue from batch to batch.
#[derive(Debug, Default)]
pub(crate) struct LiftedGraph {
    /// The id of the batch's first node.
    first: usize,
    /// The nodes of the batch, by id from `first` on.
    nodes: Vec<LiftedNode>,
    /// The id of each stored fact lifted.
    lifted: HashMap<LiftedKey, usize>,
    /// The id of each fact queried at an endpoint.
    remote: HashMap<RemoteKey, usize>,
}

impl LiftedGraph {
    /// Adds a node, and returns its id.
    fn push(&mut self, node: LiftedNode) -> usize {
        self.nodes.push(node);
        self.first + self.nodes.len() - 1
    }

    /// Returns the id of the leaf of a fact queried at an endpoint,
    /// and adds the leaf the first time.
    fn remote_leaf(&mut self, predicate: String, terms: Vec<LiftedCell>) -> usize {
        let key = RemoteKey { predicate, terms };
        if let Some(&id) = self.remote.get(&key) {
            return id;
        }
        let id = self.push(LiftedNode {
            predicate: key.predicate.clone(),
            terms: key.terms.clone(),
            proof: LiftedProof::Leaf,
        });
        self.remote.insert(key, id);
        id
    }

    /// Takes out the batch's nodes, so that the next batch's ids follow them.
    pub(crate) fn take(&mut self) -> Vec<LiftedNode> {
        self.first += self.nodes.len();
        std::mem::take(&mut self.nodes)
    }

    /// Returns the written ground terms of the batch's nodes, which the responder evaluates.
    pub(crate) fn constants(&self) -> Vec<Term> {
        let constant = |cell: &LiftedCell| match cell {
            LiftedCell::Term(term) if term.is_ground() => Some(term.clone()),
            _ => None,
        };
        let mut constants = Vec::new();
        for node in &self.nodes {
            constants.extend(node.terms.iter().filter_map(constant));
            if let LiftedProof::Derived {
                assignment,
                absences,
                ..
            } = &node.proof
            {
                constants.extend(assignment.iter().filter_map(|(_, cell)| constant(cell)));
                constants.extend(
                    absences
                        .iter()
                        .flat_map(|(_, cells)| cells.iter().filter_map(constant)),
                );
            }
        }
        constants
    }
}

/// The premise matching a positive literal of a derivation's rule.
#[derive(Debug, Clone)]
enum Premise {
    /// A proof node of the search.
    Node(Rc<ProofNode>),
    /// A fact queried at an import's endpoint instead of stored.
    Remote,
}

/// The derivation of a node by one statement, while it is lifted onto the written program.
#[derive(Debug)]
struct StageDerivation {
    /// The statement deriving the node.
    statement: StatementId,
    /// The rule whose positive literals the premises match.
    ///
    /// It is the statement's own rule, an import's view,
    /// or the rule that a bound import's binding atoms form.
    rule: Rule,
    /// The values that the evaluation computed for the rule's variables.
    assignment: HashMap<Variable, LiftedCell>,
    /// The premise that matches each literal of the rule,
    /// or `None` for a negated literal, a condition,
    /// or a positive literal that a summarized aggregate leaves out.
    premises: Vec<Option<Premise>>,
    /// The engine step of the application that derived the node.
    step: u64,
    /// For an aggregate, the number of distinct contributions
    /// the group's first aggregate ranged over.
    matches: Option<usize>,
    /// Whether a head column calls a nondeterministic function, such as `RAND()`,
    /// whose value the search cannot reproduce.
    nondeterministic: bool,
}

/// The lifting of proofs over one engine's program.
pub(crate) struct Lifter<'a> {
    /// The program pipeline.
    pipeline: &'a Pipeline,
    /// The compiled program.
    program: &'a CompiledProgram,
    /// The table store, for table names.
    manager: &'a TableManager,
    /// The dictionary, for decoding.
    dictionary: &'a Dictionary,
    /// The translation state, for names, declarations and the import plan.
    state: &'a TranslationState,
    /// For each predicate that a stage defined, by resolved name, whether an import evaluates
    /// it.
    ///
    /// The premises of the facts of a predicate that an import evaluates are remote.
    definitions: HashMap<String, bool>,
    /// For each predicate, the import that replaces a written rule deriving it.
    replacing: HashMap<String, StatementId>,
}

impl<'a> Lifter<'a> {
    /// Prepares the lifting over a loaded engine,
    /// and collects what the pipeline stages declared while they produced its compiled program.
    pub(crate) fn new(engine: &'a Engine) -> Self {
        let program = engine.program.as_ref().expect("a proof needs a program");
        let mut lifter = Self {
            pipeline: &engine.pipeline,
            program,
            manager: &engine.manager,
            dictionary: &engine.dictionary,
            state: &engine.translation,
            definitions: HashMap::new(),
            replacing: HashMap::new(),
        };
        if let Some(revision) = engine.revision {
            lifter.collect(
                engine
                    .pipeline
                    .statements(revision)
                    .map(|(id, _)| id)
                    .collect(),
            );
        }
        lifter
    }

    /// Lifts the proof of a traced fact into the graph, and returns its node's id.
    pub(crate) fn lift(&self, node: &ProofNode, graph: &mut LiftedGraph) -> usize {
        self.lift_at(node, None, graph)
    }

    /// Lifts a proof node into the graph unless it is there already, and returns its node's id.
    ///
    /// `at` contains the terms of the written literal that the node is a premise of, if any,
    /// in declaration order.
    fn lift_at(&self, node: &ProofNode, at: Option<&[Term]>, graph: &mut LiftedGraph) -> usize {
        let widened = at.filter(|written| written.len() > node.row.len());
        let key = LiftedKey {
            table: node.table,
            row: node.row.clone(),
            widened: widened.map(<[Term]>::to_vec),
        };
        if let Some(&id) = graph.lifted.get(&key) {
            return id;
        }

        let predicate = self.manager.name(node.table).to_owned();
        let terms = self.widen(&predicate, self.cells(&node.row), at);
        let lifted = match &node.proof {
            Explanation::Loaded if !self.replacing.contains_key(&predicate) => LiftedNode {
                predicate,
                terms,
                proof: LiftedProof::Leaf,
            },
            Explanation::Loaded => {
                let derivation = self.replacing_derivation(self.replacing[&predicate], &terms, 0);
                self.finish(self.climb(derivation), predicate, terms, graph)
            }
            Explanation::Derived { .. } => {
                let derivation = self.derivation(node, &terms);
                self.finish(self.climb(derivation), predicate, terms, graph)
            }
        };
        let id = graph.push(lifted);
        graph.lifted.insert(key, id);
        id
    }

    /// Collects the definitions and the imports that replace rules,
    /// following the provenance back from the given statements.
    fn collect(&mut self, mut pending: Vec<StatementId>) {
        let mut seen = HashSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let Some((declaration, sources)) = self.pipeline.declaration(id) else {
                continue;
            };
            match declaration {
                RewriteDeclaration::Replaces { view, .. } => {
                    if view.is_some()
                        && let Statement::Directive(Directive::Import(import)) =
                            self.pipeline.statement(id)
                        && let Some(name) = self.state.resolve(&import.predicate)
                    {
                        self.replacing.insert(name, id);
                    }
                    pending.push(sources[0]);
                }
                RewriteDeclaration::Defines { view } => {
                    let predicate = match self.pipeline.statement(id) {
                        Statement::Rule(rule) => &rule.head[0].predicate,
                        Statement::Directive(Directive::Import(import)) => &import.predicate,
                        Statement::Fact(_) | Statement::Directive(_) => continue,
                    };
                    if let Some(name) = self.state.resolve(predicate) {
                        self.definitions.insert(name, view.is_some());
                    }
                }
                RewriteDeclaration::DefinesFilter => {}
            }
        }
    }

    /// Returns the derivation of a derived proof node, at its statement of the
    /// compiled program.
    fn derivation(&self, node: &ProofNode, terms: &[LiftedCell]) -> StageDerivation {
        let Explanation::Derived {
            rule: index,
            step,
            witness,
            premises,
            matches,
            nondeterministic,
        } = &node.proof
        else {
            unreachable!("only a derived node has a derivation")
        };
        let statement = self.program.statements[*index];
        let rule = match self.pipeline.statement(statement) {
            Statement::Rule(rule) => rule.clone(),
            Statement::Directive(Directive::Import(import)) => {
                if let Some((RewriteDeclaration::Replaces { view: Some(_), .. }, _)) =
                    self.pipeline.declaration(statement)
                {
                    return self.replacing_derivation(statement, terms, *step);
                }
                binding_rule(import)
            }
            other => unreachable!("only rules and imports derive facts: {other}"),
        };

        let compiled = &self.program.rules[*index];
        let mut assignment = HashMap::new();
        if !witness.is_empty() {
            let positions = combined_positions(compiled);
            for (variable, name) in self.program.names[*index]
                .iter()
                .enumerate()
                .take(compiled.variables)
            {
                if let Some(name) = name {
                    assignment.insert(
                        Variable::universal(name),
                        LiftedCell::Value(self.decode(witness[positions[variable]])),
                    );
                }
            }
        }

        let mut slots = vec![None; rule.body.len()];
        let positives = rule
            .body
            .iter()
            .enumerate()
            .filter(|(_, literal)| matches!(literal, Literal::Positive(_)));
        for ((position, _), premise) in positives.zip(premises) {
            slots[position] = Some(Premise::Node(Rc::clone(premise)));
        }
        StageDerivation {
            statement,
            rule,
            assignment,
            premises: slots,
            step: *step,
            matches: *matches,
            nondeterministic: *nondeterministic,
        }
    }

    /// Returns the derivation of a fact delivered by an import that replaces a rule.
    ///
    /// The variables in the head of the import's view are bound to the fact's columns,
    /// and every positive literal of the view is remote.
    fn replacing_derivation(
        &self,
        statement: StatementId,
        terms: &[LiftedCell],
        step: u64,
    ) -> StageDerivation {
        let Some((
            RewriteDeclaration::Replaces {
                view: Some(view), ..
            },
            _,
        )) = self.pipeline.declaration(statement)
        else {
            unreachable!("an import that replaces a rule has a view")
        };
        let mut assignment = HashMap::new();
        for (term, cell) in view.head[0].terms.iter().zip(terms) {
            if let TermKind::Variable(variable) = &term.kind {
                assignment.insert(variable.clone(), cell.clone());
            }
        }
        let premises = view
            .body
            .iter()
            .map(|literal| matches!(literal, Literal::Positive(_)).then_some(Premise::Remote))
            .collect();
        StageDerivation {
            statement,
            rule: view.clone(),
            assignment,
            premises,
            step,
            matches: None,
            nondeterministic: false,
        }
    }

    /// Lifts a derivation back through every stage that rewrote its statement.
    fn climb(&self, mut derivation: StageDerivation) -> StageDerivation {
        while let Some((RewriteDeclaration::Replaces { correspondence, .. }, sources)) =
            self.pipeline.declaration(derivation.statement)
        {
            derivation = self.step_back(derivation, sources[0], correspondence);
        }
        derivation
    }

    /// Lifts a derivation one stage back, onto the input rule that its statement replaces.
    fn step_back(
        &self,
        derivation: StageDerivation,
        input: StatementId,
        correspondence: &Correspondence,
    ) -> StageDerivation {
        let Statement::Rule(rule) = self.pipeline.statement(input) else {
            unreachable!("a statement that replaces a rule is a rule or an import")
        };
        let assignment = correspondence
            .assignment
            .iter()
            .filter_map(|(variable, term)| {
                Some((variable.clone(), self.evaluate(term, &derivation)?))
            })
            .collect();
        let premises = rule
            .body
            .iter()
            .zip(&correspondence.literals)
            .map(|(literal, path)| match literal {
                Literal::Positive(_) => self.premise_at(&derivation, path),
                Literal::Negative(_) | Literal::Operation(_) => None,
            })
            .collect();
        StageDerivation {
            statement: input,
            rule: rule.clone(),
            assignment,
            premises,
            step: derivation.step,
            matches: derivation.matches,
            nondeterministic: derivation.nondeterministic,
        }
    }

    /// Returns the premise at a path of a derivation's unfolded rule.
    fn premise_at(&self, derivation: &StageDerivation, path: &[usize]) -> Option<Premise> {
        let (&position, rest) = path.split_first().expect("paths are not empty");
        if rest.is_empty() {
            return derivation.premises[position].clone();
        }
        match self.instance(derivation, position) {
            Some(definition) => self.premise_at(&definition, rest),
            None => Some(Premise::Remote),
        }
    }

    /// Returns the derivation of the definition unfolded at a literal,
    /// with its proof node lifted onto the defining statement.
    ///
    /// Returns `None` for a definition that an import evaluates,
    /// and if no derived proof node matches the literal.
    fn instance(&self, derivation: &StageDerivation, position: usize) -> Option<StageDerivation> {
        let atom = derivation.rule.body[position].atom()?;
        let name = self.state.resolve(&atom.predicate)?;
        if *self.definitions.get(&name)? {
            return None;
        }
        let Some(Premise::Node(node)) = &derivation.premises[position] else {
            return None;
        };
        if !matches!(node.proof, Explanation::Derived { .. }) {
            return None;
        }
        let terms = self.cells(&node.row);
        Some(self.climb(self.derivation(node, &terms)))
    }

    /// Returns the value of a term of an unfolded rule,
    /// or `None` if the evaluation never computed it.
    ///
    /// - A variable of the rule gets its value from the derivation's assignment.
    /// - A variable that a definition renamed apart, as `name~path`,
    ///   gets its value from the instance of that definition.
    /// - A ground term stays as written.
    fn evaluate(&self, term: &Term, derivation: &StageDerivation) -> Option<LiftedCell> {
        if term.is_ground() {
            return Some(LiftedCell::Term(term.clone()));
        }
        let TermKind::Variable(variable) = &term.kind else {
            return None;
        };
        if variable.kind == VariableKind::Anonymous {
            return None;
        }
        let Some((name, path)) = variable.name.rsplit_once('~') else {
            return derivation.assignment.get(variable).cloned();
        };
        let mut instance: Option<StageDerivation> = None;
        for step in path.split('.') {
            let position: usize = step.parse().expect("renamed paths are positions");
            let current = instance.as_ref().unwrap_or(derivation);
            instance = Some(self.instance(current, position)?);
        }
        instance?
            .assignment
            .get(&Variable {
                kind: variable.kind,
                name: name.to_owned(),
            })
            .cloned()
    }

    /// Completes a derivation at a written statement into a node,
    /// with its premises lifted into the graph and its negated literals instantiated.
    fn finish(
        &self,
        derivation: StageDerivation,
        predicate: String,
        terms: Vec<LiftedCell>,
        graph: &mut LiftedGraph,
    ) -> LiftedNode {
        let mut premises = Vec::new();
        let mut absences = Vec::new();
        for (literal, premise) in derivation.rule.body.iter().zip(&derivation.premises) {
            match literal {
                Literal::Positive(atom) => {
                    let written = self.state.positional(atom);
                    match premise {
                        Some(Premise::Node(node)) => {
                            premises.push(self.lift_at(node, Some(&written), graph))
                        }
                        Some(Premise::Remote) => premises.push(graph.remote_leaf(
                            self.name(atom),
                            instantiate(&written, &derivation.assignment),
                        )),
                        None => {}
                    }
                }
                Literal::Negative(atom) => absences.push((
                    self.name(atom),
                    instantiate(&self.state.positional(atom), &derivation.assignment),
                )),
                Literal::Operation(_) => {}
            }
        }
        LiftedNode {
            predicate,
            terms,
            proof: LiftedProof::Derived {
                statement: derivation.statement,
                step: derivation.step,
                assignment: ordered(&derivation.rule, &derivation.assignment),
                premises,
                absences,
                matches: derivation.matches,
                nondeterministic: derivation.nondeterministic,
            },
        }
    }

    /// Returns a stored row's values as cells.
    fn cells(&self, row: &[StorageValue]) -> Vec<LiftedCell> {
        row.iter()
            .map(|&value| LiftedCell::Value(self.decode(value)))
            .collect()
    }

    /// Returns a stored value, decoded.
    fn decode(&self, value: StorageValue) -> DataValue {
        self.dictionary
            .decode(value)
            .expect("stored values decode against their dictionary")
            .to_owned()
    }

    /// Widens a stored row to the width of the written literal `at`,
    /// for a predicate whose import drops columns.
    ///
    /// Each dropped column becomes the literal's term at its position.
    fn widen(
        &self,
        predicate: &str,
        cells: Vec<LiftedCell>,
        at: Option<&[Term]>,
    ) -> Vec<LiftedCell> {
        let Some(written) = at.filter(|written| written.len() > cells.len()) else {
            return cells;
        };
        let layout = self.state.plan.layout_of(predicate, written.len());
        written
            .iter()
            .enumerate()
            .map(|(kept, term)| match layout.delivered_position(kept) {
                Some(delivered) => cells[delivered].clone(),
                None => LiftedCell::Term(term.clone()),
            })
            .collect()
    }

    /// Returns the resolved name of an atom's predicate.
    fn name(&self, atom: &Atom) -> String {
        self.state
            .resolve(&atom.predicate)
            .unwrap_or_else(|| atom.predicate.to_string())
    }
}

/// Returns the rule that a bound import's fetch applies,
/// which derives the import's atom from its binding atoms.
fn binding_rule(import: &ImportDirective) -> Rule {
    let atom = Atom::new(
        import.predicate.clone(),
        import
            .variables
            .iter()
            .map(|name| Term::variable(Variable::universal(name))),
    );
    Rule::new(
        vec![atom],
        import
            .bindings
            .iter()
            .cloned()
            .map(Literal::Positive)
            .collect(),
    )
}

/// Returns the cells of written terms under an assignment.
///
/// A variable becomes its value if the evaluation computed one, and stays as written otherwise.
/// A compound term, such as `STRLANG(?org, "en")`, gets the values of its variables
/// if the evaluation computed all of them, so the responder can evaluate it.
fn instantiate(terms: &[Term], assignment: &HashMap<Variable, LiftedCell>) -> Vec<LiftedCell> {
    terms
        .iter()
        .map(|term| match &term.kind {
            TermKind::Variable(variable) => assignment
                .get(variable)
                .cloned()
                .unwrap_or_else(|| LiftedCell::Term(term.clone())),
            _ => LiftedCell::Term(substituted(term, assignment)),
        })
        .collect()
}

/// Returns a written term with the values of its variables in their place if that makes it
/// ground, and the term as written otherwise.
fn substituted(term: &Term, assignment: &HashMap<Variable, LiftedCell>) -> Term {
    if term.is_ground() {
        return term.clone();
    }
    let substitution: Substitution = assignment
        .iter()
        .filter_map(|(variable, cell)| {
            let term = match cell {
                LiftedCell::Value(value) => Term::value(value)?,
                LiftedCell::Term(term) => term.clone(),
            };
            Some((variable.clone(), term))
        })
        .collect();
    let mut instance = term.clone();
    substitution.apply(&mut instance);
    if instance.is_ground() {
        instance
    } else {
        term.clone()
    }
}

/// Returns the known part of an assignment,
/// in the order in which the rule mentions its universal variables.
///
/// The variables of the positive literals come first, then those of the rest of the body,
/// then those of the head.
fn ordered(rule: &Rule, assignment: &HashMap<Variable, LiftedCell>) -> Vec<(Variable, LiftedCell)> {
    let positives = rule
        .body
        .iter()
        .filter(|literal| matches!(literal, Literal::Positive(_)));
    let others = rule
        .body
        .iter()
        .filter(|literal| !matches!(literal, Literal::Positive(_)));
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    let variables = positives
        .flat_map(|literal| literal.variables())
        .chain(others.flat_map(|literal| literal.variables()))
        .chain(rule.head.iter().flat_map(|atom| atom.variables()));
    for variable in variables {
        if variable.kind == VariableKind::Universal
            && seen.insert(variable.clone())
            && let Some(cell) = assignment.get(variable)
        {
            ordered.push((variable.clone(), cell.clone()));
        }
    }
    ordered
}
