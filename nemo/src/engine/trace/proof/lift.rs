//! Lifting a proof onto the written program.
//!
//! The search finds proofs over the statements of the compiled program.
//! A node derived by a statement a pipeline stage created is *lifted*:
//! mapped back onto the input rule the statement replaces, one stage at a time,
//! until its statement belongs to the written program.
//!
//! - Its premises follow the literal correspondence recorded for the stage.
//!   A premise over a predicate the stage introduced is replaced by the premises of its
//!   definition.
//!   A premise over a predicate an import evaluates at its endpoint is replaced by *remote*
//!   leaves: facts queried at the endpoint instead of stored.
//! - Its assignment becomes the input rule's:
//!   each input variable takes the value of the term it became, if the evaluation computed one.
//!
//! A column whose value the evaluation never computed becomes the written term at its position,
//! and so does a constant of a written atom, which the responder evaluates.

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
        evaluate::{body::combined_positions, extensional::ExtensionalOrigin},
        trace::proof::{Proof, ProofNode},
    },
    model::{
        Atom, Directive, ImportDirective, Literal, Rule, Statement, Substitution, Term, TermKind,
        Variable, VariableKind, Walk,
    },
    pipeline::{Correspondence, Pipeline, RewriteDeclaration, StatementId},
};

/// One column of a lifted fact.
#[derive(Debug, Clone)]
pub(crate) enum LiftedCell {
    /// A value the evaluation computed.
    Value(DataValue),
    /// A written term: a constant, still to be evaluated, or a variable
    /// whose value the evaluation never computed.
    Term(Term),
}

/// A proof over the written program.
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
    /// The fact was stored as given.
    Asserted {
        /// Where its rows came from.
        origin: ExtensionalOrigin,
    },
    /// The fact was queried at an import's endpoint instead of stored.
    Remote,
    /// The fact's derivation is shown at an earlier node of the same proof.
    Repeated,
    /// The fact follows from the premises by an instance of a written
    /// statement.
    Derived {
        /// The written statement.
        statement: StatementId,
        /// The engine step of the producing application.
        step: u64,
        /// The values the evaluation computed for the statement's variables,
        /// in the order the statement mentions them.
        assignment: Vec<(Variable, LiftedCell)>,
        /// One proof per positive literal, in order, or none for a summarized aggregate.
        premises: Vec<LiftedNode>,
        /// Per negated literal, its predicate and columns.
        absences: Vec<(String, Vec<LiftedCell>)>,
        /// For an aggregate, the number of distinct contributions
        /// the group's first aggregate ranged over.
        matches: Option<usize>,
        /// Whether the statement calls a nondeterministic function
        /// (such as `RAND()` or `STRUUID()`), whose value the search cannot reproduce.
        nondeterministic: bool,
    },
}

impl LiftedNode {
    /// Collects the written constants of the proof, for the responder to evaluate.
    pub(crate) fn constants(&self, into: &mut Vec<Term>) {
        let constant = |cell: &LiftedCell| match cell {
            LiftedCell::Term(term) if term.is_ground() => Some(term.clone()),
            _ => None,
        };
        into.extend(self.terms.iter().filter_map(constant));
        if let LiftedProof::Derived {
            assignment,
            premises,
            absences,
            ..
        } = &self.proof
        {
            into.extend(assignment.iter().filter_map(|(_, cell)| constant(cell)));
            into.extend(
                absences
                    .iter()
                    .flat_map(|(_, cells)| cells.iter().filter_map(constant)),
            );
            for premise in premises {
                premise.constants(into);
            }
        }
    }
}

/// The stored facts a proof expanded already, by table and row.
type Expanded = HashSet<(TableId, Vec<StorageValue>)>;

/// The premise matching a positive literal of a derivation's rule.
#[derive(Debug, Clone)]
enum Premise {
    /// A proof node of the search.
    Node(Rc<ProofNode>),
    /// A fact queried at an import's endpoint instead of stored.
    Remote,
}

/// A node derived by one statement, on its way to the written program.
#[derive(Debug)]
struct StageDerivation {
    /// The statement deriving the node.
    statement: StatementId,
    /// The rule whose positive literals the premises match: the statement's
    /// own, an import's view, or a bound import's binding atoms.
    rule: Rule,
    /// The values the evaluation computed for the rule's variables.
    assignment: HashMap<Variable, LiftedCell>,
    /// Per literal of the rule, the premise matching it,
    /// or `None` for a negated literal, a condition,
    /// or a positive literal a summarized aggregate leaves out.
    premises: Vec<Option<Premise>>,
    /// The engine step of the producing application.
    step: u64,
    /// For an aggregate, the number of distinct contributions
    /// the group's first aggregate ranged over.
    matches: Option<usize>,
    /// Whether the statement calls a nondeterministic function
    /// (such as `RAND()` or `STRUUID()`), whose value the search cannot reproduce.
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
    /// The translation state, for names, declarations and the import
    /// plan.
    state: &'a TranslationState,
    /// Per predicate a stage defined, by resolved name, whether an import evaluates it,
    /// in which case its facts' premises are remote.
    definitions: HashMap<String, bool>,
    /// Per predicate, the import that replaces a written rule deriving it.
    replacing: HashMap<String, StatementId>,
}

impl<'a> Lifter<'a> {
    /// Prepares the lifting over a loaded engine, collecting what the
    /// pipeline stages declared while producing its compiled program.
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

    /// Lifts the proof of a traced fact.
    ///
    /// The proof expands each derived fact once, in depth-first order,
    /// and a later occurrence is a [repeated](LiftedProof::Repeated) leaf.
    pub(crate) fn lift(&self, node: &ProofNode) -> LiftedNode {
        self.lift_at(node, None, &mut Expanded::new())
    }

    /// Lifts a proof node, given the facts its proof expanded already.
    ///
    /// `at` holds the terms of the written literal the node is a premise of, if any,
    /// in declaration order.
    fn lift_at(
        &self,
        node: &ProofNode,
        at: Option<&[Term]>,
        expanded: &mut Expanded,
    ) -> LiftedNode {
        let predicate = self.manager.name(node.table).to_owned();
        let terms = self.widen(&predicate, self.cells(&node.row), at);
        if let Proof::Asserted { origin } = &node.proof
            && !self.replacing.contains_key(&predicate)
        {
            return LiftedNode {
                predicate,
                terms,
                proof: LiftedProof::Asserted { origin: *origin },
            };
        }
        if !expanded.insert((node.table, node.row.clone())) {
            return LiftedNode {
                predicate,
                terms,
                proof: LiftedProof::Repeated,
            };
        }
        let derivation = match &node.proof {
            Proof::Asserted { .. } => {
                self.replacing_derivation(self.replacing[&predicate], &terms, 0)
            }
            Proof::Derived { .. } => self.derivation(node, &terms),
        };
        let derivation = self.climb(derivation);
        self.finish(derivation, predicate, terms, expanded)
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
        let Proof::Derived {
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

    /// Returns the derivation of a fact delivered by an import that replaces a rule:
    /// its view's head takes the fact's columns, and every
    /// positive literal of the view is remote.
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

    /// Lifts a derivation one stage back, onto the input rule its statement replaces.
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
    /// its proof node lifted onto the defining statement.
    ///
    /// Returns `None` for a definition an import evaluates,
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
        if !matches!(node.proof, Proof::Derived { .. }) {
            return None;
        }
        let terms = self.cells(&node.row);
        Some(self.climb(self.derivation(node, &terms)))
    }

    /// Returns the value of a term of an unfolded rule,
    /// or `None` if the evaluation never computed it.
    ///
    /// A variable of the rule takes it from the derivation's assignment,
    /// a variable a definition renamed apart (`name~path`) from that definition's instance,
    /// and a constant is as written.
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

    /// Completes a derivation at a written statement: its premises lifted in
    /// turn, its negated literals instantiated.
    fn finish(
        &self,
        derivation: StageDerivation,
        predicate: String,
        terms: Vec<LiftedCell>,
        expanded: &mut Expanded,
    ) -> LiftedNode {
        let mut premises = Vec::new();
        let mut absences = Vec::new();
        for (literal, premise) in derivation.rule.body.iter().zip(&derivation.premises) {
            match literal {
                Literal::Positive(atom) => {
                    let written = self.state.positional(atom);
                    match premise {
                        Some(Premise::Node(node)) => {
                            premises.push(self.lift_at(node, Some(&written), expanded))
                        }
                        Some(Premise::Remote) => premises.push(LiftedNode {
                            predicate: self.name(atom),
                            terms: instantiate(&written, &derivation.assignment),
                            proof: LiftedProof::Remote,
                        }),
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

    /// Widens a stored row to the written literal's width, for a predicate
    /// whose import drops columns: each dropped column takes the literal's term.
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

/// Returns the rule a bound import's fetch applies: the import's atom
/// derived from its binding atoms.
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
/// A variable takes its value if the evaluation computed one, and stays as written otherwise.
/// A compound term, such as `STRLANG(?org, "en")`, takes the values of its variables
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

/// Returns a written term with the values of its variables in their place,
/// or the term as written unless that makes it ground.
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

/// Returns the known part of an assignment, in the order the rule
/// mentions its universal variables: its positive literals first, then
/// the rest of its body, then its head.
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
