//! The response: lifted proofs converted to the [crate::trace] protocol types,
//! over the written program.

use std::collections::HashMap;

use nemo_physical::{management::TableId, storage::StorageValue, value::DataValue};

use crate::{
    engine::{
        Engine,
        trace::proof::lift::{LiftedCell, LiftedGraph, LiftedProof},
    },
    model::{Identifier, Span, Statement, Term, TermKind},
    pipeline::StatementId,
    trace::{
        AggregateSummary, Derivation, SourceLocation, TraceNode, TraceResponse, TracedFact,
        VERSION, VariableBinding, WrittenRule, WrittenStatement, fact_text,
    },
};

/// Builds the protocol response from lifted proofs.
pub(crate) struct Responder<'a> {
    /// The traced engine.
    engine: &'a Engine,
    /// The canonical rendering of each written constant the proofs
    /// hold, for the constants that evaluate.
    canonical_constants: HashMap<Term, String>,
}

impl<'a> Responder<'a> {
    /// Prepares the responder.
    pub(crate) fn new(engine: &'a Engine, canonical_constants: HashMap<Term, String>) -> Self {
        Self {
            engine,
            canonical_constants,
        }
    }

    /// Returns the text of a fact from decoded values.
    fn fact(&self, table: TableId, values: &[DataValue]) -> String {
        let terms: Vec<String> = values
            .iter()
            .map(|value| self.engine.term_text(value.as_ref()))
            .collect();
        fact_text(self.engine.manager.name(table), &terms)
    }

    /// Returns a stored value in canonical rendering.
    pub(crate) fn rendered(&self, value: StorageValue) -> String {
        let value = self
            .engine
            .dictionary
            .decode(value)
            .expect("stored values decode against their dictionary");
        self.engine.term_text(value)
    }

    /// Returns the text of a lifted cell: a value or an evaluated constant in canonical
    /// rendering, a variable as written.
    fn cell(&self, cell: &LiftedCell) -> String {
        match cell {
            LiftedCell::Value(value) => self.engine.term_text(value.as_ref()),
            LiftedCell::Term(term) => self.term(term),
        }
    }

    /// Returns the text of a written term: an evaluated constant in canonical rendering,
    /// anything else as written.
    pub(crate) fn term(&self, term: &Term) -> String {
        self.canonical_constants
            .get(term)
            .cloned()
            .unwrap_or_else(|| term.to_string())
    }

    /// Returns the text of a fact from lifted cells.
    fn lifted_fact(&self, predicate: &str, cells: &[LiftedCell]) -> String {
        let terms: Vec<String> = cells.iter().map(|cell| self.cell(cell)).collect();
        fact_text(predicate, &terms)
    }

    /// Returns the response for the requested facts, given by table and decoded values,
    /// from their lifted proofs: the graph, and each fact's node in it if the fact holds.
    ///
    /// Leaves with the same fact become one node, and every rule is listed once.
    pub(super) fn response(
        &self,
        facts: &[(TableId, Vec<DataValue>)],
        graph: &LiftedGraph,
        roots: &[Option<usize>],
    ) -> TraceResponse {
        let mut nodes = Vec::with_capacity(graph.nodes.len());
        let mut rules = Vec::new();
        // The response's id per node of the graph, and per leaf fact and rule.
        let mut ids = Vec::with_capacity(graph.nodes.len());
        let mut leaves: HashMap<String, usize> = HashMap::new();
        let mut listed: HashMap<StatementId, usize> = HashMap::new();
        for lifted in &graph.nodes {
            let fact = self.lifted_fact(&lifted.predicate, &lifted.terms);
            let id = match &lifted.proof {
                LiftedProof::Leaf => *leaves.entry(fact).or_insert_with_key(|fact| {
                    nodes.push(TraceNode {
                        fact: fact.clone(),
                        derivation: None,
                    });
                    nodes.len() - 1
                }),
                LiftedProof::Derived {
                    statement,
                    step,
                    assignment,
                    premises,
                    absences,
                    matches,
                    nondeterministic,
                } => {
                    let rule = *listed.entry(*statement).or_insert_with(|| {
                        rules.push(written_rule(self.engine, *statement));
                        rules.len() - 1
                    });
                    nodes.push(TraceNode {
                        fact,
                        derivation: Some(Derivation {
                            rule,
                            step: *step,
                            assignment: assignment
                                .iter()
                                .map(|(variable, cell)| VariableBinding {
                                    variable: variable.to_string(),
                                    value: self.cell(cell),
                                })
                                .collect(),
                            premises: premises.iter().map(|&premise| ids[premise]).collect(),
                            absences: absences
                                .iter()
                                .map(|(predicate, cells)| self.lifted_fact(predicate, cells))
                                .collect(),
                            aggregate: matches.map(|matches| AggregateSummary { matches }),
                            nondeterministic: *nondeterministic,
                        }),
                    });
                    nodes.len() - 1
                }
            };
            ids.push(id);
        }

        let facts = facts
            .iter()
            .zip(roots)
            .map(|((table, values), root)| TracedFact {
                fact: self.fact(*table, values),
                node: root.map(|root| ids[root]),
            })
            .collect();
        TraceResponse {
            version: VERSION,
            facts,
            nodes,
            rules,
        }
    }
}

/// Returns a span as a protocol source position.
fn source_location(engine: &Engine, span: Span) -> Option<SourceLocation> {
    let (line, column) = engine.pipeline.line_column(span)?;
    Some(SourceLocation {
        file: engine.pipeline.file_name(span.file)?.to_owned(),
        start: span.start,
        end: span.end,
        line,
        column,
    })
}

/// Returns the rule reference of a written statement: its id, text, `#[name]`,
/// source, and the `@use` that instantiated it.
///
/// # Panics
///
/// Panics for a statement the written program does not hold.
pub(crate) fn written_rule(engine: &Engine, statement: StatementId) -> WrittenRule {
    let pipeline = &engine.pipeline;
    let written = pipeline.statement(statement);
    let name = match written {
        Statement::Rule(rule) => rule.attributes.iter().find_map(|attribute| {
            matches!(&attribute.name, Identifier::Bare(bare) if bare == "name")
                .then(|| attribute.arguments.first())
                .flatten()
                .and_then(|argument| match &argument.kind {
                    TermKind::String(text) => Some(text.clone()),
                    _ => None,
                })
        }),
        _ => None,
    };
    let instances = pipeline
        .source_span(statement)
        .map(|span| pipeline.instances(span.file))
        .unwrap_or_default()
        .into_iter()
        .map(|using| WrittenStatement {
            text: using.to_string(),
            source: using.span.and_then(|span| source_location(engine, span)),
        })
        .collect();
    WrittenRule {
        rule: engine
            .written
            .as_ref()
            .and_then(|program| program.id(statement))
            .expect("rule references name written statements"),
        text: written.to_string(),
        name,
        source: pipeline
            .source_span(statement)
            .and_then(|span| source_location(engine, span)),
        instances,
    }
}
