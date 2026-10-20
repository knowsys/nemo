//! The response: every written node's entries with ids and pagination,
//! its written rule neighborhoods, and its expansion,
//! flat for a shaped request and nested for an expanded one.

use std::{
    collections::{HashMap, HashSet},
    ops::{ControlFlow, Range},
};

use nemo_physical::{
    management::{ColumnOrder, Revision, TableId},
    storage::StorageValue,
};

use crate::{
    engine::{
        Engine,
        trace::{
            proof::Responder,
            query::request::{Neighborhood, NodeFacts, QueryArenas, WrittenNode},
        },
    },
    model::{Atom, Directive, Identifier, Literal, Statement, Term},
    pipeline::StatementId,
    trace::query::{
        MetaInformation, NodeResponse, PageInfo, PredicateWithParameters, QueryRule, TableEntries,
        TableEntry, TreeChildren, TreeForTableResponse,
    },
};

/// Returns the written constants a response renders: the written
/// columns of remote facts.
pub(super) fn constants(arenas: &QueryArenas) -> Vec<Term> {
    let mut constants = Vec::new();
    for node in &arenas.written {
        if let NodeFacts::Remote { columns, .. } = &node.facts {
            constants.extend(columns.iter().filter_map(|column| match column {
                Err(term) if term.is_ground() => Some(term.clone()),
                _ => None,
            }));
        }
    }
    constants
}

/// The part of the response both query kinds return for one written node.
struct NodeAnswer {
    /// The node's predicate.
    predicate: String,
    /// The node's facts.
    table_entries: TableEntries,
    /// The derivation time of its predicate.
    meta_information: MetaInformation,
    /// The rules reading its predicate.
    possible_rules_above: Vec<QueryRule>,
    /// The rules writing its predicate.
    possible_rules_below: Vec<QueryRule>,
}

/// Builds the answers of one query's nodes.
pub(super) struct ResponseAssembly<'a> {
    /// The queried engine.
    engine: &'a Engine,
    /// The query's nodes.
    arenas: &'a QueryArenas,
    /// Per predicate, the written rules around it.
    neighborhoods: &'a HashMap<String, Neighborhood>,
    /// The renderer of stored values and written terms.
    responder: Responder<'a>,
}

impl<'a> ResponseAssembly<'a> {
    /// Prepares the assembly over a query's evaluated nodes.
    pub(super) fn new(
        engine: &'a Engine,
        arenas: &'a QueryArenas,
        neighborhoods: &'a HashMap<String, Neighborhood>,
        canonical_constants: HashMap<Term, String>,
    ) -> Self {
        Self {
            engine,
            arenas,
            neighborhoods,
            responder: Responder::new(engine, canonical_constants),
        }
    }

    /// Returns the nodes in a flat list, parents before children.
    pub(super) fn flat(&self) -> Vec<NodeResponse> {
        let mut nodes = Vec::new();
        let mut stack = vec![0usize];
        while let Some(index) = stack.pop() {
            let node = &self.arenas.written[index];
            let answer = self.answer(node);
            nodes.push(NodeResponse {
                address_in_tree: node.address.clone(),
                predicate: answer.predicate,
                table_entries: answer.table_entries,
                meta_information: answer.meta_information,
                possible_rules_above: answer.possible_rules_above,
                possible_rules_below: answer.possible_rules_below,
            });
            stack.extend(node.children.iter().rev());
        }
        nodes
    }

    /// Returns the nodes as a tree, from the node at the given written index.
    pub(super) fn tree(&self, index: usize) -> TreeForTableResponse {
        let node = &self.arenas.written[index];
        let answer = self.answer(node);
        TreeForTableResponse {
            predicate: answer.predicate,
            table_entries: answer.table_entries,
            meta_information: answer.meta_information,
            possible_rules_above: answer.possible_rules_above,
            possible_rules_below: answer.possible_rules_below,
            child_information: node.expanded.map(|(statement, head)| TreeChildren {
                rule: self.rule(statement, head),
                children: node
                    .children
                    .iter()
                    .map(|&child| self.tree(child))
                    .collect(),
            }),
        }
    }

    /// Returns one written node's answer:
    /// its facts, sliced, with their entry ids if they are stored, plus the rule neighborhoods.
    ///
    /// A listing has no neighborhoods, since no rule reads or writes it as a premise.
    fn answer(&self, node: &WrittenNode) -> NodeAnswer {
        let (start, count) = match node.pagination {
            Some(page) => (page.start, page.count),
            None => (0, usize::MAX),
        };
        let (entries, total) = match &node.facts {
            NodeFacts::Compiled(compiled) => self.stored_entries(*compiled, (start, count)),
            NodeFacts::Remote { leaf, columns } => {
                self.remote_entries(*leaf, columns, (start, count))
            }
        };
        let (above, below, nanos) = match self.neighborhoods.get(&node.predicate) {
            Some(neighborhood) if !node.listing => (
                self.rules_above(&neighborhood.above),
                self.rules_below(&neighborhood.below),
                self.engine.derivation_time(&node.predicate),
            ),
            _ => (Vec::new(), Vec::new(), 0),
        };
        NodeAnswer {
            predicate: node.predicate.clone(),
            table_entries: TableEntries {
                pagination: PageInfo {
                    start,
                    more_entries_exist: start.saturating_add(entries.len()) < total,
                },
                entries,
            },
            meta_information: MetaInformation {
                execution_time: nanos / 1_000_000,
            },
            possible_rules_above: above,
            possible_rules_below: below,
        }
    }

    /// Returns the rules reading a predicate, one per head atom of each.
    fn rules_above(&self, statements: &[StatementId]) -> Vec<QueryRule> {
        let mut seen = HashSet::new();
        statements
            .iter()
            .filter(|&&statement| seen.insert(statement))
            .flat_map(|&statement| {
                let heads = match self.engine.pipeline.statement(statement) {
                    Statement::Rule(rule) => rule.head.len(),
                    _ => 1,
                };
                (0..heads).map(move |head| self.rule(statement, head))
            })
            .collect()
    }

    /// Returns the rules writing a predicate, by the head atoms that do.
    fn rules_below(&self, writers: &[(StatementId, usize)]) -> Vec<QueryRule> {
        let mut seen = HashSet::new();
        writers
            .iter()
            .filter(|&&writer| seen.insert(writer))
            .map(|&(statement, head)| self.rule(statement, head))
            .collect()
    }

    /// Returns a written rule around one head atom: a rule statement, or a bound
    /// import, whose predicate is its head and whose binding atoms its
    /// body.
    fn rule(&self, statement: StatementId, head: usize) -> QueryRule {
        let written = self.engine.pipeline.statement(statement);
        let (relevant, body) = match written {
            Statement::Rule(rule) => (
                self.atom(&rule.head[head], ""),
                rule.body
                    .iter()
                    .filter_map(|literal| match literal {
                        Literal::Positive(atom) => Some(self.atom(atom, "")),
                        Literal::Negative(atom) => Some(self.atom(atom, "~")),
                        _ => None,
                    })
                    .collect(),
            ),
            Statement::Directive(Directive::Import(import)) => (
                PredicateWithParameters {
                    name: self.name(&import.predicate),
                    parameters: import
                        .variables
                        .iter()
                        .map(|variable| format!("?{variable}"))
                        .collect(),
                },
                import
                    .bindings
                    .iter()
                    .map(|atom| self.atom(atom, ""))
                    .collect(),
            ),
            _ => unreachable!("neighborhoods hold rules and bound imports"),
        };
        QueryRule {
            id: self
                .engine
                .written
                .as_ref()
                .and_then(|program| program.id(statement))
                .expect("neighborhoods name written statements"),
            relevant_head_predicate: relevant,
            relevant_head_predicate_index: head,
            body_predicates: body,
            string_representation: written.to_string(),
        }
    }

    /// Returns a written atom: its predicate's resolved name behind a prefix,
    /// and its terms as written, by position.
    fn atom(&self, atom: &Atom, prefix: &str) -> PredicateWithParameters {
        PredicateWithParameters {
            name: format!("{prefix}{}", self.name(&atom.predicate)),
            parameters: self
                .engine
                .translation
                .positional(atom)
                .iter()
                .map(ToString::to_string)
                .collect(),
        }
    }

    /// Returns the resolved name of a predicate.
    fn name(&self, predicate: &Identifier) -> String {
        self.engine
            .translation
            .resolve(predicate)
            .unwrap_or_else(|| predicate.to_string())
    }

    /// Returns the fact set of a node over the compiled program, and the revision range
    /// to read it over.
    fn fact_set(&self, compiled: usize) -> (TableId, Range<Revision>) {
        let node = &self.arenas.compiled[compiled];
        match node.filtered {
            Some(set) => (set, Revision(0)..Revision(1)),
            None => (node.table, Revision(0)..Revision(self.engine.step.max(1))),
        }
    }

    /// Returns the stored facts of a node over the compiled program in a slice (start and count),
    /// with their entry ids per predicate, and their total.
    fn stored_entries(
        &self,
        compiled: usize,
        (start, count): (usize, usize),
    ) -> (Vec<TableEntry>, usize) {
        let node = &self.arenas.compiled[compiled];
        let now = Revision(self.engine.step.max(1));
        let identity = ColumnOrder::identity(node.arity);
        let (set, range) = self.fact_set(compiled);
        let total = self.engine.manager.row_count(set, range.clone());
        let rows = self.page_rows(set, range, &identity, start, count);
        let ids = self.entry_ids(node.table, now, &identity, &rows);
        let entries = rows
            .iter()
            .map(|row| TableEntry {
                entry_id: ids.get(row).copied(),
                term_tuple: row
                    .iter()
                    .map(|&value| self.responder.rendered(value))
                    .collect(),
            })
            .collect();
        (entries, total)
    }

    /// Returns the facts of a remote literal, in a slice (start and count),
    /// and their total.
    ///
    /// These are the leaf's rows instantiated at the written literal, each once.
    /// They are not stored, so they carry no entry ids.
    fn remote_entries(
        &self,
        leaf: usize,
        columns: &[Result<usize, Term>],
        (start, count): (usize, usize),
    ) -> (Vec<TableEntry>, usize) {
        let identity = ColumnOrder::identity(self.arenas.compiled[leaf].arity);
        let (set, range) = self.fact_set(leaf);
        let mut seen = HashSet::new();
        let mut facts: Vec<Vec<String>> = Vec::new();
        for row in self.page_rows(set, range, &identity, 0, usize::MAX) {
            let terms: Vec<String> = columns
                .iter()
                .map(|column| match column {
                    Ok(position) => self.responder.rendered(row[*position]),
                    Err(term) => self.responder.term(term),
                })
                .collect();
            if seen.insert(terms.clone()) {
                facts.push(terms);
            }
        }
        let total = facts.len();
        let entries = facts
            .into_iter()
            .skip(start)
            .take(count)
            .map(|terms| TableEntry {
                entry_id: None,
                term_tuple: terms,
            })
            .collect();
        (entries, total)
    }

    /// Returns a node's facts in the set's stored order, from `start`, at most
    /// `count` of them.
    fn page_rows(
        &self,
        set: TableId,
        range: Range<Revision>,
        identity: &ColumnOrder,
        start: usize,
        count: usize,
    ) -> Vec<Vec<StorageValue>> {
        let mut rows: Vec<Vec<StorageValue>> = Vec::new();
        if count == 0 {
            return rows;
        }
        self.engine
            .manager
            .for_each_row_from(set, range, identity, start, |row| {
                rows.push(row.to_vec());
                if rows.len() < count {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            });
        rows
    }

    /// Returns the entry ids of a page's rows: positions in the predicate's own
    /// enumeration, assigned by one walk.
    fn entry_ids(
        &self,
        table: TableId,
        now: Revision,
        identity: &ColumnOrder,
        rows: &[Vec<StorageValue>],
    ) -> HashMap<Vec<StorageValue>, usize> {
        let mut ids: HashMap<Vec<StorageValue>, usize> = HashMap::new();
        let wanted: HashSet<Vec<StorageValue>> = rows.iter().cloned().collect();
        let mut position = 0usize;
        let mut remaining = wanted.len();
        self.engine
            .manager
            .for_each_row_from(table, Revision(0)..now, identity, 0, |row| {
                if remaining > 0 && wanted.contains(row) {
                    ids.entry(row.to_vec()).or_insert(position);
                    remaining -= 1;
                }
                position += 1;
                if remaining == 0 {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            });
        ids
    }
}
