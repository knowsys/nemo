//! The evaluation of the derivation-structure queries of [trace::query](crate::trace::query).
//!
//! One evaluation serves both kinds of request.
//! A *shaped* request (`tableEntriesForTreeNodes`) is evaluated in two passes:
//! - The *valid* pass, bottom-up, gives each node the facts that the constraints of its
//!   subtree allow.
//! - The *filter* pass, top-down, shrinks each node's set to the facts that take part in a
//!   surviving derivation of its parent.
//!
//! An *expanded* request (`treeForTable`) is grown by the server, one rule per node,
//! as long as a single rule derives the node's whole fact set.
//!
//! The request names rules and literals of the written program.
//! It is mapped onto a tree over the compiled program, which the passes evaluate,
//! and the response maps the results back onto the written nodes.
//! The fact sets are computed with ordinary operation plans and stored as intermediate tables,
//! which are dropped when the query ends.
//!
//! The submodules:
//!
//! - [request] validates the request into the two arenas.
//! - [route] follows written literals into the compiled program.
//! - [passes] compute the sets of a shaped request.
//! - [expand] grows the tree of an expanded request.
//! - [response] assembles the protocol types.

mod expand;
mod passes;
mod request;
mod response;
mod route;

use std::collections::{HashMap, HashSet, hash_map::Entry};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    management::{ColumnOrder, Revision, TableId, TableManager},
    plan::{Marker, NodeId, OperationPlan},
    trie::Trie,
    value::DataValue,
};

use crate::{
    engine::{
        Engine,
        compile::compiled::CompiledProgram,
        trace::query::{
            request::{QueryArenas, QueryContext, negated_literals},
            response::ResponseAssembly,
        },
    },
    model::Statement,
    trace::query::{
        EntriesQuery, NodeResponse, QueryChildren, TableEntriesForTreeNodesQuery,
        TreeForTableQuery, TreeForTableResponse,
    },
};

/// The largest depth of a server-side expansion.
///
/// The derivations of a recursive program can be arbitrarily deep.
const MAX_DEPTH: usize = 32;

/// The largest number of nodes of a server-side expansion.
///
/// A node with two recursive premises doubles the tree on every level,
/// so the depth bound alone allows exponentially many nodes.
const MAX_NODES: usize = 1024;

/// The query evaluation over one engine's stores.
struct QueryEvaluation<'a> {
    /// The table store.
    manager: &'a mut TableManager,
    /// The dictionary.
    dictionary: &'a mut Dictionary,
    /// The compiled program.
    program: &'a CompiledProgram,
    /// The engine's current step.
    ///
    /// Every stored row's revision lies below it.
    now: u64,
    /// The evaluation context of rule bodies, which is that of the latest reasoning run.
    ///
    /// A table that several runs derived contains rows computed at several instants,
    /// as `NOW()` returns them, and a query over all of them evaluates at the last.
    context: EvaluationContext,
    /// Every intermediate table that this query created, which are dropped when it ends.
    scratch: Vec<TableId>,
    /// The grounded term selectors of each node over the compiled program.
    grounded: HashMap<usize, Vec<Vec<DataValue>>>,
}

/// How a request's tree takes its shape.
#[derive(Debug, Clone, Copy)]
enum TreeShape<'a> {
    /// The server expands it from the root.
    Expanded,
    /// The request gives it, as the root's expansion, if any.
    Given(Option<&'a QueryChildren>),
}

/// Answers a `treeForTable` query against an engine at the fixpoint.
pub(crate) fn tree_for_table(
    engine: &mut Engine,
    query: TreeForTableQuery,
) -> Result<TreeForTableResponse, Diagnostic> {
    let TreeForTableQuery {
        predicate,
        table_entries,
    } = query;
    run(
        engine,
        predicate,
        table_entries.as_ref(),
        TreeShape::Expanded,
        |assembly| assembly.tree(0),
    )
}

/// Answers a `tableEntriesForTreeNodes` query against an engine at the fixpoint.
pub(crate) fn table_entries_for_tree_nodes(
    engine: &mut Engine,
    query: TableEntriesForTreeNodesQuery,
) -> Result<Vec<NodeResponse>, Diagnostic> {
    let TableEntriesForTreeNodesQuery {
        predicate,
        table_entries,
        child_information,
    } = query;
    run(
        engine,
        predicate,
        table_entries.as_ref(),
        TreeShape::Given(child_information.as_ref()),
        |assembly| assembly.flat(),
    )
}

/// Evaluates a query against an engine at the fixpoint.
///
/// `predicate` and `entries` give the root's predicate and selection,
/// and `shape` says how the tree takes its shape.
/// `answer` builds the response from the assembled nodes.
fn run<T>(
    engine: &mut Engine,
    predicate: String,
    entries: Option<&EntriesQuery>,
    shape: TreeShape<'_>,
    answer: impl FnOnce(&ResponseAssembly<'_>) -> T,
) -> Result<T, Diagnostic> {
    // The tree's shape and predicates are checked against the written
    // program before anything runs.
    engine.whole_predicate(&predicate)?;
    let listings = listings(engine);
    let mut arenas = QueryArenas::default();
    let expansion = match shape {
        TreeShape::Expanded => None,
        TreeShape::Given(expansion) => expansion,
    };
    arenas.build(&context(engine, &listings), predicate, entries, expansion)?;

    // Term selectors get the checks of a fact of the program, as traced facts do.
    // Entry selectors are resolved against the stored order later.
    let grounded = request::ground_selectors(engine, &arenas.compiled)?;

    let program = engine
        .program
        .as_ref()
        .expect("a query checked for a program");
    let mut context = QueryContext::new(
        &engine.pipeline,
        &engine.translation,
        &engine.predicates,
        program,
        engine
            .written
            .as_ref()
            .expect("a program has a written one"),
        engine.revision.expect("a program has a revision"),
        &listings,
    );
    let mut evaluation = QueryEvaluation {
        manager: &mut engine.manager,
        dictionary: &mut engine.dictionary,
        program,
        now: engine.step.max(1),
        context: engine.contexts.latest(),
        scratch: Vec::new(),
        grounded,
    };
    let expand = matches!(shape, TreeShape::Expanded);
    let outcome = evaluation.evaluate(&mut arenas, &context, expand);
    let scratch = std::mem::take(&mut evaluation.scratch);
    let neighborhoods = std::mem::take(&mut context.neighborhoods);
    let result = outcome.map(|()| {
        let canonical_constants = engine.render_constants(response::constants(&arenas));
        answer(&ResponseAssembly::new(
            engine,
            &arenas,
            &neighborhoods,
            canonical_constants,
        ))
    });
    for table in scratch {
        engine.manager.drop_table(table);
    }
    result
}

/// Returns, for every predicate that a written rule negates,
/// its table and arity if the engine stores it whole, and the error otherwise.
fn listings(engine: &Engine) -> HashMap<String, Result<(TableId, usize), Diagnostic>> {
    let written = engine
        .written
        .as_ref()
        .expect("a query checked for a program");
    let mut listings = HashMap::new();
    for (_, statement) in engine.pipeline.statements(written.revision()) {
        let Statement::Rule(rule) = statement else {
            continue;
        };
        for atom in negated_literals(rule) {
            // The key is the resolved name, by which the expansion looks up listings.
            let name = engine
                .translation
                .resolve(&atom.predicate)
                .unwrap_or_else(|| atom.predicate.to_string());
            if let Entry::Vacant(vacant) = listings.entry(name) {
                let found = engine.whole_predicate(vacant.key());
                vacant.insert(found);
            }
        }
    }
    listings
}

/// Returns what a query reads of a loaded engine.
fn context<'a>(
    engine: &'a Engine,
    listings: &'a HashMap<String, Result<(TableId, usize), Diagnostic>>,
) -> QueryContext<'a> {
    QueryContext::new(
        &engine.pipeline,
        &engine.translation,
        &engine.predicates,
        engine
            .program
            .as_ref()
            .expect("a query checked for a program"),
        engine
            .written
            .as_ref()
            .expect("a program has a written one"),
        engine.revision.expect("a program has a revision"),
        listings,
    )
}

impl QueryEvaluation<'_> {
    /// Evaluates the whole tree.
    ///
    /// The valid sets are computed bottom-up.
    /// Then a given shape is filtered top-down, or the server expands the tree if `expand` is
    /// true.
    /// The listings come last, each over its own selection.
    fn evaluate(
        &mut self,
        arenas: &mut QueryArenas,
        context: &QueryContext<'_>,
        expand: bool,
    ) -> Result<(), Diagnostic> {
        self.valid(&mut arenas.compiled, 0)?;
        arenas.compiled[0].filtered = arenas.compiled[0].valid;
        if expand {
            self.expand(arenas, context, 0, MAX_DEPTH);
        } else {
            self.filter(&mut arenas.compiled, 0);
        }
        for listing in arenas.listings.clone() {
            self.valid(&mut arenas.compiled, listing)?;
            arenas.compiled[listing].filtered = arenas.compiled[listing].valid;
        }
        // Everything that the response reads is arranged in identity order now,
        // so that the assembly can borrow the engine immutably.
        let identity: HashSet<TableId> = arenas
            .compiled
            .iter()
            .flat_map(|node| {
                [Some(node.table), node.valid, node.filtered]
                    .into_iter()
                    .flatten()
            })
            .collect();
        // Every table's rows lie before `now`.
        // The rows of the scratch tables are at revision 0, which is below `now`.
        for table in identity {
            let arity = self.manager.arity(table);
            self.manager.ensure_order(
                table,
                Revision(0)..Revision(self.now),
                &ColumnOrder::identity(arity),
            );
        }
        Ok(())
    }

    /// Stores a plan's output trie as a new intermediate table of the given arity,
    /// and returns the table.
    fn store(&mut self, arity: usize, rows: Trie) -> TableId {
        let table = self
            .manager
            .register(format!("trace-query-{}", self.scratch.len()), arity);
        self.scratch.push(table);
        self.manager
            .add_trie(table, Revision(0), ColumnOrder::identity(arity), rows);
        table
    }

    /// Returns a load of a node's current fact set,
    /// which is `table` if `set` is `None`, and the intermediate table `set` otherwise.
    fn set_load(
        &self,
        plan: &mut OperationPlan,
        table: TableId,
        set: Option<TableId>,
        markers: Vec<Marker>,
    ) -> NodeId {
        match set {
            Some(set) => plan.load(set, Revision(0)..Revision(1), markers),
            None => plan.load(table, Revision(0)..Revision(self.now), markers),
        }
    }

    /// Returns the number of rows of a node's fact set, given as for
    /// [set_load](Self::set_load).
    fn count(&self, table: TableId, set: Option<TableId>) -> usize {
        match set {
            Some(set) => self.manager.row_count(set, Revision(0)..Revision(1)),
            None => self
                .manager
                .row_count(table, Revision(0)..Revision(self.now)),
        }
    }
}
