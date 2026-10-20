//! The request: the query tree validated against the written program,
//! and mapped onto an arena of nodes over the compiled program.
//!
//! Every written node takes its facts from a node over the compiled program:
//!
//! - usually the node of the same predicate,
//! - for a literal over a predicate an import evaluates at its endpoint,
//!   the rows of the import's predicate, instantiated at the written literal.
//!
//! If a pipeline stage moved a written literal into the definition of a new predicate,
//! its node sits below a synthetic node of that predicate, which the response never shows.
//! A negated literal's node is a *listing*:
//! a node of its own over the negated predicate, which constrains nothing.

use std::collections::HashMap;

use nemo_physical::{diagnostics::Diagnostic, management::TableId, value::DataValue};

use crate::{
    diagnostics::TraceError,
    engine::{
        Engine,
        compile::{compiled::CompiledProgram, translation::TranslationState},
        registered::Registered,
        trace::query::route::{Realization, Route, Router, view_columns},
        written::WrittenProgram,
    },
    model::{Atom, Directive, Fact, Identifier, Literal, Rule, Statement, Term},
    pipeline::{Pipeline, RevisionId, StatementId},
    trace::query::{EntriesQuery, EntryQuery, Pagination, QueryChildren, QueryNode},
};

/// The node a query leaves unconstrained.
pub(super) const UNCONSTRAINED: QueryNode = QueryNode {
    table_entries: None,
    child_information: None,
};

/// Returns the facts a query node selects.
fn selection(entries: Option<&EntriesQuery>) -> Vec<EntryQuery> {
    entries.map_or_else(Vec::new, |entries| entries.queries.clone())
}

/// Returns the slice of its facts a query node asks for.
fn page(entries: Option<&EntriesQuery>) -> Option<Pagination> {
    entries.and_then(|entries| entries.pagination)
}

/// One node of the query tree over the compiled program.
#[derive(Debug)]
pub(super) struct CompiledNode {
    /// The node's predicate table.
    pub(super) table: TableId,
    /// The predicate's arity.
    pub(super) arity: usize,
    /// The requested selection, unresolved.
    pub(super) selectors: Vec<EntryQuery>,
    /// The expansion: compiled rule, head, and per body atom the child
    /// node (aligned by position, `None` unconstrained).
    pub(super) expansion: Option<(usize, usize, Vec<Option<usize>>)>,
    /// The bottom-up fact set, as an intermediate table, or `None` for the whole predicate.
    pub(super) valid: Option<TableId>,
    /// The top-down fact set, or `None` for the whole predicate.
    pub(super) filtered: Option<TableId>,
}

/// One node of the query tree over the written program.
#[derive(Debug)]
pub(super) struct WrittenNode {
    /// The path of child indices from the root.
    pub(super) address: Vec<usize>,
    /// The node's predicate, by resolved name.
    pub(super) predicate: String,
    /// Where its facts come from.
    pub(super) facts: NodeFacts,
    /// The requested slice.
    pub(super) pagination: Option<Pagination>,
    /// The written statement expanding the node and the head deriving its facts:
    /// taken from the request for a shaped node, chosen by the server for an expanded one.
    pub(super) expanded: Option<(StatementId, usize)>,
    /// The children: one per positive literal of the expanding
    /// statement, then one listing per negated literal.
    pub(super) children: Vec<usize>,
    /// Whether the node lists a negated literal's predicate.
    pub(super) listing: bool,
}

/// Where a written node's facts come from.
#[derive(Debug)]
pub(super) enum NodeFacts {
    /// The fact set of a node over the compiled program.
    Compiled(usize),
    /// The rows of a node over the compiled program, instantiated at a written literal
    /// over a predicate an import evaluates at its endpoint.
    Remote {
        /// The node of the import's predicate.
        leaf: usize,
        /// Per written column, the column of the leaf's rows holding its value,
        /// or the written term if none does.
        columns: Vec<Result<usize, Term>>,
    },
}

/// The rules of the written program around one predicate.
#[derive(Debug, Default)]
pub(super) struct Neighborhood {
    /// The written rules reading it, positively or negated.
    pub(super) above: Vec<StatementId>,
    /// The written rules writing it, each with the head that does.
    pub(super) below: Vec<(StatementId, usize)>,
}

/// What a query reads of its engine: the written program, and how the
/// compiled program relates to it.
pub(super) struct QueryContext<'a> {
    /// The program pipeline.
    pub(super) pipeline: &'a Pipeline,
    /// The translation state, for names and declarations.
    pub(super) state: &'a TranslationState,
    /// The registered predicates, by resolved name.
    pub(super) predicates: &'a HashMap<String, Registered>,
    /// The compiled program.
    pub(super) program: &'a CompiledProgram,
    /// The written program.
    pub(super) written: &'a WrittenProgram,
    /// The routing of written literals into the compiled program.
    pub(super) router: Router<'a>,
    /// Per predicate, by resolved name, the written rules around it.
    pub(super) neighborhoods: HashMap<String, Neighborhood>,
    /// Per predicate a written rule negates, by resolved name,
    /// its table and arity, or why the engine cannot list its facts.
    pub(super) listings: &'a HashMap<String, Result<(TableId, usize), Diagnostic>>,
}

impl<'a> QueryContext<'a> {
    /// Gathers what a query reads, from the parts of a loaded engine.
    pub(super) fn new(
        pipeline: &'a Pipeline,
        state: &'a TranslationState,
        predicates: &'a HashMap<String, Registered>,
        program: &'a CompiledProgram,
        written: &'a WrittenProgram,
        revision: RevisionId,
        listings: &'a HashMap<String, Result<(TableId, usize), Diagnostic>>,
    ) -> Self {
        let mut context = Self {
            pipeline,
            state,
            predicates,
            program,
            written,
            router: Router::new(pipeline, state, program, revision),
            neighborhoods: HashMap::new(),
            listings,
        };
        context.collect_neighborhoods();
        context
    }

    /// Records, per predicate, the written rules and bound imports
    /// reading and writing it.
    fn collect_neighborhoods(&mut self) {
        for (statement, _) in self.pipeline.statements(self.written.revision()) {
            let (heads, body): (Vec<&Atom>, Vec<&Atom>) = match self.pipeline.statement(statement) {
                Statement::Rule(rule) => (
                    rule.head.iter().collect(),
                    rule.body.iter().filter_map(Literal::atom).collect(),
                ),
                Statement::Directive(Directive::Import(import)) if import.is_bound() => {
                    // The import's own predicate is written by its fetch.
                    let neighborhood = self.neighborhood_of(&import.predicate);
                    if let Some(neighborhood) = neighborhood {
                        neighborhood.below.push((statement, 0));
                    }
                    (Vec::new(), import.bindings.iter().collect())
                }
                _ => continue,
            };
            for (head, atom) in heads.into_iter().enumerate() {
                if let Some(neighborhood) = self.neighborhood_of(&atom.predicate) {
                    neighborhood.below.push((statement, head));
                }
            }
            for atom in body {
                if let Some(neighborhood) = self.neighborhood_of(&atom.predicate)
                    && neighborhood.above.last() != Some(&statement)
                {
                    neighborhood.above.push(statement);
                }
            }
        }
    }

    /// Returns the neighborhood of a predicate, by its identifier.
    fn neighborhood_of(&mut self, predicate: &Identifier) -> Option<&mut Neighborhood> {
        let name = self.state.resolve(predicate)?;
        Some(self.neighborhoods.entry(name).or_default())
    }

    /// Returns the written rules writing a predicate, with their heads.
    pub(super) fn writers(&self, predicate: &str) -> &[(StatementId, usize)] {
        self.neighborhoods
            .get(predicate)
            .map_or(&[], |neighborhood| &neighborhood.below)
    }

    /// Returns the rule of a written statement: a rule, or `None`.
    pub(super) fn rule(&self, statement: StatementId) -> Option<&'a Rule> {
        self.router.rule(statement)
    }

    /// Returns the table and arity of a negated predicate, by resolved
    /// name.
    fn listing(&self, predicate: &str) -> Result<(TableId, usize), Diagnostic> {
        self.listings
            .get(predicate)
            .expect("every negated predicate of the written program is looked up")
            .clone()
    }

    /// Returns the resolved name of an atom's predicate.
    fn name(&self, atom: &Atom) -> String {
        self.state
            .resolve(&atom.predicate)
            .unwrap_or_else(|| atom.predicate.to_string())
    }

    /// Returns the number of body atoms of a compiled rule.
    fn atoms(&self, compiled: usize) -> usize {
        self.program.rules[compiled].body.len()
    }
}

/// The two arenas of a query: its nodes over the written program, and
/// the nodes over the compiled program they take their facts from.
#[derive(Debug, Default)]
pub(super) struct QueryArenas {
    /// The nodes over the compiled program.
    pub(super) compiled: Vec<CompiledNode>,
    /// The nodes over the written program.
    pub(super) written: Vec<WrittenNode>,
    /// The nodes over the compiled program that listings read, each
    /// the root of a tree of its own.
    pub(super) listings: Vec<usize>,
}

impl QueryArenas {
    /// Validates the request's root against the written program and adds
    /// it, with its children, to the arenas.
    pub(super) fn build(
        &mut self,
        context: &QueryContext<'_>,
        predicate: String,
        entries: Option<&EntriesQuery>,
        expansion: Option<&QueryChildren>,
    ) -> Result<(), Diagnostic> {
        let &Registered { table, arity } = context
            .predicates
            .get(&predicate)
            .expect("written nodes read stored predicates");
        let compiled = self.compiled_node(table, arity, selection(entries));
        let written = self.written_node(
            Vec::new(),
            predicate,
            NodeFacts::Compiled(compiled),
            page(entries),
        );
        if let Some(expansion) = expansion {
            self.shape(context, written, compiled, expansion)?;
        }
        Ok(())
    }

    /// Adds a node over the compiled program.
    fn compiled_node(&mut self, table: TableId, arity: usize, selectors: Vec<EntryQuery>) -> usize {
        self.compiled.push(CompiledNode {
            table,
            arity,
            selectors,
            expansion: None,
            valid: None,
            filtered: None,
        });
        self.compiled.len() - 1
    }

    /// Adds a node over the written program.
    fn written_node(
        &mut self,
        address: Vec<usize>,
        predicate: String,
        facts: NodeFacts,
        pagination: Option<Pagination>,
    ) -> usize {
        self.written.push(WrittenNode {
            address,
            predicate,
            facts,
            pagination,
            expanded: None,
            children: Vec::new(),
            listing: false,
        });
        self.written.len() - 1
    }

    /// Expands a written node by the expansion a request names, and its
    /// children by theirs.
    fn shape(
        &mut self,
        context: &QueryContext<'_>,
        written: usize,
        compiled: usize,
        expansion: &QueryChildren,
    ) -> Result<(), Diagnostic> {
        let (statement, rule) = named_rule(context, expansion.rule)?;
        check_head(context, &self.written[written].predicate, rule, expansion)?;
        let positives = positive_literals(rule).len();
        let atoms = positives + negated_literals(rule).len();
        if expansion.children.len() > atoms {
            return Err(TraceError::BodyCount {
                rule: expansion.rule,
                atoms: atoms.into(),
                constrained: expansion.children.len().into(),
            }
            .diagnostic());
        }
        if expansion
            .children
            .iter()
            .skip(positives)
            .any(|child| child.child_information.is_some())
        {
            return Err(TraceError::NegatedExpansion.diagnostic());
        }
        let requested: Vec<&QueryNode> = expansion.children.iter().collect();
        let children = self.expand(
            context,
            written,
            compiled,
            statement,
            expansion.head_index,
            &requested,
        )?;
        for (child, request) in children.into_iter().zip(requested) {
            if let Some(expansion) = &request.child_information {
                let NodeFacts::Compiled(compiled) = self.written[child].facts else {
                    return Err(TraceError::RemoteExpansion.diagnostic());
                };
                self.shape(context, child, compiled, expansion)?;
            }
        }
        Ok(())
    }

    /// Expands a written node by a written rule and head, returning the positive children in
    /// order.
    ///
    /// Every positive literal is routed into the compiled program and adds one written child.
    /// One listing per negated literal follows.
    /// Each child takes the selection and pagination of the request's node at its position,
    /// and is unconstrained if the request has no node there.
    pub(super) fn expand(
        &mut self,
        context: &QueryContext<'_>,
        written: usize,
        compiled: usize,
        statement: StatementId,
        head: usize,
        requested: &[&QueryNode],
    ) -> Result<Vec<usize>, Diagnostic> {
        let rule = context
            .rule(statement)
            .expect("expansions name written rules");
        // A stage rewrites a rule into a rule or an import, and never removes one.
        let realization = context
            .router
            .realize(statement)
            .expect("every written rule runs as a compiled rule or an import");
        // Every error happens before the arenas change,
        // so that a server-side expansion can try the next rule.
        let negated: Vec<(String, (TableId, usize))> = negated_literals(rule)
            .into_iter()
            .map(|atom| {
                let predicate = context.name(atom);
                context.listing(&predicate).map(|found| (predicate, found))
            })
            .collect::<Result<_, _>>()?;
        if let Realization::Rule(rule_index) = realization {
            check_rule(context, rule_index)?;
            if self.compiled[compiled].expansion.is_none() {
                self.compiled[compiled].expansion =
                    Some((rule_index, head, vec![None; context.atoms(rule_index)]));
            }
        }
        let request = |ordinal: usize| requested.get(ordinal).copied().unwrap_or(&UNCONSTRAINED);
        let positives = positive_literals(rule);
        let mut children = Vec::with_capacity(positives.len());
        for (ordinal, &position) in positives.iter().enumerate() {
            let Literal::Positive(atom) = &rule.body[position] else {
                unreachable!("the position is a positive literal")
            };
            let entries = request(ordinal).table_entries.as_ref();
            let facts = self.literal_facts(
                context,
                realization,
                (statement, position),
                atom,
                compiled,
                selection(entries),
            );
            if matches!(facts, NodeFacts::Remote { .. }) && !selection(entries).is_empty() {
                return Err(TraceError::RemoteSelectors.diagnostic());
            }
            let mut address = self.written[written].address.clone();
            address.push(ordinal);
            let predicate = context.name(atom);
            children.push(self.written_node(address, predicate, facts, page(entries)));
        }
        let mut listings = Vec::new();
        for (offset, (predicate, (table, arity))) in negated.into_iter().enumerate() {
            let ordinal = positives.len() + offset;
            let entries = request(ordinal).table_entries.as_ref();
            let listed = self.compiled_node(table, arity, selection(entries));
            self.listings.push(listed);
            let mut address = self.written[written].address.clone();
            address.push(ordinal);
            let listing = self.written_node(
                address,
                predicate,
                NodeFacts::Compiled(listed),
                page(entries),
            );
            self.written[listing].listing = true;
            listings.push(listing);
        }
        self.written[written].expanded = Some((statement, head));
        self.written[written].children = children.iter().copied().chain(listings).collect();
        Ok(children)
    }

    /// Returns where the facts of a positive literal of a written statement come from,
    /// given how the statement runs and the node `compiled` that the statement expands.
    ///
    /// A literal over a predicate an import evaluates at its endpoint
    /// takes the import's rows, instantiated at the literal.
    /// Any other literal takes the node its route ends at, created with the given selection.
    fn literal_facts(
        &mut self,
        context: &QueryContext<'_>,
        realization: Realization,
        (statement, position): (StatementId, usize),
        atom: &Atom,
        compiled: usize,
        selection: Vec<EntryQuery>,
    ) -> NodeFacts {
        match realization {
            Realization::Import(import) => {
                let view = context
                    .router
                    .replacing_view(import)
                    .expect("an import that replaces a rule has a view");
                NodeFacts::Remote {
                    leaf: compiled,
                    columns: view_columns(view, &context.state.positional(atom)),
                }
            }
            Realization::Rule(_) => {
                let route = context.router.route(statement, &[position]);
                let target = self.route_target(context, compiled, &route, selection);
                if !route.remote {
                    return NodeFacts::Compiled(target);
                }
                let view = context
                    .router
                    .remote_view(&route)
                    .expect("a remote route ends at a defined predicate");
                NodeFacts::Remote {
                    leaf: target,
                    columns: view_columns(view, &context.state.positional(atom)),
                }
            }
        }
    }

    /// Returns the node a route ends at below a node over the compiled program,
    /// creating the synthetic nodes of the definitions on the way,
    /// each expanded by the next statement of the route.
    ///
    /// A node that already exists at a literal is reused.
    fn route_target(
        &mut self,
        context: &QueryContext<'_>,
        compiled: usize,
        route: &Route,
        selectors: Vec<EntryQuery>,
    ) -> usize {
        let mut current = compiled;
        for (index, &(statement, literal)) in route.hops.iter().enumerate() {
            current = self.child_at(context, current, statement, literal, Vec::new());
            if self.compiled[current].expansion.is_none() {
                let next = route.hops.get(index + 1).map_or(route.last.0, |hop| hop.0);
                let rule_index = context.router.compiled(next);
                self.compiled[current].expansion =
                    Some((rule_index, 0, vec![None; context.atoms(rule_index)]));
            }
        }
        let (statement, literal) = route.last;
        let selectors = if route.remote { Vec::new() } else { selectors };
        self.child_at(context, current, statement, literal, selectors)
    }

    /// Returns the child of a node over the compiled program at a literal of the statement
    /// expanding it, creating it if it does not exist yet.
    fn child_at(
        &mut self,
        context: &QueryContext<'_>,
        parent: usize,
        statement: StatementId,
        literal: usize,
        selectors: Vec<EntryQuery>,
    ) -> usize {
        let atom = context.router.atom_of(statement, literal);
        let (_, _, children) = self.compiled[parent]
            .expansion
            .as_ref()
            .expect("a node expands before its children exist");
        if let Some(child) = children[atom] {
            return child;
        }
        let predicate = context
            .router
            .predicate_at(statement, literal)
            .expect("routes end at atoms");
        let &Registered { table, arity } = context
            .predicates
            .get(&predicate)
            .expect("a literal of the compiled program reads a registered predicate");
        let child = self.compiled_node(table, arity, selectors);
        let Some((_, _, children)) = &mut self.compiled[parent].expansion else {
            unreachable!("the expansion was just read")
        };
        children[atom] = Some(child);
        child
    }
}

/// Returns the written rule an expansion names, by its statement id.
fn named_rule<'a>(
    context: &QueryContext<'a>,
    id: usize,
) -> Result<(StatementId, &'a Rule), Diagnostic> {
    let unknown = || TraceError::UnknownRule { rule: id }.diagnostic();
    let statement = context.written.statement(id).ok_or_else(unknown)?;
    match context.pipeline.statement(statement) {
        Statement::Rule(_) => Ok((statement, context.rule(statement).ok_or_else(unknown)?)),
        Statement::Directive(Directive::Import(import)) if import.is_bound() => {
            Err(TraceError::QueryFetchRule.diagnostic())
        }
        _ => Err(unknown()),
    }
}

/// Checks that an expansion's head writes the node's predicate.
fn check_head(
    context: &QueryContext<'_>,
    predicate: &str,
    rule: &Rule,
    expansion: &QueryChildren,
) -> Result<(), Diagnostic> {
    let Some(head) = rule.head.get(expansion.head_index) else {
        return Err(TraceError::UnknownHead {
            rule: expansion.rule,
            head: expansion.head_index,
        }
        .diagnostic());
    };
    if context.state.resolve(&head.predicate).as_deref() != Some(predicate) {
        return Err(TraceError::HeadPredicateMismatch {
            rule: expansion.rule,
            head: expansion.head_index,
        }
        .diagnostic());
    }
    Ok(())
}

/// Checks that the protocol can expand a compiled rule:
/// an aggregate or an existential rule is an error.
fn check_rule(context: &QueryContext<'_>, compiled: usize) -> Result<(), Diagnostic> {
    let rule = &context.program.rules[compiled];
    if rule.aggregate.is_some() || rule.existential.is_some() {
        return Err(TraceError::QueryHeadKind.diagnostic());
    }
    Ok(())
}

/// Returns the positions of a rule's positive literals.
pub(super) fn positive_literals(rule: &Rule) -> Vec<usize> {
    rule.body
        .iter()
        .enumerate()
        .filter(|(_, literal)| matches!(literal, Literal::Positive(_)))
        .map(|(position, _)| position)
        .collect()
}

/// Returns the atoms of a rule's negated literals, in body order.
pub(super) fn negated_literals(rule: &Rule) -> Vec<&Atom> {
    rule.body
        .iter()
        .filter_map(|literal| match literal {
            Literal::Negative(atom) => Some(atom),
            _ => None,
        })
        .collect()
}

/// Parses the terms of a selection, written as an atom's are:
/// `a, "b", 3`.
fn selection_terms(text: &str) -> Result<Vec<Term>, Diagnostic> {
    let refusal = |reason: String| {
        TraceError::NotTerms {
            text: text.to_owned(),
            reason,
        }
        .diagnostic()
    };
    let atom = Atom::parse(&format!("p({text})"))
        .map_err(|problem| refusal(problem.message().to_owned()))?;
    if atom.labels.is_some() {
        return Err(refusal(String::from("a selection cannot name columns")));
    }
    Ok(atom.terms)
}

/// Resolves every term selection, checked as a fact of the program is,
/// returning the grounded rows per node over the compiled program.
pub(super) fn ground_selectors(
    engine: &mut Engine,
    nodes: &[CompiledNode],
) -> Result<HashMap<usize, Vec<Vec<DataValue>>>, Diagnostic> {
    // Terms parse first, without touching the engine.
    let mut owners: Vec<usize> = Vec::new();
    let mut facts: Vec<Fact> = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        for selector in &node.selectors {
            let EntryQuery::Terms(text) = selector else {
                continue;
            };
            let terms = selection_terms(text)?;
            let name = engine.manager.name(node.table).to_owned();
            if terms.len() != node.arity {
                return Err(TraceError::SelectorArity {
                    predicate: name,
                    terms: terms.len().into(),
                    arity: node.arity.into(),
                }
                .diagnostic());
            }
            owners.push(index);
            facts.push(Fact::new(Identifier::Iri(name), terms));
        }
    }
    if facts.is_empty() {
        return Ok(HashMap::new());
    }

    let mut grounded: HashMap<usize, Vec<Vec<DataValue>>> = HashMap::new();
    for (index, (_, values)) in owners.into_iter().zip(engine.ground_facts(&facts)?) {
        grounded.entry(index).or_default().push(values);
    }
    Ok(grounded)
}
