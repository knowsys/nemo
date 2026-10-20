//! The per-rule compiler: body atoms, negation, heads, aggregates, and
//! the operation fixpoint, which resolves body operations into assignments and filters.
//!
//! A rule's columns are its variables, then its computed columns.

use std::collections::HashMap;

use nemo_physical::{
    diagnostics::Diagnostic,
    function::{OpCode, StackProgram},
    value::DataValue,
};

use crate::{
    diagnostics::TranslationError,
    engine::{
        compile::compiled::{
            AggregateColumn, BodyAtom, CompiledAggregate, CompiledAggregateFunction,
            ComputedColumn, ExistentialAtom, ExistentialTerm, FilterOrigin, HeadAtom, NegatedAtom,
            NegatedColumn, RuleBinding, RuleFilter, VariableId,
        },
        compile::translation::{
            AGGREGATE_RESULT, StackProgramBuilder, Translator, VariableScope,
            terms::{collect_aggregates, is_pattern, mentions},
            unsupported,
        },
    },
    model::{
        Aggregate, Atom, Operation, Operator, Span, Term, TermKind, Variable, VariableKind, Walk,
    },
};

/// Returns the unsafe-rule diagnostic for a variable the body does not bind.
fn unsafe_variable(role: &str, variable: &str, span: Option<Span>) -> Diagnostic {
    TranslationError::UnsafeVariable {
        role: role.to_owned(),
        variable: variable.to_owned(),
    }
    .at(span)
}

/// The per-rule compilation state.
pub(super) struct RuleCompiler<'a, 'b> {
    /// The enclosing translator.
    pub(super) translator: &'b mut Translator<'a>,
    /// Variable ids by name, in first-occurrence order.
    pub(super) names: HashMap<String, VariableId>,
    /// Number of variables assigned so far.
    pub(super) variables: usize,
    /// The body atoms compiled so far.
    pub(super) body: Vec<BodyAtom>,
    /// The filters compiled so far.
    pub(super) filters: Vec<RuleFilter>,
    /// The probing bindings compiled so far.
    pub(super) bindings: Vec<RuleBinding>,
    /// The computed columns compiled so far.
    pub(super) computed: Vec<ComputedColumn>,
    /// Equalities synthesized for expression arguments of body atoms,
    /// becoming bindings or joining the operation fixpoint.
    pub(super) deferred: Vec<Operation>,
    /// Whether a constant condition proved the rule can never fire.
    pub(super) never_fires: bool,
}

impl<'a, 'b> RuleCompiler<'a, 'b> {
    /// Creates a compiler with nothing compiled yet.
    pub(super) fn new(translator: &'b mut Translator<'a>) -> Self {
        Self {
            translator,
            names: HashMap::new(),
            variables: 0,
            body: Vec::new(),
            filters: Vec::new(),
            bindings: Vec::new(),
            computed: Vec::new(),
            deferred: Vec::new(),
            never_fires: false,
        }
    }
}

/// Returns the post programs of an aggregate head (its expressions around an aggregate)
/// over the aggregation's output rows `(group…, results…)`.
///
/// Each input is remapped from its rule column to its position in the group,
/// and the expression's own aggregate result to its position after the group columns.
fn post_columns(
    raw_posts: Vec<(usize, Vec<usize>, StackProgram)>,
    group: &[usize],
) -> Vec<ComputedColumn> {
    raw_posts
        .into_iter()
        .map(|(result, inputs, program)| ComputedColumn {
            inputs: inputs
                .into_iter()
                .map(|id| {
                    if id == AGGREGATE_RESULT {
                        // The sentinel names this expression's own
                        // aggregate within `(group…, results…)`.
                        group.len() + result
                    } else {
                        group
                            .iter()
                            .position(|&existing| existing == id)
                            .expect("post inputs joined the group above")
                    }
                })
                .collect(),
            program,
        })
        .collect()
}

impl RuleCompiler<'_, '_> {
    /// Returns the variables' source names by column of the given width,
    /// for the type analysis's messages.
    ///
    /// A column the normalization invented has `None`
    /// (its name begins with a NUL byte, which no program can write),
    /// and a name can also belong to a computed column.
    pub(super) fn source_names(&self, width: usize) -> Vec<Option<String>> {
        let mut names = vec![None; width];
        for (name, &variable) in &self.names {
            if name.starts_with('\u{0}') {
                continue;
            }
            if let Some(slot) = names.get_mut(variable) {
                *slot = Some(name.clone());
            }
        }
        names
    }

    /// Compiles a positive body atom.
    ///
    /// Constants and repeated variables normalize into fresh variables plus identity filters,
    /// since a repeated variable or a constant in an atom means term identity (`=`),
    /// not value equality.
    pub(super) fn body_atom(&mut self, atom: &Atom) -> Result<(), Diagnostic> {
        // A join reads columns, and a nullary atom has none;
        // negated nullary atoms are guards and do not come here.
        if atom.terms.is_empty() {
            return Err(unsupported("a positive nullary body atom", atom.span));
        }
        let name = self.translator.resolve(&atom.predicate, atom.span)?;
        let layout = self.translator.plan.layout_of(&name, atom.terms.len());
        let table = self
            .translator
            .table(&atom.predicate, layout.delivered(), atom.span)?;

        let mut variables = Vec::with_capacity(layout.delivered());
        for (position, term) in atom.terms.iter().enumerate() {
            // A pruned column is not stored; its term is anonymous in
            // every occurrence, so there is nothing to bind.
            if layout.is_pruned(position) {
                continue;
            }
            let variable = self.body_column(term, &variables)?;
            variables.push(variable);
        }
        self.body.push(BodyAtom { table, variables });
        Ok(())
    }

    /// Returns the variable one term of a positive body atom binds,
    /// given the variables the atom's earlier columns bind.
    fn body_column(
        &mut self,
        term: &Term,
        earlier: &[VariableId],
    ) -> Result<VariableId, Diagnostic> {
        match &term.kind {
            TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                let id = self.variable(&variable.name);
                if !earlier.contains(&id) {
                    return Ok(id);
                }
                // A repeated variable within one atom: a fresh column
                // variable constrained identical.
                let fresh = self.fresh();
                self.equality_filter(
                    vec![fresh, id],
                    vec![OpCode::Input(0), OpCode::Input(1), OpCode::Same],
                    Vec::new(),
                    FilterOrigin::Repeated(term.span),
                );
                Ok(fresh)
            }
            // Each `_` is its own never-read column variable.
            TermKind::Variable(variable) if variable.kind == VariableKind::Anonymous => {
                Ok(self.fresh())
            }
            TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
                // A parameter reads as a constant column.
                let value = self.translator.global(&variable.name, term.span)?;
                Ok(self.constant_column(value, term.span))
            }
            TermKind::Variable(_) => Err(unsupported(
                "an existential variable in a rule body",
                term.span,
            )),
            _ if mentions(term, VariableKind::Universal) || is_pattern(term) => {
                // An expression argument (or a pattern whose only
                // variables are anonymous): the column reads through
                // a fresh named variable constrained equal to the
                // expression once the operation fixpoint has bound
                // its inputs, or destructured, if it is a pattern.
                let name = format!("\u{0}expression{}", self.deferred.len());
                let id = self.variable(&name);
                self.deferred.push(Operation {
                    operator: Operator::Equal,
                    arguments: vec![
                        Term {
                            kind: TermKind::Variable(Variable::universal(name)),
                            span: term.span,
                        },
                        term.clone(),
                    ],
                    span: term.span,
                });
                Ok(id)
            }
            _ => {
                // A ground term: a fresh column variable constrained to
                // the constant.
                let value = self.translator.evaluate(term)?;
                Ok(self.constant_column(value, term.span))
            }
        }
    }

    /// Returns a fresh column variable constrained to a constant written at the given position.
    fn constant_column(&mut self, value: DataValue, span: Option<Span>) -> VariableId {
        let fresh = self.fresh();
        self.equality_filter(
            vec![fresh],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Same],
            vec![value],
            FilterOrigin::Constant(span),
        );
        fresh
    }

    /// Compiles a negated body atom.
    ///
    /// Bound variables become shared columns,
    /// and everything else is private to the atom (existential inside the negation).
    /// Constants and repetitions turn into filters over the atom's own columns.
    /// An unbound variable is local to a single negated literal,
    /// and `foreign` carries the ones earlier literals used.
    pub(super) fn negated_atom(
        &mut self,
        atom: &Atom,
        foreign: &mut HashMap<String, Option<Span>>,
    ) -> Result<NegatedAtom, Diagnostic> {
        let name = self.translator.resolve(&atom.predicate, atom.span)?;
        let layout = self.translator.plan.layout_of(&name, atom.terms.len());
        let table = self
            .translator
            .table(&atom.predicate, layout.delivered(), atom.span)?;

        let mut negated = NegatedColumns::default();
        for (position, term) in atom.terms.iter().enumerate() {
            // A pruned column is not stored; its term is anonymous in
            // every occurrence, so there is nothing to bind.
            if layout.is_pruned(position) {
                continue;
            }
            let column = self.negated_column(term, &mut negated, foreign)?;
            negated.columns.push(column);
        }
        // This literal's own variables are now foreign to later ones.
        for (name, _) in negated.local {
            foreign.insert(name, atom.span);
        }

        Ok(NegatedAtom {
            table,
            columns: negated.columns,
            filters: negated.filters,
        })
    }

    /// Returns the column one term of a negated atom stands for,
    /// adding the filters it needs over the atom's own columns.
    ///
    /// Filters and first occurrences are indexed by the *delivered* columns
    /// (the columns gathered so far),
    /// which pruning can shift left of the atom's term positions (the kept columns).
    fn negated_column(
        &mut self,
        term: &Term,
        negated: &mut NegatedColumns,
        foreign: &HashMap<String, Option<Span>>,
    ) -> Result<NegatedColumn, Diagnostic> {
        let position = negated.columns.len();
        match &term.kind {
            TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                if let Some(&id) = self.names.get(&variable.name) {
                    return Ok(match negated.shared_at.get(&id) {
                        None => {
                            negated.shared_at.insert(id, position);
                            NegatedColumn::Shared(id)
                        }
                        // A repeated bound variable: the later occurrence
                        // constrains equal, privately.
                        Some(&first) => {
                            negated
                                .filters
                                .push(equal_columns(first, position, term.span));
                            NegatedColumn::Private
                        }
                    });
                }
                if foreign.contains_key(&variable.name) {
                    return Err(TranslationError::UnboundAcrossNegations {
                        variable: variable.name.clone(),
                    }
                    .at(term.span));
                }
                match negated.local.get(&variable.name) {
                    None => {
                        negated.local.insert(variable.name.clone(), position);
                    }
                    Some(&first) => negated
                        .filters
                        .push(equal_columns(first, position, term.span)),
                }
                Ok(NegatedColumn::Private)
            }
            TermKind::Variable(variable) if variable.kind == VariableKind::Anonymous => {
                Ok(NegatedColumn::Private)
            }
            TermKind::Variable(variable) if variable.kind != VariableKind::Global => Err(
                unsupported("an existential variable in a negated atom", term.span),
            ),
            _ => {
                // A ground term (a parameter included): a private column
                // constrained to it.
                let value = self.translator.evaluate(term)?;
                negated.filters.push(RuleFilter {
                    inputs: vec![position],
                    program: StackProgram::new(
                        vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Same],
                        vec![value],
                        1,
                    ),
                    origin: FilterOrigin::Constant(term.span),
                });
                Ok(NegatedColumn::Private)
            }
        }
    }

    /// Compiles one atom of an existential head: universal variables
    /// join the frontier, existential variables index in first-sight
    /// order, everything else is a ground value.
    pub(super) fn existential_atom(
        &mut self,
        atom: &Atom,
        frontier: &mut Vec<usize>,
        existentials: &mut HashMap<String, usize>,
    ) -> Result<ExistentialAtom, Diagnostic> {
        let table = self
            .translator
            .table(&atom.predicate, atom.terms.len(), atom.span)?;
        let mut columns = Vec::with_capacity(atom.terms.len());
        for term in &atom.terms {
            let column = match &term.kind {
                TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                    let Some(&id) = self.names.get(&variable.name) else {
                        return Err(unsafe_variable("head variable", &variable.name, term.span));
                    };
                    let index = match frontier.iter().position(|&column| column == id) {
                        Some(index) => index,
                        None => {
                            frontier.push(id);
                            frontier.len() - 1
                        }
                    };
                    ExistentialTerm::Frontier(index)
                }
                TermKind::Variable(variable) if variable.kind == VariableKind::Existential => {
                    let next = existentials.len();
                    let index = *existentials.entry(variable.name.clone()).or_insert(next);
                    ExistentialTerm::Existential(index)
                }
                TermKind::Variable(variable) if variable.kind == VariableKind::Anonymous => {
                    return Err(TranslationError::AnonymousAsValue.at(term.span));
                }
                _ => {
                    let value = self.translator.evaluate(term)?;
                    // Encoded here, so that a constant the store cannot
                    // hold is an error at its span rather than at the
                    // first row the head creates.
                    self.translator
                        .dictionary
                        .encode(value.as_ref())
                        .ok_or_else(|| TranslationError::Unrepresentable.at(term.span))?;
                    ExistentialTerm::Constant(value)
                }
            };
            columns.push(column);
        }
        Ok(ExistentialAtom { table, columns })
    }

    /// Compiles an aggregate-carrying head atom: other positions form
    /// the group (expressions become computed group keys), the
    /// aggregate's input, value, and scope variables resolve to rule
    /// columns, and expressions around the aggregate become post
    /// programs over the aggregation's output.
    pub(super) fn aggregate_head(&mut self, atom: &Atom) -> Result<CompiledAggregate, Diagnostic> {
        let table = self
            .translator
            .table(&atom.predicate, atom.terms.len(), atom.span)?;

        let mut group: Vec<usize> = Vec::new();
        let mut positions: Vec<usize> = Vec::new();
        let mut columns: Vec<AggregateColumn> = Vec::new();
        let mut functions: Vec<CompiledAggregateFunction> = Vec::new();
        let mut raw_posts: Vec<(usize, Vec<usize>, StackProgram)> = Vec::new();

        for term in &atom.terms {
            let mut found = Vec::new();
            collect_aggregates(term, &mut found);
            let reference = match (&term.kind, found.len()) {
                (TermKind::Aggregate(aggregate), _) => {
                    functions.push(self.aggregate_parts(aggregate)?);
                    AggregateColumn::Result(functions.len() - 1)
                }
                (_, 0) => {
                    let id = self.group_column(term)?;
                    if positions.contains(&id) {
                        return Err(unsupported(
                            "a repeated column in an aggregate head",
                            term.span,
                        ));
                    }
                    positions.push(id);
                    let index = match group.iter().position(|&existing| existing == id) {
                        Some(index) => index,
                        None => {
                            group.push(id);
                            group.len() - 1
                        }
                    };
                    AggregateColumn::Group(index)
                }
                (_, 1) => {
                    functions.push(self.aggregate_parts(found[0])?);
                    let (inputs, program) =
                        self.compile_term(term, VariableScope::AggregateHead(&self.names))?;
                    raw_posts.push((functions.len() - 1, inputs, program));
                    AggregateColumn::Post(raw_posts.len() - 1)
                }
                _ => {
                    return Err(TranslationError::CombinedAggregates.at(term.span));
                }
            };
            columns.push(reference);
        }

        // Variables combined with an aggregate result join the group.
        for (_, inputs, _) in &raw_posts {
            for &id in inputs {
                if id != AGGREGATE_RESULT && !group.contains(&id) {
                    group.push(id);
                }
            }
        }
        self.check_aggregate(&group, &functions, atom.span)?;
        let post = post_columns(raw_posts, &group);
        Ok(CompiledAggregate {
            table,
            group,
            functions,
            post,
            columns,
        })
    }

    /// Checks an aggregate head's functions against its group.
    ///
    /// No aggregate may aggregate a group column.
    /// `#count()` counts contributions, which the body variables tell apart,
    /// so it needs a body with variables.
    fn check_aggregate(
        &self,
        group: &[usize],
        functions: &[CompiledAggregateFunction],
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        for function in functions {
            if let Some(&column) = function
                .input
                .iter()
                .chain(&function.value)
                .find(|column| group.contains(column))
            {
                return Err(TranslationError::AggregatedGroupColumn {
                    variable: self.column_name(column),
                }
                .at(span));
            }
        }
        if functions.iter().any(|function| function.input.is_none()) && self.variables == 0 {
            return Err(unsupported(
                "counting the matches of a variable-free body",
                span,
            ));
        }
        Ok(())
    }

    /// Returns an ordinary position of an aggregate head as a rule column:
    /// a group variable, or an expression that becomes a computed group key.
    fn group_column(&mut self, term: &Term) -> Result<usize, Diagnostic> {
        match &term.kind {
            TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                match self.names.get(&variable.name) {
                    Some(&id) => Ok(id),
                    None => Err(unsafe_variable("head variable", &variable.name, term.span)),
                }
            }
            TermKind::Variable(variable) if variable.kind == VariableKind::Anonymous => {
                Err(TranslationError::AnonymousAsValue.at(term.span))
            }
            TermKind::Variable(variable) if variable.kind == VariableKind::Existential => {
                unreachable!("a head with an existential variable is not an aggregate head")
            }
            _ => {
                let (inputs, program) =
                    self.compile_term(term, VariableScope::Rule(&self.names))?;
                Ok(self.computed_program(inputs, program))
            }
        }
    }

    /// Returns the source name of a rule column that a named variable binds.
    ///
    /// # Panics
    ///
    /// Panics if no variable written in the rule binds the column.
    fn column_name(&self, column: usize) -> String {
        self.names
            .iter()
            .find(|(name, id)| **id == column && !name.starts_with('\u{0}'))
            .map(|(name, _)| name.clone())
            .expect("a column both aggregated and grouped is a variable of the rule")
    }

    /// Compiles the parts of an aggregate expression: the function, the
    /// aggregated column (an input expression becomes a computed
    /// column), and the scope columns.
    fn aggregate_parts(
        &mut self,
        aggregate: &Aggregate,
    ) -> Result<CompiledAggregateFunction, Diagnostic> {
        let kind = aggregate.kind;
        let input = match &aggregate.input {
            None => None,
            Some(input) => Some(self.aggregate_column(input)?),
        };
        let value = match &aggregate.value {
            None => None,
            Some(term) => Some(self.aggregate_column(term)?),
        };
        if aggregate.legacy_list {
            // The legacy comma list means the same; the keyword says it.
            let mut warning = TranslationError::LegacyScopeList.diagnostic();
            if let Some(span) = aggregate.span {
                warning = warning.with_label(span, "write `per` before the variables");
            }
            self.translator.warn(warning);
        }
        let mut scope = Vec::new();
        for variable in &aggregate.scope {
            if variable.kind != VariableKind::Universal {
                return Err(TranslationError::ScopeVariableKind.at(aggregate.span));
            }
            let Some(&id) = self.names.get(&variable.name) else {
                return Err(unsafe_variable(
                    "scope variable",
                    &variable.name,
                    aggregate.span,
                ));
            };
            // Repeats (including the input itself) refine nothing.
            if input != Some(id) && value != Some(id) && !scope.contains(&id) {
                scope.push(id);
            }
        }
        // `#count()` counts the body matches: every body variable identifies a contribution.
        let scope = (!aggregate.counts_matches()).then_some(scope);
        Ok(CompiledAggregateFunction {
            kind,
            input,
            value,
            scope,
        })
    }

    /// Resolves one aggregate operand to a rule column: a bound
    /// variable directly, an expression as a computed column.
    fn aggregate_column(&mut self, term: &Term) -> Result<usize, Diagnostic> {
        match &term.kind {
            TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                match self.names.get(&variable.name) {
                    Some(&id) => Ok(id),
                    None => Err(unsafe_variable(
                        "aggregated variable",
                        &variable.name,
                        term.span,
                    )),
                }
            }
            _ => {
                let (inputs, program) =
                    self.compile_term(term, VariableScope::Rule(&self.names))?;
                Ok(self.computed_program(inputs, program))
            }
        }
    }

    /// Compiles a head atom over the rule's columns.
    ///
    /// Constants and repeated columns become computed columns.
    pub(super) fn head_atom(&mut self, atom: &Atom) -> Result<HeadAtom, Diagnostic> {
        let table = self
            .translator
            .table(&atom.predicate, atom.terms.len(), atom.span)?;
        if atom.terms.is_empty() {
            return Err(unsupported("a nullary head atom", atom.span));
        }

        let mut columns = Vec::with_capacity(atom.terms.len());
        for term in &atom.terms {
            let column = match &term.kind {
                TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                    let Some(&id) = self.names.get(&variable.name) else {
                        return Err(unsafe_variable("head variable", &variable.name, term.span));
                    };
                    if columns.contains(&id) {
                        // A repeated head column: computed copy.
                        self.computed_column(vec![id], vec![OpCode::Input(0)], Vec::new())
                    } else {
                        id
                    }
                }
                TermKind::Variable(variable) if variable.kind == VariableKind::Anonymous => {
                    return Err(TranslationError::AnonymousAsValue.at(term.span));
                }
                TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
                    // A parameter is a computed constant column.
                    let value = self.translator.global(&variable.name, term.span)?;
                    self.computed_column(Vec::new(), vec![OpCode::Constant(0)], vec![value])
                }
                TermKind::Variable(_) => {
                    unreachable!("a head with an existential variable compiles as existential")
                }
                TermKind::Aggregate(_) => {
                    unreachable!("a head atom with an aggregate compiles as an aggregate head")
                }
                TermKind::Operation(_)
                | TermKind::Tuple(_)
                | TermKind::Map { .. }
                | TermKind::Function { .. } => {
                    // A head expression: a computed column over its
                    // bound variables. A ground one folds to a constant,
                    // unless it calls a nondeterministic function (such as
                    // `RAND()`), which is evaluated per row.
                    let (inputs, program) =
                        self.compile_term(term, VariableScope::Rule(&self.names))?;
                    if inputs.is_empty() && !program.is_nondeterministic() {
                        let value = self.translator.evaluate(term)?;
                        self.computed_column(Vec::new(), vec![OpCode::Constant(0)], vec![value])
                    } else {
                        self.computed_program(inputs, program)
                    }
                }
                _ => {
                    // A ground head term: computed constant column.
                    let value = self.translator.evaluate(term)?;
                    self.computed_column(Vec::new(), vec![OpCode::Constant(0)], vec![value])
                }
            };
            columns.push(column);
        }
        Ok(HeadAtom { table, columns })
    }

    /// Resolves the body operations.
    ///
    /// An equality binding one fresh variable to an expression over bound ones is an
    /// assignment, a computed column.
    /// Assignments resolve to a fixpoint in body order, so chains work in any order.
    /// Everything else is a filter, standing for the part of the rule's text its origin names.
    pub(super) fn operations(
        &mut self,
        mut pending: Vec<(&Operation, FilterOrigin)>,
    ) -> Result<(), Diagnostic> {
        loop {
            let mut remaining = Vec::with_capacity(pending.len());
            let mut progressed = false;
            for (operation, origin) in pending {
                if self.assignment(operation)? {
                    progressed = true;
                } else {
                    remaining.push((operation, origin));
                }
            }
            pending = remaining;
            if !progressed {
                break;
            }
        }
        for (operation, origin) in pending {
            self.filter(operation, origin)?;
        }
        Ok(())
    }

    /// Compiles a deferred column equality into a probing binding if
    /// every variable of its expression is atom-bound. Returns whether
    /// it did.
    pub(super) fn binding(&mut self, operation: &Operation) -> Result<bool, Diagnostic> {
        let target = &operation.arguments[0];
        let expression = &operation.arguments[1];
        let TermKind::Variable(variable) = &target.kind else {
            return Ok(false);
        };
        if !self.bound(expression) {
            return Ok(false);
        }
        let (inputs, program) = self.compile_term(expression, VariableScope::Rule(&self.names))?;
        let target = *self
            .names
            .get(&variable.name)
            .expect("deferred targets are atom columns");
        self.bindings.push(RuleBinding {
            target,
            inputs,
            program,
            span: expression.span,
        });
        Ok(true)
    }

    /// Compiles an operation as an assignment if it is one: an
    /// equality with one side a fresh variable and every variable of
    /// the other side bound. Returns whether it did.
    fn assignment(&mut self, operation: &Operation) -> Result<bool, Diagnostic> {
        if operation.operator != Operator::Equal || operation.arguments.len() != 2 {
            return Ok(false);
        }
        // Either side may be the fresh variable.
        for (target, expression) in [(0usize, 1usize), (1, 0)] {
            let target = &operation.arguments[target];
            let expression = &operation.arguments[expression];
            let TermKind::Variable(variable) = &target.kind else {
                continue;
            };
            if variable.kind != VariableKind::Universal
                || self.names.contains_key(&variable.name)
                || !self.bound(expression)
            {
                continue;
            }
            let (inputs, program) =
                self.compile_term(expression, VariableScope::Rule(&self.names))?;
            let column = self.computed_program(inputs, program);
            self.names.insert(variable.name.clone(), column);
            return Ok(true);
        }
        Ok(false)
    }

    /// Returns whether every variable of a term is bound.
    ///
    /// A parameter stands for its value and is bound throughout.
    fn bound(&self, term: &Term) -> bool {
        !term.any_term(|term| match &term.kind {
            TermKind::Variable(variable) => match variable.kind {
                VariableKind::Universal => !self.names.contains_key(&variable.name),
                VariableKind::Global => false,
                _ => true,
            },
            TermKind::Aggregate(_) => true,
            _ => false,
        })
    }

    /// Compiles a body operation into a filter over bound variables.
    ///
    /// A ground condition compiles like any other, and the constant-folding pass evaluates it
    /// once.
    fn filter(&mut self, operation: &Operation, origin: FilterOrigin) -> Result<(), Diagnostic> {
        let mut builder = StackProgramBuilder::default();
        builder.operation(
            &*self.translator,
            VariableScope::Rule(&self.names),
            operation,
        )?;
        let inputs = builder.inputs.clone();
        let program = builder.finish(operation.span)?;
        self.filters.push(RuleFilter {
            inputs,
            program,
            origin,
        });
        Ok(())
    }

    /// Compiles a term in a scope over the rule's variables: the
    /// variables feeding the program, in input order, and the program.
    fn compile_term(
        &self,
        term: &Term,
        scope: VariableScope<'_>,
    ) -> Result<(Vec<VariableId>, StackProgram), Diagnostic> {
        let mut builder = StackProgramBuilder::default();
        builder.term(&*self.translator, scope, term)?;
        let inputs = builder.inputs.clone();
        Ok((inputs, builder.finish(term.span)?))
    }

    /// Returns the id of a named variable, assigned on first sight.
    pub(super) fn variable(&mut self, name: &str) -> VariableId {
        if let Some(&id) = self.names.get(name) {
            return id;
        }
        let id = self.fresh();
        self.names.insert(name.to_owned(), id);
        id
    }

    /// Returns a fresh, unnamed variable id.
    pub(super) fn fresh(&mut self) -> VariableId {
        let id = self.variables;
        self.variables += 1;
        id
    }

    /// Records an equality filter from raw program parts.
    fn equality_filter(
        &mut self,
        inputs: Vec<VariableId>,
        operations: Vec<OpCode>,
        constants: Vec<DataValue>,
        origin: FilterOrigin,
    ) {
        let arity = inputs.len();
        let program = StackProgram::new(operations, constants, arity);
        self.filters.push(RuleFilter {
            inputs,
            program,
            origin,
        });
    }

    /// Records a computed column from raw program parts, returning its
    /// rule column.
    fn computed_column(
        &mut self,
        inputs: Vec<VariableId>,
        operations: Vec<OpCode>,
        constants: Vec<DataValue>,
    ) -> usize {
        let arity = inputs.len();
        let program = StackProgram::new(operations, constants, arity);
        self.computed_program(inputs, program)
    }

    /// Records a computed column from a compiled program, returning its
    /// rule column.
    fn computed_program(&mut self, inputs: Vec<VariableId>, program: StackProgram) -> usize {
        self.computed.push(ComputedColumn { inputs, program });
        self.variables + self.computed.len() - 1
    }
}

/// The columns of a negated atom gathered so far, and what compiling them needs.
#[derive(Default)]
struct NegatedColumns {
    /// The columns, by delivered position.
    columns: Vec<NegatedColumn>,
    /// The filters over the atom's own columns.
    filters: Vec<RuleFilter>,
    /// Per bound variable, the delivered position of its first occurrence.
    shared_at: HashMap<VariableId, usize>,
    /// Per variable of the literal's own, the delivered position of its first occurrence.
    local: HashMap<String, usize>,
}

/// Returns the filter constraining two of a negated atom's columns identical,
/// for a variable written again at the given position.
fn equal_columns(first: usize, position: usize, span: Option<Span>) -> RuleFilter {
    RuleFilter {
        inputs: vec![first, position],
        program: StackProgram::new(
            vec![OpCode::Input(0), OpCode::Input(1), OpCode::Same],
            Vec::new(),
            2,
        ),
        origin: FilterOrigin::Repeated(span),
    }
}
