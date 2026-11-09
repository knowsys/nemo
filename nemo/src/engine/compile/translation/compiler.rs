//! The compiler of one rule: its body atoms, negated atoms, heads and aggregates.
//!
//! The *operation fixpoint* resolves the body's operations into assignments and filters.
//! The compiler refers to values by [rule column](crate::engine::compile::compiled).

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
        Aggregate, Atom, Fact, Identifier, Operation, Operator, Span, Term, TermKind, Variable,
        VariableKind, Walk,
    },
};

/// The hidden predicate with one row, which a rule body without positive atoms reads.
///
/// Its name begins with a NUL byte, which no program can write.
const UNIT: &str = "\u{0}unit";

/// Returns the diagnostic of an unsafe rule, for a variable that the body does not bind.
///
/// `role` says what the variable is used as, such as a head variable.
fn unsafe_variable(role: &str, variable: &str, span: Option<Span>) -> Diagnostic {
    TranslationError::UnsafeVariable {
        role: role.to_owned(),
        variable: variable.to_owned(),
    }
    .at(span)
}

/// The state of the compilation of one rule.
pub(super) struct RuleCompiler<'a, 'b> {
    /// The translator that compiles the rule.
    pub(super) translator: &'b mut Translator<'a>,
    /// The rule column of each variable, by name.
    pub(super) names: HashMap<String, VariableId>,
    /// The number of variables so far.
    pub(super) variables: usize,
    /// The body atoms compiled so far.
    pub(super) body: Vec<BodyAtom>,
    /// The filters compiled so far.
    pub(super) filters: Vec<RuleFilter>,
    /// The bindings compiled so far.
    pub(super) bindings: Vec<RuleBinding>,
    /// The computed columns compiled so far.
    pub(super) computed: Vec<ComputedColumn>,
    /// The equalities created for the expression arguments of body atoms.
    ///
    /// Each of them becomes a binding, or joins the operation fixpoint.
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

/// Returns the post programs of an aggregate head,
/// with their inputs mapped to positions in a row of the aggregation's output.
///
/// Such a row has the group columns followed by the aggregates' results.
/// An input that is a rule column is mapped to its position in the group,
/// and [AGGREGATE_RESULT] to the position of the result of the expression's own aggregate.
/// `raw_posts` contains, per post program, the index of its aggregate,
/// its inputs as rule columns, and the program.
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
                        // The placeholder stands for the result of this expression's own
                        // aggregate.
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
    /// Returns the written name of each of the first `width` rule columns,
    /// for the messages of the type analysis.
    ///
    /// A column without a written name has `None`.
    /// Such columns include those of hidden variables,
    /// whose names begin with a NUL byte, which no program can write.
    /// A name can also belong to a computed column, for a variable that an assignment binds.
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
    /// A constant or a repeated variable becomes a fresh variable with an identity filter,
    /// since a repeated variable or a constant in an atom means term identity (`=`),
    /// not value equality.
    pub(super) fn body_atom(&mut self, atom: &Atom) -> Result<(), Diagnostic> {
        let name = self.translator.resolve(&atom.predicate, atom.span)?;
        let layout = self.translator.plan.layout_of(&name, atom.terms.len());
        let table = self
            .translator
            .table(&atom.predicate, layout.delivered(), atom.span)?;

        let mut variables = Vec::with_capacity(layout.delivered());
        for (position, term) in atom.terms.iter().enumerate() {
            // A pruned column is not stored, and no rule reads its term,
            // so there is nothing to bind.
            if layout.is_pruned(position) {
                continue;
            }
            let variable = self.body_column(term, &variables)?;
            variables.push(variable);
        }
        self.body.push(BodyAtom { table, variables });
        Ok(())
    }

    /// Adds the atom that a body without positive atoms reads, which matches the one row of a
    /// hidden table.
    ///
    /// So the body's conditions are evaluated once.
    pub(super) fn unit_atom(&mut self) -> Result<(), Diagnostic> {
        let unit = Fact::new(Identifier::Iri(UNIT.to_owned()), vec![Term::integer(0)]);
        self.translator.fact(&unit)?;
        let table = self.translator.table(&unit.predicate, 1, None)?;
        let variable = self.fresh();
        self.body.push(BodyAtom {
            table,
            variables: vec![variable],
        });
        Ok(())
    }

    /// Returns the variable that one term of a positive body atom binds.
    ///
    /// `earlier` contains the variables that the atom's earlier columns bind.
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
                // A variable repeated within one atom gets a fresh column variable,
                // which must be identical to the first occurrence.
                let fresh = self.fresh();
                self.equality_filter(
                    vec![fresh, id],
                    vec![OpCode::Input(0), OpCode::Input(1), OpCode::Same],
                    Vec::new(),
                    FilterOrigin::Repeated(term.span),
                );
                Ok(fresh)
            }
            // Each `_` gets its own fresh variable, which nothing reads.
            TermKind::Variable(variable) if variable.kind == VariableKind::Anonymous => {
                Ok(self.fresh())
            }
            TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
                // A parameter becomes a column constrained to its value.
                let value = self.translator.global(&variable.name, term.span)?;
                Ok(self.constant_column(value, term.span))
            }
            TermKind::Variable(_) => Err(unsupported(
                "an existential variable in a rule body",
                term.span,
            )),
            _ if mentions(term, VariableKind::Universal) || is_pattern(term) => {
                // An expression argument, or a pattern whose only variables are anonymous.
                // The column gets a fresh named variable, and a deferred equality with the
                // term constrains it. An expression is compared once the operation
                // fixpoint has bound its inputs, and a pattern is destructured.
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
                // A ground term gets a fresh column variable constrained to its value.
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
    /// A variable that the positive body binds becomes a shared column,
    /// and every other column is private to the atom.
    /// Constants and repeated variables become filters over the atom's own columns.
    /// A variable that the positive body does not bind is local to one negated atom,
    /// so using it in two of them is an error.
    /// `foreign` contains the local variables of the earlier negated atoms,
    /// with the positions of their atoms.
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
            // A pruned column is not stored, and no rule reads its term,
            // so there is nothing to bind.
            if layout.is_pruned(position) {
                continue;
            }
            let column = self.negated_column(term, &mut negated, foreign)?;
            negated.columns.push(column);
        }
        // This atom's local variables are foreign to the later negated atoms.
        for (name, _) in negated.local {
            foreign.insert(name, atom.span);
        }

        Ok(NegatedAtom {
            table,
            columns: negated.columns,
            filters: negated.filters,
        })
    }

    /// Returns the kind of column that one term of a negated atom stands for,
    /// and adds the filters that it needs over the atom's own columns.
    ///
    /// Filters and first occurrences refer to delivered columns, the columns gathered so far.
    /// Pruning can make a term's delivered position smaller than its position among the kept
    /// columns.
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
                        // A bound variable written again: the later occurrence is a private
                        // column that must be identical to the first.
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
                // A ground term, or a parameter, becomes a private column constrained to its
                // value.
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

    /// Compiles one atom of an existential head.
    ///
    /// Universal variables join the frontier, existential variables are numbered in order of
    /// first mention, and every other term is a constant.
    /// The head's atoms share `frontier` and `existentials`, the numbers of the existential
    /// variables by name.
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
                    // The constant is encoded here, so that a constant that the dictionary
                    // cannot hold is an error at its position,
                    // rather than at the first row that the head creates.
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

    /// Compiles a head atom with aggregates.
    ///
    /// The positions without an aggregate form the group,
    /// and an expression there becomes a computed group column.
    /// The input, value and scope variables of each aggregate resolve to rule columns,
    /// and an expression around an aggregate becomes a post program over the aggregation's
    /// output.
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
    /// No aggregate may aggregate a group variable written in the rule.
    fn check_aggregate(
        &self,
        group: &[usize],
        functions: &[CompiledAggregateFunction],
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        for function in functions {
            if let Some(variable) = function
                .input
                .iter()
                .chain(&function.value)
                .filter(|column| group.contains(column))
                .find_map(|&column| self.written_name(column))
            {
                return Err(TranslationError::AggregatedGroupColumn { variable }.at(span));
            }
        }
        Ok(())
    }

    /// Returns the rule column of a position of an aggregate head without an aggregate:
    /// the column of a group variable, or a computed column for an expression.
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

    /// Returns the written name of a variable that binds a rule column,
    /// or `None` if only a hidden variable binds it,
    /// such as the variable that a check binds to a parameter without a value.
    fn written_name(&self, column: usize) -> Option<String> {
        self.names
            .iter()
            .find(|(name, id)| **id == column && !name.starts_with('\u{0}'))
            .map(|(name, _)| name.clone())
    }

    /// Compiles the parts of an aggregate: the function, the aggregated column,
    /// and the scope columns.
    ///
    /// An input expression becomes a computed column.
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
            // A scope list after a comma means the same as one after `per`,
            // but the keyword makes it explicit.
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
            // A repeated variable, or the input itself, adds nothing to the scope.
            if input != Some(id) && value != Some(id) && !scope.contains(&id) {
                scope.push(id);
            }
        }
        // `#count()` counts the body matches, so all body variables together identify a
        // contribution.
        let scope = (!aggregate.counts_matches()).then_some(scope);
        Ok(CompiledAggregateFunction {
            kind,
            input,
            value,
            scope,
        })
    }

    /// Returns the rule column of one operand of an aggregate:
    /// the variable's column for a bound variable, and a computed column for an expression.
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

    /// Compiles a head atom over rule columns.
    ///
    /// Constants, parameters, expressions and repeated variables become computed columns.
    pub(super) fn head_atom(&mut self, atom: &Atom) -> Result<HeadAtom, Diagnostic> {
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
                    if columns.contains(&id) {
                        // A variable repeated in the head atom becomes a computed copy.
                        self.computed_column(vec![id], vec![OpCode::Input(0)], Vec::new())
                    } else {
                        id
                    }
                }
                TermKind::Variable(variable) if variable.kind == VariableKind::Anonymous => {
                    return Err(TranslationError::AnonymousAsValue.at(term.span));
                }
                TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
                    // A parameter becomes a computed column with its value.
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
                    // A head expression becomes a computed column over its variables.
                    // A ground expression is evaluated once, to a constant,
                    // unless it calls a nondeterministic function such as `RAND()`,
                    // which is evaluated per row.
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
                    // A ground term becomes a computed column with its value.
                    let value = self.translator.evaluate(term)?;
                    self.computed_column(Vec::new(), vec![OpCode::Constant(0)], vec![value])
                }
            };
            columns.push(column);
        }
        Ok(HeadAtom { table, columns })
    }

    /// Resolves the body's operations by the operation fixpoint.
    ///
    /// An equality between an unbound variable and an expression over bound variables is an
    /// [assignment](Self::assignment), which becomes a computed column.
    /// The operations are passed over in body order until a pass finds no further assignment,
    /// so a chain of assignments may be written in any order.
    /// Every other operation becomes a filter,
    /// which comes from the part of the rule's text that its origin in `pending` names.
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

    /// Compiles a deferred equality into a binding if the body atoms bind every variable of
    /// its expression, and returns whether it did.
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

    /// Compiles an operation as an assignment if it is one, and returns whether it did.
    ///
    /// An assignment is an equality with an unbound universal variable on one side,
    /// and only bound variables on the other side.
    fn assignment(&mut self, operation: &Operation) -> Result<bool, Diagnostic> {
        if operation.operator != Operator::Equal || operation.arguments.len() != 2 {
            return Ok(false);
        }
        // Either side may be the unbound variable.
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
    /// A parameter stands for its value, so it is always bound.
    /// A term with an aggregate is never bound.
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
    /// A ground condition is compiled like any other,
    /// and the constant folding of the [program passes](crate::engine::compile::optimize)
    /// evaluates it once.
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

    /// Compiles a term into a program, and returns the columns that feed the program,
    /// in input order, with the program.
    ///
    /// `scope` decides which variables the term may read.
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

    /// Returns the id of a named variable, and assigns a fresh one on first use.
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

    /// Records an equality filter, given the parts of its program.
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

    /// Records a computed column, given the parts of its program, and returns its rule column.
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

    /// Records a computed column with a compiled program, and returns its rule column.
    fn computed_program(&mut self, inputs: Vec<VariableId>, program: StackProgram) -> usize {
        self.computed.push(ComputedColumn { inputs, program });
        self.variables + self.computed.len() - 1
    }
}

/// The columns of a negated atom gathered so far,
/// and what the compilation of the remaining columns needs.
#[derive(Default)]
struct NegatedColumns {
    /// The columns, by delivered position.
    columns: Vec<NegatedColumn>,
    /// The filters over the atom's own columns.
    filters: Vec<RuleFilter>,
    /// Per bound variable, the delivered position of its first occurrence.
    shared_at: HashMap<VariableId, usize>,
    /// Per local variable of the atom, the delivered position of its first occurrence.
    local: HashMap<String, usize>,
}

/// Returns the filter that requires two columns of a negated atom to be identical,
/// for a variable written again at delivered position `position`.
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
