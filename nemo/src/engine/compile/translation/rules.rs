//! Rules: compiling one into its normalized executable form.

use std::collections::HashMap;

use nemo_physical::{diagnostics::Diagnostic, function::StackProgram};

use crate::{
    diagnostics::TranslationError,
    engine::{
        compile::compiled::{
            CompiledAggregate, CompiledRule, ExistentialHead, ExistentialTerm, FilterOrigin,
            HeadAtom, NegatedAtom,
        },
        compile::translation::{
            Translator,
            compiler::RuleCompiler,
            terms::{collect_aggregates, destructure, is_structured},
            unsupported,
        },
    },
    model::{Atom, Literal, Rule, Span, TermKind, VariableKind, Walk},
};

/// A rule's compiled head: its ordinary atoms,
/// its aggregate head if it has one,
/// and its existential head if `!`-variables appear.
struct CompiledHead {
    /// The head atoms over the rule's columns.
    atoms: Vec<HeadAtom>,
    /// The aggregate head, if any.
    aggregate: Option<CompiledAggregate>,
    /// The existential head (frontier, satisfaction check, and inserted atoms), if any.
    existential: Option<ExistentialHead>,
}

/// Returns whether an atom's terms hold an aggregate anywhere.
fn aggregates(atom: &Atom) -> bool {
    let mut found = Vec::new();
    for term in &atom.terms {
        collect_aggregates(term, &mut found);
    }
    !found.is_empty()
}

/// Returns whether an existential variable is one of an atom's terms, or inside one.
///
/// The scope of an aggregate lists the variables its contributions are told apart by,
/// so an existential variable there is an error of the aggregate,
/// not a value the rule creates.
fn creates_values(atom: &Atom) -> bool {
    atom.any_term(|term| {
        matches!(&term.kind, TermKind::Variable(variable)
            if variable.kind == VariableKind::Existential)
    })
}

impl<'a> Translator<'a> {
    /// Compiles one rule into its normalized executable form.
    pub(super) fn rule(
        &mut self,
        rule: &Rule,
    ) -> Result<(CompiledRule, Vec<Option<String>>), Diagnostic> {
        let normalized = self.positional_rule(rule)?;
        let rule = normalized.as_ref().unwrap_or(rule);
        let mut compiler = RuleCompiler::new(self);
        let negated = compiler.body(rule)?;
        let head = compiler.head(rule)?;
        let nondeterministic = nondeterministic(&compiler, &negated, head.aggregate.as_ref());
        let names = compiler.source_names(compiler.variables + compiler.computed.len());

        let compiled = CompiledRule {
            variables: compiler.variables,
            // Replaced once every rule is compiled; first-occurrence
            // order until then.
            order: (0..compiler.variables).collect(),
            body: compiler.body,
            filters: compiler.filters,
            bindings: compiler.bindings,
            computed: compiler.computed,
            negated,
            heads: head.atoms,
            aggregate: head.aggregate,
            existential: head.existential,
            never_fires: compiler.never_fires,
            nondeterministic,
            last_applied: 0,
            #[cfg(feature = "sparql")]
            fetch: None,
        };
        Ok((compiled, names))
    }
}

/// Returns whether any program the rule evaluates calls a nondeterministic function
/// (such as `RAND()`).
fn nondeterministic(
    compiler: &RuleCompiler<'_, '_>,
    negated: &[NegatedAtom],
    aggregate: Option<&CompiledAggregate>,
) -> bool {
    compiler
        .filters
        .iter()
        .chain(negated.iter().flat_map(|atom| &atom.filters))
        .map(|filter| &filter.program)
        .chain(compiler.bindings.iter().map(|binding| &binding.program))
        .chain(compiler.computed.iter().map(|column| &column.program))
        .chain(
            aggregate
                .iter()
                .flat_map(|aggregate| &aggregate.post)
                .map(|post| &post.program),
        )
        .any(StackProgram::is_nondeterministic)
}

impl RuleCompiler<'_, '_> {
    /// Compiles a rule's body.
    ///
    /// The positive atoms come first,
    /// so that operations and negated atoms resolve against the full variable set wherever
    /// they appear.
    /// The conditions come next, then the negated atoms,
    /// each with its columns split into those shared with the body and its own.
    fn body(&mut self, rule: &Rule) -> Result<Vec<NegatedAtom>, Diagnostic> {
        let mut operations = Vec::new();
        let mut written = Vec::new();
        for literal in &rule.body {
            match literal {
                Literal::Positive(atom) => self.body_atom(atom)?,
                Literal::Negative(atom) => written.push(atom),
                Literal::Operation(operation) => operations.push(operation),
            }
        }
        if self.body.is_empty() {
            return Err(TranslationError::NoPositiveAtom.at(rule.span));
        }
        self.conditions(operations)?;

        let mut negated = Vec::new();
        let mut foreign: HashMap<String, Option<Span>> = HashMap::new();
        for atom in written {
            negated.push(self.negated_atom(atom, &mut foreign)?);
        }
        Ok(negated)
    }

    /// Resolves the body's conditions together with the equalities the atoms deferred.
    ///
    /// A deferred column equality whose expression is fully atom-bound becomes a probing
    /// binding.
    /// A structured expression with unbound variables is a pattern,
    /// and destructures into a shape guard plus equalities per position.
    /// The rest join the operation fixpoint (their inputs may still be bound by assignments).
    /// The guard precedes the position equalities,
    /// so among filters at one layer it rejects a mismatched row before an accessor could
    /// error on it.
    /// Every operation carries what it stands for, for the filter it may become:
    /// a written condition, an expression argument, or a pattern.
    fn conditions(&mut self, operations: Vec<&crate::model::Operation>) -> Result<(), Diagnostic> {
        let deferred = std::mem::take(&mut self.deferred);
        let mut synthesized = Vec::new();
        let mut patterns = Vec::new();
        let mut plain = Vec::new();
        for operation in &deferred {
            if self.binding(operation)? {
                continue;
            }
            let pattern = &operation.arguments[1];
            if is_structured(pattern) {
                destructure(&operation.arguments[0], pattern, &mut synthesized)?;
                patterns.resize(synthesized.len(), FilterOrigin::Pattern(pattern.span));
            } else {
                plain.push(operation);
            }
        }
        let mut pending: Vec<_> = operations
            .into_iter()
            .map(|operation| (operation, FilterOrigin::Condition(operation.span)))
            .collect();
        pending.extend(
            plain
                .into_iter()
                .map(|operation| (operation, FilterOrigin::Expression(operation.span))),
        );
        pending.extend(synthesized.iter().zip(patterns));
        self.operations(pending)
    }

    /// Compiles a rule's head.
    ///
    /// Every head atom of an existential rule joins the whole-head satisfaction check,
    /// atoms without existential variables staying ordinary heads
    /// (they fire for every delta match).
    /// Otherwise at most one atom aggregates.
    fn head(&mut self, rule: &Rule) -> Result<CompiledHead, Diagnostic> {
        let mut head = CompiledHead {
            atoms: Vec::new(),
            aggregate: None,
            existential: None,
        };
        if rule.head.iter().any(creates_values) {
            let mut frontier = Vec::new();
            let mut existentials: HashMap<String, usize> = HashMap::new();
            let mut check = Vec::new();
            let mut inserts = Vec::new();
            for atom in &rule.head {
                if aggregates(atom) {
                    return Err(unsupported(
                        "an aggregate in an existential rule",
                        atom.span,
                    ));
                }
                let compiled = self.existential_atom(atom, &mut frontier, &mut existentials)?;
                if compiled
                    .columns
                    .iter()
                    .any(|column| matches!(column, ExistentialTerm::Existential(_)))
                {
                    inserts.push(compiled.clone());
                } else {
                    head.atoms.push(self.head_atom(atom)?);
                }
                check.push(compiled);
            }
            head.existential = Some(ExistentialHead {
                frontier,
                existentials: existentials.len(),
                check,
                inserts,
            });
        } else {
            for atom in &rule.head {
                if !aggregates(atom) {
                    head.atoms.push(self.head_atom(atom)?);
                } else if head.aggregate.is_some() {
                    return Err(TranslationError::SeveralAggregates.at(atom.span));
                } else {
                    head.aggregate = Some(self.aggregate_head(atom)?);
                }
            }
        }
        Ok(head)
    }
}
