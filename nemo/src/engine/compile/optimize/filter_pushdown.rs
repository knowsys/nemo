//! Filter pushdown: the rows no rule can use, excluded at load.

use std::collections::HashMap;

use nemo_io::row_filter::RowFilter;
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    engine::{
        compile::import_plan::ImportPlan,
        compile::optimize::{ImportPass, ImportSurvey},
        compile::translation::Translator,
    },
    model::{
        Literal, Operation, Operator, Rule, Substitution, Term, TermKind, Variable, VariableKind,
        Walk,
    },
};

/// Finds the import rows no rule can use.
///
/// The pass applies to a predicate that only rules read, and only positively
/// (not a fact, rule head, export, `@input`, or negated atom),
/// if *every* body atom reading it (a *reader*) constrains some column:
/// by a constant in the atom,
/// or by a body comparison of an expression over one of its variables against a ground value.
/// Such a comparison holds for every match of the rule, wherever else the variable occurs.
/// The loader then admits only rows passing the disjunction, over the readers,
/// of each reader's constraints.
/// One unconstrained reader disables the pass for the predicate.
/// A loader that can ask its source for less, a SPARQL endpoint,
/// also receives the condition as written, to translate as far as it can.
///
/// Every rule keeps its own filter, since rows added by other means
/// ([import_rows](crate::engine::Engine::import_rows), [extend](crate::engine::Engine::extend))
/// still pass through it.
/// The pushdown therefore only reduces what is loaded, and never changes what is derived.
#[derive(Debug)]
pub(crate) struct FilterPushdown;

/// The comparison operators a reader's constraint may use.
const COMPARISONS: [Operator; 8] = [
    Operator::Equal,
    Operator::Unequal,
    Operator::ValueEqual,
    Operator::ValueUnequal,
    Operator::Greater,
    Operator::GreaterEq,
    Operator::Less,
    Operator::LessEq,
];

impl ImportPass for FilterPushdown {
    fn name(&self) -> &'static str {
        "filter"
    }

    fn run(
        &self,
        survey: &ImportSurvey,
        translator: &Translator<'_>,
        plan: &mut ImportPlan,
    ) -> Result<(), Diagnostic> {
        // Per candidate, the readers' constraints over delivered
        // columns; `None` once a reader has none.
        let mut readers: HashMap<String, Option<Vec<Operation>>> = HashMap::new();
        for rule in &survey.rules {
            let comparisons = simple_comparisons(rule);
            for literal in &rule.body {
                let Literal::Positive(atom) = literal else {
                    continue;
                };
                let name = translator.resolve(&atom.predicate, atom.span)?;
                if !survey.optimizable(&name) || survey.negated.contains(&name) {
                    continue;
                }
                let entry = readers
                    .entry(name.clone())
                    .or_insert_with(|| Some(Vec::new()));
                let Some(conjunctions) = entry else { continue };
                // The condition refers to delivered positions: the columns
                // the loader stores after the projection pass pruned.
                let layout = plan.layout_of(&name, atom.terms.len());
                let mut conjuncts = Vec::new();
                for (kept, term) in atom.terms.iter().enumerate() {
                    let Some(delivered) = layout.delivered_position(kept) else {
                        continue;
                    };
                    let column = column_variable(delivered);
                    match &term.kind {
                        TermKind::Variable(variable)
                            if variable.kind == VariableKind::Universal =>
                        {
                            if let Some(operations) = comparisons.get(&variable.name) {
                                let substitution: Substitution =
                                    [(variable.clone(), column.clone())].into_iter().collect();
                                for operation in operations {
                                    let mut operation = (*operation).clone();
                                    substitution.apply(&mut operation);
                                    conjuncts.push(operation);
                                }
                            }
                        }
                        TermKind::Variable(_) => {}
                        _ if term.is_ground() => {
                            conjuncts
                                .push(Operation::new(Operator::Equal, vec![column, term.clone()]));
                        }
                        _ => {}
                    }
                }
                match conjoin(conjuncts) {
                    Some(conjunction) => conjunctions.push(conjunction),
                    None => *entry = None,
                }
            }
        }
        for (name, conjunctions) in readers {
            let Some(conjunctions) = conjunctions else {
                continue;
            };
            let Some(disjunction) = disjoin(conjunctions) else {
                continue;
            };
            // A constraint the compiler cannot express is dropped, and
            // the rule's own compilation reports what is wrong with it.
            let Ok((inputs, program)) = translator.condition(&disjunction) else {
                continue;
            };
            plan.restrict(name, RowFilter::new(inputs, program), disjunction);
        }
        Ok(())
    }
}

/// Returns the one universal variable a term mentions, if it mentions exactly
/// one (however often) and no other kind of variable except global ones (`$x`).
fn single_variable(term: &Term) -> Option<String> {
    let mut aggregated = false;
    term.walk(&mut |term| aggregated |= matches!(term.kind, TermKind::Aggregate(_)));
    if aggregated {
        return None;
    }
    let mut found: Option<&str> = None;
    for variable in term.variables() {
        match variable.kind {
            VariableKind::Universal => match found {
                Some(name) if name != variable.name => return None,
                _ => found = Some(&variable.name),
            },
            VariableKind::Global => {}
            VariableKind::Existential | VariableKind::Anonymous => return None,
        }
    }
    found.map(str::to_owned)
}

/// Returns the pseudo-variable standing for a delivered column in an import's condition.
///
/// The language cannot write its name, so it collides with no rule variable.
pub(crate) fn column_variable(position: usize) -> Term {
    Term::variable(Variable::universal(format!("column {position}")))
}

/// Returns, per variable, the body comparisons of an expression over it alone against a ground
/// value.
fn simple_comparisons(rule: &Rule) -> HashMap<String, Vec<&Operation>> {
    let mut found: HashMap<String, Vec<&Operation>> = HashMap::new();
    for literal in &rule.body {
        let Literal::Operation(operation) = literal else {
            continue;
        };
        if !COMPARISONS.contains(&operation.operator) || operation.arguments.len() != 2 {
            continue;
        }
        // The rule draws a nondeterministic call once per row;
        // a copy at the loader would draw a second time.
        let draws = operation.arguments.iter().any(|argument| {
            argument.any_term(|term| {
                matches!(&term.kind, TermKind::Operation(call) if call.operator.is_nondeterministic())
            })
        });
        if draws {
            continue;
        }
        let (left, right) = (&operation.arguments[0], &operation.arguments[1]);
        let name = match (left.is_ground(), right.is_ground()) {
            (false, true) => single_variable(left),
            (true, false) => single_variable(right),
            _ => None,
        };
        if let Some(name) = name {
            found.entry(name).or_default().push(operation);
        }
    }
    found
}

/// Returns the conjunction of conditions, `None` for none.
pub(crate) fn conjoin(conditions: Vec<Operation>) -> Option<Operation> {
    combine(Operator::And, conditions)
}

/// Returns the disjunction of conditions, `None` for none.
fn disjoin(conditions: Vec<Operation>) -> Option<Operation> {
    combine(Operator::Or, conditions)
}

/// Folds conditions pairwise under a binary connective.
fn combine(connective: Operator, conditions: Vec<Operation>) -> Option<Operation> {
    let mut conditions = conditions.into_iter();
    let first = conditions.next()?;
    Some(conditions.fold(first, |left, right| {
        Operation::new(
            connective,
            vec![
                Term::new(TermKind::Operation(left)),
                Term::new(TermKind::Operation(right)),
            ],
        )
    }))
}
