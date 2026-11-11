//! Filter pushdown: the import pass that excludes at load the rows that no rule can use.

use std::collections::HashMap;

use nemo_io::selection::RowCondition;
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

/// The import pass that finds the rows of imported predicates that no rule can use.
///
/// A *reader* of a predicate is a positive body atom of the predicate.
/// The pass applies to a predicate that only rule bodies read, and only positively,
/// if every reader constrains some column in one of two ways:
/// - by a constant in the atom;
/// - by a body comparison between a ground value and an expression over one of the atom's
///   variables. Such a comparison holds for every match of the rule,
///   wherever else the variable occurs.
///
/// The load then admits only the rows that satisfy the disjunction, over the readers,
/// of each reader's constraints.
/// A load that can ask its source for fewer rows, such as a SPARQL query,
/// also receives the condition as written, and translates as much of it as it can.
///
/// Every rule keeps its own filter, since rows added by other means
/// ([import_rows](crate::engine::Engine::import_rows), [extend](crate::engine::Engine::extend))
/// still pass through it.
/// So the pushdown only reduces what is loaded, and never changes what is derived.
#[derive(Debug)]
pub(crate) struct FilterPushdown;

/// The comparison operators that a reader's constraint may use.
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
        // Per predicate, the readers' constraints over delivered columns,
        // or `None` once a reader has none.
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
                // The condition refers to delivered columns,
                // the columns that remain after the projection pass.
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
            // A condition that cannot be compiled is dropped,
            // and the rule's own compilation reports what is wrong with it.
            let Ok((inputs, program)) = translator.condition(&disjunction) else {
                continue;
            };
            plan.restrict(name, RowCondition::new(inputs, program), disjunction);
        }
        Ok(())
    }
}

/// Returns the universal variable that a term mentions, if it mentions exactly one,
/// however often.
///
/// The term may also mention parameters (`$x`), but no other kind of variable and no
/// aggregate.
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

/// Returns the variable that stands for a delivered column in an import's condition.
///
/// Its name, `column N`, contains a space, which no variable written in a program can,
/// so it collides with no rule variable.
pub(crate) fn column_variable(position: usize) -> Term {
    Term::variable(Variable::universal(format!("column {position}")))
}

/// Returns, per variable, the body comparisons between a ground value and an expression over
/// that variable alone.
///
/// A comparison that calls a nondeterministic function is left out.
fn simple_comparisons(rule: &Rule) -> HashMap<String, Vec<&Operation>> {
    let mut found: HashMap<String, Vec<&Operation>> = HashMap::new();
    for literal in &rule.body {
        let Literal::Operation(operation) = literal else {
            continue;
        };
        if !COMPARISONS.contains(&operation.operator) || operation.arguments.len() != 2 {
            continue;
        }
        // The rule evaluates a nondeterministic call once per row,
        // and a copy in the load would evaluate it a second time.
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

/// Returns the conjunction of the conditions, or `None` if there are none.
pub(crate) fn conjoin(conditions: Vec<Operation>) -> Option<Operation> {
    combine(Operator::And, conditions)
}

/// Returns the disjunction of the conditions, or `None` if there are none.
fn disjoin(conditions: Vec<Operation>) -> Option<Operation> {
    combine(Operator::Or, conditions)
}

/// Combines the conditions with a binary connective from left to right,
/// or returns `None` if there are none.
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
