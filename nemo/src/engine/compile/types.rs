//! The type analysis: refined types over the compiled program.
//!
//! A monotone fixpoint computes, per predicate column,
//! an upper bound on the [types](Type) of value it can hold,
//! with tuple and map types refined to their element types up to a fixed depth.
//! Facts and import formats seed it, and rules propagate it:
//! body variables intersect the columns they join, heads union what their rules can write.
//!
//! The warnings are proofs against that over-approximation, so they carry no false positives:
//! a rule whose body admits no value at some position *cannot* fire,
//! and the program still runs.
//! The declarations are held to the same over-approximation from the other side:
//! a program runs only if every value it can write into a declared column provably fits
//! the declaration, and every labeled value it builds the `@type` of its label.
//! A write that cannot fit violates the declaration;
//! one that might not is not shown to fit; both are errors.

mod columns;
mod declarations;
mod proofs;

use std::collections::{HashMap, HashSet};

use nemo_io::format::ValueFormat;
use nemo_physical::{
    diagnostics::Diagnostic,
    management::TableId,
    value::{LabelTypes, Type},
};

use crate::{
    diagnostics::TypeError,
    engine::{
        compile::compiled::CompiledRule, compile::import_plan::ImportPlan, registered::Registered,
    },
    model::Span,
};

use self::{
    columns::{each_write, joined_columns, rule_columns},
    declarations::{declaration_violations, nominal},
    proofs::{ProofContext, never_fires_warning},
};

pub(crate) use self::proofs::builtin_hint;

/// What a rule looked like before compilation, as far as reporting needs it:
/// its position, and what its variables were called.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RuleSourceInfo<'a> {
    /// The rule's source position.
    pub(crate) span: Option<Span>,
    /// The variables' source names by [VariableId](super::compiled::VariableId).
    ///
    /// The columns the normalization invented have none.
    pub(crate) names: &'a [Option<String>],
}

/// A predicate's declared columns:
/// the types a `@declare` states, and the names that let atoms fill columns by name.
#[derive(Debug, Clone)]
pub(crate) struct PredicateDeclaration {
    /// The declared types, one per column, at refinement depth.
    pub(crate) types: Vec<Type>,
    /// The declared column names, aligned with the types.
    pub(crate) names: Vec<Option<String>>,
    /// The declaration's source position.
    pub(crate) span: Option<Span>,
}

impl PredicateDeclaration {
    /// Returns the position of the column a name labels.
    pub(crate) fn position(&self, label: &str) -> Option<usize> {
        self.names
            .iter()
            .position(|declared| declared.as_deref() == Some(label))
    }
}

/// What the program declares about values:
/// the columns of its predicates (`@declare`), and the fields of its labels (`@type`).
pub(crate) struct Declared<'a> {
    /// The declared columns, by predicate name.
    pub(crate) predicates: &'a HashMap<String, PredicateDeclaration>,
    /// The type each `@type` declares for its label, by resolved label.
    pub(crate) labels: &'a LabelTypes,
}

/// What the analysis reads besides the rules.
pub(crate) struct AnalysisInputs<'a> {
    /// The registered predicates by resolved name.
    pub(crate) predicates: &'a HashMap<String, Registered>,
    /// The column type seeds per table: facts, import formats, and rows added later.
    pub(crate) seeds: &'a HashMap<TableId, Vec<Type>>,
    /// What the program declares.
    pub(crate) declared: Declared<'a>,
    /// The import-fed predicates whose columns are not statically known.
    pub(crate) open: &'a HashSet<String>,
    /// What the loaders of import-fed predicates leave out.
    pub(crate) plan: &'a ImportPlan,
    /// The column types the program was analyzed with before a change,
    /// if the analysis is of a change,
    /// so that an error can say what the change widened.
    pub(crate) previous: Option<&'a HashMap<TableId, Vec<Type>>>,
}

/// What each table is called and how wide it is, for messages and
/// declaration lookup.
type TableNames<'a> = HashMap<TableId, (&'a str, usize)>;

/// Returns the column of a predicate as the program writes it, counted from 0,
/// that a column of its table holds:
/// the same column, unless the loader leaves out columns no rule reads.
fn written_column(plan: &ImportPlan, predicate: &str, arity: usize, position: usize) -> usize {
    plan.stored_layout(predicate, arity)
        .kept_position(position)
        .unwrap_or(position)
}

/// Returns the column format that reads a declared type:
/// the format named like the type, whose cells become exactly the type's values.
///
/// A type without such a format, such as a union or a structure, is read as terms,
/// which the declaration then refuses, since terms are not shown to fit it.
pub(crate) fn declared_format(declared: &Type) -> ValueFormat {
    if declared.tuple_shapes().is_some() || declared.map_shapes().is_some() {
        return ValueFormat::Term;
    }
    ValueFormat::ALL
        .into_iter()
        .find(|format| *format != ValueFormat::Skip && format.domains() == declared.domains())
        .unwrap_or(ValueFormat::Term)
}

/// What the analysis found.
pub(crate) struct Analysis {
    /// The types each table's columns can hold.
    pub(crate) column_types: HashMap<TableId, Vec<Type>>,
    /// Per rule, the types each of its columns can hold once its conditions narrowed them,
    /// or nothing for a rule that fetches from an endpoint.
    pub(crate) rule_columns: Vec<Vec<Type>>,
    /// The warnings and errors, in rule order.
    pub(crate) findings: Vec<TypeFinding>,
}

/// A finding of the analysis.
pub(crate) struct TypeFinding {
    /// The index of the rule the finding is about, or `None` for one about no rule.
    pub(crate) rule: Option<usize>,
    /// What the analysis found.
    pub(crate) diagnostic: Diagnostic,
}

/// Runs the analysis, returning the inferred column types per table and per rule,
/// and the findings:
/// warnings for rules that provably cannot fire,
/// and errors for provable violations of a declaration.
///
/// A rule that fetches from an endpoint takes no part,
/// since its import's columns are seeded with whatever an endpoint may return.
pub(crate) fn analyze(
    rules: &[CompiledRule],
    source: &[RuleSourceInfo<'_>],
    inputs: &AnalysisInputs<'_>,
) -> Analysis {
    let AnalysisInputs {
        predicates,
        seeds,
        ref declared,
        open,
        plan,
        ..
    } = *inputs;
    let table_names: TableNames<'_> = predicates
        .iter()
        .map(|(name, &Registered { table, arity })| (table, (name.as_str(), arity)))
        .collect();
    let labels = declared.labels;
    let mut types = initial_types(predicates, seeds, declared.predicates, open, plan, labels);
    let mut problems: Vec<TypeFinding> =
        declared_arity_problems(predicates, declared.predicates, plan)
            .into_iter()
            .map(|diagnostic| TypeFinding {
                rule: None,
                diagnostic,
            })
            .collect();
    grow_to_fixpoint(rules, &mut types, labels);

    // The proofs, one report per rule.
    let mut rules_columns = Vec::with_capacity(rules.len());
    for (index, (rule, source)) in rules.iter().zip(source).enumerate() {
        if rule.fetches() {
            rules_columns.push(Vec::new());
            continue;
        }
        let columns = rule_columns(rule, &types, labels);
        if rule.never_fires {
            rules_columns.push(columns);
            continue;
        }
        let joined = joined_columns(rule, &types, labels);
        let mut found = Vec::new();
        let context = ProofContext {
            source: *source,
            types: &types,
            table_names: &table_names,
            labels,
            plan,
        };
        match never_fires_warning(rule, &context, &joined, &columns) {
            Some(warning) => found.push(warning),
            None => {
                declaration_violations(
                    rule,
                    *source,
                    &columns,
                    inputs,
                    &types,
                    &table_names,
                    &mut found,
                );
            }
        }
        problems.extend(found.into_iter().map(|diagnostic| TypeFinding {
            rule: Some(index),
            diagnostic,
        }));
        rules_columns.push(columns);
    }
    Analysis {
        column_types: types,
        rule_columns: rules_columns,
        findings: problems,
    }
}

/// Returns the types each table's columns start from.
///
/// An open predicate, a declared input or an import whose columns are not statically known,
/// holds whatever its declaration allows, or anything at all without one.
/// Everything else starts from its seeds and grows,
/// labeled values with a `@type` taken as the label's type.
fn initial_types(
    predicates: &HashMap<String, Registered>,
    seeds: &HashMap<TableId, Vec<Type>>,
    declarations: &HashMap<String, PredicateDeclaration>,
    open: &HashSet<String>,
    plan: &ImportPlan,
    labels: &LabelTypes,
) -> HashMap<TableId, Vec<Type>> {
    let mut types = HashMap::new();
    for (name, &Registered { table, arity }) in predicates {
        let columns = if open.contains(name.as_str()) {
            // A declaration refers to the kept columns, and a pruned
            // import stores only the delivered ones.
            let declared = declarations.get(name.as_str()).map(|declaration| {
                plan.layout_of(name, declaration.types.len())
                    .deliver(declaration.types.clone())
            });
            match declared {
                Some(declared) if declared.len() == arity => declared,
                _ => vec![Type::ANY; arity],
            }
        } else {
            seeds.get(&table).map_or_else(
                || vec![Type::EMPTY; arity],
                |seeds| seeds.iter().map(|seed| nominal(seed, labels)).collect(),
            )
        };
        types.insert(table, columns);
    }
    types
}

/// Returns the problems of declarations not matching the columns their predicates are used
/// with, in source order, so that the first problem reported is the first written.
fn declared_arity_problems(
    predicates: &HashMap<String, Registered>,
    declarations: &HashMap<String, PredicateDeclaration>,
    plan: &ImportPlan,
) -> Vec<Diagnostic> {
    let mut declared: Vec<(&String, &PredicateDeclaration)> = declarations.iter().collect();
    declared.sort_by_key(|(name, declaration)| {
        (
            declaration.span.map(|span| (span.file.index(), span.start)),
            *name,
        )
    });
    let mut problems = Vec::new();
    for (name, declaration) in declared {
        if let Some(&Registered { arity, .. }) = predicates.get(name)
            && plan.stored_layout(name, arity).kept() != declaration.types.len()
        {
            problems.push(
                TypeError::DeclaredArity {
                    name: name.clone(),
                    declared: declaration.types.len().into(),
                    arity: arity.into(),
                }
                .at(declaration.span),
            );
        }
    }
    problems
}

/// Grows the column types to the fixpoint: heads union what their rules can write,
/// until nothing grows.
///
/// The refinement depth is capped, so the lattice is finite and this terminates.
fn grow_to_fixpoint(
    rules: &[CompiledRule],
    types: &mut HashMap<TableId, Vec<Type>>,
    labels: &LabelTypes,
) {
    loop {
        let mut changed = false;
        for rule in rules {
            if rule.never_fires || rule.fetches() {
                continue;
            }
            let columns = rule_columns(rule, types, labels);
            each_write(rule, &columns, labels, |table, position, written, _| {
                let target = types
                    .get_mut(&table)
                    .expect("every compiled table is registered");
                let grown = target[position].union(&nominal(written, labels));
                if grown != target[position] {
                    target[position] = grown;
                    changed = true;
                }
            });
        }
        if !changed {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use nemo_physical::value::DomainSet;

    use super::*;

    #[test]
    fn a_declared_type_reads_in_the_format_of_its_name() {
        for format in ValueFormat::ALL {
            if matches!(format, ValueFormat::Skip) {
                continue;
            }
            assert_eq!(
                declared_format(&Type::of_domains(format.domains())),
                format,
                "`{}` is not read by its own format",
                format.name()
            );
        }
        // A union has no reading of its own.
        let union = Type::of_domains(DomainSet::INTEGER | DomainSet::STRING);
        assert_eq!(declared_format(&union), ValueFormat::Term);
    }
}
