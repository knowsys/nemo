//! The proofs that a rule's writes fit what the program declares:
//! the `@declare` of the columns they go into, and the `@type` of the labels they build.

use std::collections::HashMap;

use nemo_physical::{
    diagnostics::Diagnostic,
    function::Outcome,
    management::TableId,
    value::{DomainSet, LabelBound, LabelTypes, MapShape, Shape, TupleShape, Type, ValueDomain},
};

use crate::{
    codes,
    diagnostics::{AboutPredicate, TypeError},
    engine::compile::{
        compiled::CompiledRule,
        types::{
            AnalysisInputs, RuleSourceInfo, TableNames,
            columns::{each_write, read},
            proofs::{builtin_hint, variable_name},
            written_column,
        },
    },
    model::{Identifier, Span},
    values::TypeSyntax,
};

/// Adds to `problems` an error for each write of a rule that may fire
/// whose values are not shown to fit a declaration.
///
/// These are values outside the `@declare` of the column they go into,
/// and labeled values outside the `@type` of their label.
/// A write that shares no value with the declaration violates it outright,
/// and a write that shares some values is not shown to fit.
/// The error says where the values come from.
/// `columns` contains the types of the rule's columns, and `types` those of the tables.
pub(super) fn declaration_violations(
    rule: &CompiledRule,
    source: RuleSourceInfo<'_>,
    columns: &[Type],
    inputs: &AnalysisInputs<'_>,
    types: &HashMap<TableId, Vec<Type>>,
    table_names: &TableNames<'_>,
    problems: &mut Vec<Diagnostic>,
) {
    let declared = &inputs.declared;
    let labels = declared.labels;
    let context = WriteContext {
        rule,
        source,
        columns,
        inputs,
        types,
        table_names,
    };
    each_write(rule, columns, labels, |table, position, written, origin| {
        if written.is_empty() {
            return;
        }
        let (name, _) = table_names[&table];
        let column = declared
            .predicates
            .get(name)
            .and_then(|declaration| declaration.types.get(position));
        if let Some(column) = column
            && !written.is_subset(column, labels)
        {
            let written_syntax = TypeSyntax::of(written).to_string();
            let declared_syntax = TypeSyntax::of(column).to_string();
            let problem = if written.intersects(&column.unfolded(labels, Type::DEPTH)) {
                let mut problem = TypeError::RuleMayWriteOutside {
                    written: written_syntax,
                    name: name.to_owned(),
                    declared: declared_syntax,
                }
                .at(source.span)
                .with_predicate(name);
                if let Some(origin) = origin {
                    problem = context.explain(problem, origin, column);
                }
                problem
            } else {
                let mut problem = TypeError::RuleWritesOutside {
                    written: written_syntax,
                    name: name.to_owned(),
                    declared: declared_syntax,
                }
                .at(source.span)
                .with_predicate(name);
                // No condition makes such a write fit, so it gets no hint,
                // only the note on where its values come from.
                if let Some(origin) = origin {
                    problem = context.explain_column(problem, origin, column, false);
                }
                // If the declared type has no tuples, and a tuple arrives whose label is close
                // to a builtin's name, the label was probably meant as a call.
                if !column.domains().contains(ValueDomain::Tuple)
                    && let Some(hint) = builtin_hint(written)
                {
                    problem = problem.with_hint(hint);
                }
                problem
            };
            problems.push(problem);
            return;
        }
        if let Some(problem) = construction_problem(written, labels, source.span) {
            // The labeled values must fit their labels' types, so the required type is the
            // written type with every such label taken as its type.
            // A construction outside its label's type gets no hint,
            // since no condition makes it fit.
            let hinting = problem.code() != codes::RULE_BUILDS_OUTSIDE_TYPE;
            problems.push(match origin {
                Some(origin) => {
                    context.explain_column(problem, origin, &nominal(written, labels), hinting)
                }
                None => problem,
            });
        }
    });
}

/// What the explanation of a rule's write needs:
/// the rule, the types of its columns, and the types that the analysis inferred.
struct WriteContext<'a> {
    /// The rule.
    rule: &'a CompiledRule,
    /// What the reports need to know of the rule as it was written.
    source: RuleSourceInfo<'a>,
    /// The types that each of the rule's columns can hold.
    columns: &'a [Type],
    /// What the analysis reads besides the rules.
    inputs: &'a AnalysisInputs<'a>,
    /// The inferred column types per table.
    types: &'a HashMap<TableId, Vec<Type>>,
    /// The name and the arity of each table.
    table_names: &'a TableNames<'a>,
}

impl WriteContext<'_> {
    /// Adds to a problem where the values of rule column `column` come from that `required`
    /// does not contain, and a hint at a condition that establishes the type.
    ///
    /// For a variable, the note names the body column that the variable is read from.
    /// For a computed column, there is a note for each input that is not shown to be what the
    /// expression needs to produce the required type.
    /// The hints come only if inputs of the needed types are enough to make the result fit.
    fn explain(&self, problem: Diagnostic, column: usize, required: &Type) -> Diagnostic {
        self.explain_column(problem, column, required, true)
    }

    /// Adds to a problem the [explanation](Self::explain) of one column,
    /// with hints only if `hinting` is set.
    fn explain_column(
        &self,
        mut problem: Diagnostic,
        column: usize,
        required: &Type,
        hinting: bool,
    ) -> Diagnostic {
        let rule = self.rule;
        let labels = self.inputs.declared.labels;
        if column < rule.variables {
            let note = origin_note(
                rule,
                self.source,
                column,
                required,
                self.types,
                self.table_names,
                self.inputs,
            );
            if let Some(note) = note {
                problem = problem.with_note(note);
            }
            if hinting
                && let Some(hint) =
                    establishing_hint(self.source, column, &self.columns[column], required)
            {
                problem = problem.with_hint(hint);
            }
            return problem;
        }
        let computed = &rule.computed[column - rule.variables];
        let held = read(self.columns, &computed.inputs);
        // What the expression needs of each input, whatever the input can hold,
        // and whether inputs narrowed to that make the result fit.
        let open = vec![Type::ANY; held.len()];
        let needed = computed
            .program
            .narrow(&open, labels, &Outcome::Within(required.clone()));
        let narrowed: Vec<Type> = held
            .iter()
            .zip(&needed)
            .map(|(held, needed)| held.intersect(needed))
            .collect();
        let fits = |inputs: &[Type]| {
            computed
                .program
                .infer(inputs, labels)
                .bound()
                .is_subset(required, labels)
        };
        let sufficient = fits(&narrowed);
        if !sufficient {
            // The needed types do not make the result fit,
            // so look for one input whose type test does.
            for (position, &input) in computed.inputs.iter().enumerate() {
                for (domains, ..) in &ESTABLISHING {
                    let tested = narrowed[position].intersect(&Type::of_domains(*domains));
                    if tested.is_empty() || narrowed[position].is_subset(&tested, labels) {
                        continue;
                    }
                    let mut trial = narrowed.clone();
                    trial[position] = tested;
                    if fits(&trial) {
                        let established = Type::of_domains(*domains);
                        return self.explain_column(problem, input, &established, hinting);
                    }
                }
            }
        }
        for ((&input, held), needed) in computed.inputs.iter().zip(&held).zip(&needed) {
            if !held.is_subset(needed, labels) {
                problem = self.explain_column(problem, input, needed, hinting && sufficient);
            }
        }
        problem
    }
}

/// Returns the note on where the values of a written rule column come from that `declared`
/// does not contain.
///
/// For a variable, the note names the first body column that the variable is read from
/// whose type is not within the declared type.
/// It also gives the type that this column had before the analyzed change, if that type fit.
/// Returns `None` for a computed column, whose expression gives its values,
/// and for a variable whose body columns all lie within the declared type.
fn origin_note(
    rule: &CompiledRule,
    source: RuleSourceInfo<'_>,
    column: usize,
    declared: &Type,
    types: &HashMap<TableId, Vec<Type>>,
    table_names: &TableNames<'_>,
    inputs: &AnalysisInputs<'_>,
) -> Option<String> {
    let labels = inputs.declared.labels;
    if column >= rule.variables {
        return None;
    }
    rule.body.iter().find_map(|atom| {
        atom.variables
            .iter()
            .enumerate()
            .filter(|&(_, &variable)| variable == column)
            .find_map(|(position, _)| {
                let held = &types[&atom.table][position];
                if held.is_subset(declared, labels) {
                    return None;
                }
                let (predicate, arity) = table_names[&atom.table];
                let mut note = format!(
                    "{} is read from column {} of `{predicate}`, which holds {}",
                    variable_name(source, column),
                    written_column(inputs.plan, predicate, arity, position) + 1,
                    TypeSyntax::of(held)
                );
                let before = inputs
                    .previous
                    .and_then(|previous| previous.get(&atom.table))
                    .and_then(|previous| previous.get(position))
                    .filter(|before| before.is_subset(declared, labels));
                if let Some(before) = before {
                    note.push_str(&format!(
                        ", and held {} before this change",
                        TypeSyntax::of(before)
                    ));
                }
                Some(note)
            })
    })
}

/// The type tests that a hint can offer, each with the domains that it keeps,
/// and with the conversion into these domains where one is sure to produce them.
const ESTABLISHING: [(DomainSet, &str, Option<&str>); 18] = [
    (DomainSet::INTEGER, "ISINTEGER", Some("INT")),
    (DomainSet::EXACT, "ISDECIMAL", None),
    (DomainSet::NUMERIC, "ISNUMERIC", None),
    (DomainSet::DOUBLE, "ISDOUBLE", Some("DOUBLE")),
    (DomainSet::FLOAT, "ISFLOAT", Some("FLOAT")),
    (DomainSet::STRING, "ISSTRING", Some("STR")),
    (DomainSet::LANGUAGE_STRING, "ISLANGSTRING", None),
    (DomainSet::IRI, "ISIRI", Some("IRI")),
    (DomainSet::BOOLEAN, "ISBOOLEAN", None),
    (DomainSet::DATE_TIME, "ISDATETIME", Some("DATETIME")),
    (DomainSet::DATE, "ISDATE", Some("DATE")),
    (DomainSet::TIME, "ISTIME", Some("TIME")),
    (DomainSet::DURATION, "ISDURATION", Some("DURATION")),
    (
        DomainSet::DAY_TIME_DURATION,
        "ISDAYTIMEDURATION",
        Some("DAYTIMEDURATION"),
    ),
    (DomainSet::YEAR_MONTH_DURATION, "ISYEARMONTHDURATION", None),
    (DomainSet::NULL, "ISNULL", None),
    (DomainSet::TUPLE, "ISTUPLE", None),
    (DomainSet::MAP, "ISMAP", None),
];

/// Returns the hint for a variable written where it is not shown to fit a required type:
/// a condition that keeps only values of that type, and a conversion into it if one exists.
///
/// The condition keeps at least the values of the required type that the variable can hold,
/// and at most the values of the required type.
/// It is the first of these that exists:
/// - the test of the required type itself;
/// - the first test of [ESTABLISHING] whose domains lie between the two;
/// - tests combined with `OR` that cover what the variable can hold of the type.
///
/// A label with a declared type is tested by its label.
/// `held` is the type of the variable at rule column `column`.
/// Returns `None` for a variable without a written name,
/// and for a required type that no condition establishes.
fn establishing_hint(
    source: RuleSourceInfo<'_>,
    column: usize,
    held: &Type,
    required: &Type,
) -> Option<String> {
    let name = source.names.get(column)?.as_deref()?;
    let label = match (required.tuple_shapes(), required.map_shapes()) {
        (Some([Shape::Declared(label)]), None) if required.domains() == DomainSet::TUPLE => {
            Some(label)
        }
        (None, Some([Shape::Declared(label)])) if required.domains() == DomainSet::MAP => {
            Some(label)
        }
        _ => None,
    };
    if let Some(label) = label {
        // The label is written as a term: an IRI that is not a bare name in brackets.
        let label = Identifier::Bare(label.to_string());
        return Some(format!(
            "add a condition such as `LABEL(?{name}) = {label}`"
        ));
    }
    if required.tuple_shapes().is_some() || required.map_shapes().is_some() {
        return None;
    }
    let allowed = required.domains();
    let kept = held.domains() & allowed;
    let fitting = |domains: DomainSet| kept.is_subset(domains) && domains.is_subset(allowed);
    let single = ESTABLISHING
        .iter()
        .find(|(domains, ..)| *domains == allowed)
        .filter(|(domains, ..)| fitting(*domains))
        .or_else(|| ESTABLISHING.iter().find(|(domains, ..)| fitting(*domains)));
    if let Some((_, test, conversion)) = single {
        return Some(match conversion {
            Some(conversion) => format!(
                "add a condition such as `{test}(?{name})`, or convert with `{conversion}(?{name})`"
            ),
            None => format!("add a condition such as `{test}(?{name})`"),
        });
    }
    // No single test fits, so several tests cover what the variable can hold of the type.
    let mut covered = DomainSet::EMPTY;
    let mut tests = Vec::new();
    for (domains, test, _) in &ESTABLISHING {
        if domains.is_subset(allowed) && domains.intersects(kept) && !kept.is_subset(covered) {
            covered |= *domains;
            tests.push(format!("{test}(?{name})"));
        }
    }
    (tests.len() > 1 && kept.is_subset(covered))
        .then(|| format!("add a condition such as `OR({})`", tests.join(", ")))
}

/// Returns the error, at `span`, for the first labeled structure that a type describes
/// and that is not shown to fit the `@type` of its label, or `None` if every one fits.
///
/// Every written-out shape with a label that has a `@type` is checked against that type,
/// and the check goes on into the fields of every written-out shape.
/// A shape that cannot meet its label's type violates it outright,
/// and a shape that can meet it is not shown to fit.
fn construction_problem(
    written: &Type,
    labels: &LabelTypes,
    span: Option<Span>,
) -> Option<Diagnostic> {
    let check = |label: &LabelBound, built: Type| -> Option<Diagnostic> {
        let LabelBound::Exactly(Some(label)) = label else {
            return None;
        };
        let declared = labels.get(label)?;
        if built.is_subset(declared, labels) {
            return None;
        }
        let label = label.to_string();
        let declared = TypeSyntax::of(declared).to_string();
        let built_syntax = TypeSyntax::of(&built).to_string();
        let unfolded = labels.get(&label)?.unfolded(labels, Type::DEPTH);
        Some(if built.intersects(&unfolded) {
            TypeError::RuleMayBuildOutsideType {
                label,
                declared,
                built: built_syntax,
            }
            .at(span)
        } else {
            TypeError::RuleBuildsOutsideType {
                label,
                declared,
                built: built_syntax,
            }
            .at(span)
        })
    };
    for shape in written.tuple_shapes().unwrap_or_default() {
        let Shape::Structural(shape) = shape else {
            continue;
        };
        let found = check(&shape.label, Type::tuple(shape.clone())).or_else(|| {
            shape
                .elements
                .iter()
                .find_map(|element| construction_problem(element, labels, span))
        });
        if found.is_some() {
            return found;
        }
    }
    for shape in written.map_shapes().unwrap_or_default() {
        let Shape::Structural(shape) = shape else {
            continue;
        };
        let found = check(&shape.label, Type::map(shape.clone())).or_else(|| {
            shape
                .pairs()
                .iter()
                .find_map(|(_, value)| construction_problem(value, labels, span))
        });
        if found.is_some() {
            return found;
        }
    }
    None
}

/// Returns a type in which every written-out shape with a label that has a `@type`
/// is replaced by that label's type, in nested shapes too.
///
/// A construction of such a label that is not shown to fit its `@type` is an error,
/// so in a program that runs, the values with the label are exactly those of its type.
/// The replacement keeps the types of recursive constructions finite.
pub(super) fn nominal(written: &Type, labels: &LabelTypes) -> Type {
    let domains = written.domains();
    let mut result = Type::of_domains(domains & DomainSet::STRUCTURED.complement());
    if domains.contains(ValueDomain::Tuple) {
        result = result.union(&match written.tuple_shapes() {
            None => Type::of(ValueDomain::Tuple),
            Some(shapes) => shapes.iter().fold(Type::EMPTY, |sum, shape| {
                sum.union(&match shape {
                    Shape::Structural(shape) => match &shape.label {
                        LabelBound::Exactly(Some(label)) if labels.tuple_shape(label).is_some() => {
                            Type::declared(label, ValueDomain::Tuple)
                        }
                        _ => Type::tuple(TupleShape {
                            label: shape.label.clone(),
                            elements: shape
                                .elements
                                .iter()
                                .map(|element| nominal(element, labels))
                                .collect(),
                        }),
                    },
                    Shape::Declared(label) => Type::declared(label, ValueDomain::Tuple),
                })
            }),
        });
    }
    if domains.contains(ValueDomain::Map) {
        result = result.union(&match written.map_shapes() {
            None => Type::of(ValueDomain::Map),
            Some(shapes) => shapes.iter().fold(Type::EMPTY, |sum, shape| {
                sum.union(&match shape {
                    Shape::Structural(shape) => match &shape.label {
                        LabelBound::Exactly(Some(label)) if labels.map_shape(label).is_some() => {
                            Type::declared(label, ValueDomain::Map)
                        }
                        _ => Type::map(
                            MapShape::new(
                                shape.label.clone(),
                                shape
                                    .pairs()
                                    .iter()
                                    .map(|(key, value)| (key.clone(), nominal(value, labels)))
                                    .collect(),
                            )
                            .expect("the shape holds each key once"),
                        ),
                    },
                    Shape::Declared(label) => Type::declared(label, ValueDomain::Map),
                })
            }),
        });
    }
    result
}
