//! The builtins' documented signatures, held to the type analysis:
//! each argument type must be one the builtin needs,
//! and each result type must hold what the builtin yields from such arguments.

use super::*;

use crate::model::{AggregateKind, Identifier, Operator};

/// One documented signature, read back as a call.
struct Signature {
    /// The call over the variables `?a0`, `?a1`, …, as a head term.
    call: String,
    /// The argument types, as written.
    arguments: Vec<String>,
    /// The result type, as written.
    result: String,
}

/// Reads a signature: `NAME(x: t, …) -> r`, `#name(x: t) -> r`, or `t op t -> r`.
///
/// A trailing `…` repeats the argument before it once.
fn signature(written: &str) -> Signature {
    let (call, result) = written
        .split_once(" -> ")
        .unwrap_or_else(|| panic!("`{written}` names a result"));
    let result = result.to_owned();
    let Some(open) = call.find('(') else {
        // An infix operator, called by its named form.
        let [left, symbol, right] = call.split(' ').collect::<Vec<_>>()[..] else {
            panic!("`{written}` is no infix signature");
        };
        let operator = Operator::NAMES
            .iter()
            .filter_map(|name| Operator::from_name(name))
            .find(|operator| operator.infix() == Some(symbol))
            .unwrap_or_else(|| panic!("`{symbol}` is an infix operator"));
        return Signature {
            call: format!("{}(?a0, ?a1)", operator.name()),
            arguments: vec![left.to_owned(), right.to_owned()],
            result,
        };
    };
    let name = &call[..open];
    let inside = call[open + 1..]
        .strip_suffix(')')
        .unwrap_or_else(|| panic!("`{written}` closes its arguments"));
    // A map aggregate takes a key and a value.
    if let Some((key, value)) = inside.split_once(" = ") {
        let types = [key, value].map(|part| part.split_once(": ").expect("a typed part").1);
        return Signature {
            call: format!("{name}(?a0 = ?a1)"),
            arguments: types.iter().map(|&datatype| datatype.to_owned()).collect(),
            result,
        };
    }
    let mut arguments: Vec<String> = Vec::new();
    for part in inside.split(", ").filter(|part| !part.is_empty()) {
        if part == "…" {
            let repeated = arguments.last().expect("`…` repeats an argument").clone();
            arguments.push(repeated);
        } else {
            let (_, datatype) = part
                .split_once(": ")
                .unwrap_or_else(|| panic!("`{part}` of `{written}` names its type"));
            arguments.push(datatype.to_owned());
        }
    }
    let variables: Vec<String> = (0..arguments.len())
        .map(|index| format!("?a{index}"))
        .collect();
    Signature {
        call: format!("{name}({})", variables.join(", ")),
        arguments,
        result,
    }
}

/// Returns what is wrong with a signature, or nothing if the analysis agrees with it.
fn disagreement(written: &str) -> Option<String> {
    let Signature {
        call,
        arguments,
        result,
    } = signature(written);
    let columns: Vec<String> = arguments
        .iter()
        .enumerate()
        .map(|(index, datatype)| format!("a{index}: {datatype}"))
        .collect();
    let variables: Vec<String> = (0..arguments.len())
        .map(|index| format!("?a{index}"))
        .collect();
    // The declaration of `out` holds the call to its result type: N0463 otherwise.
    let mut source = format!("@declare out({result}) .\ngo(1) .\n");
    let body = if arguments.is_empty() {
        String::from("go(?g)")
    } else {
        source.push_str(&format!("@input args({}) .\n", columns.join(", ")));
        format!("go(?g), args({})", variables.join(", "))
    };
    let calling = source.len();
    source.push_str(&format!("out({call}) :- {body} .\n"));

    let report = Engine::new().check(parse(&source));
    if !report.diagnostics().is_empty() {
        return Some(format!("{:?}", report.diagnostics()));
    }
    let types = report.types().expect("the analysis ran");
    let shown = |name: &str| {
        let types = types
            .predicate(&Identifier::bare(name))
            .expect("the predicate is typed");
        types
            .iter()
            .map(|datatype| TypeSyntax::of(datatype).to_string())
            .collect::<Vec<_>>()
    };
    let mut wrong = Vec::new();
    let rule = types
        .rules()
        .iter()
        .find(|rule| rule.span().start as usize == calling)
        .expect("the rule calling the builtin is typed");
    let declared = if arguments.is_empty() {
        Vec::new()
    } else {
        shown("args")
    };
    for (index, declared) in declared.iter().enumerate() {
        let needed = rule
            .variable(&format!("a{index}"))
            .expect("the body binds the argument");
        let needed = TypeSyntax::of(needed).to_string();
        if &needed != declared {
            wrong.push(format!("argument {index} is {declared}, needs {needed}"));
        }
    }
    (!wrong.is_empty()).then(|| wrong.join("; "))
}

/// Returns every builtin operator, each once, though some have several names.
fn operators() -> Vec<Operator> {
    let mut operators = Vec::new();
    for name in Operator::NAMES {
        let operator = Operator::from_name(name).expect("a builtin name");
        if !operators.contains(&operator) {
            operators.push(operator);
        }
    }
    operators
}

#[test]
fn every_documented_signature_is_what_the_analysis_infers() {
    let aggregates = AggregateKind::ALL.iter().map(|kind| kind.documentation());
    let mut wrong = Vec::new();
    for documentation in operators()
        .iter()
        .map(Operator::documentation)
        .chain(aggregates)
    {
        for written in documentation.signatures {
            if let Some(problem) = disagreement(written) {
                wrong.push(format!("`{written}`: {problem}"));
            }
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

#[test]
fn every_signature_calls_the_builtin_by_one_of_its_names() {
    for operator in operators() {
        for written in operator.documentation().signatures {
            let called = written.starts_with(&format!("{}(", operator.name()))
                || operator
                    .infix()
                    .is_some_and(|symbol| written.contains(&format!(" {symbol} ")));
            assert!(called, "`{written}` documents `{}`", operator.name());
        }
    }
}
