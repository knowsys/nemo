//! The testcase corpus: end-to-end programs with their expected results.
//!
//! Every `.rls` program under `resources/testcases` is one test.
//! The `sources` directories hold the data and modules the programs read, and are not cases.
//! A case must load and reason without an error,
//! and its results must match the facts of its sibling `.expect` file, compared as sets of
//! values, for the predicates the program declares as `@output`
//! (every predicate a rule derives into if it declares none),
//! the predicates it exports, and the predicates the expectation names.
//! Nulls compare up to one renaming across the whole case.
//! A case with a sibling directory of its own name also compares the files its exports write
//! against the files there, line by line in any order.
//!
//! With `NEMO_BLESS` set, the results are written as the expectations instead of compared.
//! `NEMO_TESTCASES` points the harness at another corpus directory.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    path::{Path, PathBuf},
};

use libtest_mimic::{Arguments, Failed, Trial};
use nemo::{
    engine::Engine,
    model::{Directive, FileId, Identifier, Program, Statement},
};
use nemo_physical::{
    diagnostics::{Diagnostic, Severity},
    value::{DataValue, MapValue, TupleValue, ValueRef},
};

/// The rows of each checked predicate, by resolved predicate name.
type Results = BTreeMap<String, Vec<Vec<DataValue>>>;

/// Registers one test per case and runs them.
fn main() {
    let arguments = Arguments::from_args();
    let root = match std::env::var_os("NEMO_TESTCASES") {
        Some(root) => PathBuf::from(root),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join("../resources/testcases"),
    };
    let bless = std::env::var_os("NEMO_BLESS").is_some();

    let mut programs = Vec::new();
    collect(&root, &mut programs);
    programs.sort();
    assert!(
        !programs.is_empty(),
        "the corpus at {} is empty",
        root.display()
    );

    let trials = programs
        .into_iter()
        .map(|program| {
            let name = case_name(&root, &program);
            Trial::test(name, move || run(&program, bless).map_err(Failed::from))
        })
        .collect();
    libtest_mimic::run(&arguments, trials).exit();
}

/// Collects every case program under a directory.
fn collect(directory: &Path, into: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("the corpus directory is readable") {
        let path = entry.expect("the corpus directory is readable").path();
        if path.is_dir() {
            if path.file_name().is_none_or(|name| name != "sources") {
                collect(&path, into);
            }
        } else if path.extension().is_some_and(|extension| extension == "rls") {
            into.push(path);
        }
    }
}

/// The test name of a case: its path below the corpus root, without the
/// extension, its directories joined by `::`.
fn case_name(root: &Path, program: &Path) -> String {
    let relative = program
        .strip_prefix(root)
        .expect("cases live under the root")
        .with_extension("");
    relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("::")
}

/// Runs one case: reasons, then compares or blesses the results.
fn run(program: &Path, bless: bool) -> Result<(), String> {
    let expect_path = program.with_extension("expect");
    let expected = match fs::read_to_string(&expect_path) {
        Ok(text) => expectation(&text).map_err(|reason| format!("in the expectation: {reason}"))?,
        Err(_) if bless => Results::new(),
        Err(_) => {
            return Err(String::from(
                "the case has no `.expect` file; run with NEMO_BLESS=1 to write one",
            ));
        }
    };

    let (mut engine, declares_outputs) = reason(program, &expected)?;
    let actual = results(&mut engine, &expected, declares_outputs)?;
    if bless {
        fs::write(&expect_path, render(&actual)).map_err(|error| error.to_string())?;
    } else {
        compare(&actual, &expected)?;
    }
    check_exports(&mut engine, program)
}

/// Loads and reasons over a case program, returning the engine and
/// whether the program declares an `@output`.
///
/// Imports stay whole where the expectation names an imported predicate:
/// by default the loader skips what no rule reads.
fn reason(path: &Path, expected: &Results) -> Result<(Engine, bool), String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let mut engine = Engine::new();
    // Registering the source under its path lets a `@use` resolve
    // relative to the case's directory.
    let file = engine
        .add_source(path.to_string_lossy(), text.clone())
        .expect("the text is small");
    let (program, diagnostics) = Program::parse(&text, file);
    errors(&diagnostics)?;

    let imports_named = program.statements.iter().any(|statement| match statement {
        Statement::Directive(Directive::Import(import)) => {
            expected.contains_key(&import.predicate.to_string())
        }
        _ => false,
    });
    if imports_named {
        engine.retain_imports();
    }

    let program = engine.link(program).map_err(describe)?;
    let declares_outputs = program
        .statements
        .iter()
        .any(|statement| matches!(statement, Statement::Directive(Directive::Output(_))));
    engine.load(program).map_err(describe)?;
    engine.reason().map_err(describe)?;
    Ok((engine, declares_outputs))
}

/// The facts of an expectation file, read as a program of facts.
fn expectation(text: &str) -> Result<Results, String> {
    let (program, diagnostics) = Program::parse(text, FileId::new(0));
    errors(&diagnostics)?;
    let mut engine = Engine::new();
    engine.load(program).map_err(describe)?;
    engine.reason().map_err(describe)?;

    let mut results = Results::new();
    for info in engine.predicates() {
        let rows = engine.rows(&info.name).map_err(describe)?;
        results.insert(info.name, rows);
    }
    Ok(results)
}

/// Returns the rows of every checked predicate:
/// the outputs if the program declares an `@output`, or else every derived predicate,
/// the exported ones, and the ones the expectation names.
fn results(
    engine: &mut Engine,
    expected: &Results,
    declares_outputs: bool,
) -> Result<Results, String> {
    let mut predicates: BTreeSet<String> = BTreeSet::new();
    if declares_outputs {
        predicates.extend(engine.outputs());
    } else {
        predicates.extend(
            engine
                .predicates()
                .into_iter()
                .filter(|info| info.role.is_derived())
                .map(|info| info.name),
        );
    }
    predicates.extend(engine.exports().iter().map(|spec| spec.predicate.clone()));
    predicates.extend(expected.keys().cloned());

    let mut results = Results::new();
    for predicate in predicates {
        let rows = if engine.predicate(&predicate).is_some() {
            engine.rows(&predicate).map_err(describe)?
        } else {
            Vec::new()
        };
        results.insert(predicate, rows);
    }
    Ok(results)
}

/// Fails on the first error among parse diagnostics, while warnings pass.
fn errors(diagnostics: &[Diagnostic]) -> Result<(), String> {
    match diagnostics
        .iter()
        .find(|diagnostic| diagnostic.severity() == Severity::Error)
    {
        Some(error) => Err(describe(error.clone())),
        None => Ok(()),
    }
}

/// One line describing a diagnostic.
fn describe(diagnostic: Diagnostic) -> String {
    format!("[{}] {}", diagnostic.code(), diagnostic.message())
}

/// Writes results as an expectation: the predicates by name, each one's
/// facts in value order, the nulls numbered densely in the order the
/// engine created them.
fn render(results: &Results) -> String {
    let mut ids = Vec::new();
    for value in results.values().flatten().flatten() {
        nulls(value.as_ref(), &mut ids);
    }
    ids.sort_unstable();
    ids.dedup();
    let ranks: HashMap<u64, u64> = (0..).zip(&ids).map(|(rank, &id)| (id, rank)).collect();

    let mut groups = Vec::new();
    for (predicate, rows) in results {
        if rows.is_empty() {
            continue;
        }
        let name = Identifier::bare(predicate.clone());
        let mut rows = rows.clone();
        rows.sort();
        let mut lines = String::new();
        for row in rows {
            let terms: Vec<String> = row.iter().map(|value| term(value, &ranks)).collect();
            lines.push_str(&format!("{name}({}) .\n", terms.join(", ")));
        }
        groups.push(lines);
    }
    groups.join("\n")
}

/// A value in term syntax for an expectation: an IRI bare where it
/// reads back as the same name, each null by its rank.
fn term(value: &DataValue, ranks: &HashMap<u64, u64>) -> String {
    if let ValueRef::Iri(iri) = value.as_ref() {
        let name = iri.to_string();
        let mut characters = name.chars();
        let bare = characters
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic())
            && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
            && !matches!(name.as_str(), "true" | "false");
        if bare {
            return name;
        }
    }
    renamed(value.as_ref(), &mut |id| ranks[&id]).to_string()
}

/// Checks the files the exports write against the case's directory of
/// expected files.
fn check_exports(engine: &mut Engine, program: &Path) -> Result<(), String> {
    let expected_directory = program.with_extension("");
    if !expected_directory.is_dir() {
        return Ok(());
    }
    let written = std::env::temp_dir().join(format!(
        "nemo-testcase-{}-{}",
        std::process::id(),
        program
            .file_stem()
            .expect("cases are files")
            .to_string_lossy()
    ));
    fs::remove_dir_all(&written).ok();
    engine.export(&written).map_err(describe)?;

    let lines = |path: &Path| -> Result<Vec<String>, String> {
        let text = fs::read_to_string(path)
            .map_err(|_| format!("`{}` was not written", path.display()))?;
        let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
        lines.sort();
        Ok(lines)
    };
    let mut outcome = Ok(());
    for entry in fs::read_dir(&expected_directory).map_err(|error| error.to_string())? {
        let expected_file = entry.map_err(|error| error.to_string())?.path();
        let name = expected_file.file_name().expect("entries have names");
        let actual = lines(&written.join(name));
        if let Err(reason) = actual.and_then(|actual| {
            let expected = lines(&expected_file)?;
            if actual == expected {
                Ok(())
            } else {
                Err(format!(
                    "the export `{}` differs:\n{}",
                    name.to_string_lossy(),
                    difference(&actual, &expected)
                ))
            }
        }) {
            outcome = Err(reason);
            break;
        }
    }
    fs::remove_dir_all(&written).ok();
    outcome
}

/// Appends the ids of a value's nulls, nested ones too, in order of occurrence.
fn nulls(value: ValueRef<'_>, into: &mut Vec<u64>) {
    match value {
        ValueRef::Null(id) => into.push(id),
        ValueRef::Tuple(tuple) => tuple.elements().for_each(|element| nulls(element, into)),
        ValueRef::Map(map) => {
            for (key, value) in map.pairs() {
                nulls(key, into);
                nulls(value, into);
            }
        }
        _ => {}
    }
}

/// Returns a value with each null's id renamed, nested ones too,
/// visiting the nulls in the order [nulls] lists them.
fn renamed(value: ValueRef<'_>, rename: &mut impl FnMut(u64) -> u64) -> DataValue {
    match value {
        ValueRef::Null(id) => DataValue::null(rename(id)),
        ValueRef::Tuple(tuple) => {
            let elements: Vec<DataValue> = tuple
                .elements()
                .map(|element| renamed(element, rename))
                .collect();
            DataValue::tuple(match tuple.label() {
                Some(label) => TupleValue::labeled(label, elements),
                None => TupleValue::new(elements),
            })
        }
        ValueRef::Map(map) => {
            let pairs: Vec<(DataValue, DataValue)> = map
                .pairs()
                .map(|(key, value)| (renamed(key, rename), renamed(value, rename)))
                .collect();
            let renamed = match map.label() {
                Some(label) => MapValue::labeled(label, pairs),
                None => MapValue::new(pairs),
            };
            DataValue::map(renamed.expect("a renaming keeps the keys apart"))
        }
        other => other.to_owned(),
    }
}

/// One value, its nulls taken apart from the rest.
#[derive(Debug, Clone)]
struct Cell {
    /// The rendering with the nulls numbered from 0 in order of first occurrence,
    /// the same for every renaming of the value's nulls.
    shape: String,
    /// The ids of the nulls, one per occurrence.
    nulls: Vec<u64>,
}

impl Cell {
    /// Splits a value into its shape and its nulls.
    fn of(value: &DataValue) -> Self {
        let mut nulls_in_value = Vec::new();
        nulls(value.as_ref(), &mut nulls_in_value);
        let mut first = Vec::new();
        let shape = renamed(value.as_ref(), &mut |id| {
            let position = first
                .iter()
                .position(|&seen| seen == id)
                .unwrap_or_else(|| {
                    first.push(id);
                    first.len() - 1
                });
            position as u64
        });
        Cell {
            shape: shape.to_string(),
            nulls: nulls_in_value,
        }
    }
}

/// One side's rows of all predicates, as cells with densely numbered
/// nulls: the predicate's index, then the row.
type Side = Vec<(usize, Vec<(String, Vec<usize>)>)>;

/// Compares actual against expected results.
///
/// Nulls on either side switch to the comparison up to one renaming of the nulls.
fn compare(actual: &Results, expected: &Results) -> Result<(), String> {
    let mut problems = Vec::new();
    let predicates: BTreeSet<&String> = actual.keys().chain(expected.keys()).collect();
    for predicate in &predicates {
        let shapes = |results: &Results| -> Vec<String> {
            let mut rows: Vec<String> = results
                .get(*predicate)
                .into_iter()
                .flatten()
                .map(|row| {
                    row.iter()
                        .map(|value| Cell::of(value).shape)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .collect();
            rows.sort();
            rows
        };
        let (derived, wanted) = (shapes(actual), shapes(expected));
        if derived != wanted {
            problems.push(format!(
                "`{predicate}` differs:\n{}",
                difference(&derived, &wanted)
            ));
        }
    }
    if !problems.is_empty() {
        return Err(problems.join("\n"));
    }

    let (actual_side, actual_nulls) = side(actual, &predicates);
    let (expected_side, expected_nulls) = side(expected, &predicates);
    if actual_nulls == 0 && expected_nulls == 0 {
        return Ok(());
    }
    if matches_up_to_nulls(&actual_side, actual_nulls, &expected_side, expected_nulls) {
        Ok(())
    } else {
        Err(format!(
            "the rows agree value by value, but no renaming of the {actual_nulls} \
             derived nulls gives the {expected_nulls} expected ones"
        ))
    }
}

/// The lines of two sorted row lists that only one of them has.
fn difference(actual: &[String], expected: &[String]) -> String {
    const SHOWN: usize = 20;
    let missing: Vec<&String> = expected
        .iter()
        .filter(|row| !actual.contains(row))
        .collect();
    let unexpected: Vec<&String> = actual
        .iter()
        .filter(|row| !expected.contains(row))
        .collect();
    let mut text = String::new();
    for (sign, rows) in [("-", &missing), ("+", &unexpected)] {
        for row in rows.iter().take(SHOWN) {
            text.push_str(&format!("  {sign} {row}\n"));
        }
        if rows.len() > SHOWN {
            text.push_str(&format!("  {sign} … {} more\n", rows.len() - SHOWN));
        }
    }
    if missing.is_empty() && unexpected.is_empty() {
        text.push_str(&format!(
            "  the same rows, but {} derived and {} expected with repeats\n",
            actual.len(),
            expected.len()
        ));
    }
    text.push_str("  (- expected but not derived, + derived but not expected)");
    text
}

/// Numbers one side's nulls densely across all its predicates.
fn side(results: &Results, predicates: &BTreeSet<&String>) -> (Side, usize) {
    let mut nulls: HashMap<u64, usize> = HashMap::new();
    let mut rows = Vec::new();
    for (index, predicate) in predicates.iter().enumerate() {
        for row in results.get(*predicate).into_iter().flatten() {
            let cells = row
                .iter()
                .map(|value| {
                    let cell = Cell::of(value);
                    let ids = cell
                        .nulls
                        .iter()
                        .map(|&id| {
                            let next = nulls.len();
                            *nulls.entry(id).or_insert(next)
                        })
                        .collect();
                    (cell.shape, ids)
                })
                .collect();
            rows.push((index, cells));
        }
    }
    (rows, nulls.len())
}

/// Returns whether one renaming of the actual nulls into the expected ones maps the actual rows
/// onto exactly the expected rows.
///
/// Nulls are first narrowed by color refinement over the rows they occur in,
/// and the remaining ties backtrack.
fn matches_up_to_nulls(
    actual: &Side,
    actual_count: usize,
    expected: &Side,
    expected_count: usize,
) -> bool {
    if actual_count != expected_count || actual.len() != expected.len() {
        return false;
    }
    let mut actual_colors = vec![0u64; actual_count];
    let mut expected_colors = vec![0u64; expected_count];
    for _ in 0..=actual_count.min(24) {
        refine(actual, &mut actual_colors);
        refine(expected, &mut expected_colors);
    }

    // The classes of equally colored nulls must pair up by size.
    let mut classes: HashMap<u64, (Vec<usize>, Vec<usize>)> = HashMap::new();
    for (null, &color) in actual_colors.iter().enumerate() {
        classes.entry(color).or_default().0.push(null);
    }
    for (null, &color) in expected_colors.iter().enumerate() {
        classes.entry(color).or_default().1.push(null);
    }
    let classes: Vec<(Vec<usize>, Vec<usize>)> = classes.into_values().collect();
    if classes
        .iter()
        .any(|(left, right)| left.len() != right.len())
    {
        return false;
    }

    let mut target = expected.clone();
    target.sort();
    let mut search = Search {
        classes: &classes,
        actual,
        target: &target,
        map: vec![0; actual_count],
        used: vec![false; expected_count],
    };
    search.class(0)
}

/// One refinement round: a null's color absorbs the contexts of every
/// row it occurs in.
fn refine(rows: &Side, colors: &mut [u64]) {
    let mut signatures: Vec<Vec<(u64, usize, usize)>> = vec![Vec::new(); colors.len()];
    for (predicate, cells) in rows {
        let mut hasher = DefaultHasher::new();
        predicate.hash(&mut hasher);
        for (shape, nulls) in cells {
            shape.hash(&mut hasher);
            for &null in nulls {
                colors[null].hash(&mut hasher);
            }
        }
        let context = hasher.finish();
        for (column, (_, nulls)) in cells.iter().enumerate() {
            for (position, &null) in nulls.iter().enumerate() {
                signatures[null].push((context, column, position));
            }
        }
    }
    for (null, mut signature) in signatures.into_iter().enumerate() {
        signature.sort_unstable();
        let mut hasher = DefaultHasher::new();
        colors[null].hash(&mut hasher);
        signature.hash(&mut hasher);
        colors[null] = hasher.finish();
    }
}

/// The backtracking search for a renaming, class by class.
struct Search<'a> {
    /// The classes of nulls that may map onto each other.
    classes: &'a [(Vec<usize>, Vec<usize>)],
    /// The actual rows.
    actual: &'a Side,
    /// The expected rows, sorted.
    target: &'a Side,
    /// The renaming so far, from actual to expected nulls.
    map: Vec<usize>,
    /// Which expected nulls the renaming already hits.
    used: Vec<bool>,
}

impl Search<'_> {
    /// Assigns the nulls of the class at the given position and after.
    fn class(&mut self, position: usize) -> bool {
        match self.classes.get(position) {
            Some((nulls, _)) => self.assign(position, nulls, 0),
            None => self.validate(),
        }
    }

    /// Assigns the class's nulls from the given one on.
    fn assign(&mut self, position: usize, nulls: &[usize], at: usize) -> bool {
        let Some(&null) = nulls.get(at) else {
            return self.class(position + 1);
        };
        for candidate in self.classes[position].1.clone() {
            if !self.used[candidate] {
                self.used[candidate] = true;
                self.map[null] = candidate;
                if self.assign(position, nulls, at + 1) {
                    return true;
                }
                self.used[candidate] = false;
            }
        }
        false
    }

    /// Whether the complete renaming maps the rows exactly.
    fn validate(&self) -> bool {
        let mut renamed: Side = self
            .actual
            .iter()
            .map(|(predicate, cells)| {
                let cells = cells
                    .iter()
                    .map(|(shape, nulls)| {
                        (
                            shape.clone(),
                            nulls.iter().map(|&null| self.map[null]).collect(),
                        )
                    })
                    .collect();
                (*predicate, cells)
            })
            .collect();
        renamed.sort();
        renamed == *self.target
    }
}
