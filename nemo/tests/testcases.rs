//! The testcase corpus: end-to-end programs with their expected results.
//!
//! Every `.rls` program under `resources/testcases` is one test; the
//! `sources` directories hold the data the programs read and are not
//! cases. The cases under `changed` and `new` describe behaviour this
//! engine does not have and are ignored.
//!
//! A case must load and reason without an error, and its results must
//! match the facts of its sibling `.expect` file, compared as sets of
//! values: the predicates the program declares as `@output` (every
//! predicate a rule derives into where it declares none), the predicates
//! it exports, and the predicates the expectation names. Nulls compare up
//! to one renaming across the whole case. A case with a sibling directory
//! of its own name also compares the files its exports write against the
//! files there, line by line in any order.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    path::{Path, PathBuf},
};

use assert_fs::TempDir;
use libtest_mimic::{Arguments, Failed, Trial};
use nemo::{
    datavalues::AnyDataValue,
    error::{Error, report::ProgramReport},
    execution::{
        DefaultExecutionEngine, ExecutionEngine, execution_parameters::ExecutionParameters,
    },
    io::{ExportManager, ImportManager, resource_providers::ResourceProviders},
    meta::timing::TimedCode,
    rule_file::RuleFile,
    rule_model::{
        components::{output::Output, tag::Tag},
        pipeline::transformations::default::TransformationDefault,
        programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
    },
};

/// The top-level directories of the corpus whose cases are ignored.
const IGNORED: [&str; 2] = ["changed", "new"];

/// The rendered rows of each checked predicate, by predicate name.
type Results = BTreeMap<String, Vec<Vec<String>>>;

/// Registers one test per case and runs them.
fn main() {
    let mut arguments = Arguments::from_args();
    arguments.test_threads = Some(1);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../resources/testcases");

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
            let ignored = IGNORED
                .iter()
                .any(|directory| name.starts_with(&format!("{directory}::")));
            Trial::test(name, move || run(&program).map_err(Failed::from))
                .with_ignored_flag(ignored)
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

/// Runs one case: reasons, then compares the results.
fn run(program: &Path) -> Result<(), String> {
    // A case that panicked left its timers running.
    *TimedCode::instance() = TimedCode::new();

    let text = fs::read_to_string(program.with_extension("expect"))
        .map_err(|_| String::from("the case has no `.expect` file"))?;
    let expected = expectation(&text).map_err(|reason| format!("in the expectation: {reason}"))?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        let (mut engine, checked) = reason(program, &expected).await?;
        let actual = results(&mut engine, checked).await?;
        compare(&actual, &expected)?;
        check_exports(&mut engine, program).await
    })
}

/// Loads and reasons over a case program, reading its imports relative
/// to its directory; returns the engine and the predicates to check.
///
/// Where the expectation names an imported predicate, every checked
/// predicate the program knows becomes an output. The engine narrows an
/// import to what its rules read, or drops it, but reads the import of an
/// output whole; once outputs are declared, it keeps only the rules they
/// need.
async fn reason(
    path: &Path,
    expected: &Results,
) -> Result<(DefaultExecutionEngine, BTreeSet<String>), String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let file = RuleFile::new(text, path.to_string_lossy().into_owned());
    let (mut handle, report) = ProgramReport::new(file.clone())
        .merge_program_parser_report(ProgramHandle::from_file(&file))
        .map_err(|report| report.to_string())?;
    let checked = checked(&handle, expected);

    let imports_named = handle
        .imports()
        .any(|import| expected.contains_key(&import.predicate().to_string()));
    if imports_named {
        let declared: HashSet<Tag> = handle
            .outputs()
            .map(|output| output.predicate().clone())
            .collect();
        let known = handle
            .all_predicates()
            .into_iter()
            .chain(handle.import_predicates());
        let mut program = handle.materialize();
        for predicate in known.collect::<HashSet<_>>() {
            if checked.contains(&predicate.to_string()) && !declared.contains(&predicate) {
                program.add_output(Output::new(predicate));
            }
        }
        handle = ProgramHandle::from(program);
    }

    let directory = path.parent().expect("cases live in directories");
    let mut parameters = ExecutionParameters::default();
    parameters.set_import_manager(ImportManager::new(ResourceProviders::with_base_path(Some(
        directory.to_path_buf(),
    ))));
    let transformed = handle.transform(TransformationDefault::new(&parameters));
    let (handle, _) = report
        .merge_validation_report(&handle, transformed)
        .map_err(|report| report.to_string())?;

    let mut engine = ExecutionEngine::from_handle(handle, parameters)
        .await
        .map_err(describe)?;
    engine.execute().await.map_err(describe)?;
    Ok((engine, checked))
}

/// The predicates to check, by the program as written: the declared
/// outputs, or every derived predicate where it declares none, the
/// exported ones, and the ones the expectation names.
fn checked(program: &ProgramHandle, expected: &Results) -> BTreeSet<String> {
    let mut predicates: BTreeSet<String> = program
        .outputs()
        .map(|output| output.predicate().to_string())
        .collect();
    if predicates.is_empty() {
        predicates.extend(
            program
                .rules()
                .flat_map(|rule| rule.head())
                .map(|atom| atom.predicate().to_string()),
        );
    }
    predicates.extend(
        program
            .exports()
            .map(|export| export.predicate().to_string()),
    );
    predicates.extend(expected.keys().cloned());
    predicates
}

/// The rows of the checked predicates.
async fn results(
    engine: &mut DefaultExecutionEngine,
    checked: BTreeSet<String>,
) -> Result<Results, String> {
    let mut results = Results::new();
    for predicate in checked {
        let tag = Tag::new(predicate.clone());
        let rows = if engine.predicate_arity(&tag).is_some() {
            rows(engine, &tag).await?
        } else {
            Vec::new()
        };
        results.insert(predicate, rows);
    }
    Ok(results)
}

/// The rendered rows of a predicate.
async fn rows(
    engine: &mut DefaultExecutionEngine,
    predicate: &Tag,
) -> Result<Vec<Vec<String>>, String> {
    let rows = engine.predicate_rows(predicate).await.map_err(describe)?;
    Ok(rows
        .into_iter()
        .flatten()
        .map(|row| row.iter().map(ToString::to_string).collect())
        .collect())
}

/// The description of an error, without a trailing newline.
fn describe(error: Error) -> String {
    error.to_string().trim_end().to_owned()
}

/// The facts of an expectation file, each value rendered as the engine
/// renders it.
///
/// The harness reads the file itself: the engine's parser reads nulls,
/// decimals, doubles like `2d` and string escapes differently from the
/// corpus syntax.
fn expectation(text: &str) -> Result<Results, String> {
    let mut reader = Reader { text, position: 0 };
    let mut results = Results::new();
    while reader.skip_space() {
        let (predicate, row) = reader.fact()?;
        results.entry(predicate).or_default().push(row);
    }
    for rows in results.values_mut() {
        rows.sort();
        rows.dedup();
    }
    Ok(results)
}

/// A reader of the facts in an expectation file.
struct Reader<'a> {
    /// The text of the file.
    text: &'a str,
    /// The byte position of the next character.
    position: usize,
}

impl<'a> Reader<'a> {
    /// One fact: a predicate, its terms in parentheses, and a period.
    fn fact(&mut self) -> Result<(String, Vec<String>), String> {
        let predicate = match self.peek() {
            Some('<') => self.iri()?,
            _ => self.name()?.to_owned(),
        };
        self.skip_space();
        self.expect('(')?;
        let mut row = Vec::new();
        self.skip_space();
        if !self.eat(')') {
            loop {
                self.skip_space();
                row.push(self.term()?);
                self.skip_space();
                if self.eat(')') {
                    break;
                }
                self.expect(',')?;
            }
        }
        self.skip_space();
        self.expect('.')?;
        Ok((predicate, row))
    }

    /// One term, rendered as the engine renders it.
    fn term(&mut self) -> Result<String, String> {
        let value = match self.peek() {
            Some('<') => AnyDataValue::new_iri(self.iri()?),
            Some('"') => self.literal()?,
            Some('_') => return self.null(),
            Some(character) if character.is_ascii_digit() || matches!(character, '+' | '-') => {
                self.number()?
            }
            _ => match self.name()? {
                "true" => AnyDataValue::new_boolean(true),
                "false" => AnyDataValue::new_boolean(false),
                name => AnyDataValue::new_iri(name.to_owned()),
            },
        };
        Ok(value.to_string())
    }

    /// An IRI in angle brackets, returned without them.
    fn iri(&mut self) -> Result<String, String> {
        self.expect('<')?;
        let iri = self.take_while(|character| character != '>').to_owned();
        self.expect('>')?;
        Ok(iri)
    }

    /// A bare name, which stands for an IRI or a boolean.
    fn name(&mut self) -> Result<&'a str, String> {
        let name = self.take_while(|character| character.is_alphanumeric() || character == '_');
        if name.is_empty() {
            return Err(self.error("expected a term"));
        }
        Ok(name)
    }

    /// A null: `_:` and its number.
    fn null(&mut self) -> Result<String, String> {
        if !self.text[self.position..].starts_with("_:") {
            return Err(self.error("expected a null"));
        }
        self.position += 2;
        let id = self.take_while(|character| character.is_ascii_digit());
        if id.is_empty() {
            return Err(self.error("expected the number of a null"));
        }
        Ok(format!("_:{id}"))
    }

    /// A number: an integer, a decimal with a fraction, a double with an
    /// exponent or a `d` suffix, or a float with an `f` suffix.
    fn number(&mut self) -> Result<AnyDataValue, String> {
        let start = self.position;
        if matches!(self.peek(), Some('+' | '-')) {
            self.position += 1;
        }
        self.take_while(|character| character.is_ascii_digit());
        let fraction = self.text[self.position..].starts_with('.')
            && self.text[self.position + 1..]
                .starts_with(|character: char| character.is_ascii_digit());
        if fraction {
            self.position += 1;
            self.take_while(|character| character.is_ascii_digit());
        }
        let exponent = matches!(self.peek(), Some('e' | 'E'));
        if exponent {
            self.position += 1;
            if matches!(self.peek(), Some('+' | '-')) {
                self.position += 1;
            }
            self.take_while(|character| character.is_ascii_digit());
        }
        let lexical = self.text[start..self.position].to_owned();

        let value = if self.eat('f') {
            match lexical.parse() {
                Ok(float) => AnyDataValue::new_float_from_f32(float),
                Err(error) => return Err(self.error(&error.to_string())),
            }
        } else if self.eat('d') || exponent {
            AnyDataValue::new_from_double_literal(lexical)
        } else if fraction {
            AnyDataValue::new_from_decimal_literal(lexical)
        } else {
            AnyDataValue::new_from_integer_literal(lexical)
        };
        value.map_err(|error| self.error(&error.to_string()))
    }

    /// A string, plain, with a language tag or typed.
    fn literal(&mut self) -> Result<AnyDataValue, String> {
        let text = self.string()?;
        if self.eat('@') {
            let tag = self.take_while(|character| character.is_alphanumeric() || character == '-');
            Ok(AnyDataValue::new_language_tagged_string(
                text,
                tag.to_owned(),
            ))
        } else if self.text[self.position..].starts_with("^^") {
            self.position += 2;
            let datatype = self.iri()?;
            AnyDataValue::new_from_typed_literal(text, datatype)
                .map_err(|error| self.error(&error.to_string()))
        } else {
            Ok(AnyDataValue::new_plain_string(text))
        }
    }

    /// The text of a quoted string. The escapes are `\\`, `\"`, `\n`, `\t`
    /// and `\r`; any other backslash stands for itself.
    fn string(&mut self) -> Result<String, String> {
        self.expect('"')?;
        let mut text = String::new();
        loop {
            match self.bump() {
                None => return Err(self.error("the string is not closed")),
                Some('"') => return Ok(text),
                Some('\\') => {
                    let escaped = match self.peek() {
                        Some('\\') => '\\',
                        Some('"') => '"',
                        Some('n') => '\n',
                        Some('t') => '\t',
                        Some('r') => '\r',
                        _ => {
                            text.push('\\');
                            continue;
                        }
                    };
                    self.position += 1;
                    text.push(escaped);
                }
                Some(character) => text.push(character),
            }
        }
    }

    /// The next character.
    fn peek(&self) -> Option<char> {
        self.text[self.position..].chars().next()
    }

    /// Consumes the next character.
    fn bump(&mut self) -> Option<char> {
        let character = self.peek()?;
        self.position += character.len_utf8();
        Some(character)
    }

    /// Consumes the given character if it comes next.
    fn eat(&mut self, expected: char) -> bool {
        let next = self.peek() == Some(expected);
        if next {
            self.position += expected.len_utf8();
        }
        next
    }

    /// Consumes the given character, which must come next.
    fn expect(&mut self, expected: char) -> Result<(), String> {
        if self.eat(expected) {
            Ok(())
        } else {
            Err(self.error(&format!("expected `{expected}`")))
        }
    }

    /// Consumes the characters up to the first one that fails the test.
    fn take_while(&mut self, test: impl Fn(char) -> bool) -> &'a str {
        let rest = &self.text[self.position..];
        let length = rest
            .find(|character| !test(character))
            .unwrap_or(rest.len());
        self.position += length;
        &rest[..length]
    }

    /// Skips white space; whether any text remains.
    fn skip_space(&mut self) -> bool {
        self.take_while(char::is_whitespace);
        self.position < self.text.len()
    }

    /// An error at the current position.
    fn error(&self, message: &str) -> String {
        let line = self.text[..self.position].matches('\n').count() + 1;
        format!("line {line}: {message}")
    }
}

/// Checks the files the exports write against the case's directory of
/// expected files.
async fn check_exports(engine: &mut DefaultExecutionEngine, program: &Path) -> Result<(), String> {
    let expected_directory = program.with_extension("");
    if !expected_directory.is_dir() {
        return Ok(());
    }
    let written = TempDir::new().map_err(|error| error.to_string())?;
    let manager = ExportManager::default().set_base_path(written.path().to_path_buf());
    for (predicate, export) in engine.exports() {
        let rows = engine.predicate_rows(&predicate).await.map_err(describe)?;
        manager
            .export_table(&predicate, &export, rows)
            .map_err(describe)?;
    }

    let lines = |path: &Path| -> Result<Vec<String>, String> {
        let text = fs::read_to_string(path)
            .map_err(|_| format!("`{}` was not written", path.display()))?;
        let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
        lines.sort();
        Ok(lines)
    };
    for entry in fs::read_dir(&expected_directory).map_err(|error| error.to_string())? {
        let expected_file = entry.map_err(|error| error.to_string())?.path();
        let name = expected_file.file_name().expect("entries have names");
        let actual = lines(&written.path().join(name))?;
        let expected = lines(&expected_file)?;
        if actual != expected {
            return Err(format!(
                "the export `{}` differs:\n{}",
                name.to_string_lossy(),
                difference(&actual, &expected)
            ));
        }
    }
    Ok(())
}

/// One rendered value, its nulls cut out: the text with a marker in
/// place of each null's id, and the ids in order.
#[derive(Debug, Clone)]
struct Cell {
    /// The rendering with every null's id replaced by [Cell::SLOT].
    shape: String,
    /// The ids of the nulls, in order of occurrence.
    nulls: Vec<String>,
}

impl Cell {
    /// The marker of a null's id in a shape: no rendering contains it.
    const SLOT: char = '\u{0}';

    /// Splits a rendered value. A null renders as `_:` and its id, which
    /// never occurs inside a quoted string or an IRI.
    fn of(text: &str) -> Self {
        let mut shape = String::new();
        let mut nulls = Vec::new();
        let mut characters = text.chars().peekable();
        let mut previous = ' ';
        while let Some(character) = characters.next() {
            shape.push(character);
            match character {
                '"' => {
                    while let Some(inner) = characters.next() {
                        shape.push(inner);
                        match inner {
                            '\\' => shape.extend(characters.next()),
                            '"' => break,
                            _ => {}
                        }
                    }
                }
                '<' => {
                    for inner in characters.by_ref() {
                        shape.push(inner);
                        if inner == '>' {
                            break;
                        }
                    }
                }
                '_' if !previous.is_alphanumeric() && characters.peek() == Some(&':') => {
                    shape.push(characters.next().expect("peeked"));
                    let mut id = String::new();
                    while let Some(digit) = characters.next_if(char::is_ascii_digit) {
                        id.push(digit);
                    }
                    if !id.is_empty() {
                        shape.push(Self::SLOT);
                        nulls.push(id);
                    }
                }
                _ => {}
            }
            previous = character;
        }
        Cell { shape, nulls }
    }
}

/// One side's rows of all predicates, as cells with densely numbered
/// nulls: the predicate's index, then the row.
type Side = Vec<(usize, Vec<(String, Vec<usize>)>)>;

/// Compares actual against expected results; nulls on either side
/// switch to the comparison up to one renaming of the nulls.
fn compare(actual: &Results, expected: &Results) -> Result<(), String> {
    let mut problems = Vec::new();
    let predicates: BTreeSet<&String> = actual.keys().chain(expected.keys()).collect();
    for predicate in &predicates {
        let blind = |results: &Results| -> Vec<String> {
            let mut rows: Vec<String> = results
                .get(*predicate)
                .into_iter()
                .flatten()
                .map(|row| {
                    row.iter()
                        .map(|value| Cell::of(value).shape.replace(Cell::SLOT, "?"))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .collect();
            rows.sort();
            rows
        };
        let (derived, wanted) = (blind(actual), blind(expected));
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
            "the rows agree with the nulls blinded, but no renaming of the {actual_nulls} \
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
    let mut nulls: HashMap<String, usize> = HashMap::new();
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
                        .map(|id| {
                            let next = nulls.len();
                            *nulls.entry(id.clone()).or_insert(next)
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

/// Whether one renaming of the actual nulls into the expected ones maps
/// the actual rows onto exactly the expected rows. Nulls are first
/// narrowed by color refinement over the rows they occur in; the
/// remaining ties backtrack.
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
