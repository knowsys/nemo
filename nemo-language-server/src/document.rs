//! One open document: its text, the results of parsing and checking it,
//! and the index of its names.

use std::{
    collections::HashMap,
    panic::{self, AssertUnwindSafe},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use nemo::{
    engine::{Engine, InferredTypes},
    model::{
        Atom, ColumnDeclaration, Directive, FileId, Identifier, Literal, Program, Span, Statement,
        TypeExpression, TypeLabel,
    },
};
use nemo_physical::{
    diagnostics::{Diagnostic, Severity},
    value::Type,
};

use crate::{position::Lines, symbols::Symbols};

/// The file id of a document's spans.
///
/// A document is parsed on its own, so one id serves every document.
/// The check registers the document first, so that the engine gives it this id too.
/// The modules that the check reads get other ids from the engine.
const DOCUMENT: FileId = FileId::new(0);

/// One open document, parsed and checked.
///
/// It is parsed and checked once per edit, and every request after that only looks up what
/// was stored, so that a keystroke does not cost a parse per request.
#[derive(Debug)]
pub(crate) struct Document {
    /// The text as the editor has it.
    text: String,
    /// The index of where the text's lines begin.
    lines: Lines,
    /// The diagnostics of parsing and of the engine's check.
    diagnostics: Vec<Diagnostic>,
    /// The index of the names in the text.
    symbols: Symbols,
    /// The parsed program, kept for formatting, which prints it back.
    program: Program,
    /// The column names that the editor shows before arguments, in offset order.
    hints: Vec<ColumnHint>,
    /// The documentation of each declared predicate, by name.
    documentation: HashMap<String, Documentation>,
    /// The documentation of each value label that a `@type` declares, by label.
    labels: HashMap<String, Documentation>,
    /// The comment blocks directly above the statements that derive each predicate, by name.
    comments: HashMap<String, Vec<String>>,
    /// The types inferred by the last check of a text that parsed.
    types: Arc<KnownTypes>,
    /// Whether the types were inferred from this text.
    ///
    /// If this text does not parse, the types of an earlier text are kept instead.
    fresh: bool,
}

/// The documentation of a predicate or a value label:
/// its declaration, and the comment block written above it.
#[derive(Debug)]
pub(crate) struct Documentation {
    /// The declared name with its columns or fields, as `person(name: string, age: int)`.
    pub signature: String,
    /// The comment lines directly above the declaration, without their `%` markers.
    pub comment: Option<String>,
}

/// The types that a check inferred, for predicates and for the variables of each rule.
#[derive(Debug, Default)]
pub(crate) struct KnownTypes {
    /// The types that the check inferred, in which the types of predicates are looked up.
    inferred: Option<InferredTypes>,
    /// The types of each rule's variables, keyed by the rule's text.
    ///
    /// Unlike the rule's position, its text stays the same when the document is edited
    /// elsewhere.
    rules: HashMap<String, Vec<(String, Type)>>,
}

impl KnownTypes {
    /// Returns the types of a predicate's columns, if they are known.
    pub(crate) fn predicate(&self, name: &Identifier) -> Option<&[Type]> {
        self.inferred.as_ref()?.predicate(name)
    }

    /// Returns the type of the variable `name` of the rule whose text is `rule`, if it is known.
    ///
    /// `name` is written without the `?`.
    pub(crate) fn variable(&self, rule: &str, name: &str) -> Option<&Type> {
        self.rules
            .get(rule)?
            .iter()
            .find(|(variable, _)| variable == name)
            .map(|(_, datatype)| datatype)
    }
}

/// An inline column name, which the editor shows before an argument whose column a
/// declaration names.
#[derive(Debug)]
pub(crate) struct ColumnHint {
    /// The byte offset of the hint, which is the start of the argument.
    pub offset: u32,
    /// The text shown, as `name:`.
    pub label: String,
}

impl Document {
    /// Parses and checks a text, without reading any data.
    ///
    /// The diagnostics of parsing come first, followed by those of the engine's check,
    /// such as resolution, directives, rule compilation and stratification.
    /// `path` is the document's file, relative to which its `@use` directives find their
    /// modules.
    /// Without a path, the document is checked as a program of its own.
    /// `open` contains the paths and texts of the other open documents.
    /// A `@use` of one of them reads its open text rather than the file on disk.
    pub(crate) fn new(text: String, path: Option<PathBuf>, open: &[(PathBuf, &str)]) -> Self {
        let lines = Lines::of(&text);
        let (program, mut diagnostics) = Program::parse_with_file(&text, DOCUMENT);
        let symbols = Symbols::of(&program, &text);

        let parsed = !diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity() == Severity::Error);

        // The check runs on an engine of its own for this edit, so a panic in it loses only
        // that engine. The document is registered first, so its id is `DOCUMENT` and the
        // check's spans point into this text.
        let mut inferred = None;
        diagnostics.extend(checked(|| {
            let mut engine = Engine::new();
            let registered = engine.add_source(
                path.as_deref().map_or_else(
                    || String::from("this document"),
                    |path| path.display().to_string(),
                ),
                text.clone(),
            );
            match registered {
                Ok(registered) => debug_assert!(
                    registered == DOCUMENT,
                    "the first registered source takes the document's id"
                ),
                Err(error) => return vec![error],
            }
            // The other open documents are registered under the names with which a `@use`
            // in this document refers to them. A document that no such name reaches, or
            // that is too large to address, is not registered, and a `@use` of it reads the
            // file on disk.
            if let Some(directory) = path.as_deref().and_then(Path::parent) {
                for (other, buffer) in open {
                    if let Some(name) = module_name(directory, other) {
                        let _ = engine.add_module(&name, *buffer);
                    }
                }
            }
            // Linking reads the modules, relative to the document's path,
            // and its warnings and its error are part of the check.
            let linked = engine.link(program.clone());
            let mut found = engine.reports().to_vec();
            match linked {
                Ok(linked) => {
                    let report = engine.check(linked);
                    inferred = report.types().cloned();
                    found.extend(report.into_diagnostics());
                }
                Err(problem) => found.push(problem),
            }
            found
        }));

        // The types inferred from a text that does not parse ignore its broken statements,
        // so such a text keeps the types of an earlier text instead.
        let fresh = parsed && inferred.is_some();
        let types = match inferred.filter(|_| fresh) {
            Some(inferred) => KnownTypes {
                rules: inferred
                    .rules()
                    .iter()
                    .filter(|rule| rule.span().file == DOCUMENT)
                    .filter_map(|rule| {
                        let written =
                            text.get(rule.span().start as usize..rule.span().end as usize)?;
                        let variables = rule
                            .variables()
                            .map(|(name, datatype)| (name.to_owned(), datatype.clone()))
                            .collect();
                        Some((written.to_owned(), variables))
                    })
                    .collect(),
                inferred: Some(inferred),
            },
            None => KnownTypes::default(),
        };

        let hints = column_hints(&program);
        let (documentation, labels) = documentation(&program, &text);
        let comments = derivation_comments(&program, &text);
        Self {
            text,
            lines,
            diagnostics,
            symbols,
            program,
            hints,
            documentation,
            labels,
            comments,
            types: Arc::new(types),
            fresh,
        }
    }

    /// Uses the types of `previous`, the document that this one replaces,
    /// if this document's types were not inferred from its own text.
    pub(crate) fn inherit_types(&mut self, previous: &Document) {
        if !self.fresh {
            self.types = Arc::clone(&previous.types);
        }
    }

    /// Returns the types inferred by the last check of a text that parsed.
    pub(crate) fn types(&self) -> &KnownTypes {
        &self.types
    }

    /// Returns the parsed statements.
    pub(crate) fn statements(&self) -> &[Statement] {
        &self.program.statements
    }

    /// Returns the whole text.
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    /// Returns the diagnostics of parsing and checking.
    pub(crate) fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    /// Returns the index of the document's names.
    pub(crate) fn symbols(&self) -> &Symbols {
        &self.symbols
    }

    /// Returns the column names that the editor shows before arguments, in offset order.
    pub(crate) fn hints(&self) -> &[ColumnHint] {
        &self.hints
    }

    /// Returns the documentation of a predicate, if the document declares it.
    pub(crate) fn documentation(&self, name: &str) -> Option<&Documentation> {
        self.documentation.get(name)
    }

    /// Returns the documentation of a value label, if a `@type` of the document declares it.
    pub(crate) fn label(&self, name: &str) -> Option<&Documentation> {
        self.labels.get(name)
    }

    /// Returns the comment blocks directly above the statements that derive a predicate,
    /// in source order, without repeats.
    pub(crate) fn comments(&self, name: &str) -> &[String] {
        self.comments.get(name).map_or(&[], Vec::as_slice)
    }

    /// Returns the protocol position of a byte offset.
    pub(crate) fn position(&self, offset: u32) -> (u32, u32) {
        self.lines.position(&self.text, offset as usize)
    }

    /// Returns the byte offset of a protocol position.
    pub(crate) fn offset(&self, line: u32, character: u32) -> u32 {
        u32::try_from(self.lines.offset(&self.text, line, character)).unwrap_or(u32::MAX)
    }

    /// Returns the protocol positions at which a span begins and ends,
    /// or the start of the document for a span in another file.
    ///
    /// A span in another file, such as a label of a diagnostic in a module, has no place in
    /// this document, and placing it in this text would be wrong.
    pub(crate) fn range(&self, span: Span) -> ((u32, u32), (u32, u32)) {
        if span.file != DOCUMENT {
            return ((0, 0), (0, 0));
        }
        (
            self.lines.position(&self.text, span.start as usize),
            self.lines.position(&self.text, span.end as usize),
        )
    }

    /// Returns whether a span is in this document.
    pub(crate) fn holds(&self, span: Span) -> bool {
        span.file == DOCUMENT
    }

    /// Returns the name characters immediately before a byte offset,
    /// together with the marker before them, if there is one.
    ///
    /// This is the part of a word that the user has typed so far,
    /// and its marker decides what the word can be.
    pub(crate) fn prefix(&self, offset: u32) -> &str {
        let offset = usize::try_from(offset).unwrap_or(0).min(self.text.len());
        let bytes = self.text.as_bytes();
        let mut start = offset;
        while start > 0 && is_name(bytes[start - 1]) {
            start -= 1;
        }
        // A marker belongs to the word it introduces.
        if start > 0 && matches!(bytes[start - 1], b'@' | b'?' | b'!' | b'$' | b'#') {
            start -= 1;
        }
        self.text.get(start..offset).unwrap_or("")
    }

    /// Returns the span of `name` inside `span`, the span of an occurrence of the name.
    ///
    /// An occurrence's span can cover a whole atom or directive,
    /// while renaming and highlighting need the span of the name itself.
    /// The name is found as the first whole name inside `span` that does not follow an `@`.
    /// Returns `None` if it is not found, so that an edit never goes to a guessed position.
    pub(crate) fn name_span(&self, name: &str, span: Span) -> Option<Span> {
        if !self.holds(span) || name.is_empty() {
            return None;
        }
        let source = self.slice(span);
        let bytes = source.as_bytes();
        let mut from = 0;
        while let Some(found) = source.get(from..)?.find(name) {
            let at = from + found;
            let after = at + name.len();
            let bounded = (at == 0 || !is_name(bytes[at - 1]))
                && (after == bytes.len() || !is_name(bytes[after]));
            // `@import` is not the predicate `import`.
            let introduced = at > 0 && bytes[at - 1] == b'@';
            if bounded && !introduced {
                let start = span.start + u32::try_from(at).ok()?;
                return Some(Span {
                    start,
                    end: start + u32::try_from(name.len()).ok()?,
                    ..span
                });
            }
            from = at + 1;
        }
        None
    }

    /// Returns the text of a span, or an empty text for a span in another file.
    pub(crate) fn slice(&self, span: Span) -> &str {
        if span.file != DOCUMENT {
            return "";
        }
        self.text
            .get(span.start as usize..span.end as usize)
            .unwrap_or("")
    }
}

/// Runs `check` and returns its diagnostics, or an internal error if it panicked.
fn checked(check: impl FnOnce() -> Vec<Diagnostic>) -> Vec<Diagnostic> {
    panic::catch_unwind(AssertUnwindSafe(check)).unwrap_or_else(|payload| {
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("a panic without a message");
        vec![Diagnostic::internal(message)]
    })
}

/// Returns whether a byte can be part of a name.
fn is_name(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b':' | b'.') || byte >= 0x80
}

/// Returns the documentation of each declared predicate and of each declared value label.
///
/// A predicate is declared by a `@declare`, or by an `@input` with columns,
/// and a `@declare` takes precedence.
/// A value label is declared by a `@type`.
/// The documentation consists of the declaration and the comment block directly above it.
fn documentation(
    program: &Program,
    text: &str,
) -> (
    HashMap<String, Documentation>,
    HashMap<String, Documentation>,
) {
    let mut predicates = HashMap::new();
    let mut labels = HashMap::new();
    for statement in &program.statements {
        let Statement::Directive(directive) = statement else {
            continue;
        };
        let comment = directive
            .span()
            .map(|span| comment_above(text, span.start as usize))
            .filter(|comment| !comment.is_empty());
        match directive {
            Directive::Declare(declare) => {
                predicates.insert(
                    declare.predicate.to_string(),
                    Documentation {
                        signature: signature(&declare.predicate, &declare.columns),
                        comment,
                    },
                );
            }
            Directive::Input(input) => {
                let Some(columns) = &input.columns else {
                    continue;
                };
                predicates
                    .entry(input.predicate.to_string())
                    .or_insert(Documentation {
                        signature: signature(&input.predicate, columns),
                        comment,
                    });
            }
            Directive::Type(declaration) => {
                let (TypeExpression::Tuple {
                    label: TypeLabel::Named(label),
                    ..
                }
                | TypeExpression::Map {
                    label: TypeLabel::Named(label),
                    ..
                }) = &declaration.definition
                else {
                    continue;
                };
                labels.insert(
                    label.to_string(),
                    Documentation {
                        signature: format!("@type {}", declaration.definition),
                        comment,
                    },
                );
            }
            _ => {}
        }
    }
    (predicates, labels)
}

/// Returns the comment blocks directly above the statements that derive each predicate,
/// in source order and without repeats.
///
/// These statements are the predicate's facts, the rules with it in the head,
/// its imports and its `@input`.
/// A comment that a blank line separates from the next statement belongs to no statement.
fn derivation_comments(program: &Program, text: &str) -> HashMap<String, Vec<String>> {
    let mut comments: HashMap<String, Vec<String>> = HashMap::new();
    let mut note = |predicate: &Identifier, span: Option<Span>| {
        let Some(comment) = span
            .map(|span| comment_above(text, span.start as usize))
            .filter(|comment| !comment.is_empty())
        else {
            return;
        };
        let noted = comments.entry(predicate.to_string()).or_default();
        if !noted.contains(&comment) {
            noted.push(comment);
        }
    };
    for statement in &program.statements {
        match statement {
            Statement::Fact(fact) => note(&fact.predicate, fact.span),
            Statement::Rule(rule) => {
                for head in &rule.head {
                    note(&head.predicate, rule.span);
                }
            }
            Statement::Directive(Directive::Import(import)) => {
                note(&import.predicate, import.span);
            }
            Statement::Directive(Directive::Input(input)) => note(&input.predicate, input.span),
            Statement::Directive(_) => {}
        }
    }
    comments
}

/// Returns a predicate with its declared columns, as `person(name: string, age: int)`.
fn signature(predicate: &Identifier, columns: &[ColumnDeclaration]) -> String {
    let columns: Vec<String> = columns.iter().map(ToString::to_string).collect();
    format!("{predicate}({})", columns.join(", "))
}

/// Returns the consecutive `%` comment lines directly above the line of an offset,
/// in source order and without their markers and surrounding whitespace.
fn comment_above(text: &str, offset: usize) -> String {
    let mut lines = Vec::new();
    let mut end = text[..offset.min(text.len())]
        .rfind('\n')
        .map_or(0, |newline| newline + 1);
    while end > 0 {
        let start = text[..end - 1].rfind('\n').map_or(0, |newline| newline + 1);
        let line = text[start..end].trim();
        if !line.starts_with('%') {
            break;
        }
        lines.push(line.trim_start_matches('%').trim());
        end = start;
    }
    lines.reverse();
    lines.join("\n")
}

/// Returns the inline column names of a program, in offset order.
///
/// Every positional atom of a predicate whose `@declare` names its columns gets a hint
/// before each argument, with the name of the argument's column.
/// An atom that names its own columns gets no hints,
/// and neither does an atom whose number of arguments differs from the declaration's.
fn column_hints(program: &Program) -> Vec<ColumnHint> {
    let mut declarations: HashMap<String, Vec<Option<String>>> = HashMap::new();
    for statement in &program.statements {
        if let Statement::Directive(Directive::Declare(declare)) = statement
            && declare.columns.iter().any(|column| column.name.is_some())
        {
            declarations.insert(
                declare.predicate.to_string(),
                declare
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
            );
        }
    }
    if declarations.is_empty() {
        return Vec::new();
    }

    let mut hints = Vec::new();
    let mut atom =
        |predicate: &Identifier, terms: &[nemo::model::Term], labels: &Option<Vec<String>>| {
            if labels.is_some() {
                return;
            }
            let Some(names) = declarations.get(&predicate.to_string()) else {
                return;
            };
            if names.len() != terms.len() {
                return;
            }
            for (term, name) in terms.iter().zip(names) {
                if let (Some(span), Some(name)) = (term.span, name) {
                    hints.push(ColumnHint {
                        offset: span.start,
                        label: format!("{name}:"),
                    });
                }
            }
        };
    for statement in &program.statements {
        match statement {
            Statement::Fact(fact) => atom(&fact.predicate, &fact.terms, &fact.labels),
            Statement::Rule(rule) => {
                let atoms = rule
                    .head
                    .iter()
                    .chain(rule.body.iter().filter_map(|literal| match literal {
                        Literal::Positive(inner) | Literal::Negative(inner) => Some(inner),
                        Literal::Operation(_) => None,
                    }));
                for Atom {
                    predicate,
                    terms,
                    labels,
                    ..
                } in atoms
                {
                    atom(predicate, terms, labels);
                }
            }
            Statement::Directive(_) => {}
        }
    }
    hints.sort_by_key(|hint| hint.offset);
    hints
}

/// Returns the module name with which a `@use` in a file in `directory` refers to the module
/// file `file`.
///
/// Returns `None` for a file that is not an `.rls` file, or that no module name can refer to,
/// such as a file on another drive.
fn module_name(directory: &Path, file: &Path) -> Option<String> {
    if file.extension()? != "rls" {
        return None;
    }
    let file = file.with_extension("");
    let from: Vec<Component<'_>> = directory.components().collect();
    let to: Vec<Component<'_>> = file.components().collect();
    let shared = from.iter().zip(&to).take_while(|(a, b)| a == b).count();
    // Files on different drives share no directory to go up to.
    if shared == 0 {
        return None;
    }
    let mut parts = vec!["super"; from.len() - shared];
    for component in &to[shared..] {
        let Component::Normal(part) = component else {
            return None;
        };
        parts.push(part.to_str()?);
    }
    Some(parts.join("::"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The occurrence at an offset is found through the document's index,
    /// with the span of its whole atom.
    #[test]
    fn a_document_finds_occurrences_through_its_index() {
        let source = "edge(1, 2) .\nreach(?x) :- edge(?x, 1) .\n";
        let document = Document::new(source.to_string(), None, &[]);
        assert!(document.diagnostics().is_empty());

        // The cursor is on the `edge` of the rule's body.
        let offset = source.rfind("edge").expect("the body mentions it");
        let found = document
            .symbols()
            .at(u32::try_from(offset).expect("the source is small"))
            .expect("an occurrence is here");
        assert_eq!(found.name, "edge");
        assert_eq!(document.slice(found.span), "edge(?x, 1)");
    }

    /// A document with a parse error still indexes the statements that parsed.
    #[test]
    fn a_broken_document_still_indexes_what_parsed() {
        let document = Document::new("p(1) .\nq(?x) :- p(?x\n".to_string(), None, &[]);
        assert!(
            !document.diagnostics().is_empty(),
            "the tail does not parse"
        );
        // The fact before the error can still be navigated to.
        assert_eq!(document.symbols().occurrences("p").count(), 1);
    }

    /// The check finds a problem that parsing cannot, without reading the imported data,
    /// and labels it in the document.
    #[test]
    fn a_semantic_problem_is_found_without_reading_data() {
        // `?y` is never bound, which only the check finds. The import's missing file is not
        // reported, because nothing reads it.
        let document = Document::new(
            "@import p from csv { resource = \"nowhere.csv\", format = (int) } .\n\
             q(?y) :- p(?x) .\n"
                .to_string(),
            None,
            &[],
        );
        let [problem] = document.diagnostics() else {
            panic!("expected the one problem: {:?}", document.diagnostics());
        };
        let label = problem.labels().first().expect("the problem points home");
        assert!(document.holds(label.span), "the span is this document's");
    }

    /// A warning of the type analysis is among the document's diagnostics.
    #[test]
    fn a_type_warning_reaches_the_document() {
        use nemo_physical::diagnostics::Severity;

        // `?x` joins a string column with an integer column, so the type analysis warns
        // that the rule can never derive anything.
        let document = Document::new(
            "word(\"a\") . value(1) .\ndead(?x) :- word(?x), value(?x) .\n".to_string(),
            None,
            &[],
        );
        let [warning] = document.diagnostics() else {
            panic!("expected the one warning: {:?}", document.diagnostics());
        };
        assert_eq!(warning.severity(), Severity::Warning);
        assert!(document.holds(warning.labels()[0].span));
    }

    /// A check that panics is reported as an internal error with the panic's message,
    /// whether the message is a literal or formatted.
    #[test]
    fn a_panicking_check_reports_an_internal_error() {
        use nemo_physical::diagnostics::Code;

        for (reports, message) in [
            (
                checked(|| panic!("an invariant broke")),
                "an invariant broke",
            ),
            (
                checked(|| panic!("table {} is unknown", 7)),
                "table 7 is unknown",
            ),
        ] {
            let [report] = reports.as_slice() else {
                panic!("expected the one report: {reports:?}");
            };
            assert_eq!(report.code(), Code::INTERNAL);
            assert_eq!(report.message(), message);
        }
    }

    /// The column names of a `@declare` become hints before the arguments of the
    /// predicate's positional atoms.
    #[test]
    fn declared_column_names_become_hints() {
        let source = "@declare person(name: string, age: int) .\n\
                      person(\"alice\", 34) .\n\
                      adult(?p) :- person(?p, ?a), ?a >= 18 .\n\
                      named(?p) :- person(name: ?p, age: ?a) .\n";
        let document = Document::new(source.to_string(), None, &[]);
        let labels: Vec<(&str, usize)> = document
            .hints()
            .iter()
            .map(|hint| (hint.label.as_str(), hint.offset as usize))
            .collect();
        // The fact and the positional atom get hints,
        // and the atom that names its own columns gets none.
        assert_eq!(
            labels,
            [
                ("name:", source.find("\"alice\"").expect("it is there")),
                ("age:", source.find("34").expect("it is there")),
                ("name:", source.find("(?p, ?a").expect("it is there") + 1),
                ("age:", source.find("?a)").expect("it is there")),
            ]
        );
    }

    /// A declared predicate's documentation is its signature and the comment block above
    /// its declaration.
    #[test]
    fn a_declared_predicate_carries_its_documentation() {
        let document = Document::new(
            "%%% A person and their age.\n% Extra detail.\n\
             @declare person(name: string, age: int) .\nperson(\"a\", 1) .\n"
                .to_string(),
            None,
            &[],
        );
        let documentation = document.documentation("person").expect("it is declared");
        assert_eq!(
            documentation.comment.as_deref(),
            Some("A person and their age.\nExtra detail.")
        );
        assert_eq!(documentation.signature, "person(name: string, age: int)");
        assert!(document.documentation("q").is_none());
    }

    /// A value label's documentation is its `@type` declaration and the comment above it.
    #[test]
    fn a_declared_label_carries_its_documentation() {
        let document = Document::new(
            "% An angle in degrees.\n@type angle(decimal) .\nspin(angle(1.5)) .\n".to_string(),
            None,
            &[],
        );
        let documentation = document.label("angle").expect("it is declared");
        assert_eq!(documentation.signature, "@type angle(decimal)");
        assert_eq!(
            documentation.comment.as_deref(),
            Some("An angle in degrees.")
        );
    }

    /// A text that does not parse keeps the types of the document it replaces,
    /// and a text that parses has its own.
    #[test]
    fn the_types_of_a_text_that_does_not_parse_are_the_last_ones_inferred() {
        let typed = Document::new("p(1) .\nq(?x) :- p(?x) .\n".to_string(), None, &[]);
        let q = Identifier::bare("q");
        let shown = |document: &Document| {
            document
                .types()
                .predicate(&q)
                .map(|types| nemo::values::TypeSyntax::of(&types[0]).to_string())
        };
        assert_eq!(shown(&typed).as_deref(), Some("int"));
        assert_eq!(
            typed
                .types()
                .variable("q(?x) :- p(?x) .", "x")
                .map(|datatype| nemo::values::TypeSyntax::of(datatype).to_string())
                .as_deref(),
            Some("int")
        );

        // In the middle of an edit, the text does not parse, so the earlier types are kept.
        let mut broken = Document::new("p(1) .\nq(?x) :- p(?x\n".to_string(), None, &[]);
        assert_eq!(shown(&broken), None);
        broken.inherit_types(&typed);
        assert_eq!(shown(&broken).as_deref(), Some("int"));

        // A text that parses has its own.
        let mut changed = Document::new("p(\"a\") .\nq(?x) :- p(?x) .\n".to_string(), None, &[]);
        changed.inherit_types(&broken);
        assert_eq!(shown(&changed).as_deref(), Some("string"));
    }

    /// A `@use` of an open document in the same directory, a subdirectory or a sibling
    /// directory reads the open text rather than the disk.
    #[test]
    fn a_use_reads_an_open_buffer_instead_of_the_disk() {
        // Neither file exists on disk. The module is the text that the editor holds,
        // registered under the name with which the `@use` refers to it.
        let module = "@input edge .\n@output reach .\nreach(?x, ?y) :- edge(?x, ?y) .\n";
        for (path, name) in [
            ("/nowhere/app/tc.rls", "tc"),
            ("/nowhere/app/lib/tc.rls", "lib::tc"),
            ("/nowhere/lib/tc.rls", "super::lib::tc"),
        ] {
            let document = Document::new(
                format!("link(1, 2) .\n@use {name} {{ edge = link }} as t .\n"),
                Some(PathBuf::from("/nowhere/app/main.rls")),
                &[(PathBuf::from(path), module)],
            );
            assert!(
                document.diagnostics().is_empty(),
                "{name}: {:?}",
                document.diagnostics()
            );
        }
    }

    /// An open module file is named by its path relative to the using document's directory,
    /// and a file that is not an `.rls` file gets no name.
    #[test]
    fn a_buffer_is_named_relative_to_the_document() {
        let directory = Path::new("/rules/app");
        assert_eq!(
            module_name(directory, Path::new("/rules/app/graph/tc.rls")).as_deref(),
            Some("graph::tc")
        );
        assert_eq!(
            module_name(directory, Path::new("/rules/util/pairs.rls")).as_deref(),
            Some("super::util::pairs")
        );
        assert_eq!(
            module_name(directory, Path::new("/tc.rls")).as_deref(),
            Some("super::super::tc")
        );
        assert_eq!(
            module_name(directory, Path::new("/rules/app/data.csv")),
            None
        );
    }

    /// The document converts protocol positions to offsets, and spans to protocol ranges.
    #[test]
    fn positions_round_trip_through_the_document() {
        let document = Document::new("p(1) .\nq(2) .\n".to_string(), None, &[]);
        assert_eq!(document.offset(1, 0), 7);
        let span = Span {
            file: DOCUMENT,
            start: 7,
            end: 13,
        };
        assert_eq!(document.range(span), ((1, 0), (1, 6)));
    }
}
