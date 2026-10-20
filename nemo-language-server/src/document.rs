//! One open document:
//! its text, what parsing and checking made of it, and the index that answers questions
//! about it.

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

/// The file id a document's spans carry.
///
/// A document is parsed on its own, so its spans only ever address its
/// own text and one id serves every document. Composition across files
/// is the engine's business, and carries the engine's ids.
const DOCUMENT: FileId = FileId::new(0);

/// One open document, parsed and checked.
///
/// Parsing and checking happen once per edit, here,
/// and everything the server responds with afterwards is a lookup,
/// which keeps a keystroke from costing a parse per request.
#[derive(Debug)]
pub(crate) struct Document {
    /// The text as the editor has it.
    text: String,
    /// Where its lines begin.
    lines: Lines,
    /// What the parser and the engine's check reported about it.
    diagnostics: Vec<Diagnostic>,
    /// What it says about itself.
    symbols: Symbols,
    /// What parsing made of it, kept because formatting prints it back.
    program: Program,
    /// The column names the editor shows inline, by offset.
    hints: Vec<ColumnHint>,
    /// What the document says each declared predicate is, by name.
    documentation: HashMap<String, Documentation>,
    /// What the document says each value label a `@type` declares is, by label.
    labels: HashMap<String, Documentation>,
    /// The comment blocks directly above the statements that derive each predicate, by name.
    comments: HashMap<String, Vec<String>>,
    /// The types the last check that saw the whole text inferred.
    types: Arc<KnownTypes>,
    /// Whether this text's own check inferred the types,
    /// rather than an earlier text's, kept because this one does not parse.
    fresh: bool,
}

/// What a declaration says about a predicate or a value label:
/// the declaration itself, and the comment block written above it.
#[derive(Debug)]
pub(crate) struct Documentation {
    /// The declared name with its columns or fields, as `person(name: string, age: int)`.
    pub signature: String,
    /// The comment lines directly above it, markers stripped.
    pub comment: Option<String>,
}

/// The types a check inferred, as the hover reads them.
#[derive(Debug, Default)]
pub(crate) struct KnownTypes {
    /// What the check inferred, which resolves predicate names.
    inferred: Option<InferredTypes>,
    /// The variable types of each rule, by the rule's text,
    /// which outlives edits elsewhere in the document.
    rules: HashMap<String, Vec<(String, Type)>>,
}

impl KnownTypes {
    /// Returns the types of a predicate's columns, if they are known.
    pub(crate) fn predicate(&self, name: &Identifier) -> Option<&[Type]> {
        self.inferred.as_ref()?.predicate(name)
    }

    /// Returns the type of a variable of the rule written as `rule`, by the variable's name
    /// without the `?`, if it is known.
    pub(crate) fn variable(&self, rule: &str, name: &str) -> Option<&Type> {
        self.rules
            .get(rule)?
            .iter()
            .find(|(variable, _)| variable == name)
            .map(|(_, datatype)| datatype)
    }
}

/// One inline column name: what the editor shows before an argument
/// whose position a declaration names.
#[derive(Debug)]
pub(crate) struct ColumnHint {
    /// The byte offset the hint sits at, the argument's start.
    pub offset: u32,
    /// What is shown, as `name:`.
    pub label: String,
}

impl Document {
    /// Parses a text into a document and checks it,
    /// with no data read:
    /// the parser's diagnostics first,
    /// then the engine's (resolution, directives, rule compilation, stratification).
    ///
    /// `path` is the document's location, against which its modules resolve.
    /// Without one it is checked as a program of its own.
    /// `open` are the other documents the editor holds, by path:
    /// a `@use` of one of them reads that text, not the disk's.
    pub(crate) fn new(text: String, path: Option<PathBuf>, open: &[(PathBuf, &str)]) -> Self {
        let lines = Lines::of(&text);
        let (program, mut diagnostics) = Program::parse(&text, DOCUMENT);
        let symbols = Symbols::of(&program, &text);

        let parsed = !diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity() == Severity::Error);

        // The check runs on an engine of this edit's own, so a bug that
        // panics in it takes down that engine and nothing else. The
        // document registers first, so its id is [DOCUMENT] and the
        // check's spans point back into this text.
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
            // The other buffers, under the names a `@use` in the document writes for them.
            // One that no name reaches, or too large to address, stays unregistered,
            // and a `@use` reads the disk.
            if let Some(directory) = path.as_deref().and_then(Path::parent) {
                for (other, buffer) in open {
                    if let Some(name) = module_name(directory, other) {
                        let _ = engine.add_module(&name, *buffer);
                    }
                }
            }
            // Linking reads the modules, resolved beside the document's path,
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

        // Types inferred from a text that does not parse leave out what the
        // broken statements say, so such a text keeps an earlier text's.
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

    /// Takes over the types of the document this one replaces,
    /// if its own check inferred none from the whole text.
    pub(crate) fn inherit_types(&mut self, previous: &Document) {
        if !self.fresh {
            self.types = Arc::clone(&previous.types);
        }
    }

    /// Returns the types the last check that saw the whole text inferred.
    pub(crate) fn types(&self) -> &KnownTypes {
        &self.types
    }

    /// Returns the statements parsing made of it.
    pub(crate) fn statements(&self) -> &[Statement] {
        &self.program.statements
    }

    /// Returns the whole text.
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    /// Returns what the parser reported.
    pub(crate) fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    /// Returns what the document says about itself.
    pub(crate) fn symbols(&self) -> &Symbols {
        &self.symbols
    }

    /// Returns the column names the editor shows inline, in offset order.
    pub(crate) fn hints(&self) -> &[ColumnHint] {
        &self.hints
    }

    /// Returns what the document says a declared predicate is, if it does.
    pub(crate) fn documentation(&self, name: &str) -> Option<&Documentation> {
        self.documentation.get(name)
    }

    /// Returns what the document says a value label a `@type` declares is, if it does.
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

    /// Returns the byte offset a protocol position addresses.
    pub(crate) fn offset(&self, line: u32, character: u32) -> u32 {
        u32::try_from(self.lines.offset(&self.text, line, character)).unwrap_or(u32::MAX)
    }

    /// Returns the protocol positions a span begins and ends at,
    /// or the start of the file for a span that addresses a different one.
    ///
    /// Nothing points into another file yet.
    /// When something does, a module's diagnostic labelling the file it came from,
    /// pointing at this document's text would be silently wrong rather than absent.
    pub(crate) fn range(&self, span: Span) -> ((u32, u32), (u32, u32)) {
        if span.file != DOCUMENT {
            return ((0, 0), (0, 0));
        }
        (
            self.lines.position(&self.text, span.start as usize),
            self.lines.position(&self.text, span.end as usize),
        )
    }

    /// Returns whether a span addresses this document at all.
    pub(crate) fn holds(&self, span: Span) -> bool {
        span.file == DOCUMENT
    }

    /// Returns the run of name characters immediately before a byte offset,
    /// with the marker that introduces it:
    /// what the editor has typed so far, and what decides what it could mean.
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

    /// Returns the span of an occurrence's *name*, as against the atom or directive that holds
    /// it.
    ///
    /// The index points at what the cursor can land on,
    /// while renaming and highlighting need the name itself.
    /// It is found by looking for it as a whole name inside what holds it,
    /// or `None` if it cannot be found, so that an edit never lands at a guessed position.
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

    /// Returns the text a span covers, for showing it back.
    pub(crate) fn slice(&self, span: Span) -> &str {
        if span.file != DOCUMENT {
            return "";
        }
        self.text
            .get(span.start as usize..span.end as usize)
            .unwrap_or("")
    }
}

/// Returns what a check reported, or for a check that panicked,
/// the internal error reporting the bug.
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

/// Returns what each declared predicate and each declared value label is:
/// its `@declare` (or its `@input` with columns), or its `@type`,
/// and the comment block written directly above it,
/// the place a meaning is documented.
///
/// A predicate's `@declare` counts over its `@input`.
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

/// Returns the comment blocks directly above the statements that derive each predicate:
/// its facts, the rules with it in the head, its imports and its `@input`,
/// in source order, a block repeated by several statements once.
///
/// A comment separated from a statement by a blank line belongs to no statement.
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

/// Returns the contiguous `%` comment lines directly above an offset, markers
/// and surrounding whitespace stripped, in source order.
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

/// Returns the inline column names of a program:
/// for every positional atom of a predicate whose declaration names its columns,
/// the name each argument fills.
///
/// An atom that names its own columns needs none.
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

/// Returns the name a `@use` in a file in `directory` writes for the module file `file`,
/// or `None` for a file that is no `.rls` file, or that no module name reaches.
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

    #[test]
    fn a_document_finds_occurrences_through_its_index() {
        let source = "edge(1, 2) .\nreach(?x) :- edge(?x, 1) .\n";
        let document = Document::new(source.to_string(), None, &[]);
        assert!(document.diagnostics().is_empty());

        // The cursor on the body's `edge`.
        let offset = source.rfind("edge").expect("the body mentions it");
        let found = document
            .symbols()
            .at(u32::try_from(offset).expect("the source is small"))
            .expect("an occurrence is here");
        assert_eq!(found.name, "edge");
        assert_eq!(document.slice(found.span), "edge(?x, 1)");
    }

    #[test]
    fn a_broken_document_still_indexes_what_parsed() {
        let document = Document::new("p(1) .\nq(?x) :- p(?x\n".to_string(), None, &[]);
        assert!(
            !document.diagnostics().is_empty(),
            "the tail does not parse"
        );
        // The fact before the damage is still there to navigate to.
        assert_eq!(document.symbols().occurrences("p").count(), 1);
    }

    #[test]
    fn a_semantic_problem_is_found_without_reading_data() {
        // `?y` is never bound, which only the check knows, and the
        // import's missing file goes unnoticed, because nothing reads it.
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

    #[test]
    fn a_type_warning_reaches_the_document() {
        use nemo_physical::diagnostics::Severity;

        // `?x` joins a string column with an integer column, so the type
        // analysis proves the rule dead, as a warning.
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
        // The fact and the positional atom get hints; the atom naming
        // its own columns needs none.
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

        // Mid-edit, the text does not parse: the types stay.
        let mut broken = Document::new("p(1) .\nq(?x) :- p(?x\n".to_string(), None, &[]);
        assert_eq!(shown(&broken), None);
        broken.inherit_types(&typed);
        assert_eq!(shown(&broken).as_deref(), Some("int"));

        // A text that parses has its own.
        let mut changed = Document::new("p(\"a\") .\nq(?x) :- p(?x) .\n".to_string(), None, &[]);
        changed.inherit_types(&broken);
        assert_eq!(shown(&changed).as_deref(), Some("string"));
    }

    #[test]
    fn a_use_reads_an_open_buffer_instead_of_the_disk() {
        // Neither file exists on disk: the module is what the editor
        // holds, registered under the name the `@use` writes.
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
