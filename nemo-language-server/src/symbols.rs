//! What a program says about itself, indexed by position.
//!
//! Every navigation question
//! (what is under the cursor, where is it derived, what does this file contain)
//! is a lookup in one index built once per parse, never a new walk of the program.

use nemo::model::{
    AggregateKind, Directive, Identifier, Literal, Operation, Operator, Program, Rule, Span,
    Statement, Term, TermKind, Variable, Walk,
};

/// Whether an occurrence of a predicate derives it or reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Use {
    /// A rule head, a fact, or an import: the predicate takes rows here.
    Derives,
    /// A rule body, or a directive that names it without giving it
    /// anything: the predicate is read here.
    Reads,
}

/// Where an occurrence that derives a predicate gives it rows from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Origin {
    /// A fact.
    Fact,
    /// A rule head.
    Rule,
    /// An import, in the named format.
    Import(String),
    /// An `@input`.
    Input,
}

/// One occurrence of a predicate name in the source.
#[derive(Debug, Clone)]
pub(crate) struct Occurrence {
    /// The predicate as it is written.
    pub name: String,
    /// The predicate's identifier, which resolves against the program's prefixes.
    pub identifier: Identifier,
    /// The arity it has here, if the atom or directive it is in states one.
    ///
    /// An `@import` names a predicate without saying how wide it is.
    pub arity: Option<usize>,
    /// What the occurrence does with it.
    pub used: Use,
    /// Where the rows come from, for an occurrence that derives the predicate.
    pub origin: Option<Origin>,
    /// The span to point at: the atom, or the directive that names it.
    pub span: Span,
}

/// Something inside a statement, other than a predicate, that a hover describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Mention {
    /// A variable of a rule, with the rule's span.
    Variable(Variable, Span),
    /// A builtin operator, by its name or its infix symbol.
    Builtin(Operator),
    /// An aggregate, by its name.
    Aggregate(AggregateKind),
    /// The label of a tuple or a map value, as it is written.
    Label(String),
}

/// A mention and the span of its name.
#[derive(Debug, Clone)]
pub(crate) struct Mentioned {
    /// What is mentioned.
    pub mention: Mention,
    /// The span of the name, or of the variable.
    pub span: Span,
}

/// What a statement contributes to a file's outline.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    /// The name to call it by, not a rendering of the whole thing.
    pub name: String,
    /// The rest of what it says, if there is more to say.
    pub detail: Option<String>,
    /// What it is.
    pub kind: Kind,
    /// Its whole span.
    pub span: Span,
}

/// The kind of a statement, as an outline shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A fact.
    Fact,
    /// A rule.
    Rule,
    /// A directive.
    Directive,
}

/// One rule and the variables it names, which a completion inside it can offer,
/// since a variable means nothing outside the rule that binds it.
#[derive(Debug, Clone)]
pub(crate) struct Scope {
    /// The rule's span.
    pub span: Span,
    /// The variables it names, as they are written, without repeats.
    pub variables: Vec<String>,
}

/// The index of one parsed program.
#[derive(Debug, Default)]
pub(crate) struct Symbols {
    /// Every predicate occurrence, in source order.
    occurrences: Vec<Occurrence>,
    /// The statements, in source order.
    outline: Vec<Entry>,
    /// The rules, with the variables each names.
    scopes: Vec<Scope>,
    /// The variables, builtins, aggregates and labels the statements mention, in source order.
    mentions: Vec<Mentioned>,
    /// The predicates an `@output` or an `@export` names, as they are written.
    outputs: Vec<String>,
}

impl Symbols {
    /// Indexes a parsed program against the text it was parsed from.
    pub(crate) fn of(program: &Program, text: &str) -> Self {
        let mut symbols = Self::default();
        for statement in &program.statements {
            symbols.statement(statement, text);
        }
        symbols
    }

    /// Returns the occurrence a byte offset sits in,
    /// the narrowest, so that a name inside a wider one would still find the name.
    pub(crate) fn at(&self, offset: u32) -> Option<&Occurrence> {
        self.occurrences
            .iter()
            .filter(|occurrence| occurrence.span.start <= offset && offset < occurrence.span.end)
            .min_by_key(|occurrence| occurrence.span.end.saturating_sub(occurrence.span.start))
    }

    /// Returns the mention whose name a byte offset sits in.
    pub(crate) fn mention_at(&self, offset: u32) -> Option<&Mentioned> {
        self.mentions
            .iter()
            .find(|mentioned| mentioned.span.start <= offset && offset < mentioned.span.end)
    }

    /// Returns whether the program writes a predicate out:
    /// it is named by an `@output` or an `@export`,
    /// or the program names none and rules derive the predicate.
    pub(crate) fn is_output(&self, name: &str) -> bool {
        if self.outputs.is_empty() {
            self.definitions(name)
                .any(|occurrence| occurrence.origin == Some(Origin::Rule))
        } else {
            self.outputs.iter().any(|output| output == name)
        }
    }

    /// Returns every occurrence of a predicate.
    pub(crate) fn occurrences<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Occurrence> {
        self.occurrences
            .iter()
            .filter(move |occurrence| occurrence.name == name)
    }

    /// Returns where a predicate is derived.
    pub(crate) fn definitions<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Occurrence> {
        self.occurrences(name)
            .filter(|occurrence| occurrence.used == Use::Derives)
    }

    /// Returns the statements of the file, in source order.
    pub(crate) fn outline(&self) -> &[Entry] {
        &self.outline
    }

    /// Returns the rule a byte offset is in, if it is in one.
    pub(crate) fn scope(&self, offset: u32) -> Option<&Scope> {
        self.scopes
            .iter()
            .find(|scope| scope.span.start <= offset && offset < scope.span.end)
    }

    /// Returns every predicate the file names, in source order, without repeats,
    /// each with an arity if one is stated.
    pub(crate) fn predicates(&self) -> Vec<(&str, Option<usize>)> {
        let mut seen: Vec<(&str, Option<usize>)> = Vec::new();
        for occurrence in &self.occurrences {
            match seen
                .iter_mut()
                .find(|(name, _)| *name == occurrence.name.as_str())
            {
                Some((_, arity)) => *arity = arity.or(occurrence.arity),
                None => seen.push((occurrence.name.as_str(), occurrence.arity)),
            }
        }
        seen
    }

    /// Indexes one statement.
    fn statement(&mut self, statement: &Statement, text: &str) {
        match statement {
            Statement::Fact(fact) => {
                // A fact's span runs through its terminating ` .`, but
                // the occurrence the cursor finds is the atom.
                let atom = fact.span.map(|span| trimmed(text, span));
                self.record(
                    &fact.predicate,
                    Some(fact.terms.len()),
                    Some(Origin::Fact),
                    atom,
                );
                for term in &fact.terms {
                    self.term(term, text, None);
                }
                self.entry(fact.predicate.to_string(), None, Kind::Fact, fact.span);
            }
            Statement::Rule(rule) => {
                for head in &rule.head {
                    self.record(
                        &head.predicate,
                        Some(head.terms.len()),
                        Some(Origin::Rule),
                        head.span,
                    );
                    for term in &head.terms {
                        self.term(term, text, rule.span);
                    }
                }
                for literal in &rule.body {
                    match literal {
                        Literal::Positive(atom) | Literal::Negative(atom) => {
                            self.record(&atom.predicate, Some(atom.terms.len()), None, atom.span);
                            for term in &atom.terms {
                                self.term(term, text, rule.span);
                            }
                        }
                        Literal::Operation(operation) => {
                            self.operation(operation, text, rule.span);
                        }
                    }
                }
                // A rule is named by what it derives, which is what a
                // reader looks for in an outline.
                let derived: Vec<String> = rule
                    .head
                    .iter()
                    .map(|head| head.predicate.to_string())
                    .collect();
                self.entry(derived.join(", "), None, Kind::Rule, rule.span);
                if let Some(span) = rule.span {
                    self.scopes.push(Scope {
                        span,
                        variables: variables(rule),
                    });
                }
            }
            Statement::Directive(directive) => self.directive(directive, text),
        }
    }

    /// Indexes one directive: the predicates it names, and what it is called in the outline.
    ///
    /// A directive names a predicate as surely as an atom does,
    /// and an `@import` is the source of an imported predicate's rows,
    /// so it *derives* it, and a reader asking where the rows are made lands there.
    fn directive(&mut self, directive: &Directive, text: &str) {
        let span = directive.span();
        // Each predicate a directive names is found where its name is written,
        // so that a directive naming several answers for each by its own name.
        let mut from = span.map_or(0, |span| span.start);
        let mut written = |name: &Identifier| written_name(text, span, &mut from, name);
        let name = match directive {
            Directive::Import(import) => {
                let origin = Origin::Import(import.format.to_string());
                self.record(
                    &import.predicate,
                    None,
                    Some(origin),
                    written(&import.predicate),
                );
                // A bound import reads its binding predicates, as a
                // rule body would.
                for atom in &import.bindings {
                    self.record(&atom.predicate, Some(atom.terms.len()), None, atom.span);
                }
                for condition in &import.conditions {
                    self.operation(condition, text, None);
                }
                import.predicate.to_string()
            }
            Directive::Export(export) => {
                self.record(&export.predicate, None, None, written(&export.predicate));
                self.outputs.push(export.predicate.to_string());
                export.predicate.to_string()
            }
            Directive::Output(output) => {
                for predicate in &output.predicates {
                    self.record(predicate, None, None, written(predicate));
                    self.outputs.push(predicate.to_string());
                }
                names(&output.predicates)
            }
            Directive::Declare(declare) => {
                self.record(
                    &declare.predicate,
                    Some(declare.columns.len()),
                    None,
                    written(&declare.predicate),
                );
                declare.predicate.to_string()
            }
            // A `@type` declaration names a value label, not a predicate.
            Directive::Type(declaration) => declaration.definition.to_string(),
            // An input is where the predicate's rows arrive from, like
            // an import.
            Directive::Input(input) => {
                self.record(
                    &input.predicate,
                    input.columns.as_ref().map(Vec::len),
                    Some(Origin::Input),
                    written(&input.predicate),
                );
                input.predicate.to_string()
            }
            Directive::Use(directive) => {
                // The actuals are the using program's predicates; the
                // formals are the module's, and name nothing here.
                for (_, actual) in &directive.bindings {
                    self.record(actual, None, None, written(actual));
                }
                directive.alias.clone()
            }
            Directive::Define(define) => define.name.to_string(),
            Directive::Extern(external) => external.name.to_string(),
            Directive::Parameter(parameter) => format!("${}", parameter.variable.name),
            Directive::Prefix(prefix) => format!("{}:", prefix.prefix),
            Directive::Base(_) => String::from("@base"),
        };
        self.entry(name, Some(directive.to_string()), Kind::Directive, span);
    }

    /// Records one predicate occurrence, if it has a span to record:
    /// one that derives the predicate if it has an origin, and one that reads it otherwise.
    fn record(
        &mut self,
        predicate: &Identifier,
        arity: Option<usize>,
        origin: Option<Origin>,
        span: Option<Span>,
    ) {
        let Some(span) = span else {
            return;
        };
        self.occurrences.push(Occurrence {
            name: predicate.to_string(),
            identifier: predicate.clone(),
            arity,
            used: if origin.is_some() {
                Use::Derives
            } else {
                Use::Reads
            },
            origin,
            span,
        });
    }

    /// Records what a term and its subterms mention:
    /// variables (inside a rule, whose span is `rule`), builtins, aggregates and labels.
    fn term(&mut self, term: &Term, text: &str, rule: Option<Span>) {
        match &term.kind {
            TermKind::Variable(variable) => {
                if let (Some(span), Some(rule)) = (term.span, rule) {
                    self.mention(Mention::Variable(variable.clone(), rule), span);
                }
            }
            TermKind::Operation(operation) => {
                self.operation(operation, text, rule);
                return;
            }
            TermKind::Aggregate(aggregate) => {
                let name = format!("#{}", aggregate.kind.name());
                if let Some(span) = term.span.and_then(|span| name_at(text, span, &name)) {
                    self.mention(Mention::Aggregate(aggregate.kind), span);
                }
            }
            TermKind::Function { name, .. }
            | TermKind::Map {
                label: Some(name), ..
            } => {
                let written = name.to_string();
                if let Some(span) = term.span.and_then(|span| name_at(text, span, &written)) {
                    self.mention(Mention::Label(written), span);
                }
            }
            _ => {}
        }
        for subterm in term.subterms() {
            self.term(subterm, text, rule);
        }
    }

    /// Records a builtin application, by its name or its infix symbol,
    /// and what its arguments mention.
    fn operation(&mut self, operation: &Operation, text: &str, rule: Option<Span>) {
        if let Some(span) = operation
            .span
            .and_then(|span| operator_at(text, span, operation))
        {
            self.mention(Mention::Builtin(operation.operator), span);
        }
        for argument in &operation.arguments {
            self.term(argument, text, rule);
        }
    }

    /// Records one mention.
    fn mention(&mut self, mention: Mention, span: Span) {
        self.mentions.push(Mentioned { mention, span });
    }

    /// Records one outline entry, if it has a span to record.
    fn entry(&mut self, name: String, detail: Option<String>, kind: Kind, span: Option<Span>) {
        let Some(span) = span else {
            return;
        };
        self.outline.push(Entry {
            name,
            detail,
            kind,
            span,
        });
    }
}

/// Returns a span with the statement terminator trimmed back off its end.
fn trimmed(text: &str, span: Span) -> Span {
    let start = usize::try_from(span.start).unwrap_or(0);
    let mut end = usize::try_from(span.end).unwrap_or(0).min(text.len());
    let bytes = text.as_bytes();
    while end > start && matches!(bytes[end - 1], b'.' | b' ' | b'\t' | b'\r' | b'\n') {
        end -= 1;
    }
    Span {
        end: u32::try_from(end).unwrap_or(span.end),
        ..span
    }
}

/// Returns the span of a predicate's name where a directive writes it, at or after `from`,
/// moving `from` past it,
/// or the directive's span if the name is not written there as a whole name.
fn written_name(
    text: &str,
    directive: Option<Span>,
    from: &mut u32,
    name: &Identifier,
) -> Option<Span> {
    let directive = directive?;
    let written = name.to_string();
    let end = usize::try_from(directive.end).ok()?.min(text.len());
    let mut start = usize::try_from(*from).ok()?;
    while let Some(found) = text.get(start..end)?.find(&written) {
        let at = start + found;
        let after = at + written.len();
        if is_whole_name(text, at, after) {
            *from = u32::try_from(after).ok()?;
            return Some(Span {
                start: u32::try_from(at).ok()?,
                end: *from,
                ..directive
            });
        }
        start = at + 1;
    }
    Some(directive)
}

/// Returns whether the text between two offsets is a whole name:
/// no name continues it on either side, and no marker such as the `@` of a directive
/// introduces it.
///
/// A dot right after it ends the statement, unless a name character follows the dot.
fn is_whole_name(text: &str, at: usize, after: usize) -> bool {
    let bytes = text.as_bytes();
    let is_name = |byte: u8| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b':') || byte >= 0x80
    };
    let open = at == 0 || {
        let before = bytes[at - 1];
        !is_name(before) && !matches!(before, b'.' | b'@' | b'?' | b'!' | b'$' | b'#')
    };
    let closed = match bytes.get(after) {
        None => true,
        Some(b'.') => !bytes.get(after + 1).is_some_and(|&byte| is_name(byte)),
        Some(&byte) => !is_name(byte),
    };
    open && closed
}

/// Returns the span of a name written at the start of a span, or `None` if it is not there.
fn name_at(text: &str, span: Span, name: &str) -> Option<Span> {
    let start = usize::try_from(span.start).ok()?;
    let written = text.get(start..start + name.len())?;
    let length = u32::try_from(name.len()).ok()?;
    written.eq_ignore_ascii_case(name).then_some(Span {
        end: span.start + length,
        ..span
    })
}

/// Returns the span of the name or infix symbol an operation is written with,
/// or `None` if it is written another way, such as `?t[0]` for `GET`.
fn operator_at(text: &str, span: Span, operation: &Operation) -> Option<Span> {
    let start = usize::try_from(span.start).ok()?;
    let rest = text.get(start..)?;
    // A named call: one of the builtin's spellings, before the parenthesis.
    let length = rest
        .find(|character: char| !(character.is_alphanumeric() || character == '_'))
        .unwrap_or(rest.len());
    if length > 0 && Operator::from_name(&rest[..length]) == Some(operation.operator) {
        return name_at(text, span, &rest[..length]);
    }
    // An infix operator: the symbol between its operands.
    let symbol = operation.operator.infix()?;
    let [left, right] = operation.arguments.as_slice() else {
        return None;
    };
    let (after, before) = (left.span?.end, right.span?.start);
    let between = text.get(usize::try_from(after).ok()?..usize::try_from(before).ok()?)?;
    if between.trim() != symbol {
        return None;
    }
    let offset = u32::try_from(between.find(symbol)?).ok()?;
    let length = u32::try_from(symbol.len()).ok()?;
    Some(Span {
        start: after + offset,
        end: after + offset + length,
        ..span
    })
}

/// Returns the variables a rule names, as they are written, in the order they
/// first appear.
fn variables(rule: &Rule) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    for variable in rule.variables() {
        let written = variable.to_string();
        // A nameless placeholder names nothing to offer.
        if written != "_" && !found.contains(&written) {
            found.push(written);
        }
    }
    found
}

/// Returns several names as one outline label.
fn names(identifiers: &[Identifier]) -> String {
    identifiers
        .iter()
        .map(Identifier::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    use nemo::model::FileId;

    /// The symbols of a parsed program.
    fn symbols(source: &str) -> Symbols {
        let (program, diagnostics) = Program::parse(source, FileId::new(0));
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        Symbols::of(&program, source)
    }

    /// The occurrence at a substring's first character.
    fn at<'a>(symbols: &'a Symbols, source: &str, needle: &str) -> &'a Occurrence {
        let offset = source.find(needle).expect("the source holds it");
        symbols
            .at(u32::try_from(offset).expect("the source is small"))
            .expect("an occurrence is here")
    }

    #[test]
    fn a_predicate_is_found_where_it_is_written() {
        let source = "edge(1, 2) .\nreach(?x, ?y) :- edge(?x, ?y) .\n";
        let symbols = symbols(source);

        // Inside the body atom `edge(?x, ?y)` of the rule.
        let offset = source.rfind("edge").expect("the body mentions it");
        let found = symbols
            .at(u32::try_from(offset).expect("the source is small"))
            .expect("an occurrence is here");
        assert_eq!(found.name, "edge");
        assert_eq!(found.used, Use::Reads);
        assert_eq!(found.arity, Some(2));

        assert_eq!(symbols.occurrences("edge").count(), 2);
        assert_eq!(
            symbols.definitions("edge").count(),
            1,
            "the fact derives it"
        );
    }

    #[test]
    fn an_imported_predicate_is_derived_by_its_import() {
        let source =
            "@import edge from csv { resource = \"e.csv\" } .\nreach(?x) :- edge(?x, 1) .\n";
        let symbols = symbols(source);
        // Where the rows come from is where the import is written.
        assert_eq!(symbols.definitions("edge").count(), 1);
        assert_eq!(symbols.occurrences("edge").count(), 2);
        // And the cursor on the import's name for it finds it.
        let found = at(&symbols, source, "edge from");
        assert_eq!(found.name, "edge");
        assert_eq!(found.used, Use::Derives);
    }

    #[test]
    fn each_name_of_a_directive_is_found_where_it_is_written() {
        let source = "a(1) . b(1) . count(1) .\n@output a, b, count.\n";
        let symbols = symbols(source);
        let directive = source.find("@output").expect("the source holds it");
        for name in ["a", "b", "count"] {
            let offset = directive + source[directive..].find(name).expect("it is named");
            let found = symbols
                .at(u32::try_from(offset).expect("the source is small"))
                .expect("an occurrence is here");
            assert_eq!(found.name, name);
            assert_eq!(
                &source[found.span.start as usize..found.span.end as usize],
                name,
                "the occurrence is the name, a dot after it ending the statement"
            );
        }
    }

    #[test]
    fn a_directive_that_only_names_a_predicate_reads_it() {
        let symbols = symbols("p(1) .\n@output p .\n@export p to csv {} .\n@declare p(int) .\n");
        assert_eq!(symbols.definitions("p").count(), 1, "only the fact derives");
        assert_eq!(symbols.occurrences("p").count(), 4);
        // The fact states the width first.
        assert_eq!(symbols.predicates(), vec![("p", Some(1))]);
    }

    #[test]
    fn a_facts_occurrence_ends_where_its_atom_does() {
        let source = "edge(1, 2) .\n";
        let symbols = symbols(source);
        let found = at(&symbols, source, "edge");
        assert_eq!(
            found.span.end as usize,
            source.find(" .").expect("it is terminated")
        );
        // The terminator belongs to the statement, not to the atom.
        let terminator = u32::try_from(source.find('.').expect("it is terminated"))
            .expect("the source is small");
        assert!(symbols.at(terminator).is_none());
    }

    #[test]
    fn the_head_of_a_rule_is_found_before_its_body() {
        let source = "p(?x) :- q(?x) .\n";
        let symbols = symbols(source);
        let found = symbols.at(0).expect("the head is here");
        assert_eq!(found.name, "p");
        assert_eq!(found.used, Use::Derives);
        let body = at(&symbols, source, "q(");
        assert_eq!(body.used, Use::Reads);
    }

    #[test]
    fn a_negated_atom_reads_its_predicate() {
        let symbols = symbols("p(1) . q(2) .\nr(?x) :- p(?x), ~q(?x) .\n");
        assert_eq!(symbols.occurrences("q").count(), 2);
        assert_eq!(
            symbols.definitions("q").count(),
            1,
            "only the fact derives it"
        );
    }

    #[test]
    fn builtins_are_found_by_their_name_or_their_symbol() {
        let source = "q(?y, #count(?x)) :- p(?x), ?y = strlen(?x)+1 .
";
        let symbols = symbols(source);
        let mentioned = |needle: &str| {
            let offset = source.find(needle).expect("the source holds it");
            let mentioned = symbols
                .mention_at(u32::try_from(offset).expect("the source is small"))
                .expect("a mention is here");
            let span = mentioned.span.start as usize..mentioned.span.end as usize;
            (mentioned.mention.clone(), &source[span])
        };
        // Names are case-insensitive.
        assert_eq!(
            mentioned("strlen"),
            (Mention::Builtin(Operator::StringLength), "strlen")
        );
        assert_eq!(mentioned("+"), (Mention::Builtin(Operator::Add), "+"));
        assert_eq!(mentioned("= "), (Mention::Builtin(Operator::Equal), "="));
        assert_eq!(
            mentioned("#count"),
            (Mention::Aggregate(AggregateKind::Count), "#count")
        );
        let (variable, written) = mentioned("?y =");
        assert!(matches!(variable, Mention::Variable(..)), "{variable:?}");
        assert_eq!(written, "?y");
    }

    #[test]
    fn the_outline_lists_every_statement_by_name() {
        let symbols = symbols("@output p .\np(1) .\np(?x) :- q(?x) .\n");
        let kinds: Vec<Kind> = symbols.outline().iter().map(|entry| entry.kind).collect();
        assert_eq!(kinds, vec![Kind::Directive, Kind::Fact, Kind::Rule]);
        assert_eq!(
            symbols.outline()[0].name,
            "p",
            "a directive is named by what it is about"
        );
        assert_eq!(
            symbols.outline()[0].detail.as_deref(),
            Some("@output p ."),
            "the rest of it is detail, not the name"
        );
        assert_eq!(
            symbols.outline()[2].name,
            "p",
            "a rule is named by its head"
        );
    }
}
