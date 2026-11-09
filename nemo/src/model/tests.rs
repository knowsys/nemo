//! Tests of the model: display in Nemo syntax, equality, and the names of builtins.

use super::*;

/// A bare name keeps its percent-encoding when it is parsed, and is displayed bare again.
#[test]
fn percent_encoded_bare_names_render_bare() {
    let term = Term::parse("Hello%20World").expect("parses");
    assert_eq!(term.to_string(), "Hello%20World");
    assert_eq!(Term::parse(&term.to_string()).expect("re-parses"), term);
}

/// A name with `-` between its parts is displayed bare,
/// and a name that the lexer would split at a `-` is displayed in angle brackets.
#[test]
fn hyphenated_names_render_bare_where_they_read_back() {
    for name in ["my-lib", "item-1", "a-_b"] {
        let term = Term::constant(name);
        assert_eq!(term.to_string(), name);
        assert_eq!(Term::parse(name).expect("re-parses"), term);
    }
    for name in ["a-", "a--b", "a-%20", "-a"] {
        assert_eq!(Term::constant(name).to_string(), format!("<{name}>"));
    }
}

/// A predicate whose name would be parsed as something else before a `(`
/// is displayed in angle brackets, both in a program and in a traced fact.
#[test]
fn predicates_named_like_builtins_render_in_brackets() {
    for name in ["strlen", "STRLEN", "_", "true", "http://example.org/edge"] {
        let atom = Atom::new(Identifier::bare(name), vec![Term::integer(1)]);
        assert_eq!(atom.to_string(), format!("<{name}>(1)"));
        let fact = crate::trace::fact_text(name, &["1".to_owned()]);
        assert_eq!(fact, format!("<{name}>(1)"));
    }
    assert_eq!(
        Atom::new(Identifier::bare("edge"), Vec::<Term>::new()).to_string(),
        "edge()"
    );
}

/// The special floating-point values are displayed as XSD writes them,
/// in typed literals that can be read back.
#[test]
fn special_doubles_render_as_typed_literals() {
    assert_eq!(
        Term::double(f64::INFINITY).to_string(),
        "\"INF\"^^<http://www.w3.org/2001/XMLSchema#double>"
    );
    assert_eq!(
        Term::double(f64::NAN).to_string(),
        "\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>"
    );
}

/// Returns a universal variable term.
fn var(name: &str) -> Term {
    Term::variable(Variable::universal(name))
}

/// Returns an atom with a bare predicate name.
fn atom(predicate: &str, terms: Vec<Term>) -> Atom {
    Atom::new(Identifier::bare(predicate), terms)
}

/// Terms and rules that differ only in their spans are equal.
#[test]
fn equality_ignores_spans() {
    let spanned = Term {
        kind: Term::integer(42).kind,
        span: Some(Span {
            file: FileId::new(0),
            start: 3,
            end: 5,
        }),
    };
    assert_eq!(spanned, Term::integer(42));

    let rule = Rule::new(
        vec![atom("p", vec![var("x")])],
        vec![Literal::Positive(atom("q", vec![var("x")]))],
    );
    let mut spanned_rule = rule.clone();
    spanned_rule.span = Some(Span {
        file: FileId::new(1),
        start: 0,
        end: 20,
    });
    assert_eq!(rule, spanned_rule);
}

/// A rule is displayed in Nemo syntax, with each attribute on a line before it.
#[test]
fn rules_display_as_nemo_syntax() {
    let mut rule = Rule::new(
        vec![atom("adult", vec![var("p")])],
        vec![
            Literal::Positive(atom("person", vec![var("p"), var("age")])),
            Literal::Negative(atom("minor", vec![var("p")])),
            Literal::Operation(Operation::new(
                Operator::GreaterEq,
                vec![var("age"), Term::integer(18)],
            )),
        ],
    );
    assert_eq!(
        rule.to_string(),
        "adult(?p) :- person(?p, ?age), ~minor(?p), ?age >= 18 ."
    );

    rule.attributes.push(Attribute {
        name: Identifier::bare("name"),
        arguments: vec![Term::string("adults")],
        span: None,
    });
    assert_eq!(
        rule.to_string(),
        "#[name(\"adults\")]\nadult(?p) :- person(?p, ?age), ~minor(?p), ?age >= 18 ."
    );
}

/// Each kind of term is displayed in Nemo syntax.
#[test]
fn terms_display_every_constant_form() {
    let cases: Vec<(Term, &str)> = vec![
        (Term::constant("alice"), "alice"),
        (
            Term::new(TermKind::Constant(Identifier::Iri(String::from(
                "https://example.org/a",
            )))),
            "<https://example.org/a>",
        ),
        (
            Term::new(TermKind::Constant(Identifier::Prefixed {
                prefix: String::from("foaf"),
                name: String::from("Person"),
            })),
            "foaf:Person",
        ),
        (
            Term::new(TermKind::Constant(Identifier::Qualified {
                module: String::from("geo"),
                name: String::from("area"),
            })),
            "geo::area",
        ),
        (Term::string("say \"hi\""), "\"say \\\"hi\\\"\""),
        (
            Term::new(TermKind::LanguageString {
                text: String::from("hallo"),
                language: String::from("de"),
            }),
            "\"hallo\"@de",
        ),
        (Term::new(TermKind::Boolean(true)), "true"),
        (Term::integer(-7), "-7"),
        (Term::double(2.5), "2.5d"),
        (Term::double(9.0), "9d"),
        (Term::float(1.5), "1.5f"),
        (
            Term::new(TermKind::RdfLiteral {
                lexical: String::from("P1Y"),
                datatype: Identifier::Prefixed {
                    prefix: String::from("xsd"),
                    name: String::from("duration"),
                },
            }),
            "\"P1Y\"^^xsd:duration",
        ),
        (
            Term::new(TermKind::Tuple(vec![Term::integer(1), Term::integer(2)])),
            "(1, 2)",
        ),
        (
            Term::new(TermKind::Map {
                label: None,
                pairs: vec![(Term::constant("port"), Term::integer(80))],
            }),
            "{port = 80}",
        ),
        (
            Term::new(TermKind::Map {
                label: Some(Identifier::bare("config")),
                pairs: vec![(Term::constant("port"), Term::integer(80))],
            }),
            "config{port = 80}",
        ),
        (
            Term::new(TermKind::Function {
                name: Identifier::bare("pair"),
                arguments: vec![var("x"), var("y")],
                labels: None,
            }),
            "pair(?x, ?y)",
        ),
    ];
    for (term, expected) in cases {
        assert_eq!(term.to_string(), expected);
    }
}

/// A nested infix operation is put in parentheses where precedence requires it,
/// and a call is displayed with the builtin's name.
#[test]
fn operations_parenthesize_nested_infix() {
    let sum = Term::new(TermKind::Operation(Operation::new(
        Operator::Add,
        vec![var("x"), Term::integer(1)],
    )));
    let product = Operation::new(Operator::Multiply, vec![sum, var("y")]);
    assert_eq!(product.to_string(), "(?x + 1) * ?y");

    let call = Operation::new(Operator::StringLength, vec![var("s")]);
    assert_eq!(call.to_string(), "STRLEN(?s)");
}

/// An aggregate is displayed with its scope after `per`, if it has one.
#[test]
fn aggregates_display_their_scope() {
    let scoped = Aggregate::new(AggregateKind::Sum, var("v")).per(vec![Variable::universal("g")]);
    assert_eq!(scoped.to_string(), "#sum(?v per ?g)");

    let value = Aggregate::new(AggregateKind::Count, var("v"));
    assert_eq!(value.to_string(), "#count(?v)");

    assert_eq!(Aggregate::matches().to_string(), "#count()");
    assert_eq!(
        Aggregate::new(AggregateKind::Average, var("v")).to_string(),
        "#avg(?v)"
    );
}

/// [Term::apply] gives an operation only for the name of a builtin.
#[test]
fn applications_resolve_known_builtins_only() {
    assert!(matches!(
        Term::apply("STRLEN", [var("s")]).kind,
        TermKind::Operation(_)
    ));
    assert!(matches!(
        Term::apply("FROBNICATE", [var("s")]).kind,
        TermKind::Function { .. }
    ));
    assert_eq!(Operator::from_name("SUM"), Some(Operator::Add));
}

/// Each kind of directive is displayed in Nemo syntax.
#[test]
fn directives_display_as_nemo_syntax() {
    let cases: Vec<(Directive, &str)> = vec![
        (
            Directive::Base(BaseDirective {
                iri: String::from("https://example.org/"),
                span: None,
            }),
            "@base <https://example.org/> .",
        ),
        (
            Directive::Prefix(PrefixDirective {
                prefix: String::from("xsd"),
                iri: String::from("http://www.w3.org/2001/XMLSchema#"),
                span: None,
            }),
            "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        ),
        (
            Directive::Import(ImportDirective::new(
                "edge",
                "csv",
                vec![(Term::constant("resource"), Term::string("edge.csv"))],
            )),
            "@import edge from csv { resource = \"edge.csv\" } .",
        ),
        (
            Directive::Import(ImportDirective {
                variables: vec![String::from("x"), String::from("y")],
                bindings: vec![atom("b", vec![var("x")]), atom("c", vec![var("y")])],
                ..ImportDirective::new(
                    "q",
                    "sparql",
                    vec![(Term::constant("endpoint"), Term::string("https://e/sparql"))],
                )
            }),
            "@import q(?x, ?y) from sparql { endpoint = \"https://e/sparql\" } where b(?x), c(?y) .",
        ),
        (
            Directive::Output(OutputDirective {
                predicates: vec![Identifier::bare("reach")],
                span: None,
            }),
            "@output reach .",
        ),
        (
            Directive::Declare(DeclareDirective {
                predicate: Identifier::bare("city"),
                columns: vec![
                    ColumnDeclaration {
                        name: Some(String::from("name")),
                        datatype: TypeExpression::Base(BaseType::String),
                    },
                    ColumnDeclaration {
                        name: Some(String::from("location")),
                        datatype: TypeExpression::Tuple {
                            label: TypeLabel::Named(Identifier::bare("point")),
                            fields: vec![
                                TypeExpression::Base(BaseType::Double),
                                TypeExpression::Base(BaseType::Double),
                            ],
                        },
                    },
                ],
                span: None,
            }),
            "@declare city(name: string, location: point(double, double)) .",
        ),
        (
            Directive::Parameter(ParameterDirective {
                variable: Variable::global("threshold"),
                datatype: Some(TypeExpression::Base(BaseType::Int)),
                value: Some(Term::integer(5)),
                span: None,
            }),
            "@parameter $threshold: int = 5 .",
        ),
        (
            Directive::Input(InputDirective {
                predicate: Identifier::bare("edge"),
                columns: Some(vec![
                    ColumnDeclaration {
                        name: Some(String::from("source")),
                        datatype: TypeExpression::Base(BaseType::Any),
                    },
                    ColumnDeclaration {
                        name: None,
                        datatype: TypeExpression::Base(BaseType::Any),
                    },
                ]),
                span: None,
            }),
            "@input edge(source: any, any) .",
        ),
        (
            Directive::Use(UseDirective {
                module: ModuleName {
                    parents: 1,
                    segments: vec![String::from("lib"), String::from("tc")],
                },
                bindings: vec![(Identifier::bare("edge"), Identifier::bare("link"))],
                values: vec![(String::from("limit"), Term::integer(3))],
                alias: String::from("t"),
                span: None,
            }),
            "@use super::lib::tc { edge = link, $limit = 3 } as t .",
        ),
        (
            Directive::Define(DefineDirective {
                name: Identifier::bare("is_adult"),
                parameters: vec![Variable::universal("p")],
                body: Term::new(TermKind::Operation(Operation::new(
                    Operator::GreaterEq,
                    vec![var("p"), Term::integer(18)],
                ))),
                span: None,
            }),
            "@define is_adult(?p) = ?p >= 18 .",
        ),
        (
            Directive::Extern(ExternDirective {
                name: Identifier::bare("geodistance"),
                parameters: vec![
                    TypeExpression::Base(BaseType::Any),
                    TypeExpression::Base(BaseType::Any),
                ],
                result: TypeExpression::Base(BaseType::Double),
                span: None,
            }),
            "@extern geodistance(any, any) -> double .",
        ),
    ];
    for (directive, expected) in cases {
        assert_eq!(directive.to_string(), expected);
    }
}

/// A program is displayed with one statement per line.
#[test]
fn programs_display_line_per_statement() {
    let program = Program::new(vec![
        Statement::Fact(Fact::new(
            Identifier::bare("edge"),
            vec![Term::integer(1), Term::integer(2)],
        )),
        Statement::Rule(Rule::new(
            vec![atom("reach", vec![var("x"), var("y")])],
            vec![Literal::Positive(atom("edge", vec![var("x"), var("y")]))],
        )),
    ]);
    assert_eq!(
        program.to_string(),
        "edge(1, 2) .\nreach(?x, ?y) :- edge(?x, ?y) ."
    );
}

/// All NaN doubles are the same term, while `0.0` and `-0.0` are different terms.
#[test]
fn doubles_canonicalize_nan() {
    assert_eq!(Term::double(f64::NAN), Term::double(-f64::NAN));
    assert_ne!(Term::double(0.0), Term::double(-0.0));
}
