//! The directive grammar: a dispatch over the directive kinds, one parser per kind.

mod import;
mod types;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{
        BaseDirective, DeclareDirective, DefineDirective, Directive, ExternDirective,
        InputDirective, OutputDirective, ParameterDirective, PrefixDirective, TypeDirective,
        UseDirective, Variable, VariableKind,
    },
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses one directive, through its terminating ` .`.
    pub(super) fn directive(&mut self) -> Result<Directive, Diagnostic> {
        let start = self.peek_span();
        let token = self.expect(&TokenKind::At, "a directive")?;
        let mut directive = match self.marked_text(&token) {
            "base" => Directive::Base(self.base_directive()?),
            "prefix" => Directive::Prefix(self.prefix_directive()?),
            "import" => Directive::Import(self.import_directive()?),
            "export" => Directive::Export(self.export_directive()?),
            "output" => Directive::Output(self.output_directive()?),
            "declare" => Directive::Declare(self.declare_directive()?),
            "type" => Directive::Type(self.type_directive()?),
            "parameter" => Directive::Parameter(self.parameter_directive()?),
            "input" => Directive::Input(self.input_directive()?),
            "use" => Directive::Use(self.use_directive()?),
            "define" => Directive::Define(self.define_directive()?),
            "extern" => Directive::Extern(self.extern_directive()?),
            unknown => {
                return Err(ParseError::UnknownDirective {
                    name: unknown.to_owned(),
                }
                .at(Some(token.span)));
            }
        };
        self.expect(&TokenKind::Dot, "`.` at the end of the directive")?;
        directive.set_span(Some(self.span_from(start)));
        Ok(directive)
    }

    /// Parses `@base <iri>`.
    fn base_directive(&mut self) -> Result<BaseDirective, Diagnostic> {
        let iri = self.expect(&TokenKind::Iri, "the base IRI in angle brackets")?;
        Ok(BaseDirective {
            iri: self.iri_text(&iri).to_owned(),
            span: None,
        })
    }

    /// Parses `@prefix name: <iri>`.
    ///
    /// `@prefix : <iri>` declares the empty prefix.
    fn prefix_directive(&mut self) -> Result<PrefixDirective, Diagnostic> {
        let prefix = match self.eat(&TokenKind::Ident) {
            Some(prefix) => {
                let text = self.text(&prefix).to_owned();
                self.expect(&TokenKind::Colon, "`:` after the prefix")?;
                text
            }
            None => {
                self.expect(&TokenKind::Colon, "the declared prefix")?;
                String::new()
            }
        };
        let iri = self.expect(&TokenKind::Iri, "the expanded IRI in angle brackets")?;
        Ok(PrefixDirective {
            prefix,
            iri: self.iri_text(&iri).to_owned(),
            span: None,
        })
    }

    /// Parses `@output predicate, …`.
    fn output_directive(&mut self) -> Result<OutputDirective, Diagnostic> {
        Ok(OutputDirective {
            predicates: self.separated(Self::identifier)?,
            span: None,
        })
    }

    /// Parses `@declare predicate(column, …)`.
    fn declare_directive(&mut self) -> Result<DeclareDirective, Diagnostic> {
        let predicate = self.identifier()?;
        self.expect(&TokenKind::ParenOpen, "`(` opening the column declarations")?;
        let columns = self.columns("`)` closing the column declarations")?;
        Ok(DeclareDirective {
            predicate,
            columns,
            span: None,
        })
    }

    /// Parses `@type`, which declares the argument types of a constructor,
    /// written as the type of its values: `@type point(double, double) .`.
    fn type_directive(&mut self) -> Result<TypeDirective, Diagnostic> {
        Ok(TypeDirective {
            definition: self.type_expression()?,
            span: None,
        })
    }

    /// Parses `@parameter $name`, with an optional `: type` and
    /// `= value`.
    fn parameter_directive(&mut self) -> Result<ParameterDirective, Diagnostic> {
        let name =
            self.expect_variable(VariableKind::Global, "a global variable like `$threshold`")?;
        let datatype = self.after(&TokenKind::Colon, Self::type_expression)?;
        let value = self.after(&TokenKind::Equal, Self::term)?;
        Ok(ParameterDirective {
            variable: Variable::global(name),
            datatype,
            value,
            span: None,
        })
    }

    /// Parses `@input predicate`, with optional column declarations.
    fn input_directive(&mut self) -> Result<InputDirective, Diagnostic> {
        let predicate = self.identifier()?;
        let columns = self.after(&TokenKind::ParenOpen, |parser| {
            parser.columns("`)` closing the input's columns")
        })?;
        Ok(InputDirective {
            predicate,
            columns,
            span: None,
        })
    }

    /// Parses `@use lib::tc { input = actual, $x = value, … } as alias`.
    fn use_directive(&mut self) -> Result<UseDirective, Diagnostic> {
        let module = self.module_name()?;
        let mut bindings = Vec::new();
        let mut values = Vec::new();
        if self.eat(&TokenKind::BraceOpen).is_some() {
            self.list(
                &TokenKind::BraceClose,
                "`}` closing the bindings",
                |parser| {
                    // The `$` tells a value binding from an input binding.
                    if let Some(name) = parser.eat_variable(VariableKind::Global) {
                        parser.expect(&TokenKind::Equal, "`=` binding the parameter")?;
                        values.push((name, parser.term()?));
                    } else {
                        let input = parser.identifier()?;
                        parser.expect(&TokenKind::Equal, "`=` binding the input")?;
                        bindings.push((input, parser.identifier()?));
                    }
                    Ok(())
                },
            )?;
        }
        self.expect_word("as", "`as` before the module alias", || {
            ParseError::ExpectedAlias
        })?;
        let alias = self.name("the module alias")?;
        Ok(UseDirective {
            module,
            bindings,
            values,
            alias,
            span: None,
        })
    }

    /// Parses `@define name(?parameter, …) = expression`.
    fn define_directive(&mut self) -> Result<DefineDirective, Diagnostic> {
        let name = self.identifier()?;
        self.expect(&TokenKind::ParenOpen, "`(` opening the parameter list")?;
        let parameters = self.list(
            &TokenKind::ParenClose,
            "`)` closing the parameter list",
            |parser| parser.variable("a parameter variable"),
        )?;
        self.expect(&TokenKind::Equal, "`=` before the defining expression")?;
        let body = self.term()?;
        Ok(DefineDirective {
            name,
            parameters,
            body,
            span: None,
        })
    }

    /// Parses `@extern name(type, …) -> type`.
    fn extern_directive(&mut self) -> Result<ExternDirective, Diagnostic> {
        let name = self.identifier()?;
        self.expect(&TokenKind::ParenOpen, "`(` opening the parameter types")?;
        let parameters = self.list(
            &TokenKind::ParenClose,
            "`)` closing the parameter types",
            Self::type_expression,
        )?;
        self.expect(&TokenKind::ArrowRight, "`->` before the result type")?;
        let result = self.type_expression()?;
        Ok(ExternDirective {
            name,
            parameters,
            result,
            span: None,
        })
    }
}
