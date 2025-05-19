//! This module defines [ASTProgramTranslation].

pub(crate) mod attribute;
pub(crate) mod basic;
pub(crate) mod complex;
pub(crate) mod directive;
pub(crate) mod fact;
pub(crate) mod literal;
pub(crate) mod rule;
mod term;

use std::{collections::HashMap, ops::Range};

use attribute::{process_attributes, KnownAttributes};
use directive::{handle_define_directive, handle_use_directive};
use nom::InputLength;

use crate::{
    parser::{
        ast::{self, ProgramAST},
        error::translate_error_tree,
        input::ParserInput,
        ParserErrorReport, ParserState,
    },
    util::bag::Bag,
};

use super::{
    components::{fact::Fact, rule::Rule, term::Term},
    error::{translation_error::TranslationError, TranslationReport},
    program::Program,
};

/// Object for handling the translation of the ast representation
/// of a nemo program into its logical representation
#[derive(Debug, Default)]
pub struct ASTProgramTranslation {
    /// Prefix mapping
    prefix_mapping: HashMap<String, (String, Range<usize>)>,
    /// Base
    base: Option<(String, Range<usize>)>,

    /// Attributes for the statement currently being translated
    statement_attributes: Bag<KnownAttributes, Vec<Term>>,

    /// Current program
    program: Program,

    /// Current error report
    report: TranslationReport,
}

impl ASTProgramTranslation {
    /// Return a reference to attributes of the current statement.
    pub(crate) fn statement_attributes(&self) -> &Bag<KnownAttributes, Vec<Term>> {
        &self.statement_attributes
    }
}

impl ASTProgramTranslation {
    /// Process attributes attached to a statement
    fn process_attributes<'a>(&mut self, statement: &ast::statement::Statement<'a>) {
        let expected_attributes = match statement.kind() {
            ast::statement::StatementKind::Rule(_) => Rule::EXPECTED_ATTRIBUTES,
            _ => &[],
        };

        if let Some(attributes) =
            process_attributes(self, statement.attributes().iter(), expected_attributes)
        {
            self.statement_attributes = attributes;
        }
    }

    /// Translate the given [ProgramAST] into a [Program].
    pub fn translate<'a>(
        mut self,
        ast: &ast::program::Program<'a>,
    ) -> Result<Program, TranslationReport> {
        // First, handle directives
        for statement in ast.statements() {
            if let ast::statement::StatementKind::Directive(directive) = statement.kind() {
                handle_define_directive(&mut self, directive);
            }
        }

        // Now handle facts and rules
        for statement in ast.statements() {
            self.process_attributes(statement);

            match statement.kind() {
                ast::statement::StatementKind::Fact(fact) => {
                    if let Some(fact) = Fact::build_component(&mut self, fact) {
                        self.program.add_fact(fact);
                    }
                }
                ast::statement::StatementKind::Rule(rule) => {
                    if let Some(rule) = Rule::build_component(&mut self, rule) {
                        self.program.add_rule(rule);
                    }
                }
                ast::statement::StatementKind::Directive(directive) => {
                    handle_use_directive(&mut self, directive);
                }
                ast::statement::StatementKind::Error(_token) => {
                    panic!(
                        "Faulty statement should result in a parser error and not be propagated."
                    )
                }
            }

            self.statement_attributes.clear();
        }

        self.report.result_value(self.program)
    }

    /// Recreate the name from a [ast::tag::structure::StructureTag]
    /// by resolving prefixes or bases.
    pub(super) fn resolve_tag<'a>(
        &mut self,
        tag: &ast::tag::structure::StructureTag<'a>,
    ) -> Option<String> {
        Some(match tag.kind() {
            ast::tag::structure::StructureTagKind::Plain(token) => {
                let token_string = token.to_string();

                if let Some((base, _)) = &self.base {
                    format!("{base}{token_string}")
                } else {
                    token_string
                }
            }
            ast::tag::structure::StructureTagKind::Prefixed { prefix, tag } => {
                if let Some((expanded_prefix, _)) = self.prefix_mapping.get(&prefix.to_string()) {
                    format!("{expanded_prefix}{tag}")
                } else {
                    self.report.add(
                        prefix,
                        TranslationError::PrefixUnknown {
                            prefix: prefix.to_string(),
                        },
                    );

                    return None;
                }
            }
            ast::tag::structure::StructureTagKind::Iri(iri) => iri.content(),
        })
    }
}

/// Errors due to either parsing a program
/// or translating the resulting AST
#[derive(Debug)]
pub enum ComponentParseReport {
    /// Error while parsing
    Parsing(ParserErrorReport),
    /// Error while translating the AST
    Translation(TranslationReport),
}

/// Trait implemented by program components
/// that can be constructed from an AST node
pub(crate) trait TranslationComponent: Sized {
    type Ast<'a>: 'a + ProgramAST<'a>;

    /// Build component from AST node.
    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        ast: &Self::Ast<'a>,
    ) -> Option<Self>;

    /// Construct this object from a string.
    fn parse(input: &str) -> Result<Self, ComponentParseReport> {
        let parser_input = ParserInput::new(input, ParserState::default());

        let (tail, ast) = Self::Ast::parse(parser_input).map_err(|error| {
            ComponentParseReport::Parsing(ParserErrorReport::from(translate_error_tree(&error)))
        })?;

        if tail.input_len() != 0 {
            panic!("parsing should always succeed");
        }

        let mut translation = ASTProgramTranslation::default();

        match Self::build_component(&mut translation, &ast) {
            Some(component) => Ok(component),
            None => Err(ComponentParseReport::Translation(translation.report)),
        }
    }
}

/// Implements conversions commonly used when defining newtype wrappers
#[macro_export]
macro_rules! newtype_wrapper {
    ($wrapper:ty: $inner:ty) => {
        impl From<$wrapper> for $inner {
            fn from(value: $wrapper) -> $inner {
                value.0
            }
        }

        impl $wrapper {
            pub fn into_inner(self) -> $inner {
                self.0
            }
        }
    };
}
