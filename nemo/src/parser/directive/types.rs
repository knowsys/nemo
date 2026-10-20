//! Type expressions and the column declarations made of them.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{BaseType, ColumnDeclaration, Identifier, TypeExpression, TypeLabel},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses column declarations up to and including the closing `)`,
    /// `closing` naming it for the message.
    pub(super) fn columns(&mut self, closing: &str) -> Result<Vec<ColumnDeclaration>, Diagnostic> {
        self.list(&TokenKind::ParenClose, closing, Self::column)
    }

    /// Parses one column declaration: an optional `name:` and a type.
    fn column(&mut self) -> Result<ColumnDeclaration, Diagnostic> {
        let name = if self.at(&TokenKind::Ident) && self.peek_second() == &TokenKind::Colon {
            let token = self.advance();
            self.advance();
            Some(self.text(&token).to_owned())
        } else {
            None
        };
        Ok(ColumnDeclaration {
            name,
            datatype: self.type_expression()?,
        })
    }

    /// Parses a type expression: one type, or a union of types separated by `|`.
    pub(super) fn type_expression(&mut self) -> Result<TypeExpression, Diagnostic> {
        let first = self.type_member()?;
        if !self.at(&TokenKind::Bar) {
            return Ok(first);
        }
        let mut members = vec![first];
        while self.eat(&TokenKind::Bar).is_some() {
            members.push(self.type_member()?);
        }
        Ok(TypeExpression::Union(members))
    }

    /// Parses one type of a union: a base type, a tuple type (unlabeled,
    /// labeled, or `_`-labeled), or a map type (likewise).
    fn type_member(&mut self) -> Result<TypeExpression, Diagnostic> {
        if let Some(shaped) = self.shaped_type(TypeLabel::Unlabeled)? {
            return Ok(shaped);
        }

        let token = self.expect(&TokenKind::Ident, "a type")?;
        let name = self.text(&token);
        // `_` states a shape without constraining the label.
        let label = if name == "_" {
            TypeLabel::Anonymous
        } else {
            TypeLabel::Named(Identifier::bare(name))
        };
        if let Some(shaped) = self.shaped_type(label)? {
            return Ok(shaped);
        }

        Ok(match BaseType::from_name(name) {
            Some(base) => TypeExpression::Base(base),
            // Any other name refers to a type declared with `@type`.
            // Translation checks that the declaration exists.
            None => TypeExpression::Named(Identifier::bare(name)),
        })
    }

    /// Parses a tuple type's fields in parentheses or a map type's fields in braces,
    /// under the given label, if either opens next.
    fn shaped_type(&mut self, label: TypeLabel) -> Result<Option<TypeExpression>, Diagnostic> {
        if self.eat(&TokenKind::ParenOpen).is_some() {
            let fields = self.list(
                &TokenKind::ParenClose,
                "`)` closing the tuple type",
                Self::type_expression,
            )?;
            return Ok(Some(TypeExpression::Tuple { label, fields }));
        }
        if self.eat(&TokenKind::BraceOpen).is_some() {
            let fields = self.list(
                &TokenKind::BraceClose,
                "`}` closing the map type",
                |parser| {
                    let name = parser.name("the map field's name")?;
                    parser.expect(&TokenKind::Colon, "`:` after the field name")?;
                    Ok((name, parser.type_expression()?))
                },
            )?;
            return Ok(Some(TypeExpression::Map { label, fields }));
        }
        Ok(None)
    }
}
