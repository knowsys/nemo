//! Parsing type expressions, and the column declarations that contain them.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{BaseType, ColumnDeclaration, Identifier, TypeExpression, TypeLabel},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses column declarations through the closing `)`.
    ///
    /// `closing` describes the `)` in the error if it is missing.
    pub(super) fn columns(&mut self, closing: &str) -> Result<Vec<ColumnDeclaration>, Diagnostic> {
        self.list(&TokenKind::ParenClose, closing, Self::column)
    }

    /// Parses one column declaration, which is a type with an optional `name:` before it.
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

    /// Parses one member of a union of types.
    ///
    /// A member is a base type, the name of a type declared with `@type`,
    /// or a tuple or map type.
    /// A tuple or map type has no label, a label as in `point(…)`,
    /// or the label `_`, as in `_(…)`, which allows any label.
    fn type_member(&mut self) -> Result<TypeExpression, Diagnostic> {
        if let Some(shaped) = self.shaped_type(TypeLabel::Unlabeled)? {
            return Ok(shaped);
        }

        let token = self.expect(&TokenKind::Ident, "a type")?;
        let name = self.text(&token);
        // `_` gives a shape without restricting the label.
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

    /// Parses the fields of a tuple type in parentheses, or of a map type in braces,
    /// with the given label.
    ///
    /// Returns `None` if neither `(` nor `{` is next.
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
