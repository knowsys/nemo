//! This module defines [Rule] and [RuleBuilder]

use std::{collections::HashSet, fmt::Display, hash::Hash};

use similar_string::find_best_similarity;

use crate::rule_model::{
    error::{hint::Hint, validation_error::ValidationErrorKind, ValidationErrorBuilder},
    origin::Origin,
};

use super::{
    atom::Atom,
    literal::Literal,
    term::{
        operation::Operation,
        primitive::{variable::Variable, Primitive},
        Term,
    },
    IterableVariables, ProgramComponent,
};

/// Rule
///
/// A logical statement that defines a relationship between a head (conjunction of [Atom]s)
/// and a body (conjunction of [Literal]s).
/// It specifies how new facts can be inferred from existing ones.
#[derive(Debug, Clone, Eq)]
pub struct Rule {
    /// Origin of this component
    origin: Origin,

    /// Name of the rule
    name: Option<String>,

    /// Head of the rule
    head: Vec<Atom>,
    /// Body of the rule
    body: Vec<Literal>,
}

impl Rule {
    /// Return a [RuleBuilder].
    pub fn builder() -> RuleBuilder {
        RuleBuilder::default()
    }

    /// Create a new [Rule].
    pub fn new(head: Vec<Atom>, body: Vec<Literal>) -> Self {
        Self {
            origin: Origin::Created,
            name: None,
            head,
            body,
        }
    }

    /// Set the name of the rule.
    pub fn set_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Return a reference to the body of the rule.
    pub fn body(&self) -> &Vec<Literal> {
        &self.body
    }

    /// Return a mutable reference to the body of the rule.
    pub fn body_mut(&mut self) -> &mut Vec<Literal> {
        &mut self.body
    }

    /// Return a reference to the head of the rule.
    pub fn head(&self) -> &Vec<Atom> {
        &self.head
    }

    /// Return a mutable reference to the head of the rule.
    pub fn head_mut(&mut self) -> &mut Vec<Atom> {
        &mut self.head
    }

    /// Return a list of "safe" variables.
    ///
    /// A variable is considered safe,
    /// if it occurs in a positive body atom,
    /// or is derived via the equality operation
    /// from other safe variables.
    pub fn safe_variables(&self) -> HashSet<&Variable> {
        let mut result = HashSet::new();

        for literal in &self.body {
            if let Literal::Positive(atom) = literal {
                for term in atom.subterms() {
                    if let Term::Primitive(Primitive::Variable(variable)) = term {
                        result.insert(variable);
                    }
                }
            }
        }

        loop {
            let current_count = result.len();

            for literal in &self.body {
                if let Literal::Operation(operation) = literal {
                    if let Some((variable, term)) = operation.variable_assignment() {
                        if term.variables().all(|variable| result.contains(variable)) {
                            result.insert(variable);
                        }
                    }
                }
            }

            if result.len() == current_count {
                break;
            }
        }

        result
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (head_index, head_atom) in self.head.iter().enumerate() {
            write!(f, "{}", head_atom)?;

            if head_index < self.head.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(" :- ")?;

        for (body_index, body_literal) in self.body.iter().enumerate() {
            write!(f, "{}", body_literal)?;

            if body_index < self.body.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(" .")
    }
}

impl PartialEq for Rule {
    fn eq(&self, other: &Self) -> bool {
        self.head == other.head && self.body == other.body
    }
}

impl Hash for Rule {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.head.hash(state);
        self.body.hash(state);
    }
}

impl ProgramComponent for Rule {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        let safe_variables = self.safe_variables();

        for atom in &self.head {
            for term in atom.subterms() {
                if let Term::Primitive(Primitive::Variable(head_variable)) = term {
                    if !safe_variables.contains(head_variable) {
                        let head_variable_name = head_variable
                            .name()
                            .expect("anonymous variables not allowed in the head");

                        let info = builder.report_error(
                            head_variable.origin().clone(),
                            ValidationErrorKind::HeadUnsafe(head_variable.clone()),
                        );

                        if let Some(closest_option) = find_best_similarity(
                            head_variable_name.clone(),
                            &safe_variables
                                .iter()
                                .filter_map(|variable| variable.name())
                                .collect::<Vec<_>>(),
                        ) {
                            if head_variable_name.len() > 2
                                && closest_option.0.len() > 2
                                && closest_option.1 > 0.75
                            {
                                info.add_hint(Hint::SimilarExists {
                                    kind: "variable".to_string(),
                                    name: closest_option.0,
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Builder for a rule
#[derive(Debug, Default)]
pub struct RuleBuilder {
    /// Origin of the rule
    origin: Origin,

    /// Name of the rule
    name: Option<String>,

    /// Head of the rule
    head: Vec<Atom>,
    /// Body of the rule
    body: Vec<Literal>,
}

impl RuleBuilder {
    /// Set the name of the built rule.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set the [Origin] of the built rule.
    pub fn origin(mut self, origin: Origin) -> Self {
        self.origin = origin;
        self
    }

    /// Add a positive atom to the body of the rule.
    pub fn add_body_positive(mut self, atom: Atom) -> Self {
        self.body.push(Literal::Positive(atom));
        self
    }

    /// Add a positive atom to the body of the rule.
    pub fn add_body_positive_mut(&mut self, atom: Atom) -> &mut Self {
        self.body.push(Literal::Positive(atom));
        self
    }

    /// Add a negative atom to the body of the rule.
    pub fn add_body_negative(mut self, atom: Atom) -> Self {
        self.body.push(Literal::Negative(atom));
        self
    }

    /// Add a negative atom to the body of the rule.
    pub fn add_body_negative_mut(&mut self, atom: Atom) -> &mut Self {
        self.body.push(Literal::Negative(atom));
        self
    }

    /// Add an operation to the body of the rule.
    pub fn add_body_operation(mut self, opreation: Operation) -> Self {
        self.body.push(Literal::Operation(opreation));
        self
    }

    /// Add an operation to the body of the rule.
    pub fn add_body_operation_mut(&mut self, opreation: Operation) -> &mut Self {
        self.body.push(Literal::Operation(opreation));
        self
    }

    /// Add a literal to the body of the rule.
    pub fn add_body_literal(mut self, literal: Literal) -> Self {
        self.body.push(literal);
        self
    }

    /// Add a literal to the body of the rule.
    pub fn add_body_literal_mut(&mut self, literal: Literal) -> &mut Self {
        self.body.push(literal);
        self
    }

    /// Add an atom to the head of the rule.
    pub fn add_head_atom(mut self, atom: Atom) -> Self {
        self.head.push(atom);
        self
    }

    /// Add an atom to the head of the rule.
    pub fn add_head_atom_mut(&mut self, atom: Atom) -> &mut Self {
        self.head.push(atom);
        self
    }

    /// Finish building and return a [Rule].
    pub fn finalize(self) -> Rule {
        let rule = Rule::new(self.head, self.body).set_origin(self.origin);

        match &self.name {
            Some(name) => rule.set_name(name),
            None => rule,
        }
    }
}
