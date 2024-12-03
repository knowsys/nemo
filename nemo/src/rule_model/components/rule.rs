//! This module defines [Rule] and [RuleBuilder].

use std::{collections::HashSet, fmt::Display, hash::Hash};

use nemo_physical::datavalues::DataValue;

use crate::{
    parse_component,
    parser::ast::ProgramAST,
    rule_model::{
        error::{
            hint::Hint, info::Info, validation_error::ValidationErrorKind, ComplexErrorLabelKind,
            ValidationErrorBuilder,
        },
        origin::Origin,
        substitution::Substitution,
        translation::ASTProgramTranslation,
    },
};

use super::{
    atom::Atom,
    literal::Literal,
    parse::ComponentParseError,
    term::{
        operation::Operation,
        primitive::{variable::Variable, Primitive},
        Term,
    },
    IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
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
    /// How an instantiated version of this rule should be displayed
    display: Option<Term>,

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
            display: None,
            head,
            body,
        }
    }

    /// Set the name of the rule.
    pub fn set_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set how an instantiated version of the rule should be displayed.
    pub fn set_display(mut self, display: Term) -> Self {
        self.display = Some(display);
        self
    }

    /// Return a string representation of the rule instantiated with the given [Substitution].
    /// This will either return
    ///     * The content of the display attribute for this rule
    ///     * The name of the rule
    ///     * a canonical string representation of the rule (i.e. [Display] representation)
    /// whichever is the first defined in this list.
    pub fn display_instantiated(&self, substitution: &Substitution) -> String {
        if let Some(mut display) = self.display.clone() {
            substitution.apply(&mut display);
            if let Term::Primitive(Primitive::Ground(ground)) = display.reduce() {
                if let Some(result) = ground.value().to_plain_string() {
                    return result;
                }
            }
        }

        if let Some(name) = &self.name {
            return name.clone();
        }

        let mut rule_instantiated = self.clone();
        substitution.apply(&mut rule_instantiated);

        rule_instantiated.to_string()
    }

    /// Return a reference to the body of the rule.
    pub fn body(&self) -> &Vec<Literal> {
        &self.body
    }

    /// Return an iterator over the positive literals in the body of this rule.
    pub fn body_positive(&self) -> impl Iterator<Item = &Atom> {
        self.body.iter().filter_map(|literal| match literal {
            Literal::Positive(atom) => Some(atom),
            Literal::Negative(_) | Literal::Operation(_) => None,
        })
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

    /// Return the set of variables that are bound in positive body atoms.
    pub fn positive_variables(&self) -> HashSet<&Variable> {
        let mut result = HashSet::new();

        for literal in &self.body {
            if let Literal::Positive(atom) = literal {
                for term in atom.arguments() {
                    if let Term::Primitive(Primitive::Variable(variable)) = term {
                        if variable.is_universal() && variable.name().is_some() {
                            result.insert(variable);
                        }
                    }
                }
            }
        }

        result
    }

    /// Return a set of "safe" variables.
    ///
    /// A variable is considered safe,
    /// if it occurs in a positive body atom,
    /// or is derived via the equality operation
    /// from other safe variables.
    pub fn safe_variables(&self) -> HashSet<&Variable> {
        let mut result = self.positive_variables();

        loop {
            let current_count = result.len();

            for literal in &self.body {
                if let Literal::Operation(operation) = literal {
                    if let Some((variable, term)) = operation.variable_assignment() {
                        if variable.is_universal()
                            && variable.name().is_some()
                            && term.variables().all(|variable| result.contains(variable))
                        {
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

    /// Check if
    ///     * are no complex terms occurring in the head
    ///     * an aggregate occurs at most once
    ///     * there is no aggregation over a group-by variable
    fn validate_term_head(
        builder: &mut ValidationErrorBuilder,
        term: &Term,
        group_by_variable: &HashSet<&Variable>,
    ) -> Option<bool> {
        if term.is_map() || term.is_tuple() || term.is_function() {
            builder
                .report_error(*term.origin(), ValidationErrorKind::UnsupportedComplexTerm)
                .add_hint_option(Self::hint_term_operation(term));
            return None;
        }

        let mut first_aggregate = if let Term::Aggregate(aggregate) = term {
            if let Term::Primitive(Primitive::Variable(aggregate_variable)) =
                aggregate.aggregate_term()
            {
                if group_by_variable.contains(aggregate_variable) {
                    builder.report_error(
                        *aggregate.aggregate_term().origin(),
                        ValidationErrorKind::AggregateOverGroupByVariable {
                            variable: aggregate_variable.name().unwrap_or_default(),
                        },
                    );
                    return None;
                }
            }

            true
        } else {
            false
        };

        for subterm in term.arguments() {
            let contains_aggregate = Self::validate_term_head(builder, subterm, group_by_variable)?;

            if contains_aggregate && first_aggregate {
                builder.report_error(
                    *subterm.origin(),
                    ValidationErrorKind::UnsupportedAggregateMultiple,
                );

                return None;
            }

            first_aggregate |= contains_aggregate;
        }

        Some(first_aggregate)
    }

    /// Check if
    ///     * body does not contain any existential variables
    ///     * body does not contain aggregation
    ///     * body does not contain any complex term
    ///     * used operations do not use anonymous variables
    ///     * operations only use safe variables
    fn validate_term_body(
        builder: &mut ValidationErrorBuilder,
        term: &Term,
        safe_variables: &HashSet<&Variable>,
    ) -> Option<()> {
        if let Term::Primitive(Primitive::Variable(Variable::Existential(existential))) = term {
            builder.report_error(
                *existential.origin(),
                ValidationErrorKind::BodyExistential(Variable::Existential(existential.clone())),
            );
            return None;
        }

        if term.is_aggregate() {
            builder.report_error(*term.origin(), ValidationErrorKind::BodyAggregate);
            return None;
        }

        if term.is_operation() {
            for operation_variable in term.variables() {
                if operation_variable.name().is_none() {
                    builder.report_error(
                        *operation_variable.origin(),
                        ValidationErrorKind::OperationAnonymous,
                    );
                    return None;
                }

                if !safe_variables.contains(operation_variable) {
                    builder.report_error(
                        *operation_variable.origin(),
                        ValidationErrorKind::OperationUnsafe(operation_variable.clone()),
                    );
                    return None;
                }
            }
        }

        if term.is_map() || term.is_tuple() || term.is_function() {
            builder
                .report_error(*term.origin(), ValidationErrorKind::UnsupportedComplexTerm)
                .add_hint_option(Self::hint_term_operation(term));
            return None;
        }

        for subterm in term.arguments() {
            Self::validate_term_body(builder, subterm, safe_variables)?;
        }

        Some(())
    }

    /// If the given [Term] is a function term,
    /// then this function returns a [Hint] returning the operation
    /// with the closest name to its tag.
    fn hint_term_operation(term: &Term) -> Option<Hint> {
        if let Term::FunctionTerm(function) = term {
            Hint::similar_operation(function.tag().to_string())
        } else {
            None
        }
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
    fn parse(string: &str) -> Result<Self, ComponentParseError>
    where
        Self: Sized,
    {
        parse_component!(
            string,
            crate::parser::ast::rule::Rule::parse,
            ASTProgramTranslation::build_rule
        )
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        if self.body_positive().next().is_none() {
            builder.report_error(
                self.origin,
                ValidationErrorKind::UnsupportedNoPositiveLiterals,
            );
            return None;
        }

        let safe_variables = self.safe_variables();
        let is_existential = self
            .head()
            .iter()
            .flat_map(|atom| atom.variables())
            .any(|variable| variable.is_existential());

        if let Some(display) = &self.display {
            display.validate(builder)?;

            for variable in display.variables() {
                if !safe_variables.contains(variable) {
                    builder.report_error(
                        *variable.origin(),
                        ValidationErrorKind::AttributeRuleUnsafe {
                            variable: variable.to_string(),
                        },
                    );
                    return None;
                }
            }
        }

        for atom in self.head() {
            atom.validate(builder)?;

            for variable in atom.variables() {
                if let Some(variable_name) = variable.name() {
                    if !variable.is_existential() && !safe_variables.contains(variable) {
                        builder
                            .report_error(
                                *variable.origin(),
                                ValidationErrorKind::HeadUnsafe(variable.clone()),
                            )
                            .add_hint_option(Hint::similar(
                                "variable",
                                variable_name,
                                safe_variables.iter().flat_map(|variable| variable.name()),
                            ));

                        return None;
                    }
                } else {
                    builder.report_error(*variable.origin(), ValidationErrorKind::HeadAnonymous);
                    return None;
                }
            }

            let group_by_variables = atom
                .arguments()
                .flat_map(|term| {
                    if let Term::Primitive(Primitive::Variable(variable)) = term {
                        Some(variable)
                    } else {
                        None
                    }
                })
                .collect::<HashSet<_>>();

            let mut contains_aggregate = false;
            for term in atom.arguments() {
                if let Some(aggregate) =
                    Self::validate_term_head(builder, term, &group_by_variables)
                {
                    if aggregate && contains_aggregate {
                        builder.report_error(
                            *term.origin(),
                            ValidationErrorKind::UnsupportedAggregateMultiple,
                        );
                    }

                    if aggregate && is_existential {
                        builder.report_error(
                            *term.origin(),
                            ValidationErrorKind::UnsupportedAggregatesAndExistentials,
                        );
                    }

                    contains_aggregate |= aggregate;
                }
            }
        }

        let mut negative_variables = HashSet::<&Variable>::new();

        for literal in self.body() {
            literal.validate(builder)?;

            for term in literal.arguments() {
                let _ = Self::validate_term_body(builder, term, &safe_variables);
            }

            let mut current_negative_variables = HashSet::<&Variable>::new();
            if let Literal::Negative(negative) = literal {
                for negative_subterm in negative.arguments() {
                    if let Term::Primitive(Primitive::Variable(variable)) = negative_subterm {
                        if !safe_variables.contains(variable) {
                            current_negative_variables.insert(variable);
                        }
                    }
                }
            }

            for repeated_variable in current_negative_variables.intersection(&negative_variables) {
                let first_use = negative_variables
                    .get(repeated_variable)
                    .expect("value is contained in the intersection");
                let repeated_use = current_negative_variables
                    .get(repeated_variable)
                    .expect("value is contained in the intersection");

                builder
                    .report_error(
                        *repeated_use.origin(),
                        ValidationErrorKind::MultipleNegativeLiteralsUnsafe(
                            (*repeated_use).clone(),
                        ),
                    )
                    .add_label(
                        ComplexErrorLabelKind::Information,
                        *first_use.origin(),
                        Info::FirstUse,
                    );
            }

            negative_variables.extend(current_negative_variables);
        }

        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Rule
    }
}

impl IterableVariables for Rule {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(
            self.head()
                .iter()
                .flat_map(|atom| atom.variables())
                .chain(self.body().iter().flat_map(|literal| literal.variables())),
        )
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        let head_variables = self.head.iter_mut().flat_map(|atom| atom.variables_mut());

        let body_variables = self
            .body
            .iter_mut()
            .flat_map(|literal| literal.variables_mut());

        Box::new(head_variables.chain(body_variables))
    }
}

impl IterablePrimitives for Rule {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        let head_primitives = self.head().iter().flat_map(|atom| atom.primitive_terms());
        let body_primitives = self
            .body()
            .iter()
            .flat_map(|literal| literal.primitive_terms());

        Box::new(head_primitives.chain(body_primitives))
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        let head_primitives = self
            .head
            .iter_mut()
            .flat_map(|atom| atom.primitive_terms_mut());
        let body_primitives = self
            .body
            .iter_mut()
            .flat_map(|literal| literal.primitive_terms_mut());

        Box::new(head_primitives.chain(body_primitives))
    }
}

/// Builder for a rule
#[derive(Debug, Default)]
pub struct RuleBuilder {
    /// Origin of the rule
    origin: Origin,

    /// Name of the rule
    name: Option<String>,
    /// How an instantiated version of this rule should be displayed
    display: Option<Term>,

    /// Head of the rule
    head: Vec<Atom>,
    /// Body of the rule
    body: Vec<Literal>,
}

impl RuleBuilder {
    /// Set the name of the built rule.
    pub fn name_mut(&mut self, name: &str) -> &mut Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set the display property of the built rule.
    pub fn display_mut(&mut self, display: Term) -> &mut Self {
        self.display = Some(display);
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
        let mut rule = Rule::new(self.head, self.body).set_origin(self.origin);

        if let Some(name) = &self.name {
            rule = rule.set_name(name);
        }

        if let Some(display) = self.display {
            rule = rule.set_display(display);
        }

        rule
    }
}
