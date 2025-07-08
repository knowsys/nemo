//! This module defines [Rule].

use std::{collections::HashSet, fmt::Display, hash::Hash};

use nemo_physical::datavalues::DataValue;

use crate::rule_model::{
    error::{hint::Hint, info::Info, validation_error::ValidationError, ValidationReport},
    origin::Origin,
    pipeline::id::ProgramComponentId,
    substitution::Substitution,
};

use super::{
    atom::Atom,
    component_iterator, component_iterator_mut,
    literal::Literal,
    term::{
        primitive::{variable::Variable, Primitive},
        Term,
    },
    ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent, IterablePrimitives,
    IterableVariables, ProgramComponentKind,
};

/// Rule
///
/// A logical statement that defines a relationship between a head (conjunction of [Atom]s)
/// and a body (conjunction of [Literal]s).
/// It specifies how new facts can be inferred from existing ones.
#[derive(Debug, Clone)]
pub struct Rule {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Name of the rule
    name: Option<String>,
    /// [Term] specifying how an instance of the rule should be displayed
    /// in e.g. tracing
    display: Option<Term>,

    /// Head of the rule
    head: Vec<Atom>,
    /// Body of the rule
    body: Vec<Literal>,
}

impl Rule {
    /// Create a new [Rule].
    pub fn new(head: Vec<Atom>, body: Vec<Literal>) -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            name: None,
            display: None,
            head,
            body,
        }
    }

    /// Create a new empty [Rule].
    pub fn empty() -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            name: None,
            display: None,
            head: Vec::default(),
            body: Vec::default(),
        }
    }

    /// Set the name of the rule.
    pub fn set_name(&mut self, name: &str) {
        self.name = Some(name.to_string());
    }

    /// Set how an instantiated version of the rule should be displayed.
    pub fn set_display(&mut self, display: Term) {
        self.display = Some(display);
    }

    /// Return the name of the rule, if it is given one.
    pub fn name(&self) -> Option<String> {
        self.name.clone()
    }

    /// Return a string representation of the rule's description with the given [Substitution].
    ///
    /// Returns `None` if `description` results not in a ground term after applying the substitution.
    pub fn instantiated_display(&self, substitution: &Substitution) -> Option<String> {
        if let Some(mut display) = self.display.clone() {
            substitution.apply(&mut display);
            if let Term::Primitive(Primitive::Ground(ground)) = display.reduce()? {
                return ground.value().to_plain_string();
            }
        }

        None
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

    pub fn existential_variables(&self) -> HashSet<&Variable> {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }

    pub fn positive_variables_iter(&self) -> impl Iterator<Item = &Variable> {
        self.body_positive().flat_map(|atom| {
            atom.terms()
                .filter_map(|term| match term {
                    Term::Primitive(Primitive::Variable(variable)) => Some(variable),
                    _ => None,
                })
                .filter(|variable| variable.is_universal() && variable.name().is_some())
        })
    }

    /// Return the set of variables that are bound in positive body atoms.
    pub fn positive_variables(&self) -> HashSet<&Variable> {
        self.positive_variables_iter().collect()
    }

    /// Return an iterator over all positive and negative [Atom]s
    /// contained in the body of this rule.
    pub fn body_atoms(&self) -> impl Iterator<Item = &Atom> {
        self.body.iter().filter_map(|literal| match literal {
            Literal::Positive(atom) | Literal::Negative(atom) => Some(atom),
            Literal::Operation(_) => None,
        })
    }

    /// Return an iterator over all [Atom]s contained in this rule,
    /// including the head and the positive and negative body.
    pub fn atoms(&self) -> impl Iterator<Item = &Atom> {
        self.head.iter().chain(self.body_atoms())
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
        report: &mut ValidationReport,
        term: &Term,
        group_by_variable: &HashSet<&Variable>,
    ) -> bool {
        if term.is_map() || term.is_tuple() || term.is_function() {
            report
                .add(term, ValidationError::UnsupportedComplexTerm)
                .add_hint_option(Self::hint_term_operation(term));
        }

        let mut first_aggregate = if let Term::Aggregate(aggregate) = term {
            if let Term::Primitive(Primitive::Variable(aggregate_variable)) =
                aggregate.aggregate_term()
            {
                if group_by_variable.contains(aggregate_variable) {
                    report.add(
                        aggregate.aggregate_term(),
                        ValidationError::AggregateOverGroupByVariable {
                            variable: Box::new(aggregate_variable.clone()),
                        },
                    );
                }
            }

            true
        } else {
            false
        };

        for subterm in term.terms() {
            let contains_aggregate = Self::validate_term_head(report, subterm, group_by_variable);

            if contains_aggregate && first_aggregate {
                report.add(subterm, ValidationError::UnsupportedAggregateMultiple);
            }

            first_aggregate |= contains_aggregate;
        }

        first_aggregate
    }

    /// Check if
    ///     * body does not contain any existential variables
    ///     * body does not contain aggregation
    ///     * body does not contain any complex term
    ///     * used operations do not use anonymous variables
    ///     * operations only use safe variables
    fn validate_term_body(
        report: &mut ValidationReport,
        term: &Term,
        safe_variables: &HashSet<&Variable>,
    ) {
        if let Term::Primitive(Primitive::Variable(Variable::Existential(existential))) = term {
            report.add(
                existential,
                ValidationError::BodyExistential {
                    variable: existential.clone(),
                },
            );
        }

        if term.is_aggregate() {
            report.add(term, ValidationError::BodyAggregate);
        }

        if term.is_operation() {
            for operation_variable in term.variables() {
                if operation_variable.name().is_none() {
                    continue;
                }

                if !safe_variables.contains(operation_variable) {
                    report.add(
                        operation_variable,
                        ValidationError::OperationUnsafe {
                            variable: Box::new(operation_variable.clone()),
                        },
                    );
                }
            }
        }

        if term.is_map() || term.is_tuple() || term.is_function() {
            report
                .add(term, ValidationError::UnsupportedComplexTerm)
                .add_hint_option(Self::hint_term_operation(term));
        }

        for subterm in term.terms() {
            Self::validate_term_body(report, subterm, safe_variables);
        }
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

impl ComponentBehavior for Rule {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Rule
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        for child in self.children() {
            report.merge(child.validate());
        }

        if self.body_positive().next().is_none() {
            report.add(self, ValidationError::UnsupportedNoPositiveLiterals);
            return report.result();
        }

        let safe_variables = self.safe_variables();
        let is_existential = self
            .head()
            .iter()
            .flat_map(|atom| atom.variables())
            .any(|variable| variable.is_existential());

        if let Some(display) = &self.display {
            report.merge(display.validate());

            for variable in display.variables() {
                if !safe_variables.contains(variable) {
                    report.add(
                        variable,
                        ValidationError::AttributeRuleUnsafe {
                            variable: Box::new(variable.clone()),
                        },
                    );
                }
            }
        }

        for atom in self.head() {
            for variable in atom.variables() {
                if let Some(variable_name) = variable.name() {
                    if !variable.is_existential() && !safe_variables.contains(variable) {
                        report
                            .add(
                                variable,
                                ValidationError::HeadUnsafe {
                                    variable: Box::new(variable.clone()),
                                },
                            )
                            .add_hint_option(Hint::similar(
                                "variable",
                                variable_name,
                                safe_variables.iter().flat_map(|variable| variable.name()),
                            ));
                    }
                } else {
                    report.add(variable, ValidationError::HeadAnonymous);
                }
            }

            let group_by_variables = atom
                .terms()
                .flat_map(|term| {
                    if let Term::Primitive(Primitive::Variable(variable)) = term {
                        Some(variable)
                    } else {
                        None
                    }
                })
                .collect::<HashSet<_>>();

            let mut contains_aggregate = false;
            for term in atom.terms() {
                let aggregate = Self::validate_term_head(&mut report, term, &group_by_variables);

                if aggregate && contains_aggregate {
                    report.add(term, ValidationError::UnsupportedAggregateMultiple);
                }

                if aggregate && is_existential {
                    report.add(term, ValidationError::UnsupportedAggregatesAndExistentials);
                }

                contains_aggregate |= aggregate;
            }
        }

        let mut negative_variables = HashSet::<&Variable>::new();

        for literal in self.body() {
            for term in literal.terms() {
                Self::validate_term_body(&mut report, term, &safe_variables);
            }

            let mut current_negative_variables = HashSet::<&Variable>::new();
            if let Literal::Negative(negative) = literal {
                for negative_subterm in negative.terms() {
                    if let Term::Primitive(Primitive::Variable(variable)) = negative_subterm {
                        if !safe_variables.contains(&variable) {
                            current_negative_variables.insert(&variable);
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

                report
                    .add(
                        *repeated_use,
                        ValidationError::MultipleNegativeLiteralsUnsafe {
                            variable: Box::new((*repeated_use).clone()),
                        },
                    )
                    .add_context(*first_use, Info::FirstUse);
            }

            negative_variables.extend(current_negative_variables);
        }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn super::ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for Rule {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for Rule {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for Rule {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn super::ProgramComponent> + 'a> {
        let head_iterator = component_iterator(self.head.iter());
        let body_iterator = component_iterator(self.body.iter());

        Box::new(head_iterator.chain(body_iterator))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn super::ProgramComponent> + 'a> {
        let head_iterator = component_iterator_mut(self.head.iter_mut());
        let body_iterator = component_iterator_mut(self.body.iter_mut());

        Box::new(head_iterator.chain(body_iterator))
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (head_index, head_atom) in self.head.iter().enumerate() {
            write!(f, "{head_atom}")?;

            if head_index < self.head.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(" :- ")?;

        for (body_index, body_literal) in self.body.iter().enumerate() {
            write!(f, "{body_literal}")?;

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

impl Eq for Rule {}

impl Hash for Rule {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.head.hash(state);
        self.body.hash(state);
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

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
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
