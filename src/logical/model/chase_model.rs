//! A variant of the rule model suitable for computing the chase.

use std::collections::{HashMap, HashSet};

use crate::{io::parser::ParseError, logical::types::LogicalTypeEnum};

use super::rule_model::{
    Atom, DataSource, DataSourceDeclaration, Fact, Filter, Identifier, Literal,
    OutputPredicateSelection, QualifiedPredicateName, Term, Variable,
};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Rule {
    /// Head atoms of the rule
    head: Vec<Atom>,
    /// Positive Body literals of the rule
    positive_body: Vec<Atom>,
    /// Filters applied to the body
    positive_filters: Vec<Filter>,
    /// Negative Body literals of the rule
    negative_body: Vec<Atom>,
    /// Filters applied to the body
    negative_filters: Vec<Filter>,
}

#[allow(dead_code)]
impl Rule {
    /// Construct a new rule.
    pub fn new(head: Vec<Atom>, body: Vec<Literal>, positive_filters: Vec<Filter>) -> Self {
        let mut positive_body = Vec::new();
        let mut negative_body = Vec::new();

        for literal in body {
            match literal {
                Literal::Positive(atom) => positive_body.push(atom),
                Literal::Negative(atom) => negative_body.push(atom),
            }
        }

        Self {
            head,
            positive_body,
            negative_body,
            positive_filters,
            negative_filters: Vec::new(),
        }
    }

    /// Construct a new rule, validating constraints on variable usage.
    pub(crate) fn new_validated(
        head: Vec<Atom>,
        body: Vec<Literal>,
        filters: Vec<Filter>,
    ) -> Result<Self, ParseError> {
        // Check if existential variables occur in the body.
        let existential_variables = body
            .iter()
            .flat_map(|literal| literal.existential_variables())
            .collect::<Vec<_>>();

        if !existential_variables.is_empty() {
            return Err(ParseError::BodyExistential(
                existential_variables
                    .first()
                    .expect("is not empty here")
                    .name(),
            ));
        }

        // Check if some variable in the body occurs only in negative literals.
        let (positive, negative): (Vec<_>, Vec<_>) = body
            .iter()
            .cloned()
            .partition(|literal| literal.is_positive());
        let negative_variables = negative
            .iter()
            .flat_map(|literal| literal.universal_variables())
            .collect::<HashSet<_>>();
        let positive_varibales = positive
            .iter()
            .flat_map(|literal| literal.universal_variables())
            .collect();
        let negative_variables = negative_variables
            .difference(&positive_varibales)
            .collect::<Vec<_>>();

        if !negative_variables.is_empty() {
            return Err(ParseError::UnsafeNegatedVariable(
                negative_variables
                    .first()
                    .expect("is not empty here")
                    .name(),
            ));
        }

        // Check if a variable occurs with both existential and universal quantification.
        let universal_variables = body
            .iter()
            .flat_map(|literal| literal.universal_variables())
            .collect::<HashSet<_>>();

        let existential_variables = head
            .iter()
            .flat_map(|atom| atom.existential_variables())
            .collect();

        let common_variables = universal_variables
            .intersection(&existential_variables)
            .take(1)
            .collect::<Vec<_>>();

        if !common_variables.is_empty() {
            return Err(ParseError::BothQuantifiers(
                common_variables.first().expect("is not empty here").name(),
            ));
        }

        // Check if there are universal variables in the head which do not occur in a positive body literal
        let head_universal_variables = head
            .iter()
            .flat_map(|atom| atom.universal_variables())
            .collect::<HashSet<_>>();

        for head_variable in head_universal_variables {
            if !universal_variables.contains(head_variable) {
                return Err(ParseError::UnsafeHeadVariable(head_variable.name()));
            }
        }

        // Check if filters are correctly formed
        for filter in &filters {
            let mut filter_variables = vec![&filter.lhs];

            if let Term::Variable(right_variable) = &filter.rhs {
                filter_variables.push(right_variable);
            }

            for variable in filter_variables {
                match variable {
                    Variable::Universal(universal_variable) => {
                        if !positive_varibales.contains(variable) {
                            return Err(ParseError::UnsafeFilterVariable(
                                universal_variable.name(),
                            ));
                        }
                    }
                    Variable::Existential(existential_variable) => {
                        return Err(ParseError::BodyExistential(existential_variable.name()))
                    }
                }
            }
        }

        Ok(Rule::new(head, body, filters))
    }

    /// Return the head atoms of the rule - immutable.
    #[must_use]
    pub fn head(&self) -> &Vec<Atom> {
        &self.head
    }

    /// Return the head atoms of the rule - mutable.
    #[must_use]
    pub fn head_mut(&mut self) -> &mut Vec<Atom> {
        &mut self.head
    }

    /// Return all the atoms occuring in this rule.
    /// This includes the postive body atoms, the negative body atoms as well as the head atoms.
    pub fn all_atoms(&self) -> impl Iterator<Item = &Atom> {
        self.all_body().chain(self.head.iter())
    }

    /// Return the all the atoms of the rules.
    /// This does not distinguish between positive and negative atoms.
    pub fn all_body(&self) -> impl Iterator<Item = &Atom> {
        self.positive_body.iter().chain(self.negative_body.iter())
    }

    /// Return the positive body atoms of the rule - immutable.
    #[must_use]
    pub fn positive_body(&self) -> &Vec<Atom> {
        &self.positive_body
    }

    /// Return the positive body atoms of the rule - mutable.
    #[must_use]
    pub fn positive_body_mut(&mut self) -> &mut Vec<Atom> {
        &mut self.positive_body
    }

    /// Return all the filters of the rule.
    pub fn all_filters(&self) -> impl Iterator<Item = &Filter> {
        self.positive_filters
            .iter()
            .chain(self.negative_filters.iter())
    }

    /// Return the positive filters of the rule - immutable.
    #[must_use]
    pub fn positive_filters(&self) -> &Vec<Filter> {
        &self.positive_filters
    }

    /// Return the positive filters of the rule - mutable.
    #[must_use]
    pub fn positive_filters_mut(&mut self) -> &mut Vec<Filter> {
        &mut self.positive_filters
    }

    /// Return the negative body atons of the rule - immutable.
    #[must_use]
    pub fn negative_body(&self) -> &Vec<Atom> {
        &self.negative_body
    }

    /// Return the negative body atoms of the rule - mutable.
    #[must_use]
    pub fn negative_body_mut(&mut self) -> &mut Vec<Atom> {
        &mut self.negative_body
    }

    /// Return the negative filters of the rule - immutable.
    #[must_use]
    pub fn negative_filters(&self) -> &Vec<Filter> {
        &self.negative_filters
    }

    /// Return the negative filters of the rule - mutable.
    #[must_use]
    pub fn negative_filters_mut(&mut self) -> &mut Vec<Filter> {
        &mut self.negative_filters
    }
}

impl TryFrom<super::rule_model::Rule> for Rule {
    type Error = ParseError;

    fn try_from(rule: super::rule_model::Rule) -> Result<Self, Self::Error> {
        Self::new_validated(
            rule.head().to_vec(),
            rule.body().to_vec(),
            rule.filters().to_vec(),
        )
    }
}

#[allow(dead_code)]
/// A full program.
#[derive(Debug, Default, Clone)]
pub struct Program {
    base: Option<String>,
    prefixes: HashMap<String, String>,
    sources: Vec<DataSourceDeclaration>,
    rules: Vec<Rule>,
    facts: Vec<Fact>,
    parsed_predicate_declarations: HashMap<Identifier, Vec<LogicalTypeEnum>>,
    output_predicates: OutputPredicateSelection,
}

impl From<Vec<Rule>> for Program {
    fn from(rules: Vec<Rule>) -> Self {
        Self {
            rules,
            ..Default::default()
        }
    }
}

impl From<(Vec<DataSourceDeclaration>, Vec<Rule>)> for Program {
    fn from((sources, rules): (Vec<DataSourceDeclaration>, Vec<Rule>)) -> Self {
        Self {
            sources,
            rules,
            ..Default::default()
        }
    }
}

#[allow(dead_code)]
impl Program {
    /// Construct a new program.
    pub fn new(
        base: Option<String>,
        prefixes: HashMap<String, String>,
        sources: Vec<DataSourceDeclaration>,
        rules: Vec<Rule>,
        facts: Vec<Fact>,
        parsed_predicate_declarations: HashMap<Identifier, Vec<LogicalTypeEnum>>,
        output_predicates: OutputPredicateSelection,
    ) -> Self {
        Self {
            base,
            prefixes,
            sources,
            rules,
            facts,
            parsed_predicate_declarations,
            output_predicates,
        }
    }

    /// Get the base IRI, if set.
    #[must_use]
    pub fn base(&self) -> Option<String> {
        self.base.clone()
    }

    /// Return all rules in the program - immutable.
    #[must_use]
    pub fn rules(&self) -> &Vec<Rule> {
        &self.rules
    }

    /// Return all rules in the program - mutable.
    #[must_use]
    pub fn rules_mut(&mut self) -> &mut Vec<Rule> {
        &mut self.rules
    }

    /// Return all facts in the program.
    #[must_use]
    pub fn facts(&self) -> &Vec<Fact> {
        &self.facts
    }

    /// Return a HashSet of all predicates in the program (in rules and facts).
    #[must_use]
    pub fn predicates(&self) -> HashSet<Identifier> {
        self.rules()
            .iter()
            .flat_map(|rule| {
                rule.head()
                    .iter()
                    .map(|atom| atom.predicate())
                    .chain(rule.all_body().map(|atom| atom.predicate()))
            })
            .chain(self.facts().iter().map(|atom| atom.0.predicate()))
            .collect()
    }

    /// Return a HashSet of all idb predicates (predicates occuring rule heads) in the program.
    #[must_use]
    pub fn idb_predicates(&self) -> HashSet<Identifier> {
        self.rules()
            .iter()
            .flat_map(|rule| rule.head())
            .map(|atom| atom.predicate())
            .collect()
    }

    /// Return a HashSet of all edb predicates (all predicates minus idb predicates) in the program.
    #[must_use]
    pub fn edb_predicates(&self) -> HashSet<Identifier> {
        self.predicates()
            .difference(&self.idb_predicates())
            .cloned()
            .collect()
    }

    /// Return an Iterator over all output predicates
    pub fn output_predicates(&self) -> impl Iterator<Item = Identifier> {
        let result: Vec<_> = match &self.output_predicates {
            OutputPredicateSelection::AllIDBPredicates => {
                self.idb_predicates().iter().cloned().collect()
            }
            OutputPredicateSelection::SelectedPredicates(predicates) => predicates
                .iter()
                .map(
                    |QualifiedPredicateName {
                         identifier,
                         arity: _,
                     }| identifier,
                )
                .cloned()
                .collect(),
        };

        result.into_iter()
    }

    /// Return all prefixes in the program.
    #[must_use]
    pub fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
    }

    /// Return all data sources in the program.
    pub fn sources(&self) -> impl Iterator<Item = ((&Identifier, usize), &DataSource)> {
        self.sources
            .iter()
            .map(|source| ((&source.predicate, source.arity), &source.source))
    }

    /// Look up a given prefix.
    #[must_use]
    pub fn resolve_prefix(&self, tag: &str) -> Option<String> {
        self.prefixes.get(tag).cloned()
    }

    /// Return parsed predicate declarations
    #[must_use]
    pub fn parsed_predicate_declarations(&self) -> HashMap<Identifier, Vec<LogicalTypeEnum>> {
        self.parsed_predicate_declarations.clone()
    }

    /// Force the given selection of output predicates.
    pub fn force_output_predicate_selection(
        &mut self,
        output_predicates: OutputPredicateSelection,
    ) {
        self.output_predicates = output_predicates;
    }
}

impl TryFrom<super::rule_model::Program> for Program {
    type Error = ParseError;

    fn try_from(value: super::rule_model::Program) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.base(),
            value.prefixes().clone(),
            value
                .sources()
                .map(|((predicate, arity), source)| {
                    DataSourceDeclaration::new(predicate.clone(), arity, source.clone())
                })
                .collect(),
            value
                .rules()
                .iter()
                .map(|rule| rule.clone().try_into())
                .collect::<Result<Vec<_>, _>>()?,
            value.facts().to_vec(),
            value.parsed_predicate_declarations(),
            value
                .output_predicates()
                .map(|identifier| QualifiedPredicateName {
                    identifier,
                    arity: None,
                })
                .collect::<Vec<_>>()
                .into(),
        ))
    }
}
