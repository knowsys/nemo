//! Defines a variant of [`crate::model::Program`], suitable for computing the chase.

use std::collections::{HashMap, HashSet};

use crate::{
    error::Error,
    model::{
        DataSourceDeclaration, Identifier, OutputPredicateSelection, PrimitiveType, Program,
        QualifiedPredicateName,
    },
};

use super::{ChaseAtom, ChaseFact, ChaseRule};

#[allow(dead_code)]
/// Representation of a datalog program that is used for generating execution plans for the physical layer.
#[derive(Debug, Default, Clone)]
pub struct ChaseProgram {
    base: Option<String>,
    prefixes: HashMap<String, String>,
    sources: Vec<DataSourceDeclaration>,
    rules: Vec<ChaseRule>,
    facts: Vec<ChaseFact>,
    parsed_predicate_declarations: HashMap<Identifier, Vec<PrimitiveType>>,
    output_predicates: OutputPredicateSelection,
}

impl From<Vec<ChaseRule>> for ChaseProgram {
    fn from(rules: Vec<ChaseRule>) -> Self {
        Self {
            rules,
            ..Default::default()
        }
    }
}

impl From<(Vec<DataSourceDeclaration>, Vec<ChaseRule>)> for ChaseProgram {
    fn from((sources, rules): (Vec<DataSourceDeclaration>, Vec<ChaseRule>)) -> Self {
        Self {
            sources,
            rules,
            ..Default::default()
        }
    }
}

#[allow(dead_code)]
impl ChaseProgram {
    /// Construct a new program.
    pub fn new(
        base: Option<String>,
        prefixes: HashMap<String, String>,
        sources: Vec<DataSourceDeclaration>,
        rules: Vec<ChaseRule>,
        facts: Vec<ChaseFact>,
        parsed_predicate_declarations: HashMap<Identifier, Vec<PrimitiveType>>,
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
    pub fn rules(&self) -> &Vec<ChaseRule> {
        &self.rules
    }

    /// Return all rules in the program - mutable.
    #[must_use]
    pub fn rules_mut(&mut self) -> &mut Vec<ChaseRule> {
        &mut self.rules
    }

    /// Return all facts in the program.
    #[must_use]
    pub fn facts(&self) -> &Vec<ChaseFact> {
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
            .chain(self.facts().iter().map(|atom| atom.predicate()))
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
                .map(|QualifiedPredicateName { identifier, .. }| identifier)
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
    pub fn sources(&self) -> impl Iterator<Item = &DataSourceDeclaration> {
        self.sources.iter()
    }

    /// Look up a given prefix.
    #[must_use]
    pub fn resolve_prefix(&self, tag: &str) -> Option<String> {
        self.prefixes.get(tag).cloned()
    }

    /// Return parsed predicate declarations
    #[must_use]
    pub fn parsed_predicate_declarations(&self) -> HashMap<Identifier, Vec<PrimitiveType>> {
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

impl TryFrom<Program> for ChaseProgram {
    type Error = Error;

    fn try_from(program: Program) -> Result<Self, Error> {
        Ok(Self::new(
            program.base(),
            program.prefixes().clone(),
            program.sources().cloned().collect(),
            program
                .rules()
                .iter()
                .map(|rule| rule.clone().try_into())
                .collect::<Result<Vec<ChaseRule>, Error>>()?,
            program
                .facts()
                .iter()
                .map(|f| ChaseFact::from_flat_atom(&f.0))
                .collect(),
            program.parsed_predicate_declarations(),
            program
                .output_predicates()
                .map(QualifiedPredicateName::new)
                .collect::<Vec<_>>()
                .into(),
        ))
    }
}
