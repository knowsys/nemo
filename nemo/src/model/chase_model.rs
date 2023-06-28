//! A variant of the rule model suitable for computing the chase.

use std::collections::{HashMap, HashSet};

use crate::types::LogicalTypeEnum;

use super::{
    rule_model::{
        Atom, DataSource, DataSourceDeclaration, Fact, Filter, Identifier, Literal,
        OutputPredicateSelection, QualifiedPredicateName,
    },
    ArityOrTypes, Program, Rule, Term, TermOperation, TermTree, Variable,
};

#[derive(Debug, Clone)]
pub struct ChaseAtom {
    predicate: Identifier,
    terms: Vec<Term>,
}

impl ChaseAtom {
    /// Construct a new Atom.
    pub fn new(predicate: Identifier, terms: Vec<Term>) -> Self {
        Self { predicate, terms }
    }

    pub fn from_flat_atom(atom: Atom) -> Self {
        let terms: Vec<Term> = atom
            .terms()
            .into_iter()
            .map(|t| {
                if let TermOperation::Term(term) = t.operation() {
                    term.clone()
                } else {
                    panic!("Expected term trees that are only leaves");
                }
            })
            .collect();

        Self {
            predicate: atom.predicate(),
            terms,
        }
    }

    /// Return the predicate [`Identifier`].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the terms in the atom - immutable.
    #[must_use]
    pub fn terms(&self) -> &Vec<Term> {
        &self.terms
    }

    /// Return the terms in the atom - mutable.
    #[must_use]
    pub fn terms_mut(&mut self) -> &mut Vec<Term> {
        &mut self.terms
    }

    /// Return all variables in the atom.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        self.terms().iter().filter_map(|term| match term {
            Term::Variable(var) => Some(var),
            _ => None,
        })
    }

    /// Return all universally quantified variables in the atom.
    pub fn universal_variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        self.variables()
            .filter(|var| matches!(var, Variable::Universal(_)))
    }

    /// Return all existentially quantified variables in the atom.
    pub fn existential_variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        self.variables()
            .filter(|var| matches!(var, Variable::Existential(_)))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct ChaseRule {
    /// Head atoms of the rule
    head: Vec<ChaseAtom>,
    /// Head constructions
    constructors: HashMap<Variable, TermTree>,
    /// Positive Body literals of the rule
    positive_body: Vec<ChaseAtom>,
    /// Filters applied to the body
    positive_filters: Vec<Filter>,
    /// Negative Body literals of the rule
    negative_body: Vec<ChaseAtom>,
    /// Filters applied to the body
    negative_filters: Vec<Filter>,
}

#[allow(dead_code)]
impl ChaseRule {
    /// Construct a new rule.
    pub fn new(
        head: Vec<ChaseAtom>,
        constructors: HashMap<Variable, TermTree>,
        positive_body: Vec<ChaseAtom>,
        positive_filters: Vec<Filter>,
        negative_body: Vec<ChaseAtom>,
        negative_filters: Vec<Filter>,
    ) -> Self {
        Self {
            head,
            constructors,
            positive_body,
            positive_filters,
            negative_body,
            negative_filters,
        }
    }
    /// Return the head atoms of the rule - immutable.
    #[must_use]
    pub fn head(&self) -> &Vec<ChaseAtom> {
        &self.head
    }

    /// Return the head atoms of the rule - mutable.
    #[must_use]
    pub fn head_mut(&mut self) -> &mut Vec<ChaseAtom> {
        &mut self.head
    }

    /// Return all the atoms occuring in this rule.
    /// This includes the postive body atoms, the negative body atoms as well as the head atoms.
    pub fn all_atoms(&self) -> impl Iterator<Item = &ChaseAtom> {
        self.all_body().chain(self.head.iter())
    }

    /// Return the all the atoms of the rules.
    /// This does not distinguish between positive and negative atoms.
    pub fn all_body(&self) -> impl Iterator<Item = &ChaseAtom> {
        self.positive_body.iter().chain(self.negative_body.iter())
    }

    /// Return the positive body atoms of the rule - immutable.
    #[must_use]
    pub fn positive_body(&self) -> &Vec<ChaseAtom> {
        &self.positive_body
    }

    /// Return the positive body atoms of the rule - mutable.
    #[must_use]
    pub fn positive_body_mut(&mut self) -> &mut Vec<ChaseAtom> {
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
    pub fn negative_body(&self) -> &Vec<ChaseAtom> {
        &self.negative_body
    }

    /// Return the negative body atoms of the rule - mutable.
    #[must_use]
    pub fn negative_body_mut(&mut self) -> &mut Vec<ChaseAtom> {
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

impl From<Rule> for ChaseRule {
    fn from(rule: Rule) -> ChaseRule {
        let mut positive_body = Vec::new();
        let mut negative_body = Vec::new();

        for literal in rule.body().iter().cloned() {
            match literal {
                Literal::Positive(atom) => positive_body.push(ChaseAtom::from_flat_atom(atom)),
                Literal::Negative(atom) => negative_body.push(ChaseAtom::from_flat_atom(atom)),
            }
        }

        let mut constructors = HashMap::<Variable, TermTree>::new();
        let mut head_atoms = Vec::<ChaseAtom>::new();
        let mut term_counter: usize = 1;
        for atom in rule.head() {
            let mut new_terms = Vec::<Term>::new();

            for term_tree in atom.terms() {
                if let TermOperation::Term(term) = term_tree.operation() {
                    new_terms.push(term.clone());
                } else {
                    let new_variable =
                        Variable::Universal(Identifier(format!("HEAD_OPERATION_{term_counter}")));
                    new_terms.push(Term::Variable(new_variable.clone()));
                    let exists = constructors.insert(new_variable, term_tree.clone());

                    assert!(exists.is_none())
                }

                term_counter += 1;
            }

            head_atoms.push(ChaseAtom::new(atom.predicate(), new_terms));
        }

        Self {
            head: head_atoms,
            constructors,
            positive_body,
            negative_body,
            positive_filters: rule.filters().clone(),
            negative_filters: Vec::new(),
        }
    }
}

#[allow(dead_code)]
/// A full program.
#[derive(Debug, Default, Clone)]
pub struct ChaseProgram {
    base: Option<String>,
    prefixes: HashMap<String, String>,
    sources: Vec<DataSourceDeclaration>,
    rules: Vec<ChaseRule>,
    facts: Vec<Fact>,
    parsed_predicate_declarations: HashMap<Identifier, Vec<LogicalTypeEnum>>,
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
    pub fn sources(
        &self,
    ) -> impl Iterator<Item = (&Identifier, usize, &Vec<LogicalTypeEnum>, &DataSource)> {
        self.sources.iter().map(|source| {
            (
                &source.predicate,
                source.arity,
                &source.input_types,
                &source.source,
            )
        })
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

impl From<Program> for ChaseProgram {
    fn from(value: Program) -> Self {
        Self::new(
            value.base(),
            value.prefixes().clone(),
            value
                .sources()
                .map(|(predicate, _arity, input_types, source)| {
                    DataSourceDeclaration::new(
                        predicate.clone(),
                        ArityOrTypes::Types(input_types.clone()),
                        source.clone(),
                    )
                })
                .collect(),
            value
                .rules()
                .iter()
                .map(|rule| rule.clone().into())
                .collect(),
            value.facts().to_vec(),
            value.parsed_predicate_declarations(),
            value
                .output_predicates()
                .map(QualifiedPredicateName::new)
                .collect::<Vec<_>>()
                .into(),
        )
    }
}
