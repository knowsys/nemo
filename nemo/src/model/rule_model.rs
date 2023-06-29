//! The data model.

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    ops::Neg,
    path::{Path, PathBuf},
};

use nemo_physical::datatypes::Double;
use sanitise_file_name::{sanitise_with_options, Options};

use crate::io::parser::ParseError;

use crate::types::LogicalTypeEnum;

/// An identifier for, e.g., a Term or a Predicate.
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Identifier(pub(crate) String);

impl Identifier {
    /// Returns the associated name
    pub fn name(&self) -> String {
        self.0.clone()
    }

    /// Returns a sanitised path with respect to the associated name
    pub fn sanitised_file_name(&self, mut path: PathBuf) -> PathBuf {
        let sanitise_options = Options::<Option<char>> {
            url_safe: true,
            ..Default::default()
        };
        let file_name = sanitise_with_options(&self.name(), &sanitise_options);
        path.push(file_name);
        path
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.name())
    }
}

impl From<String> for Identifier {
    fn from(value: String) -> Self {
        Identifier(value)
    }
}

/// An enum capturing a predicates types or just its arity
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub enum ArityOrTypes {
    /// Variant only capturing the arity of a predicate
    Arity(usize),
    /// Variant capturing the types of a predicate, allowing to infer the arity
    Types(Vec<LogicalTypeEnum>),
}

impl ArityOrTypes {
    fn arity(&self) -> usize {
        match self {
            Self::Arity(u) => *u,
            Self::Types(ts) => ts.len(),
        }
    }

    fn types_owned(self) -> Option<Vec<LogicalTypeEnum>> {
        match self {
            Self::Types(ts) => Some(ts),
            _ => None,
        }
    }
}

/// A qualified predicate name, i.e., a predicate name together with its arity.
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd)]
pub struct QualifiedPredicateName {
    /// The predicate name
    pub(crate) identifier: Identifier,
    /// The arity
    pub(crate) arity: Option<usize>,
    /// The logical types
    pub(crate) logical_types: Option<Vec<LogicalTypeEnum>>,
}

impl QualifiedPredicateName {
    /// Construct a new qualified predicate name from an identifier,
    /// leaving the arity unspecified.
    pub fn new(identifier: Identifier) -> Self {
        Self {
            identifier,
            arity: None,
            logical_types: None,
        }
    }

    /// Construct a new qualified predicate name with the given arity or a list of types.
    pub fn with_arity_or_types(identifier: Identifier, arity_or_types: ArityOrTypes) -> Self {
        Self {
            identifier,
            arity: Some(arity_or_types.arity()),
            logical_types: arity_or_types.types_owned(),
        }
    }
}

impl From<Identifier> for QualifiedPredicateName {
    fn from(identifier: Identifier) -> Self {
        Self::new(identifier)
    }
}

/// Variable occuring in a rule
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum Variable {
    /// A universally quantified variable.
    Universal(Identifier),
    /// An existentially quantified variable.
    Existential(Identifier),
}

impl Variable {
    /// Return the name of the variable.
    pub fn name(&self) -> String {
        match self {
            Self::Universal(identifier) | Self::Existential(identifier) => identifier.name(),
        }
    }
}

impl std::fmt::Display for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.name())
    }
}

/// Terms occurring in programs.
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Term {
    /// An (abstract) constant.
    Constant(Identifier),
    /// A variable.
    Variable(Variable),
    /// A numeric literal.
    NumericLiteral(NumericLiteral),
    /// A string literal.
    StringLiteral(String),
    /// An RDF literal.
    RdfLiteral(RdfLiteral),
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Term::Constant(term) => write!(f, "{term}"),
            Term::Variable(term) => write!(f, "{term}"),
            Term::NumericLiteral(term) => write!(f, "{term}"),
            Term::StringLiteral(term) => write!(f, "{term}"),
            Term::RdfLiteral(term) => write!(f, "{term}"),
        }
    }
}

impl Term {
    /// Check if the term is ground.
    pub fn is_ground(&self) -> bool {
        matches!(
            self,
            Self::Constant(_) | Self::NumericLiteral(_) | Self::RdfLiteral(_)
        )
    }
}

/// A numerical literal.
#[derive(Debug, Eq, PartialEq, Copy, Clone, PartialOrd, Ord)]
pub enum NumericLiteral {
    /// An integer literal.
    Integer(i64),
    /// A decimal literal.
    Decimal(i64, u64),
    /// A double literal.
    Double(Double),
}

impl std::fmt::Display for NumericLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumericLiteral::Integer(value) => write!(f, "{value}"),
            NumericLiteral::Decimal(left, right) => write!(f, "{left}.{right}"),
            NumericLiteral::Double(value) => write!(f, "{value}"),
        }
    }
}

/// An RDF literal.
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum RdfLiteral {
    /// A language string.
    LanguageString {
        /// The literal value.
        value: String,
        /// The language tag.
        tag: String,
    },
    /// A literal with a datatype.
    DatatypeValue {
        /// The literal value.
        value: String,
        /// The datatype IRI.
        datatype: String,
    },
}

impl std::fmt::Display for RdfLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdfLiteral::LanguageString { value, tag } => write!(f, "\"{value}\"@{tag}"),
            RdfLiteral::DatatypeValue { value, datatype } => write!(f, "\"{value}\"^^{datatype}"),
        }
    }
}

/// An atom.
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct Atom {
    /// The predicate.
    predicate: Identifier,
    /// The terms.
    terms: Vec<Term>,
}

impl Atom {
    /// Construct a new Atom.
    pub fn new(predicate: Identifier, terms: Vec<Term>) -> Self {
        Self { predicate, terms }
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

/// A literal.
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Literal {
    /// A non-negated literal.
    Positive(Atom),
    /// A negated literal.
    Negative(Atom),
}

impl Literal {
    /// Check if the literal is positive.
    pub fn is_positive(&self) -> bool {
        matches!(self, Self::Positive(_))
    }

    /// Check if the literal is negative.
    pub fn is_negative(&self) -> bool {
        matches!(self, Self::Negative(_))
    }

    /// Returns the underlying atom
    pub fn atom(&self) -> &Atom {
        match self {
            Self::Positive(atom) => atom,
            Self::Negative(atom) => atom,
        }
    }
}

impl Neg for Literal {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            Literal::Positive(atom) => Self::Negative(atom),
            Literal::Negative(atom) => Self::Positive(atom),
        }
    }
}

generate_forwarder!(forward_to_atom; Positive, Negative);

impl Literal {
    /// Return the predicate [`Identifier`].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        forward_to_atom!(self, predicate)
    }

    /// Return the terms in the literal.
    #[must_use]
    pub fn terms(&self) -> &Vec<Term> {
        forward_to_atom!(self, terms)
    }

    /// Return the variables in the literal.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        forward_to_atom!(self, variables)
    }

    /// Return the universally quantified variables in the literal.
    pub fn universal_variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        forward_to_atom!(self, universal_variables)
    }

    /// Return the existentially quantified variables in the literal.
    pub fn existential_variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        forward_to_atom!(self, existential_variables)
    }
}

/// Operation for a filter
#[derive(Debug, Eq, PartialEq, Clone, Copy, PartialOrd, Ord)]
pub enum FilterOperation {
    /// Variable es equal than term
    Equals,
    /// Value of variable is less than the value of the term
    LessThan,
    /// Value of variable is greater than the value of the term
    GreaterThan,
    /// Value of variable is less than or equal to the value of the term
    LessThanEq,
    /// Value of variable is gretaer than or equal to the value of the term
    GreaterThanEq,
}

impl FilterOperation {
    /// Flips the operation: for `op`, returns a suitable operation
    /// `op'` such that `x op y` iff `y op' x`.
    pub fn flip(&self) -> Self {
        match self {
            Self::Equals => Self::Equals,
            Self::LessThan => Self::GreaterThan,
            Self::GreaterThan => Self::LessThan,
            Self::LessThanEq => Self::GreaterThanEq,
            Self::GreaterThanEq => Self::LessThanEq,
        }
    }
}

/// Filter of the form `<variable> <operation> <term>`
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct Filter {
    /// Operation to be performed
    pub operation: FilterOperation,
    /// Left-hand side
    pub lhs: Variable,
    /// Right-hand side
    pub rhs: Term,
}

impl Filter {
    /// Creates a new [`Filter`]
    pub fn new(operation: FilterOperation, lhs: Variable, rhs: Term) -> Self {
        Self {
            operation,
            lhs,
            rhs,
        }
    }

    /// Creates a new [`Filter]` with the arguments flipped
    pub fn flipped(operation: FilterOperation, lhs: Term, rhs: Variable) -> Self {
        Self::new(operation.flip(), rhs, lhs)
    }
}

/// A rule.
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct Rule {
    /// Head atoms of the rule
    head: Vec<Atom>,
    /// Body literals of the rule
    body: Vec<Literal>,
    /// Filters applied to the body
    filters: Vec<Filter>,
}

impl Rule {
    /// Construct a new rule.
    pub fn new(head: Vec<Atom>, body: Vec<Literal>, filters: Vec<Filter>) -> Self {
        Self {
            head,
            body,
            filters,
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

        Ok(Rule {
            head,
            body,
            filters,
        })
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

    /// Return the body literals of the rule - immutable.
    #[must_use]
    pub fn body(&self) -> &Vec<Literal> {
        &self.body
    }

    /// Return the body literals of the rule - mutable.
    #[must_use]
    pub fn body_mut(&mut self) -> &mut Vec<Literal> {
        &mut self.body
    }

    /// Return the filters of the rule - immutable.
    #[must_use]
    pub fn filters(&self) -> &Vec<Filter> {
        &self.filters
    }

    /// Return the filters of the rule - mutable.
    #[must_use]
    pub fn filters_mut(&mut self) -> &mut Vec<Filter> {
        &mut self.filters
    }
}

/// A (ground) fact.
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct Fact(pub Atom);

/// A statement that can occur in the program.
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Statement {
    /// A fact.
    Fact(Fact),
    /// A rule.
    Rule(Rule),
}

/// Which predicates to output
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub enum OutputPredicateSelection {
    /// Output every IDB predicate
    #[default]
    AllIDBPredicates,
    /// Output only selected predicates
    SelectedPredicates(Vec<QualifiedPredicateName>),
}

impl From<Vec<QualifiedPredicateName>> for OutputPredicateSelection {
    fn from(predicates: Vec<QualifiedPredicateName>) -> Self {
        if predicates.is_empty() {
            Self::AllIDBPredicates
        } else {
            Self::SelectedPredicates(predicates)
        }
    }
}

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
                    .chain(rule.body().iter().map(|literal| literal.predicate()))
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

/// A directive that can occur in the program.
#[derive(Debug)]
pub enum Directive {
    /// Import another source file.
    Import(Box<Path>),
    /// Import another source file with a relative path.
    ImportRelative(Box<Path>),
}

/// A SPARQL query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparqlQuery {
    /// The SPARQL endpoint, should be an IRI.
    endpoint: String,
    /// The projection clause (the list of variables to select) for the SPARQL query.
    projection: String,
    /// The actual query.
    query: String,
}

impl SparqlQuery {
    /// Construct a new SPARQL query.
    pub fn new(endpoint: String, projection: String, query: String) -> Self {
        Self {
            endpoint,
            projection,
            query,
        }
    }

    /// Get the endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        self.endpoint.as_ref()
    }

    /// Get the projection clause.
    #[must_use]
    pub fn projection(&self) -> &str {
        self.projection.as_ref()
    }

    /// Get the query part.
    #[must_use]
    pub fn query(&self) -> &str {
        self.query.as_ref()
    }

    /// Format a full SPARQL query.
    #[must_use]
    pub fn format_query(&self) -> String {
        format!("SELECT {} WHERE {{ {} }}", self.projection, self.query)
    }
}

/// An external data source.
#[derive(Clone, PartialEq, Eq)]
pub enum DataSource {
    /// A DSV (delimiter-separated values) file data source with the given path and delimiter.
    DsvFile {
        /// the DSV file
        file: Box<PathBuf>,
        /// the delimiter separating the values
        delimiter: u8,
    },
    /// An RDF file data source with the given path.
    RdfFile(Box<PathBuf>),
    /// A SPARQL query data source.
    SparqlQuery(Box<SparqlQuery>),
}

impl DataSource {
    /// Construct a new CSV file data source from a given path.
    pub fn csv_file(path: &str) -> Result<Self, ParseError> {
        Ok(Self::DsvFile {
            file: Box::new(PathBuf::from(path)),
            delimiter: b',',
        })
    }

    /// Construct a new TSV file data source from a given path.
    pub fn tsv_file(path: &str) -> Result<Self, ParseError> {
        Ok(Self::DsvFile {
            file: Box::new(PathBuf::from(path)),
            delimiter: b'\t',
        })
    }

    /// Construct a new RDF file data source from a given path.
    pub fn rdf_file(path: &str) -> Result<Self, ParseError> {
        Ok(Self::RdfFile(Box::new(PathBuf::from(path))))
    }

    /// Construct a new SPARQL query data source from a given query.
    pub fn sparql_query(query: SparqlQuery) -> Result<Self, ParseError> {
        Ok(Self::SparqlQuery(Box::new(query)))
    }

    /// Get the logical types that should be used for columns in the datasource if no type declaration is given explicitely
    pub fn default_type(&self) -> LogicalTypeEnum {
        match self {
            Self::DsvFile { .. } => LogicalTypeEnum::String,
            Self::RdfFile(_) => LogicalTypeEnum::Any,
            Self::SparqlQuery(_) => LogicalTypeEnum::Any,
        }
    }
}

impl Debug for DataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DsvFile {
                file,
                delimiter: b',',
            } => f.debug_tuple("CSV file").field(file).finish(),
            Self::DsvFile {
                file,
                delimiter: b'\t',
            } => f.debug_tuple("TSV file").field(file).finish(),
            Self::DsvFile { file, delimiter } => {
                let description = format!("DSV file with delimiter {delimiter:?}");
                f.debug_tuple(&description).field(file).finish()
            }
            Self::RdfFile(arg0) => f.debug_tuple("RDF file").field(arg0).finish(),
            Self::SparqlQuery(arg0) => f.debug_tuple("SparqlQuery").field(arg0).finish(),
        }
    }
}

/// A Data source declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSourceDeclaration {
    pub(crate) predicate: Identifier,
    pub(crate) arity: usize,
    pub(crate) input_types: Vec<LogicalTypeEnum>,
    pub(crate) source: DataSource,
}

impl DataSourceDeclaration {
    /// Construct a new data source declaration.
    pub fn new(predicate: Identifier, arity_or_types: ArityOrTypes, source: DataSource) -> Self {
        let arity = arity_or_types.arity();

        Self {
            predicate,
            arity,
            input_types: arity_or_types
                .types_owned()
                .unwrap_or(vec![source.default_type(); arity]),
            source,
        }
    }

    /// Construct a new data source declaration, validating constraints on, e.g., arity.
    pub(crate) fn new_validated(
        predicate: Identifier,
        arity_or_types: ArityOrTypes,
        source: DataSource,
    ) -> Result<Self, ParseError> {
        let arity = arity_or_types.arity();

        match source {
            DataSource::DsvFile {
                file: _,
                delimiter: _,
            } => (), // no validation for CSV files
            DataSource::RdfFile(ref path) => {
                if arity != 3 {
                    return Err(ParseError::RdfSourceInvalidArity(
                        predicate.name(),
                        path.to_str()
                            .unwrap_or("<path is invalid unicode>")
                            .to_owned(),
                        arity,
                    ));
                }
            }
            DataSource::SparqlQuery(ref query) => {
                let variables = query.projection().split(',').count();
                if variables != arity {
                    return Err(ParseError::SparqlSourceInvalidArity(
                        predicate.name(),
                        variables,
                        arity,
                    ));
                }
            }
        };

        Ok(Self::new(predicate, arity_or_types, source))
    }
}
