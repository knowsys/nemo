//! The data model.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    ops::Neg,
    path::{Path, PathBuf},
};

use crate::{
    generate_forwarder,
    io::parser::{ParseError, RuleParser},
    physical::{
        datatypes::{DataValueT, Double},
        dictionary::Dictionary,
    },
};

/// An identifier for, e.g., a Term or a Predicate.
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone, PartialOrd, Ord)]
pub struct Identifier(pub(crate) usize);

impl Identifier {
    /// Make the [`Identifier`] pretty-printable using the given
    /// [`Dictionary`].
    pub fn format<'a, 'b, Dict: Dictionary>(
        &'a self,
        dictionary: &'b Dict,
    ) -> PrintableIdentifier<'b, Dict>
    where
        'a: 'b,
    {
        PrintableIdentifier {
            identifier: *self,
            dictionary,
        }
    }

    /// TODO(mx): ugly hack, this needs to go.
    pub fn to_constant_u64(self) -> u64 {
        self.0 as u64
    }
}

/// A pretty-printable identifier that can be resolved using the dictionary.
#[derive(Debug)]
pub struct PrintableIdentifier<'a, Dict: Dictionary> {
    identifier: Identifier,
    dictionary: &'a Dict,
}

impl<Dict: Dictionary> Display for PrintableIdentifier<'_, Dict> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ident = self.identifier.0;
        write!(
            f,
            "{}",
            self.dictionary
                .entry(ident)
                .unwrap_or_else(|| format!("<unresolved identifier {ident}>"))
        )
    }
}

/// Variable occuring in a rule
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone, PartialOrd, Ord)]
pub enum Variable {
    /// A universally quantified variable.
    Universal(Identifier),
    /// An existentially quantified variable.
    Existential(Identifier),
}

/// Terms occurring in programs.
#[derive(Debug, Eq, PartialEq, Copy, Clone, PartialOrd, Ord)]
pub enum Term {
    /// An (abstract) constant.
    Constant(Identifier),
    /// A variable.
    Variable(Variable),
    /// A numeric literal.
    NumericLiteral(NumericLiteral),
    /// An RDF literal.
    RdfLiteral(RdfLiteral),
}

impl Term {
    /// Check if the term is ground.
    pub fn is_ground(&self) -> bool {
        matches!(
            self,
            Self::Constant(_) | Self::NumericLiteral(_) | Self::RdfLiteral(_)
        )
    }

    // TODO: Not sure if this is a sane way to do it, some discussion needed
    /// Coverts term to DataValueT
    pub fn to_datavalue_t(&self) -> Option<DataValueT> {
        match self {
            Self::Constant(Identifier(i)) => Some(DataValueT::U64((*i).try_into().ok()?)),
            Self::Variable(_) => unreachable!(),
            Self::NumericLiteral(n) => match n {
                NumericLiteral::Integer(i) => Some(DataValueT::U64((*i).try_into().ok()?)),
                NumericLiteral::Decimal(_, _) => todo!(),
                NumericLiteral::Double(d) => Some(DataValueT::Double(*d)),
            },
            Self::RdfLiteral(_) => todo!(),
        }
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

/// An RDF literal.
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone, PartialOrd, Ord)]
pub enum RdfLiteral {
    /// A language string.
    LanguageString {
        /// The literal value.
        value: usize,
        /// The language tag.
        tag: usize,
    },
    /// A literal with a datatype.
    DatatypeValue {
        /// The literal value.
        value: usize,
        /// The datatype IRI.
        datatype: usize,
    },
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
        self.predicate
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
    pub fn variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.terms().iter().filter_map(|&term| match term {
            Term::Variable(var) => Some(var),
            _ => None,
        })
    }

    /// Return all universally quantified variables in the atom.
    pub fn universal_variables(&self) -> impl Iterator<Item = Identifier> + '_ {
        self.variables().filter_map(|var| match var {
            Variable::Universal(identifier) => Some(identifier),
            _ => None,
        })
    }

    /// Return all existentially quantified variables in the atom.
    pub fn existential_variables(&self) -> impl Iterator<Item = Identifier> + '_ {
        self.variables().filter_map(|var| match var {
            Variable::Existential(identifier) => Some(identifier),
            _ => None,
        })
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
    pub fn variables(&self) -> impl Iterator<Item = Variable> + '_ {
        forward_to_atom!(self, variables)
    }

    /// Return the universally quantified variables in the literal.
    pub fn universal_variables(&self) -> impl Iterator<Item = Identifier> + '_ {
        forward_to_atom!(self, universal_variables)
    }

    /// Return the existentially quantified variables in the literal.
    pub fn existential_variables(&self) -> impl Iterator<Item = Identifier> + '_ {
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

/// Filter of the form <variable> <operation> <term>
#[derive(Debug, Eq, PartialEq, Clone, Copy, PartialOrd, Ord)]
pub struct Filter {
    /// Operation to be performed
    pub operation: FilterOperation,
    /// Left-hand side
    pub left: Variable,
    /// Right-hand side
    pub right: Term,
}

impl Filter {
    /// Creates a new [`Filter`]
    pub fn new(operation: FilterOperation, left: Variable, right: Term) -> Self {
        Self {
            operation,
            left,
            right,
        }
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
    pub(crate) fn new_validated<Dict: Dictionary>(
        head: Vec<Atom>,
        body: Vec<Literal>,
        filters: Vec<Filter>,
        parser: &RuleParser<Dict>,
    ) -> Result<Self, ParseError> {
        // Check if existential variables occur in the body.
        let existential_variables = body
            .iter()
            .flat_map(|literal| literal.existential_variables())
            .collect::<Vec<_>>();

        if !existential_variables.is_empty() {
            return Err(ParseError::BodyExistential(
                parser
                    .resolve_identifier(existential_variables.first().expect("is not empty here"))
                    .expect("identifier has been parsed, so must be known"),
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
                parser
                    .resolve_identifier(negative_variables.first().expect("is not empty here"))
                    .expect("identifier has been parsed, so must be known"),
            ));
        }

        // Check if a variable occurs with both existential and universal quantification.
        let mut universal_variables = body
            .iter()
            .flat_map(|literal| literal.universal_variables())
            .collect::<HashSet<_>>();
        universal_variables.extend(head.iter().flat_map(|atom| atom.universal_variables()));
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
                parser
                    .resolve_identifier(common_variables.first().expect("is not empty here"))
                    .expect("identifier has been parsed, so must be known"),
            ));
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

/// A full program.
#[derive(Debug)]
pub struct Program<Dict: Dictionary> {
    base: Option<usize>,
    prefixes: HashMap<String, usize>,
    sources: Vec<DataSourceDeclaration>,
    rules: Vec<Rule>,
    facts: Vec<Fact>,

    dict_names: Dict,
    dict_constants: Dict,
}

impl<Dict: Dictionary> Program<Dict> {
    /// Construct a new program.
    pub fn new(
        base: Option<usize>,
        prefixes: HashMap<String, usize>,
        sources: Vec<DataSourceDeclaration>,
        rules: Vec<Rule>,
        facts: Vec<Fact>,
        dict_names: Dict,
        dict_constants: Dict,
    ) -> Self {
        Self {
            base,
            prefixes,
            sources,
            rules,
            facts,
            dict_names,
            dict_constants,
        }
    }

    /// Get the base IRI, if set.
    #[must_use]
    pub fn base(&self) -> Option<usize> {
        self.base
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
            .copied()
            .collect()
    }

    /// Return all prefixes in the program.
    #[must_use]
    pub fn prefixes(&self) -> &HashMap<String, usize> {
        &self.prefixes
    }

    /// Return all data sources in the program.
    pub fn sources(&self) -> impl Iterator<Item = ((Identifier, usize), &DataSource)> {
        self.sources
            .iter()
            .map(|source| ((source.predicate, source.arity), &source.source))
    }

    /// Look up a given prefix.
    #[must_use]
    pub fn resolve_prefix(&self, tag: &str) -> Option<usize> {
        self.prefixes.get(tag).copied()
    }

    /// Return the dictionary for translating names of constants
    pub fn get_dict_constants(&self) -> &Dict {
        &self.dict_constants
    }

    /// Return the dictionary for translating e.g. predicate names
    pub fn get_dict_names(&self) -> &Dict {
        &self.dict_names
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataSource {
    /// A CSV file data source with the given path.
    CsvFile(Box<PathBuf>),
    /// An RDF file data source with the given path.
    RdfFile(Box<PathBuf>),
    /// A SPARQL query data source.
    SparqlQuery(Box<SparqlQuery>),
}

impl DataSource {
    /// Construct a new CSV file data source from a given path.
    pub fn csv_file(path: &str) -> Result<Self, ParseError> {
        Ok(Self::CsvFile(Box::new(PathBuf::from(path))))
    }

    /// Construct a new RDF file data source from a given path.
    pub fn rdf_file(path: &str) -> Result<Self, ParseError> {
        Ok(Self::RdfFile(Box::new(PathBuf::from(path))))
    }

    /// Construct a new SPARQL query data source from a given query.
    pub fn sparql_query(query: SparqlQuery) -> Result<Self, ParseError> {
        Ok(Self::SparqlQuery(Box::new(query)))
    }
}

/// A Data source declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSourceDeclaration {
    predicate: Identifier,
    arity: usize,
    source: DataSource,
}

impl DataSourceDeclaration {
    /// Construct a new data source declaration.
    pub fn new(predicate: Identifier, arity: usize, source: DataSource) -> Self {
        Self {
            predicate,
            arity,
            source,
        }
    }

    /// Construct a new data source declaration, validating constraints on, e.g., arity.
    pub(crate) fn new_validated<Dict: Dictionary>(
        predicate: Identifier,
        arity: usize,
        source: DataSource,
        parser: &RuleParser<Dict>,
    ) -> Result<Self, ParseError> {
        match source {
            DataSource::CsvFile(_) => (), // no validation for CSV files
            DataSource::RdfFile(ref path) => {
                if arity != 3 {
                    return Err(ParseError::RdfSourceInvalidArity(
                        parser
                            .resolve_identifier(&predicate)
                            .expect("predicate has been parsed, must be known"),
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
                        parser
                            .resolve_identifier(&predicate)
                            .expect("predicate has been parsed, must be known"),
                        variables,
                        arity,
                    ));
                }
            }
        };

        Ok(Self {
            predicate,
            arity,
            source,
        })
    }
}
