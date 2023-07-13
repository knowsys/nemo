use std::fmt::Debug;

use nemo_physical::table_reader::Resource;

use crate::{
    io::parser::ParseError,
    model::{PrimitiveType, TupleConstraint, TypeConstraint},
};

use super::Identifier;

/// Trait capturing capabilities of data sources
pub trait DataSource {
    /// Get the logical types that should be used for columns in the datasource if no type declaration is given explicitly
    fn input_types(&self) -> TupleConstraint;

    /// Get a uri for this resource
    fn resources(&self) -> Vec<Resource>;
}

/// A Delimiter-separated values file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DsvFile {
    /// the DSV resource
    pub resource: Resource,
    /// the delimiter separating the values
    pub delimiter: u8,
    /// Input Types
    input_types: TupleConstraint,
}

impl DsvFile {
    const DEFAULT_COLUMN_TYPE: PrimitiveType = PrimitiveType::String;

    /// Construct a new DSV file data source from a given path.
    pub fn new(path: &str, delimiter: u8, input_types: TupleConstraint) -> Self {
        Self {
            resource: path.to_string(),
            delimiter,
            input_types: input_types
                .iter()
                .map(|tc| match tc {
                    TypeConstraint::None => TypeConstraint::AtLeast(Self::DEFAULT_COLUMN_TYPE),
                    _ => tc.clone(),
                })
                .collect(),
        }
    }

    /// Construct a new CSV file data source from a given path.
    pub fn csv_file(path: &str, input_types: TupleConstraint) -> Self {
        Self::new(path, b',', input_types)
    }

    /// Construct a new TSV file data source from a given path.
    pub fn tsv_file(path: &str, input_types: TupleConstraint) -> Self {
        Self::new(path, b'\t', input_types)
    }
}

impl DataSource for DsvFile {
    fn input_types(&self) -> TupleConstraint {
        self.input_types.clone()
    }

    fn resources(&self) -> Vec<Resource> {
        vec![self.resource.clone()]
    }
}

/// An RDF file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdfFile {
    /// the RDF resource
    pub resource: Resource,
    /// the optional base IRI
    pub base: Option<String>,
}

impl RdfFile {
    const ARITY: usize = 3;

    /// Construct a new Rdf Source
    pub fn new(path: &str, base: Option<String>) -> Self {
        Self {
            resource: path.to_string(),
            base,
        }
    }

    // TODO: it should not be possible to specify types or arities for Rdf sources;
    // this will change in the future
    pub(crate) fn new_validated(
        path: &str,
        base: Option<String>,
        predicate: &Identifier,
        tuple_constraint: TupleConstraint,
    ) -> Result<Self, ParseError> {
        let arity = tuple_constraint.arity();

        if arity != Self::ARITY {
            return Err(ParseError::RdfSourceInvalidArity(
                predicate.name(),
                path.to_string(),
                arity,
            ));
        }

        Ok(Self::new(path, base))
    }
}

impl DataSource for RdfFile {
    fn input_types(&self) -> TupleConstraint {
        TupleConstraint::from_arity(Self::ARITY)
            .iter()
            .map(|_| TypeConstraint::AtLeast(PrimitiveType::Any))
            .collect()
    }

    fn resources(&self) -> Vec<Resource> {
        vec![self.resource.clone()]
    }
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

    // TODO: it should not be possible to specify types or arities for Rdf sources;
    // this will change in the future
    pub(crate) fn new_validated(
        endpoint: String,
        projection: String,
        query: String,
        predicate: &Identifier,
        tuple_constraint: TupleConstraint,
    ) -> Result<Self, ParseError> {
        let new_self = Self::new(endpoint, projection, query);

        let target_arity = new_self.arity();
        let arity = tuple_constraint.arity();

        if target_arity != arity {
            return Err(ParseError::SparqlSourceInvalidArity(
                predicate.name(),
                target_arity,
                arity,
            ));
        }

        Ok(new_self)
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

    /// Get Arity of SparqlQuery
    #[must_use]
    pub fn arity(&self) -> usize {
        self.projection().split(',').count()
    }
}

impl DataSource for SparqlQuery {
    fn input_types(&self) -> TupleConstraint {
        TupleConstraint::from_arity(self.arity())
            .iter()
            .map(|_| TypeConstraint::AtLeast(PrimitiveType::Any))
            .collect()
    }

    fn resources(&self) -> Vec<Resource> {
        vec![]
    }
}

/// An external data source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataSourceT {
    /// A DSV (delimiter-separated values) resource data source with the given path and delimiter.
    DsvFile(DsvFile),
    /// An RDF file data source with the given path and optional base IRI.
    RdfFile(RdfFile),
    /// A SPARQL query data source.
    SparqlQuery(SparqlQuery),
}

impl DataSource for DataSourceT {
    fn input_types(&self) -> TupleConstraint {
        match self {
            Self::DsvFile(d) => d.input_types(),
            Self::RdfFile(r) => r.input_types(),
            Self::SparqlQuery(s) => s.input_types(),
        }
    }

    fn resources(&self) -> Vec<Resource> {
        match self {
            Self::DsvFile(d) => d.resources(),
            Self::RdfFile(r) => r.resources(),
            Self::SparqlQuery(s) => s.resources(),
        }
    }
}

/// A Data source declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSourceDeclaration {
    pub(crate) predicate: Identifier,
    pub(crate) source: DataSourceT,
}

impl DataSourceDeclaration {
    /// Construct a new data source declaration.
    pub(crate) fn new(predicate: Identifier, source: DataSourceT) -> Self {
        Self { predicate, source }
    }
}

impl DataSource for DataSourceDeclaration {
    fn input_types(&self) -> TupleConstraint {
        self.source.input_types()
    }

    fn resources(&self) -> Vec<Resource> {
        self.source.resources()
    }
}
