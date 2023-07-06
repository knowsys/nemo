use std::fmt::Debug;

use nemo_physical::table_reader::Resource;

use crate::{
    io::parser::ParseError,
    model::{PrimitiveType, TupleConstraint, TypeConstraint},
};

use super::Identifier;

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
    /// A DSV (delimiter-separated values) resource data source with the given path and delimiter.
    DsvFile {
        /// the DSV resource
        resource: Resource,
        /// the delimiter separating the values
        delimiter: u8,
    },
    /// An RDF file data source with the given path and optional base IRI.
    RdfFile {
        /// the RDF resource
        resource: Resource,
        /// the optional base IRI
        base: Option<String>,
    },
    /// A SPARQL query data source.
    SparqlQuery(Box<SparqlQuery>),
}

impl DataSource {
    /// Construct a new CSV file data source from a given path.
    pub fn csv_file(path: &str) -> Result<Self, ParseError> {
        Ok(Self::DsvFile {
            resource: String::from(path),
            delimiter: b',',
        })
    }

    /// Construct a new TSV file data source from a given path.
    pub fn tsv_file(path: &str) -> Result<Self, ParseError> {
        Ok(Self::DsvFile {
            resource: String::from(path),
            delimiter: b'\t',
        })
    }

    /// Construct a new RDF file data source from a given path.
    pub fn rdf_file(path: &str) -> Result<Self, ParseError> {
        Self::rdf_file_with_base(path, None)
    }

    /// Construct a new RDF file data source from a given path and base IRI.
    pub fn rdf_file_with_base(path: &str, base: Option<String>) -> Result<Self, ParseError> {
        Ok(Self::RdfFile {
            resource: String::from(path),
            base,
        })
    }

    /// Construct a new SPARQL query data source from a given query.
    pub fn sparql_query(query: SparqlQuery) -> Result<Self, ParseError> {
        Ok(Self::SparqlQuery(Box::new(query)))
    }

    /// Get the logical types that should be used for columns in the datasource if no type declaration is given explicitly
    pub fn default_type(&self) -> PrimitiveType {
        match self {
            Self::DsvFile { .. } => PrimitiveType::String,
            Self::RdfFile { .. } => PrimitiveType::Any,
            Self::SparqlQuery(_) => PrimitiveType::Any,
        }
    }

    /// Check whether this data source supports a given arity
    pub fn check_arity(&self, predicate: &Identifier, arity: usize) -> Result<(), ParseError> {
        match self {
            DataSource::DsvFile { .. } => (), // no validity checks
            DataSource::RdfFile { resource, .. } => {
                if arity != 3 {
                    return Err(ParseError::RdfSourceInvalidArity(
                        predicate.name(),
                        resource.clone(),
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

        Ok(())
    }

    /// Get a uri for this resource
    pub fn resources(&self) -> Vec<Resource> {
        match self {
            DataSource::DsvFile { resource, .. } => vec![resource.clone()],
            DataSource::RdfFile { resource, .. } => vec![resource.clone()],
            DataSource::SparqlQuery(_) => vec![],
        }
    }
}

impl Debug for DataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DsvFile {
                resource: file,
                delimiter: b',',
            } => f.debug_tuple("CSV file").field(file).finish(),
            Self::DsvFile {
                resource: file,
                delimiter: b'\t',
            } => f.debug_tuple("TSV file").field(file).finish(),
            Self::DsvFile {
                resource: file,
                delimiter,
            } => {
                let description = format!("DSV file with delimiter {delimiter:?}");
                f.debug_tuple(&description).field(file).finish()
            }
            Self::RdfFile {
                resource,
                base: None,
            } => f.debug_tuple("RDF file").field(resource).finish(),
            Self::RdfFile {
                resource,
                base: Some(base),
            } => f
                .debug_tuple(&format!("RDF file with base {base:?}"))
                .field(resource)
                .finish(),
            Self::SparqlQuery(arg0) => f.debug_tuple("SparqlQuery").field(arg0).finish(),
        }
    }
}

/// A Data source declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSourceDeclaration {
    pub(crate) predicate: Identifier,
    pub(crate) type_constraint: TupleConstraint,
    pub(crate) source: DataSource,
}

impl DataSourceDeclaration {
    /// Construct a new data source declaration.
    pub(crate) fn new(
        predicate: Identifier,
        type_constraint: TupleConstraint,
        source: DataSource,
    ) -> Self {
        let type_constraint = type_constraint
            .iter()
            .map(|c| match c {
                TypeConstraint::None => TypeConstraint::AtLeast(source.default_type()),
                c => c.clone(),
            })
            .collect();

        Self {
            predicate,
            type_constraint,
            source,
        }
    }

    /// Get the resources specified in this source declaration.
    pub fn resources(&self) -> Vec<Resource> {
        self.source.resources()
    }

    /// Construct a new data source declaration, validating constraints on, e.g., arity.
    pub(crate) fn new_validated(
        predicate: Identifier,
        type_constraint: TupleConstraint,
        source: DataSource,
    ) -> Result<Self, ParseError> {
        source.check_arity(&predicate, type_constraint.arity())?;

        Ok(Self::new(predicate, type_constraint, source))
    }
}
