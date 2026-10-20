//! Ground terms as SPARQL terms.
//!
//! A term is evaluated to its value, which `nemo_io` writes as a SPARQL term.

use nemo_io::sparql::spargebra::term::GroundTerm;
use nemo_physical::value::DataValue;

use crate::{
    engine::transform::sparql_rewrite::context::RewriteContext,
    model::{TermKind, VariableKind},
};

impl RewriteContext {
    /// Returns a ground term as a SPARQL term,
    /// or `None` if a query cannot carry it:
    /// a term without a value, or a value without an RDF form.
    pub(super) fn ground_term(&self, kind: &TermKind) -> Option<GroundTerm> {
        nemo_io::sparql::SparqlQuery::ground_term(&self.value(kind)?)
    }

    /// Returns the value of a ground term:
    /// constants are resolved in the scope, parameters to their declared values.
    ///
    /// Returns `None` for structured and non-ground terms.
    fn value(&self, kind: &TermKind) -> Option<DataValue> {
        match kind {
            TermKind::Constant(name) => Some(DataValue::iri(self.scope.resolve(name, None).ok()?)),
            TermKind::String(text) => Some(DataValue::string(text.as_str())),
            TermKind::LanguageString { text, language } => {
                DataValue::language_string(text.as_str(), language)
            }
            TermKind::Boolean(value) => Some(DataValue::boolean(*value)),
            TermKind::Number(value) => Some(DataValue::from(value.clone())),
            TermKind::RdfLiteral { lexical, datatype } => {
                let datatype = self.scope.resolve(datatype, None).ok()?;
                DataValue::typed_literal(&datatype, lexical)
            }
            TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
                self.value(&self.parameters.get(&variable.name)?.kind)
            }
            _ => None,
        }
    }
}
