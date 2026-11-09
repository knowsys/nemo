//! Ground terms of a rule as the SPARQL terms in which an endpoint may store their values.

use nemo_io::sparql::spargebra::term::GroundTerm;
use nemo_physical::value::DataValue;

use crate::{
    engine::transform::sparql_rewrite::context::RewriteContext,
    model::{TermKind, VariableKind},
};

impl RewriteContext {
    /// Returns a ground term as the SPARQL terms in which an endpoint may store its value
    /// ([ground_terms](nemo_io::sparql::SparqlQuery::ground_terms)),
    /// or `None` if a query cannot contain it:
    /// for a term without a value, or a value without an RDF form.
    pub(super) fn ground_terms(&self, kind: &TermKind) -> Option<Vec<GroundTerm>> {
        let terms = nemo_io::sparql::SparqlQuery::ground_terms(&self.value(kind)?);
        (!terms.is_empty()).then_some(terms)
    }

    /// Returns the value of a ground term.
    ///
    /// Constants are resolved in the scope, and parameters to their declared values.
    /// Returns `None` for any other term, such as a structured term, an operation or a blank
    /// node.
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
