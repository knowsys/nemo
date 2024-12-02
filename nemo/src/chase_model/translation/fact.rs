//! This module defines a function for translating
//! logical facts into chase facts.

use crate::{
    chase_model::components::{atom::ground_atom::GroundAtom, ChaseComponent},
    rule_model::components::{
        term::{primitive::Primitive, Term},
        ProgramComponent,
    },
};

use super::ProgramChaseTranslation;

impl ProgramChaseTranslation {
    /// Translate a [Fact][crate::rule_model::components::fact::Fact]
    /// into a [GroundAtom].
    ///
    /// # Panics
    /// Panics if the facts contains non-primitive terms or variables.
    pub(crate) fn build_fact(
        &mut self,
        fact: &crate::rule_model::components::fact::Fact,
    ) -> GroundAtom {
        let origin = *fact.origin();
        let predicate = fact.predicate().clone();
        let mut terms = Vec::new();

        for term in fact.subterms() {
            if let Term::Primitive(Primitive::Ground(value)) = term {
                terms.push(value.clone());
            } else {
                unreachable!("invalid program: fact contains non-primitive values")
            }
        }

        self.predicate_arity.insert(predicate.clone(), terms.len());

        GroundAtom::new(predicate, terms).set_origin(origin)
    }
}
