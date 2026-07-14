//! This module defines [TransformationN3Default].

pub mod builtins;
pub mod split_triples;

use std::path::PathBuf;

use builtins::TransformationN3Builtins;
use nemo_physical::datavalues::{AnyDataValue, DataValue};
use split_triples::TransformationN3SplitTriples;

use crate::{
    execution::execution_parameters::ExecutionParameters,
    rule_model::{
        components::{
            import_export::{
                ImportDirective, attribute::ImportExportAttribute, specification::ImportExportSpec,
            },
            tag::Tag,
            term::{
                Term,
                primitive::{Primitive, ground::GroundTerm},
            },
        },
        error::ValidationReport,
        programs::{ProgramWrite, handle::ProgramHandle},
        translation::directive::FormatContext,
    },
    syntax::n3::translation::IMPORTS_PREDICATE,
};

use super::{ProgramTransformation, default::TransformationDefault};

/// Default transformation
///
/// This transformation will be applied to every nemo program
/// before executing
#[derive(Debug, Clone)]
pub struct TransformationN3Default<'a> {
    /// Execution Parameters
    parameters: &'a ExecutionParameters,
    /// Extra triples to import
    extra_triples: Option<PathBuf>,
}

impl<'a> TransformationN3Default<'a> {
    /// Create a new [TransformationN3Default].
    pub fn new(extra_triples: Option<PathBuf>, parameters: &'a ExecutionParameters) -> Self {
        Self {
            extra_triples,
            parameters,
        }
    }
}

impl ProgramTransformation for TransformationN3Default<'_> {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork_full();

        if let Some(extra_triples) = &self.extra_triples {
            commit.add_import(ImportDirective::new(
                Tag::new(IMPORTS_PREDICATE.to_string()),
                ImportExportSpec::new(
                    "rdf",
                    vec![(
                        ImportExportAttribute::from("resource"),
                        Term::Primitive(Primitive::Ground(GroundTerm::new(
                            AnyDataValue::new_plain_string(
                                extra_triples
                                    .clone()
                                    .into_os_string()
                                    .into_string()
                                    .expect("path is valid UTF-8"),
                            ),
                        ))),
                    )],
                ),
                vec![],
                FormatContext::default(),
            ))
        }

        commit
            .submit()?
            .transform(TransformationN3Builtins::default())?
            .transform(TransformationN3SplitTriples::new(
                self.extra_triples.is_some(),
            ))?
            .transform(TransformationDefault::new(self.parameters))
    }
}

fn tag_from_term(term: &Term) -> Option<Tag> {
    if let Term::Primitive(primitive) = term
        && let Primitive::Ground(ground) = primitive
    {
        return ground.value().to_iri().map(Tag::new);
    }

    None
}
