//! This module defines a helper function for obtaining a [Hint]
//! that points the user to a similar string exist in a collection of source strings.

use similar_string::find_best_similarity;
use strum::IntoEnumIterator;

use crate::rule_model::components::term::operation::operation_kind::OperationKind;

use super::Hint;

const SIMILARITY_MIN_LENGTH: usize = 3;
const SIMILARITY_THRESHOLD: f64 = 0.6;

impl Hint {
    /// Checks whether a similar string exist in a collection of source strings.
    /// Returns the most similar string, if it meets the threshold.
    pub fn similar<S, Options: IntoIterator<Item = S>>(
        kind: &str,
        target: impl AsRef<str>,
        options: Options,
    ) -> Option<Self>
    where
        S: AsRef<str>,
    {
        if target.as_ref().len() < SIMILARITY_MIN_LENGTH {
            return None;
        }

        let options = options.into_iter().collect::<Vec<_>>();
        let (best, confidence) = find_best_similarity(target, &options)?;

        if best.len() >= SIMILARITY_MIN_LENGTH && confidence >= SIMILARITY_THRESHOLD {
            return Some(Hint::SimilarExists {
                kind: kind.to_string(),
                name: best,
            });
        }

        None
    }

    /// Checks whether a similar string exists within [OperationKind]
    /// and returns the most similar one, if it meets the threshold.
    pub fn similar_operation(target: impl AsRef<str>) -> Option<Self> {
        let options = OperationKind::iter().map(|kind| kind.name());

        Self::similar("operation", target, options)
    }
}
