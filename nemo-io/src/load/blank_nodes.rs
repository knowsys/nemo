//! The blank nodes of a document, as labeled nulls.

use nemo_physical::{
    dictionary::{Dictionary, LabelScope},
    value::DataValue,
};

/// The blank nodes of one read of a document, each a null that its label names
/// in a [label scope](LabelScope).
///
/// A read given a scope names its nulls in that scope, which other reads may share,
/// such as every response of one SPARQL endpoint.
/// Otherwise the read opens a scope of its own, which closes when the read ends.
/// Either way, a label names the same null throughout the document,
/// for example across the graph blocks of a TriG file or across the rows of a response.
#[derive(Debug)]
pub(crate) struct BlankNodes {
    /// The scope in which the labels name nulls.
    scope: LabelScope,
    /// Whether the read opened the scope, and so must close it.
    owned: bool,
}

impl BlankNodes {
    /// Returns the blank nodes of a new read.
    ///
    /// The read names its nulls in the `given` scope,
    /// or, if that is `None`, in a fresh scope called `name`.
    pub(crate) fn open(given: Option<LabelScope>, name: &str, dictionary: &mut Dictionary) -> Self {
        match given {
            Some(scope) => Self {
                scope,
                owned: false,
            },
            None => Self {
                scope: dictionary.fresh_label_scope(name),
                owned: true,
            },
        }
    }

    /// Returns the null that a label names, minting it on the label's first use.
    pub(crate) fn null(&self, label: &str, dictionary: &mut Dictionary) -> DataValue {
        DataValue::null(dictionary.labeled_null(self.scope, label))
    }

    /// Ends the read, closing the scope if the read opened it.
    pub(crate) fn close(&self, dictionary: &mut Dictionary) {
        if self.owned {
            dictionary.close_label_scope(self.scope);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A label names one null within a read,
    /// and the same null in another read only if both share a scope.
    #[test]
    fn labels_name_one_null_each() {
        let mut dictionary = Dictionary::new();
        let blanks = BlankNodes::open(None, "document", &mut dictionary);
        let first = blanks.null("a", &mut dictionary);
        assert_eq!(blanks.null("a", &mut dictionary), first);
        assert_ne!(blanks.null("b", &mut dictionary), first);
        blanks.close(&mut dictionary);

        // Another read names its own nulls, unless both share a scope.
        let other = BlankNodes::open(None, "document", &mut dictionary);
        assert_ne!(other.null("a", &mut dictionary), first);
        let shared = dictionary.shared_label_scope("endpoint");
        let one = BlankNodes::open(Some(shared), "document", &mut dictionary);
        let two = BlankNodes::open(Some(shared), "document", &mut dictionary);
        let null = one.null("a", &mut dictionary);
        one.close(&mut dictionary);
        assert_eq!(two.null("a", &mut dictionary), null);
    }
}
