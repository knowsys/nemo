use crate::tabular::{table_types::trie::Trie, traits::table::Table};

use super::triescan_project::ProjectReordering;

/// Given a [`Trie`] remove columns and reorder them according to the given
/// [`ProjectReordering`] and return the resulting [`Trie`].
pub fn project_and_reorder(trie: &Trie, project_reordering: &ProjectReordering) -> Trie {
    let trie_as_matrix = trie.as_column_vector();
    let reordered_trie_matrix = project_reordering.transform_consumed(trie_as_matrix);

    Trie::from_cols(reordered_trie_matrix)
}
