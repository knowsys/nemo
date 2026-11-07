//! The labels of nulls: the scopes within which a label names one null.
//!
//! Within one open scope a label names one null.
//! A closed scope forgets its labels, and its nulls stay values like any other.
//! A shared scope is found by its name while it is open,
//! and a fresh scope only through the [LabelScope] that opened it.
//! Every null remembers the name of the scope that minted it by a label, if one did,
//! kept per run of consecutively minted nulls rather than per null.
//!
//! A closed scope keeps only its number and the number of its name, which every scope of that
//! name shares, so that many short-lived scopes cost little.

use std::collections::HashMap;

/// A scope of null labels, as [Dictionary::label_scope](super::Dictionary::label_scope)
/// and [Dictionary::fresh_label_scope](super::Dictionary::fresh_label_scope) open it.
///
/// A scope belongs to the dictionary that opened it.
/// Another dictionary may panic on it, or read it as one of its own scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LabelScope(u32);

/// One scope: its name, whether it is found by its name, and its labels while it is open.
#[derive(Debug)]
struct Scope {
    /// The number of the scope's name, which the nulls it minted report.
    name: u32,
    /// Whether [shared](NullLabels::shared) opened the scope, so that it is found by its name.
    shared: bool,
    /// The null each label names, or `None` once the scope closed.
    #[expect(
        clippy::box_collection,
        reason = "a closed scope, of which a long run leaves many, keeps a pointer's width"
    )]
    labels: Option<Box<HashMap<Box<str>, u64>>>,
}

/// The scopes of null labels and the provenance of the nulls minted by a label.
#[derive(Debug, Default)]
pub(super) struct NullLabels {
    /// Every scope opened so far, by its number.
    scopes: Vec<Scope>,
    /// The names of the scopes, by their numbers.
    names: Vec<Box<str>>,
    /// The number of each name.
    numbers: HashMap<Box<str>, u32>,
    /// The open shared scopes, by the number of their name.
    shared: HashMap<u32, u32>,
    /// Per run of consecutively minted nulls, the index of its first null
    /// and the scope that minted its nulls by label, or `None` for nulls minted fresh.
    runs: Vec<(u64, Option<u32>)>,
}

impl NullLabels {
    /// Returns the open scope with the given name, opened if no open scope has it.
    pub(super) fn shared(&mut self, name: &str) -> LabelScope {
        let name = self.name(name);
        if let Some(&number) = self.shared.get(&name) {
            return LabelScope(number);
        }
        let scope = self.open(name, true);
        self.shared.insert(name, scope.0);
        scope
    }

    /// Opens a scope that no other call returns. The name only reports provenance.
    pub(super) fn fresh(&mut self, name: &str) -> LabelScope {
        let name = self.name(name);
        self.open(name, false)
    }

    /// Returns the number of a name, numbered on first sight.
    fn name(&mut self, name: &str) -> u32 {
        if let Some(&number) = self.numbers.get(name) {
            return number;
        }
        let number = u32::try_from(self.names.len()).expect("fewer than 2^32 scope names");
        self.names.push(name.into());
        self.numbers.insert(name.into(), number);
        number
    }

    /// Opens a scope under the number of its name.
    fn open(&mut self, name: u32, shared: bool) -> LabelScope {
        let number = u32::try_from(self.scopes.len()).expect("fewer than 2^32 label scopes");
        self.scopes.push(Scope {
            name,
            shared,
            labels: Some(Box::default()),
        });
        LabelScope(number)
    }

    /// Closes a scope, forgetting its labels. Closing a closed scope does nothing.
    pub(super) fn close(&mut self, scope: LabelScope) {
        let entry = &mut self.scopes[scope.0 as usize];
        if entry.labels.take().is_some() && entry.shared {
            self.shared.remove(&entry.name);
        }
    }

    /// Returns the null a label names in an open scope, if it names one yet.
    ///
    /// # Panics
    ///
    /// Panics if the scope is closed.
    pub(super) fn find(&self, scope: LabelScope, label: &str) -> Option<u64> {
        self.labels(scope).get(label).copied()
    }

    /// Records the null a label names in an open scope.
    ///
    /// # Panics
    ///
    /// Panics if the scope is closed.
    pub(super) fn insert(&mut self, scope: LabelScope, label: &str, id: u64) {
        self.scopes[scope.0 as usize]
            .labels
            .as_mut()
            .expect("a label scope is used only while it is open")
            .insert(label.into(), id);
    }

    /// Returns the labels of an open scope.
    fn labels(&self, scope: LabelScope) -> &HashMap<Box<str>, u64> {
        self.scopes[scope.0 as usize]
            .labels
            .as_ref()
            .expect("a label scope is used only while it is open")
    }

    /// Records that the null of the given index was minted, by a label of a scope or fresh.
    pub(super) fn minted(&mut self, index: u64, scope: Option<LabelScope>) {
        let scope = scope.map(|scope| scope.0);
        if self.runs.last().is_none_or(|&(_, last)| last != scope) {
            self.runs.push((index, scope));
        }
    }

    /// Returns the name of the scope that minted the null of the given index by a label,
    /// or `None` for a null minted fresh.
    pub(super) fn scope_of(&self, index: u64) -> Option<&str> {
        let run = self.runs.partition_point(|&(start, _)| start <= index);
        let (_, scope) = self.runs.get(run.checked_sub(1)?)?;
        scope.map(|number| &*self.names[self.scopes[number as usize].name as usize])
    }

    /// Returns the bytes the scopes, their names and labels and the runs have allocated,
    /// approximately.
    pub(super) fn memory_bytes(&self) -> usize {
        let labels: usize = self
            .scopes
            .iter()
            .filter_map(|scope| scope.labels.as_deref())
            .map(|labels| {
                size_of::<HashMap<Box<str>, u64>>()
                    + labels.capacity() * (size_of::<Box<str>>() + size_of::<u64>() + 1)
                    + labels.keys().map(|label| label.len()).sum::<usize>()
            })
            .sum();
        let names: usize = self.names.iter().map(|name| 2 * name.len()).sum();
        self.scopes.capacity() * size_of::<Scope>()
            + self.names.capacity() * size_of::<Box<str>>()
            + self.numbers.capacity() * (size_of::<Box<str>>() + size_of::<u32>() + 1)
            + self.shared.capacity() * (2 * size_of::<u32>() + 1)
            + self.runs.capacity() * size_of::<(u64, Option<u32>)>()
            + names
            + labels
    }
}
