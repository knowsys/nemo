//! The labels of nulls, and the scopes within which a label names one null.
//!
//! Within one open scope, a label names one null.
//! A closed scope forgets its labels, and its nulls remain values like any other.
//! A *shared* scope can be found by its name while it is open.
//! A *fresh* scope can be used only through the [LabelScope] that opened it.
//!
//! The name of the scope that minted each labeled null is recorded
//! once per range of consecutively minted nulls, rather than once per null.
//! A closed scope keeps only its number and the number of its name,
//! which all scopes of that name share, so that many short-lived scopes cost little.

use std::collections::HashMap;

/// A scope of null labels, opened by
/// [Dictionary::shared_label_scope](super::Dictionary::shared_label_scope) or
/// [Dictionary::fresh_label_scope](super::Dictionary::fresh_label_scope).
///
/// A scope belongs to the dictionary that opened it.
/// Another dictionary may panic on it, or mistake it for one of its own scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LabelScope(u32);

/// One scope, with its name, its kind, and its labels while it is open.
#[derive(Debug)]
struct ScopeEntry {
    /// The number of the scope's name.
    name: u32,
    /// Whether [shared](NullLabels::shared) opened the scope, so that it is found by its name.
    shared: bool,
    /// The id of the null that each label names, or `None` once the scope is closed.
    #[expect(
        clippy::box_collection,
        reason = "a closed scope, of which a long run leaves many, keeps a pointer's width"
    )]
    labels: Option<Box<HashMap<Box<str>, u64>>>,
}

/// The scopes of null labels, and the record of which scope minted each labeled null.
#[derive(Debug, Default)]
pub(super) struct NullLabels {
    /// Every scope opened so far, by its number.
    scopes: Vec<ScopeEntry>,
    /// The distinct scope names, by their numbers.
    names: Vec<Box<str>>,
    /// The number of each name.
    numbers: HashMap<Box<str>, u32>,
    /// The open shared scopes, by the number of their name.
    shared: HashMap<u32, LabelScope>,
    /// The ranges of consecutively minted nulls with the same scope, in minting order.
    ranges: Vec<MintedRange>,
}

/// A range of consecutively minted nulls that one scope's labels named,
/// or that were all minted fresh.
///
/// The range ends where the next range begins.
#[derive(Debug, Clone, Copy)]
struct MintedRange {
    /// The index of the range's first null.
    first: u64,
    /// The scope whose labels named the range's nulls, or `None` for fresh nulls.
    scope: Option<LabelScope>,
}

impl NullLabels {
    /// Returns the open shared scope with the given name, and opens one if there is none.
    pub(super) fn shared(&mut self, name: &str) -> LabelScope {
        let name = self.intern_name(name);
        if let Some(&scope) = self.shared.get(&name) {
            return scope;
        }
        let scope = self.open(name, true);
        self.shared.insert(name, scope);
        scope
    }

    /// Opens a new scope, which no other call returns.
    ///
    /// The name serves only to report which scope minted a null.
    pub(super) fn fresh(&mut self, name: &str) -> LabelScope {
        let name = self.intern_name(name);
        self.open(name, false)
    }

    /// Returns the number of a name, and numbers the name if it is new.
    fn intern_name(&mut self, name: &str) -> u32 {
        if let Some(&number) = self.numbers.get(name) {
            return number;
        }
        let number = u32::try_from(self.names.len()).expect("fewer than 2^32 scope names");
        self.names.push(name.into());
        self.numbers.insert(name.into(), number);
        number
    }

    /// Opens a scope whose name has the given number.
    ///
    /// `shared` tells whether the scope is shared or fresh.
    fn open(&mut self, name: u32, shared: bool) -> LabelScope {
        let number = u32::try_from(self.scopes.len()).expect("fewer than 2^32 label scopes");
        self.scopes.push(ScopeEntry {
            name,
            shared,
            labels: Some(Box::default()),
        });
        LabelScope(number)
    }

    /// Closes a scope, which forgets its labels.
    ///
    /// Closing a closed scope does nothing.
    pub(super) fn close(&mut self, scope: LabelScope) {
        let entry = &mut self.scopes[scope.0 as usize];
        if entry.labels.take().is_some() && entry.shared {
            self.shared.remove(&entry.name);
        }
    }

    /// Returns the id of the null that a label names in an open scope,
    /// or `None` if the label names none yet.
    ///
    /// # Panics
    ///
    /// Panics if the scope is closed.
    pub(super) fn find(&self, scope: LabelScope, label: &str) -> Option<u64> {
        self.labels(scope).get(label).copied()
    }

    /// Records the id of the null that a label names in an open scope.
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
    ///
    /// Panics if the scope is closed.
    fn labels(&self, scope: LabelScope) -> &HashMap<Box<str>, u64> {
        self.scopes[scope.0 as usize]
            .labels
            .as_ref()
            .expect("a label scope is used only while it is open")
    }

    /// Records that the null with the given index was minted.
    ///
    /// `scope` is the scope whose label names the null, or `None` for a fresh null.
    /// Nulls must be recorded in minting order.
    pub(super) fn minted(&mut self, index: u64, scope: Option<LabelScope>) {
        if self.ranges.last().is_none_or(|range| range.scope != scope) {
            self.ranges.push(MintedRange {
                first: index,
                scope,
            });
        }
    }

    /// Returns the name of the scope whose label named the null with the given index,
    /// or `None` for a fresh null.
    pub(super) fn scope_of(&self, index: u64) -> Option<&str> {
        let range = self.ranges.partition_point(|range| range.first <= index);
        let scope = self.ranges.get(range.checked_sub(1)?)?.scope?;
        Some(&*self.names[self.scopes[scope.0 as usize].name as usize])
    }

    /// Returns an estimate of the bytes allocated for the scopes, their names and labels,
    /// and the ranges.
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
        self.scopes.capacity() * size_of::<ScopeEntry>()
            + self.names.capacity() * size_of::<Box<str>>()
            + self.numbers.capacity() * (size_of::<Box<str>>() + size_of::<u32>() + 1)
            + self.shared.capacity() * (size_of::<u32>() + size_of::<LabelScope>() + 1)
            + self.ranges.capacity() * size_of::<MintedRange>()
            + names
            + labels
    }
}
