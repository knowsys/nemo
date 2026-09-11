use std::collections::{HashMap, HashSet};
use std::fmt;

/// A substitution mapping (dense, per-rule) variable ids to other variable ids.
///
/// Whether a resolved id actually denotes a ground value is *not* tracked here: that is
/// determined by looking the id up in the `consts` map of whichever rule it belongs to
/// (see `Rule::consts`).
#[derive(Debug, Clone, Default)]
pub(crate) struct Substitution {
    map: HashMap<usize, usize>,
}

impl Substitution {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, from: usize, to: usize) {
        self.map.insert(from, to);
    }

    /// Check if this is the identity substitution.
    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Add a new mapping from `old` to `new`, and adjust existing mappings onto `old` to point to
    /// `new` instead (keeping the map "flat": every resolution takes at most one hop).
    pub(crate) fn remap(&mut self, old: usize, new: usize) {
        debug_assert!(old != new, "trying to add identity substitution");
        debug_assert!(
            !self.map.contains_key(&old),
            "domain and range of unifier are not disjoint"
        );
        for v in self.map.values_mut() {
            if *v == old {
                *v = new;
            }
        }
        self.map.insert(old, new);
    }

    /// Resolve a variable w.r.t. the substitution, returning the variable itself if unmapped.
    pub(crate) fn resolve(&self, var: usize) -> usize {
        self.map.get(&var).copied().unwrap_or(var)
    }

    /// Resolve a variable w.r.t. the substitution, returning `None` if it is unmapped.
    pub(crate) fn get_variable(&self, var: usize) -> Option<usize> {
        self.map.get(&var).copied()
    }

    /// Check if the given variable is mapped by this substitution.
    pub(crate) fn contains_variable(&self, var: usize) -> bool {
        self.map.contains_key(&var)
    }

    /// Resolve all variables in the iterator w.r.t. the substitution, dropping any that are
    /// unmapped.
    pub(crate) fn substitute_variables<'a>(
        &'a self,
        vars: impl IntoIterator<Item = usize> + 'a,
    ) -> impl Iterator<Item = usize> + 'a {
        vars.into_iter().filter_map(move |v| self.get_variable(v))
    }

    /// Restrict this substitution to the given domain of variables.
    pub(crate) fn restriction(&self, domain: &HashSet<usize>) -> Self {
        Self {
            map: self
                .map
                .iter()
                .filter(|(from, _to)| domain.contains(from))
                .map(|(&from, &to)| (from, to))
                .collect(),
        }
    }

    /// `f.compose(g)` means `(f ∘ g)(x) = f(g(x))`.
    pub(crate) fn compose(&self, other: &Self) -> Self {
        let mapped_by_other = other.map.iter().map(|(&k, &v)| {
            let resolved = self.map.get(&v).copied().unwrap_or(v);
            (k, resolved)
        });
        let only_in_self = self
            .map
            .iter()
            .filter(|(k, _)| !other.map.contains_key(k))
            .map(|(&k, &v)| (k, v));

        Self {
            map: mapped_by_other
                .chain(only_in_self)
                .filter(|&(k, v)| k != v)
                .collect(),
        }
    }
}

impl fmt::Display for Substitution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for (i, (from, to)) in self.map.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{from} -> {to}")?;
        }
        write!(f, "}}")
    }
}
