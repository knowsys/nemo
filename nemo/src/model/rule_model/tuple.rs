use std::{ops::Deref, sync::Arc};

use super::Constant;

/// A Tuple: a [Constant]
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct Tuple {
    values: Arc<[Constant]>,
}

impl Default for Tuple {
    fn default() -> Self {
        Self {
            values: [].into_iter().collect(),
        }
    }
}

impl Tuple {
    /// Returns the size of the tuple.
    pub fn arity(&self) -> usize {
        self.values.len()
    }
}

impl Deref for Tuple {
    type Target = [Constant];

    fn deref(&self) -> &[Constant] {
        &self.values
    }
}

impl FromIterator<Constant> for Tuple {
    fn from_iter<T: IntoIterator<Item = Constant>>(iter: T) -> Self {
        Self {
            values: iter.into_iter().collect(),
        }
    }
}

impl std::fmt::Display for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        f.write_str(
            &self
                .iter()
                .map(|x| ToString::to_string(x))
                .by_ref()
                .intersperse(", ".into())
                .collect::<String>(),
        )?;
        write!(f, ")")
    }
}
