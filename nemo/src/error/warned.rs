//! This module defnes [Warned].

use std::fmt::Debug;

/// Warning or object
#[derive(Debug)]
pub struct Warned<O, W>
where
    O: Debug,
    W: Debug,
{
    /// Object
    object: O,
    /// Warning
    warning: W,
}

impl<O, W> Warned<O, W>
where
    O: Debug,
    W: Debug,
{
    /// Create a new [Warned].
    pub fn new(object: O, warning: W) -> Self {
        Self { object, warning }
    }

    /// Return a reference to the object.
    pub fn object(&self) -> &O {
        &self.object
    }

    /// Return a reference to the warning.
    pub fn warning(&self) -> &W {
        &self.warning
    }

    /// Return the object and warning as a pair
    pub fn pair(self) -> (O, W) {
        (self.object, self.warning)
    }

    /// Turn this into the object.
    pub fn into_object(self) -> O {
        self.object
    }

    /// Translate this into a warning of another type.
    pub fn into<V>(self) -> Warned<O, V>
    where
        V: Debug + From<W>,
    {
        Warned {
            object: self.object,
            warning: V::from(self.warning),
        }
    }
}
