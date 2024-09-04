/// An identifier for, e.g., a Term or a Predicate.
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Identifier(pub(crate) String);

impl Identifier {
    /// Create a new [Identifier].
    pub fn new(name: String) -> Self {
        Identifier(name)
    }

    /// Returns the associated name
    pub fn name(&self) -> String {
        self.0.clone()
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.name())
    }
}

impl From<String> for Identifier {
    fn from(value: String) -> Self {
        Identifier(value)
    }
}
