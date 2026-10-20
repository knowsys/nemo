//! The nodes of a proof tree, and the facts and bindings they show.

use serde::{Deserialize, Serialize};

use crate::trace::{SourceLocation, WrittenRule};

/// A fact: a predicate and its values, each in canonical term syntax.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FactText {
    /// The predicate, by its resolved name.
    pub predicate: String,
    /// The values, in canonical term-syntax rendering.
    ///
    /// If the run never had a value, the written term appears instead: a variable,
    /// or `_`, which in a pattern also marks a column left open.
    pub terms: Vec<String>,
}

impl std::fmt::Display for FactText {
    /// Writes the fact in term syntax, the predicate in angle brackets
    /// where its resolved name does not read back bare.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let predicate = crate::model::Identifier::bare(self.predicate.clone());
        write!(f, "{predicate}({})", self.terms.join(", "))
    }
}

/// One node of a proof tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum TraceNode {
    /// An extensional leaf: the fact was loaded, not derived.
    #[serde(rename_all = "camelCase")]
    Asserted {
        /// The fact.
        fact: FactText,
        /// Where the rows came from.
        origin: AssertedOrigin,
        /// The source positions of the statements that can have supplied it:
        /// the predicate's fact statements and import directives.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        defined_at: Vec<SourceLocation>,
    },
    /// A rule application: the fact follows from the premises by the
    /// rule under the assignment.
    #[serde(rename_all = "camelCase")]
    Derived {
        /// The fact.
        fact: FactText,
        /// The deriving rule.
        rule: WrittenRule,
        /// The engine step of the application that derived the fact.
        ///
        /// Steps order the engine's writes to the store, and step 0 holds the loaded facts.
        /// If the engine kept no record of the application,
        /// this is a step after the one that stored the fact.
        step: u64,
        /// The variable assignment of the rule instance, for the
        /// source-named variables.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        assignment: Vec<VariableBinding>,
        /// The positive premises, one subtree each.
        premises: Vec<TraceNode>,
        /// The negated premises: patterns that hold by *absence* in the
        /// saturated lower stratum (`_` marks columns private to the
        /// negation).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        absences: Vec<FactText>,
        /// For an aggregate rule: the group evidence, summarized.
        #[serde(skip_serializing_if = "Option::is_none")]
        aggregate: Option<AggregateSummary>,
        /// Whether the rule uses a nondeterministic function (such as `RAND()`).
        ///
        /// The values such functions returned cannot be reproduced,
        /// so the premises explain the fact up to the values from nondeterministic functions.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        nondeterministic: bool,
    },
    /// A derived fact whose derivation the tree shows at an earlier node.
    ///
    /// A tree expands each derived fact once, in depth-first order,
    /// so a proof whose premises share derivations stays as large as its distinct facts.
    #[serde(rename_all = "camelCase")]
    Repeated {
        /// The fact.
        fact: FactText,
    },
}

impl TraceNode {
    /// Returns the node's fact.
    pub fn fact(&self) -> &FactText {
        match self {
            TraceNode::Asserted { fact, .. }
            | TraceNode::Derived { fact, .. }
            | TraceNode::Repeated { fact } => fact,
        }
    }
}

/// Where an extensional fact's rows came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AssertedOrigin {
    /// The program's own facts and imports.
    Program,
    /// Rows added through `import_rows`.
    ImportRows,
}

/// One variable binding of a rule instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VariableBinding {
    /// The variable, with its `?`.
    pub variable: String,
    /// The bound value, in canonical rendering.
    pub value: String,
}

/// The group evidence of an aggregate node.
///
/// The body matches are summarized rather than inlined,
/// and the derivation-structure queries ([query](crate::trace::query)) expand them on demand.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateSummary {
    /// The number of distinct contributions the aggregate counted in this group.
    pub matches: usize,
}
