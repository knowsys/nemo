//! The nodes of a derivation: facts, and how a rule derived them.

use serde::{Deserialize, Serialize};

use crate::model::Identifier;

/// One node of a [TraceResponse](crate::trace::TraceResponse): a fact,
/// loaded or derived by a rule from premises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceNode {
    /// The fact, in term syntax.
    ///
    /// If the run never had a value at a position, the written term appears there instead:
    /// a variable, or `_`.
    pub fact: String,
    /// How a rule derived the fact,
    /// or `None` for a fact that was loaded or that an import's endpoint returned.
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub derivation: Option<Derivation>,
}

impl TraceNode {
    /// Returns the instantiated rule that derives the fact as one line,
    /// given the facts of its premises, or the fact alone if no rule derives it.
    pub fn inference<'a>(&self, premises: impl IntoIterator<Item = &'a str>) -> String {
        let Some(derivation) = &self.derivation else {
            return self.fact.clone();
        };
        let mut body: Vec<String> = premises.into_iter().map(str::to_owned).collect();
        body.extend(
            derivation
                .absences
                .iter()
                .map(|absent| format!("~{absent}")),
        );
        let mut line = format!("{} :- {} .", self.fact, body.join(", "));
        if let Some(summary) = derivation.aggregate {
            let matches = summary.matches;
            let noun = if matches == 1 { "match" } else { "matches" };
            line.push_str(&format!("  % aggregated over {matches} {noun}"));
        }
        line
    }
}

/// How a rule derived a fact: the rule instance and its premises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Derivation {
    /// The rule, by its position in the response's rules.
    pub rule: usize,
    /// The engine step of the application that derived the fact.
    ///
    /// Steps order the engine's writes to the store, and step 0 holds the loaded facts.
    /// If the engine kept no record of the application,
    /// this is the step at which the fact was stored.
    pub step: u64,
    /// The variable assignment of the rule instance, for the source-named variables.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assignment: Vec<VariableBinding>,
    /// The positive premises, by node id, one per positive body literal of the rule;
    /// none for an aggregate summarized without a witness.
    pub premises: Vec<usize>,
    /// The negated premises: patterns that hold by *absence* in the saturated lower stratum,
    /// in term syntax (`_` marks columns private to the negation).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absences: Vec<String>,
    /// For an aggregate rule: the group evidence, summarized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<AggregateSummary>,
    /// Whether the rule uses a nondeterministic function (such as `RAND()`).
    ///
    /// The values such functions returned cannot be reproduced,
    /// so the premises explain the fact up to the values from nondeterministic functions.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nondeterministic: bool,
}

/// One variable binding of a rule instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateSummary {
    /// The number of distinct contributions the aggregate counted in this group.
    pub matches: usize,
}

/// Returns a fact in term syntax from its predicate and its terms,
/// the predicate in angle brackets where its name does not read back bare.
pub(crate) fn fact_text(predicate: &str, terms: &[String]) -> String {
    let predicate = Identifier::bare(predicate.to_owned());
    format!("{predicate}({})", terms.join(", "))
}
