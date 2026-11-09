//! The nodes of a derivation: facts, and how a rule derived them.

use serde::{Deserialize, Serialize};

use crate::model::Identifier;

/// One node of a [TraceResponse](crate::trace::TraceResponse),
/// which is a fact that was loaded or that a rule derived from premises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceNode {
    /// The fact, in term syntax.
    ///
    /// Where no value was computed at a position, the written term appears there instead,
    /// which is a variable or `_`.
    pub fact: String,
    /// How a rule derived the fact,
    /// or `None` for a fact that was loaded or that an import's endpoint returned.
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub derivation: Option<Derivation>,
}

impl TraceNode {
    /// Returns the rule instance that derives the fact, as one line,
    /// or the fact alone if no rule derives it.
    ///
    /// `premises` are the facts of the node's premises, in order.
    /// The line lists them, then the negated premises,
    /// and notes the number of matches of an aggregate.
    /// [trace_predicates](crate::engine::Engine::trace_predicates) shows an example.
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
    /// Steps order the engine's writes to the stored tables, and step 0 is the loaded facts.
    /// If the engine kept no record of the application,
    /// this is the step at which the fact was stored.
    pub step: u64,
    /// The variable assignment of the rule instance, for the variables written in the rule.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assignment: Vec<VariableBinding>,
    /// The positive premises, by node id, one per positive body literal of the rule,
    /// or none for an aggregate summarized without a witness.
    pub premises: Vec<usize>,
    /// The negated premises, in term syntax.
    ///
    /// Each is a pattern that holds because no fact matches it once the lower strata are
    /// saturated.
    /// A variable that only the negated literal mentions stays as written.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absences: Vec<String>,
    /// For an aggregate rule, the summary of the group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<AggregateSummary>,
    /// Whether a head column of the rule calls a nondeterministic function, such as `RAND()`.
    ///
    /// The value that such a function returned cannot be reproduced,
    /// so the premises explain the fact except for that column.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nondeterministic: bool,
}

/// One variable binding of a rule instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VariableBinding {
    /// The variable, with its `?`.
    pub variable: String,
    /// The bound value, in canonical term syntax.
    pub value: String,
}

/// The summary of the group of an aggregate node.
///
/// The body matches of the group are counted rather than listed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateSummary {
    /// The number of distinct contributions that the group's first aggregate ranged over.
    pub matches: usize,
}

/// Returns a fact in term syntax from its predicate and its terms.
///
/// The predicate is written in angle brackets if its name would not be parsed back as a bare
/// name.
pub(crate) fn fact_text(predicate: &str, terms: &[String]) -> String {
    let predicate = Identifier::bare(predicate.to_owned());
    format!("{predicate}({})", terms.join(", "))
}
