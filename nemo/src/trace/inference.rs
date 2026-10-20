//! The flattened form of a trace response: one inference per distinct conclusion.

use serde::{Deserialize, Serialize};

use crate::trace::{TraceNode, TraceResponse};

/// The flattened, deduplicated form of a response: one inference per distinct conclusion.
///
/// It serializes the proof trees as a DAG, in which shared subderivations appear once.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceList {
    /// The protocol version.
    pub version: u32,
    /// The requested facts that hold.
    pub final_conclusions: Vec<String>,
    /// The requested facts that do not.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub not_derived: Vec<String>,
    /// The inferences, parents before children.
    pub inferences: Vec<Inference>,
}

/// One inference of an [InferenceList].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Inference {
    /// The deriving rule's text, or `None` for an asserted fact.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
    /// The deriving rule's `#[name]`, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The concluded fact.
    pub conclusion: String,
    /// The premise facts (empty for asserted facts).
    pub premises: Vec<String>,
}

impl TraceResponse {
    /// Flattens the response into its [InferenceList]: one inference per
    /// distinct conclusion, shared subderivations deduplicated.
    pub fn inferences(&self) -> InferenceList {
        let mut list = InferenceList {
            version: self.version,
            final_conclusions: Vec::new(),
            not_derived: Vec::new(),
            inferences: Vec::new(),
        };
        let mut seen = std::collections::HashSet::new();
        for traced in &self.facts {
            match &traced.tree {
                Some(tree) => {
                    list.final_conclusions.push(traced.fact.to_string());
                    collect_inferences(tree, &mut seen, &mut list.inferences);
                }
                None => list.not_derived.push(traced.fact.to_string()),
            }
        }
        list
    }
}

/// Adds a node's inference (and its subtree's), parents before
/// children, each conclusion once.
fn collect_inferences(
    node: &TraceNode,
    seen: &mut std::collections::HashSet<String>,
    out: &mut Vec<Inference>,
) {
    // A repeated node's inference is listed at its first occurrence.
    let conclusion = node.fact().to_string();
    if matches!(node, TraceNode::Repeated { .. }) || !seen.insert(conclusion.clone()) {
        return;
    }
    match node {
        TraceNode::Asserted { .. } => out.push(Inference {
            rule: None,
            name: None,
            conclusion,
            premises: Vec::new(),
        }),
        TraceNode::Derived { rule, premises, .. } => {
            out.push(Inference {
                rule: Some(rule.text.clone()),
                name: rule.name.clone(),
                conclusion,
                premises: premises
                    .iter()
                    .map(|child| child.fact().to_string())
                    .collect(),
            });
            for child in premises {
                collect_inferences(child, seen, out);
            }
        }
        TraceNode::Repeated { .. } => {}
    }
}
