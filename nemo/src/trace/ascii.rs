//! Proof trees as ASCII trees, for a terminal.

use crate::trace::{TraceNode, TraceResponse};

impl TraceResponse {
    /// Renders every derived fact's proof tree as an ASCII tree, and
    /// every fact that does not hold as a note, indented as a tree's root.
    pub fn ascii(&self) -> String {
        let mut out = String::new();
        for (index, traced) in self.facts.iter().enumerate() {
            if index > 0 {
                out.push('\n');
            }
            match &traced.tree {
                Some(tree) => ascii_node(tree, "", "", &mut out),
                None => {
                    out.push_str(&format!(" {} does not hold\n", traced.fact));
                }
            }
        }
        out
    }
}

/// Returns the instantiated one-line label of a node: the fact for a leaf, the
/// instantiated rule for a derivation.
fn ascii_label(node: &TraceNode) -> String {
    match node {
        TraceNode::Asserted { fact, .. } => fact.to_string(),
        TraceNode::Repeated { fact } => format!("{fact} (shown above)"),
        TraceNode::Derived {
            fact,
            premises,
            absences,
            aggregate,
            ..
        } => {
            let mut body: Vec<String> = premises
                .iter()
                .map(|premise| premise.fact().to_string())
                .collect();
            body.extend(absences.iter().map(|absent| format!("~{absent}")));
            let mut label = format!("{fact} :- {} .", body.join(", "));
            if let Some(summary) = aggregate {
                let matches = summary.matches;
                let noun = if matches == 1 { "match" } else { "matches" };
                label.push_str(&format!("  % aggregated over {matches} {noun}"));
            }
            label
        }
    }
}

/// Writes one node and its subtree with box-drawing prefixes.
fn ascii_node(node: &TraceNode, lead: &str, descent: &str, out: &mut String) {
    out.push_str(&format!(" {lead}{}\n", ascii_label(node)));
    if let TraceNode::Derived { premises, .. } = node {
        for (index, child) in premises.iter().enumerate() {
            let last = index + 1 == premises.len();
            let (branch, extend) = if last {
                ("└─ ", "   ")
            } else {
                ("├─ ", "│  ")
            };
            ascii_node(
                child,
                &format!("{descent}{branch}"),
                &format!("{descent}{extend}"),
                out,
            );
        }
    }
}
