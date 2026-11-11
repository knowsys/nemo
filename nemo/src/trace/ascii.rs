//! Derivations as text for a terminal: as trees, and a node as its inference.

use std::{collections::HashSet, fmt};

use crate::trace::{Proof, TraceResponse};

impl TraceResponse {
    /// Renders every requested fact that holds as a tree of its derivation,
    /// drawn with box-drawing characters, and every fact that does not hold as a note.
    ///
    /// Each line of a tree is a statement: the rule instance that derives a fact,
    /// or the fact statement of a fact that no rule derives.
    /// A derived fact shown before, in the same tree or an earlier one,
    /// refers back instead of being expanded again.
    pub fn ascii(&self) -> String {
        let mut out = String::new();
        let mut shown = HashSet::new();
        for (index, traced) in self.facts.iter().enumerate() {
            if index > 0 {
                out.push('\n');
            }
            match self.proof(index) {
                Some(proof) => ascii_node(proof, "", "", &mut shown, &mut out),
                None => out.push_str(&format!("{} does not hold\n", traced.fact)),
            }
        }
        out
    }
}

impl fmt::Display for Proof<'_> {
    /// Writes the rule instance that derives the fact, as one line,
    /// or the fact statement if no rule derives it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let premises = self.premises().map(|premise| premise.fact());
        f.write_str(&self.node().inference(premises))
    }
}

/// Writes one node and, unless it was shown before, its premises, with box-drawing prefixes.
///
/// `lead` prefixes the node's own line, and `descent` prefixes the lines of its premises.
/// `shown` contains the derived nodes shown so far.
fn ascii_node(
    proof: Proof<'_>,
    lead: &str,
    descent: &str,
    shown: &mut HashSet<usize>,
    out: &mut String,
) {
    if proof.derivation().is_some() && !shown.insert(proof.id()) {
        out.push_str(&format!("{lead}{} (shown above)\n", proof.fact()));
        return;
    }
    out.push_str(&format!("{lead}{proof}\n"));
    let premises: Vec<Proof<'_>> = proof.premises().collect();
    for (index, premise) in premises.iter().enumerate() {
        let (branch, extend) = if index + 1 == premises.len() {
            ("└─ ", "   ")
        } else {
            ("├─ ", "│  ")
        };
        ascii_node(
            *premise,
            &format!("{descent}{branch}"),
            &format!("{descent}{extend}"),
            shown,
            out,
        );
    }
}
