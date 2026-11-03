//! Derivations as text for a terminal: as trees, or as one inference per line.

use std::{collections::HashSet, fmt};

use crate::trace::{Proof, TraceResponse};

impl TraceResponse {
    /// Renders every requested fact that holds as an ASCII tree of its derivation,
    /// and every fact that does not hold as a note, indented as a tree's root.
    ///
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
                None => out.push_str(&format!(" {} does not hold\n", traced.fact)),
            }
        }
        out
    }

    /// Renders every derived node as one line, its instantiated rule,
    /// each after the lines of its premises.
    pub fn inference_lines(&self) -> String {
        let mut out = String::new();
        for id in 0..self.nodes.len() {
            let proof = self.node(id);
            if proof.derivation().is_some() {
                out.push_str(&format!("{proof}\n"));
            }
        }
        out
    }
}

impl fmt::Display for Proof<'_> {
    /// Writes the instantiated rule that derives the fact, as one line,
    /// or the fact alone if no rule derives it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(derivation) = self.derivation() else {
            return f.write_str(self.fact());
        };
        let mut body: Vec<String> = self
            .premises()
            .map(|premise| premise.fact().to_owned())
            .collect();
        body.extend(
            derivation
                .absences
                .iter()
                .map(|absent| format!("~{absent}")),
        );
        write!(f, "{} :- {} .", self.fact(), body.join(", "))?;
        if let Some(summary) = derivation.aggregate {
            let matches = summary.matches;
            let noun = if matches == 1 { "match" } else { "matches" };
            write!(f, "  % aggregated over {matches} {noun}")?;
        }
        Ok(())
    }
}

/// Writes one node and, unless it was shown before, its premises, with box-drawing prefixes.
fn ascii_node(
    proof: Proof<'_>,
    lead: &str,
    descent: &str,
    shown: &mut HashSet<usize>,
    out: &mut String,
) {
    if proof.derivation().is_some() && !shown.insert(proof.id()) {
        out.push_str(&format!(" {lead}{} (shown above)\n", proof.fact()));
        return;
    }
    out.push_str(&format!(" {lead}{proof}\n"));
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
