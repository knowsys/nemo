//! Tracing: derivations of facts, and the format they are exchanged in.
//!
//! A derivation is a static object over the written program:
//! the program as the user wrote it, with its modules instantiated.
//! A derived fact comes with the rule instance that derives it:
//! its rule, its variable assignment, and one premise per positive body literal.
//! The facts at the bottom were loaded.
//! Validity makes no reference to the history of the run:
//! substitute the assignment, and every derivation checks out against the written rules.
//! [Engine::trace](crate::engine::Engine::trace) finds a derivation for each requested fact
//! by a grounded backward search over the compiled program,
//! and translates it back onto the written program.
//!
//! The derivations of one response form a graph, a [TraceResponse]:
//! every fact is one node, a premise of several derivations included,
//! and a derived node names its premises by node id.
//! A [Proof] walks it like a tree.
//! Rules are listed once per response, and each carries its *statement id*,
//! its position in the written program.
//! If the run never had a value at a position
//! (a variable a query to an endpoint did not return, a column no rule reads),
//! a fact shows the written term there instead, a variable or `_`.
//!
//! The types here are the response format of [trace](crate::engine::Engine::trace),
//! used by the command line and the Python and WebAssembly bindings.
//! They are serde-derived and camelCase on the wire,
//! and every response carries a [version](VERSION) field.
//! Facts and terms are written in canonical term syntax, which parses back,
//! except for nulls, which are written `_:id`.
//! The derivation-structure queries of nemo-web's explanation viewer use that viewer's own
//! format, described in [query].

mod ascii;
mod node;
mod part;
mod proof;
pub mod query;
mod response;
mod written;

pub(crate) use self::node::fact_text;
pub use self::{
    node::{AggregateSummary, Derivation, TraceNode, VariableBinding},
    part::TracePart,
    proof::Proof,
    response::{TraceResponse, TracedFact},
    written::{SourceLocation, WrittenRule, WrittenStatement},
};

/// The version of the trace response this build writes, which every response carries.
pub const VERSION: u32 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    /// A response of one derived fact over a loaded one.
    fn response() -> TraceResponse {
        TraceResponse {
            version: VERSION,
            facts: vec![
                TracedFact {
                    fact: "reach(<a>, <b>)".into(),
                    node: Some(1),
                },
                TracedFact {
                    fact: "reach(<b>, <a>)".into(),
                    node: None,
                },
            ],
            nodes: vec![
                TraceNode {
                    fact: "edge(<a>, <b>)".into(),
                    derivation: None,
                },
                TraceNode {
                    fact: "reach(<a>, <b>)".into(),
                    derivation: Some(Derivation {
                        rule: 0,
                        step: 1,
                        assignment: vec![VariableBinding {
                            variable: "?x".into(),
                            value: "<a>".into(),
                        }],
                        premises: vec![0],
                        absences: Vec::new(),
                        aggregate: None,
                        nondeterministic: false,
                    }),
                },
            ],
            rules: vec![WrittenRule {
                rule: 0,
                text: "reach(?x, ?y) :- edge(?x, ?y) .".into(),
                name: None,
                source: None,
                instances: Vec::new(),
            }],
        }
    }

    /// A response round-trips through its JSON rendering, whose field names are camelCase,
    /// a derivation's fields sit in its node, and empty lists are left out.
    #[test]
    fn responses_round_trip_through_camel_case_json() {
        let response = response();
        let json = serde_json::to_string(&response).expect("serializes");
        assert!(json.contains(r#"{"fact":"edge(<a>, <b>)"}"#), "{json}");
        assert!(json.contains(r#""rule":0,"step":1,"#), "{json}");
        assert!(json.contains(r#""node":null"#), "{json}");
        assert!(!json.contains("absences"), "{json}");
        let back: TraceResponse = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, response);
    }

    /// A proof walks the graph from a requested fact to the loaded facts.
    #[test]
    fn proofs_walk_the_graph_like_a_tree() {
        let response = response();
        let proof = response.proof(0).expect("the fact holds");
        assert_eq!(proof.rule().map(|rule| rule.rule), Some(0));
        let premises: Vec<Proof<'_>> = proof.premises().collect();
        assert_eq!(premises.len(), 1);
        assert_eq!(premises[0].fact(), "edge(<a>, <b>)");
        assert!(premises[0].derivation().is_none());
        assert!(response.proof(1).is_none());
    }
}
