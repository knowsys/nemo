//! Tracing: the derivations of facts, and the format in which they are exchanged.
//!
//! A *derivation* explains a fact over the *written program*,
//! which is the program as the user wrote it, with its modules instantiated.
//! A derived fact comes with the rule instance that derives it,
//! which consists of its rule, its variable assignment, and one premise per positive body
//! literal.
//! The facts at the bottom were loaded.
//! A derivation does not depend on how reasoning went:
//! with its assignment substituted, each rule instance can be checked against the written rules.
//!
//! [Engine::trace](crate::engine::Engine::trace) returns the derivations of the requested facts
//! as one graph, a [TraceResponse], in which every fact is one node.
//! A [Proof] walks the graph like a tree.
//! Each rule is listed once per response, with its *statement id*,
//! which is its position in the written program.
//! [Engine::trace_predicates](crate::engine::Engine::trace_predicates) hands out a response in
//! [parts](TracePart).
//!
//! The types here are serde types whose JSON field names are camelCase.
//! Every [TraceResponse] has a [version](VERSION) field, while its parts have none.
//! Facts and terms are written in canonical term syntax, which parses back,
//! except for nulls, which are written `_:id`.
//! Where reasoning never computed a value, a fact shows the written term instead,
//! which is a variable or `_`.
//! This happens, for example, for a variable that a query to an endpoint did not return.
//! The derivation-structure queries of [query] follow the format of an external viewer.

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

/// The version of the trace response format that this build writes into every response.
pub const VERSION: u32 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns a response with one derived fact over a loaded one, and one fact that does not
    /// hold.
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

    /// A response round-trips through JSON, in which the field names are camelCase,
    /// a derivation's fields are inlined into its node, and empty lists are left out.
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
