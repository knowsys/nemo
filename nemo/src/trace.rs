//! Tracing: proof trees for derived facts, and the format they are exchanged in.
//!
//! A proof tree is a static object over the written program:
//! the program as the user wrote it, with its modules instantiated.
//! A node is a fact together with the rule instance that derives it:
//! its rule, its variable assignment, and one child per body premise.
//! Leaves are extensional facts.
//! Validity makes no reference to the history of the run:
//! substitute the assignment, and every node checks out against the written rules.
//! [Engine::trace](crate::engine::Engine::trace) finds such a tree for each requested fact
//! by a grounded backward search over the compiled program.
//! The tree is then translated back onto the written program.
//!
//! Rules are named by *statement id*, a statement's position in the written program.
//! If the run never had a value at a position
//! (a variable a query to an endpoint did not return, a column no rule reads),
//! a fact shows the written term there instead, a variable or `_`.
//!
//! The types here are the response format of [trace](crate::engine::Engine::trace),
//! used by the command line and the Python and WebAssembly bindings.
//! They are serde-derived and camelCase on the wire,
//! and every response carries a [version](VERSION) field.
//! Terms are written in canonical term syntax, which parses back,
//! except for nulls, which are written `_:id`.
//! The derivation-structure queries of nemo-web's explanation viewer use that viewer's own
//! format, described in [query].

mod ascii;
mod inference;
mod node;
pub mod query;
mod response;
mod written;

pub use self::{
    inference::{Inference, InferenceList},
    node::{AggregateSummary, AssertedOrigin, FactText, TraceNode, VariableBinding},
    response::{TraceResponse, TracedFact},
    written::{SourceLocation, WrittenRule, WrittenStatement},
};

/// The version of the trace response this build writes, which every response carries.
pub const VERSION: u32 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    /// A response round-trips through its JSON rendering unchanged in
    /// meaning, and the JSON field names are camelCase.
    #[test]
    fn responses_round_trip_through_camel_case_json() {
        let response = TraceResponse {
            version: VERSION,
            facts: vec![TracedFact {
                fact: FactText {
                    predicate: "reach".into(),
                    terms: vec!["<a>".into(), "<b>".into()],
                },
                holds: true,
                tree: Some(TraceNode::Derived {
                    fact: FactText {
                        predicate: "reach".into(),
                        terms: vec!["<a>".into(), "<b>".into()],
                    },
                    rule: WrittenRule {
                        rule: 0,
                        text: "reach(?x, ?y) :- edge(?x, ?y) .".into(),
                        name: None,
                        source: None,
                        instances: Vec::new(),
                    },
                    step: 1,
                    assignment: vec![VariableBinding {
                        variable: "?x".into(),
                        value: "<a>".into(),
                    }],
                    premises: vec![TraceNode::Asserted {
                        fact: FactText {
                            predicate: "edge".into(),
                            terms: vec!["<a>".into(), "<b>".into()],
                        },
                        origin: AssertedOrigin::Program,
                        defined_at: Vec::new(),
                    }],
                    absences: Vec::new(),
                    aggregate: None,
                    nondeterministic: false,
                }),
            }],
        };
        let json = serde_json::to_string(&response).expect("serializes");
        assert!(!json.contains("\"finalConclusion\""));
        assert!(!json.contains("\"definedAt\""), "empty lists are omitted");
        assert!(json.contains("\"kind\":\"derived\""), "{json}");
        assert!(json.contains("\"origin\":\"program\""), "{json}");
        let back: TraceResponse = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back.facts.len(), 1);
        assert!(back.facts[0].holds);
    }

    /// The inference list deduplicates shared subderivations.
    #[test]
    fn inferences_deduplicate_by_conclusion() {
        let leaf = TraceNode::Asserted {
            fact: FactText {
                predicate: "edge".into(),
                terms: vec!["<a>".into()],
            },
            origin: AssertedOrigin::Program,
            defined_at: Vec::new(),
        };
        let derived = TraceNode::Derived {
            fact: FactText {
                predicate: "reach".into(),
                terms: vec!["<a>".into()],
            },
            rule: WrittenRule {
                rule: 0,
                text: "reach(?x) :- edge(?x), edge(?x) .".into(),
                name: None,
                source: None,
                instances: Vec::new(),
            },
            step: 1,
            assignment: Vec::new(),
            premises: vec![leaf.clone(), leaf],
            absences: Vec::new(),
            aggregate: None,
            nondeterministic: false,
        };
        let response = TraceResponse {
            version: VERSION,
            facts: vec![TracedFact {
                fact: FactText {
                    predicate: "reach".into(),
                    terms: vec!["<a>".into()],
                },
                holds: true,
                tree: Some(derived),
            }],
        };
        let list = response.inferences();
        assert_eq!(list.inferences.len(), 2, "the shared leaf appears once");
        assert_eq!(list.final_conclusions, vec!["reach(<a>)"]);
    }
}
