//! The kinds of problems tracing reports.

use nemo_physical::{diagnostic_kinds, diagnostics::Count};

diagnostic_kinds! {
    /// A tracing request that cannot be answered as posed.
    pub enum TraceError {
        PendingUpdates
            => PENDING_UPDATES = 209, error,
               "the engine has updates that reasoning has not processed",
               explain "The engine holds updates that reasoning has not processed yet. \
                        Traces describe the stored facts after reasoning.",
               hint "call `reason()` before tracing";
        FactArity { predicate: String, arity: Count, fact: String, found: Count }
            => FACT_ARITY = 501, error,
               "`{predicate}` has {arity} column{arity:#}, but `{fact}` has {found}",
               explain "A traced fact has a different number of terms than its predicate \
                        has columns.";
        UnknownRule { rule: usize }
            => UNKNOWN_RULE = 502, error, "statement {rule} is not a rule",
               explain "A query names a statement id that is not a rule of the program as \
                        written.";
        QueryHeadKind
            => QUERY_HEAD_KIND = 503, error,
               "cannot expand a rule with an aggregate or an existential variable in its head",
               explain "A query expands a rule whose head has an aggregate or an \
                        existential variable. The query protocol only describes rules \
                        without them.",
               hint "use `trace()` to explain derivations of such rules";
        UnknownHead { rule: usize, head: usize }
            => UNKNOWN_HEAD = 504, error, "rule {rule} has no head atom {head}",
               explain "A query names a head atom index that the rule does not have.";
        HeadPredicateMismatch { rule: usize, head: usize }
            => HEAD_PREDICATE_MISMATCH = 505, error,
               "head atom {head} of rule {rule} does not derive the predicate of this node",
               explain "A query expands a node through a head atom that derives a \
                        different predicate.";
        BodyCount { rule: usize, atoms: Count, constrained: Count }
            => BODY_COUNT = 506, error,
               "rule {rule} has {atoms} body atom{atoms:#}, but the query gives \
                {constrained} child node{constrained:#}",
               explain "A query expands a node with more child nodes than the rule has \
                        body atoms.";
        SelectorArity { predicate: String, terms: Count, arity: Count }
            => SELECTOR_ARITY = 507, error,
               "selection has {terms} term{terms:#}, but `{predicate}` has {arity} \
                column{arity:#}",
               explain "A query selects a fact by a different number of terms than the \
                        predicate has columns.";
        NotTerms { text: String, reason: String }
            => NOT_TERMS = 508, error, "cannot parse `{text}` as terms: {reason}",
               explain "A query selects a fact by terms that do not parse.";
        UnknownEntry { predicate: String, id: usize }
            => UNKNOWN_ENTRY = 509, error, "`{predicate}` has no fact with entry id {id}",
               explain "A query refers to a fact by an entry id that the predicate does \
                        not have.";
        Unfinished
            => UNFINISHED = 510, error, "reasoning has not reached the fixpoint",
               explain "Reasoning has not run since the program was loaded, or its last run \
                        was cancelled, failed, or suspended before reaching the fixpoint. \
                        Traces describe the stored facts after reasoning has finished.",
               hint "call `reason()` before tracing";
        QueryFetchRule
            => QUERY_FETCH_RULE = 511, error, "fetched facts cannot be expanded",
               explain "A query expands the fetch rule of a bound import, an import with a \
                        `where` clause. Its facts were returned by the endpoint, not derived \
                        by a rule.",
               hint "use `trace()` to see which bindings the facts were fetched with";
        UnknownBlankNode { label: String }
            => UNKNOWN_BLANK_NODE = 512, error,
               "the program has no blank node `_:{label}`",
               explain "A traced fact names a blank node that the program never created. \
                        No stored null has that label.",
               hint "refer to a null by the label the engine prints for it";
        RemoteSelectors
            => REMOTE_SELECTORS = 513, error,
               "cannot select facts of a premise that is queried at its endpoint",
               explain "A query constrains a premise whose facts were queried at the \
                        endpoint of its import, such as a SPARQL endpoint. These facts are \
                        not stored, so no selector can pick among them.";
        RemoteExpansion
            => REMOTE_EXPANSION = 514, error,
               "a premise that is queried at its endpoint has no derivation to expand",
               explain "A query expands a premise whose facts were queried at the endpoint \
                        of its import, such as a SPARQL endpoint. No rule derives these \
                        facts.";
        NegatedExpansion
            => NEGATED_EXPANSION = 515, error,
               "the node of a negated literal has no derivation to expand",
               explain "A query expands the node of a negated literal. Such a node lists \
                        the facts of the negated predicate, not premises of the \
                        derivation.";
    }
}
