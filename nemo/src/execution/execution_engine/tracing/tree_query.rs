//! This module implements tracing for tree queries.

use std::collections::{HashMap, hash_map::Entry};

use itertools::Itertools;
use nemo_physical::datavalues::AnyDataValue;

use crate::{
    error::Error,
    execution::{
        ExecutionEngine,
        planning::{
            normalization::{
                atom::ground::GroundAtom, program::NormalizedProgram, rule::NormalizedRule,
            },
            strategy::tracing::StrategyTracing,
        },
        selection_strategy::strategy::RuleSelectionStrategy,
        tracing::{
            error::TracingError,
            shared::{PaginationResponse, Rule as TraceRule, TableEntryQuery, TableEntryResponse},
            tree_query::{TreeForTableQuery, TreeForTableResponse, TreeForTableResponseSuccessor},
        },
    },
    rule_model::components::{
        atom::Atom,
        tag::Tag,
        term::primitive::{Primitive, ground::GroundTerm, variable::Variable},
    },
};

/// For a given rule, fact and head index,
/// compute the partial grounding resulting,
/// from assigning the variable to the corresponding
/// term in the fact.
///
/// Returns `None`, if the atoms are not unifiable.
fn partial_grounding_for_rule_head_and_fact(
    rule: NormalizedRule,
    head_index: usize,
    fact: GroundAtom,
) -> Option<HashMap<Variable, AnyDataValue>> {
    let head_atom = &rule.head()[head_index];

    let is_aggregate_atom = Some(head_index) == rule.aggregate_index();
    let aggregate_variable = rule
        .aggregate()
        .map(|aggregate| aggregate.output_variable());

    if head_atom.predicate() != fact.predicate() {
        return None;
    }

    // Unify the head atom with the given fact

    // If unification is possible `compatible` remains true
    let mut compatible = true;
    // Contains the head variable and the ground term it aligns with.
    let mut grounding = HashMap::<Variable, AnyDataValue>::new();

    for (head_term, fact_term) in head_atom.terms().zip(fact.terms()) {
        match head_term {
            Primitive::Ground(ground) => {
                if ground != fact_term {
                    compatible = false;
                    break;
                }
            }
            Primitive::Variable(variable) => {
                // Matching with existential variables should not produce any restrictions,
                // so we just consider universal variables here
                if variable.is_existential() {
                    continue;
                }

                // Aggregate variables are not grounded
                if is_aggregate_atom && Some(variable) == aggregate_variable {
                    continue;
                }

                match grounding.entry(variable.clone()) {
                    Entry::Occupied(entry) => {
                        if *entry.get() != fact_term.value() {
                            compatible = false;
                            break;
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(fact_term.value());
                    }
                }
            }
        }
    }

    if !compatible {
        // Fact could not have been unified
        return None;
    }

    Some(grounding)
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// The results for negated nodes is not computed as part of the normal
    /// evaluation of the query, and instead appendning the the full table,
    /// as an explanation for negated facts.
    async fn trace_tree_add_negation(
        &mut self,
        response: &mut TreeForTableResponse,
        program: &NormalizedProgram,
    ) {
        if let Some(next) = &mut response.next {
            for child in &mut next.children {
                Box::pin(self.trace_tree_add_negation(child, program)).await;
            }

            let rule = program.rules()[next.rule.id].clone();
            for negative_atom in rule.negative() {
                let rows =
                    if let Ok(Some(rows)) = self.predicate_rows(&negative_atom.predicate()).await {
                        rows.collect::<Vec<_>>()
                    } else {
                        Vec::default()
                    };

                let mut entries = Vec::default();
                for row in rows {
                    let entry_id = self
                        .table_manager
                        .table_row_id(&negative_atom.predicate(), &row)
                        .await
                        .expect("row should be contained somewhere");

                    let table_response = TableEntryResponse {
                        entry_id,
                        terms: row,
                    };

                    entries.push(table_response);
                }

                let negative_response = TreeForTableResponse {
                    predicate: negative_atom.predicate().to_string(),
                    entries,
                    pagination: PaginationResponse {
                        start: 0,
                        more: false,
                    },
                    possible_rules_above: Vec::default(),
                    possible_rules_below: Vec::default(),
                    next: None,
                };

                next.children.push(negative_response);
            }
        }
    }

    fn trace_next_facts(
        grounding: &[AnyDataValue],
        next_facts: &mut [Vec<GroundAtom>],
        rule: &NormalizedRule,
        variables: Vec<Variable>,
    ) {
        let variable_assignment: HashMap<Variable, AnyDataValue> = variables
            .into_iter()
            .zip(grounding.iter().cloned())
            .collect();

        for (body_index, body_atom) in rule.positive().iter().enumerate() {
            let next_fact_predicate = body_atom.predicate();
            let next_fact_terms = body_atom
                .terms()
                .map(|variable| {
                    GroundTerm::from(
                        variable_assignment
                            .get(variable)
                            .expect("Query must assign value to each variable.")
                            .clone(),
                    )
                })
                .collect::<Vec<_>>();

            let next_fact = GroundAtom::new(next_fact_predicate, next_fact_terms);
            next_facts[body_index].push(next_fact);
        }
    }

    /// Recursive part of `trace_tree`
    async fn trace_tree_recursive(
        &mut self,
        facts: Vec<GroundAtom>,
        program: &NormalizedProgram,
    ) -> Option<TreeForTableResponse> {
        let predicate = if let Some(first_fact) = facts.first() {
            first_fact.predicate() // We assume that all facts have the same predicate
        } else {
            return None;
        };

        // Prepare the result, which contains some information independant of tracing results below
        let entries = {
            let predicate_rows = self
                .predicate_rows(&predicate)
                .await
                .ok()
                .flatten()?
                .collect::<Vec<_>>();
            facts
                .iter()
                .map(|fact| {
                    Some(TableEntryResponse {
                        entry_id: predicate_rows.iter().position(|row| {
                            fact.terms().map(|t| t.value()).collect::<Vec<_>>() == *row
                        })?,
                        terms: fact.terms().map(|term| term.value()).collect(),
                    })
                })
                .collect::<Option<Vec<_>>>()?
        };

        let possible_rules_above = program
            .rules_with_body_predicate(&predicate)
            .flat_map(|index| {
                TraceRule::all_possible_single_head_rules(index, self.program().rule(index))
            })
            .collect::<Vec<_>>();

        let possible_rules_below = program
            .rules_with_head_predicate(&predicate)
            .flat_map(|index| {
                TraceRule::possible_rules_for_head_predicate(
                    index,
                    self.program().rule(index),
                    &predicate,
                )
            })
            .collect::<Vec<_>>();

        let mut result = TreeForTableResponse {
            predicate: predicate.to_string(),
            entries,
            pagination: PaginationResponse {
                start: 0,
                more: false,
            },
            possible_rules_above,
            possible_rules_below,
            next: None,
        };

        // Get the steps
        let steps = {
            let mut entries: Vec<_> = vec![];
            for fact in &facts {
                let dvs = fact.datavalues().collect::<Vec<_>>();
                let entry = self.table_manager.find_table_row(&predicate, &dvs);
                entries.push(entry.await);
            }
            entries.into_iter().collect::<Option<Vec<usize>>>()?
        };

        let contains_edb = facts
            .iter()
            .any(|fact| !program.derived_predicates().contains(&fact.predicate()));
        let loaded_edb = steps.iter().contains(&0);

        // Some of the traced facts come from the input database
        if contains_edb || loaded_edb {
            return Some(result);
        }

        // Get the rule that has been applied in each step
        // We can only proceed if every fact has been derived by the same rule
        let rule_index = self.rule_history[steps[0]];
        if steps
            .iter()
            .any(|&step| self.rule_history[step] != rule_index)
        {
            return Some(result);
        }

        let chase_rule = &program.rules()[rule_index].clone();
        let logical_rule = self.program().rule(rule_index).clone();

        for (head_index, _) in chase_rule.head().iter().enumerate() {
            let Some(combination) = facts
                .iter()
                .cloned()
                .map(|fact| {
                    partial_grounding_for_rule_head_and_fact(chase_rule.clone(), head_index, fact)
                })
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };

            let mut query_results = Vec::new();

            for (partial_grounding, &step) in combination.into_iter().zip(steps.iter()) {
                let rule = &program.rules()[rule_index];

                let trace_strategy = StrategyTracing::new(rule, partial_grounding);

                // Here, we simply compute all results to iterate over each
                // possible grounding (as different groundings may lead to different results)
                let query_result = trace_strategy
                    .execute_all(&mut self.table_manager, step)
                    .await
                    .ok()?
                    .pop()
                    .unwrap();

                query_results.push(query_result);
            }

            let results = query_results
                .iter()
                .map(|query_result| {
                    self.table_manager
                        .trie_row_iterator(query_result)
                        .map(|res| res.collect::<Vec<_>>())
                })
                .collect::<Result<Vec<_>, Error>>()
                .ok()?;

            if program.rules()[rule_index].aggregate().is_some() {
                // If rule is an aggregate rule then we continue with all matches

                let mut next_facts =
                    vec![Vec::new(); program.rules()[rule_index].positive_all().len()];

                for groundings in results {
                    for grounding in groundings {
                        let rule = &program.rules()[rule_index];
                        let tracing_variables =
                            StrategyTracing::new(rule, HashMap::default()).output_variables();

                        Self::trace_next_facts(
                            &grounding,
                            &mut next_facts,
                            rule,
                            tracing_variables,
                        );
                    }
                }

                let children_option = {
                    let mut entries: Vec<_> = vec![];
                    for facts in next_facts {
                        let entry = self.trace_tree_recursive(facts, program);
                        entries.push(Box::pin(entry).await);
                    }
                    entries.into_iter().collect::<Option<Vec<_>>>()
                };

                if let Some(children) = children_option {
                    result.next = Some(TreeForTableResponseSuccessor {
                        rule: TraceRule::from_rule_and_head(
                            rule_index,
                            &logical_rule,
                            head_index,
                            &logical_rule.head()[head_index],
                        ),
                        children,
                    });

                    return Some(result);
                }
            } else {
                // If rule is not an aggregate rule then we try all combination of matches
                // to see which one works

                for groundings in results.into_iter().multi_cartesian_product() {
                    let mut next_facts =
                        vec![Vec::new(); program.rules()[rule_index].positive_all().len()];

                    for grounding in groundings {
                        let rule = &program.rules()[rule_index];
                        let tracing_variables =
                            StrategyTracing::new(rule, HashMap::default()).output_variables();
                        Self::trace_next_facts(
                            &grounding,
                            &mut next_facts,
                            rule,
                            tracing_variables,
                        );
                    }

                    let children_option = {
                        let mut entries: Vec<_> = vec![];
                        for facts in next_facts {
                            let entry = self.trace_tree_recursive(facts, program);
                            entries.push(Box::pin(entry).await);
                        }
                        entries.into_iter().collect::<Option<Vec<_>>>()
                    };

                    if let Some(children) = children_option {
                        result.next = Some(TreeForTableResponseSuccessor {
                            rule: TraceRule::from_rule_and_head(
                                rule_index,
                                &logical_rule,
                                head_index,
                                &logical_rule.head()[head_index],
                            ),
                            children,
                        });

                        return Some(result);
                    }
                }
            }
        }

        None
    }

    /// Evaluate a [TreeForTableQuery].
    pub async fn trace_tree(
        &mut self,
        query: TreeForTableQuery,
    ) -> Result<TreeForTableResponse, Error> {
        let program = self.program.clone();

        let mut facts = Vec::new();

        for fact_query in query.queries {
            let fact = match fact_query {
                TableEntryQuery::Entry(row_index) => {
                    let terms_to_trace: Vec<AnyDataValue> = self
                        .predicate_rows(&Tag::new(query.predicate.clone()))
                        .await?
                        .into_iter()
                        .flatten()
                        .nth(row_index)
                        .ok_or(Error::TracingError(TracingError::InvalidFactId {
                            predicate: query.predicate.clone(),
                            id: row_index,
                        }))?;

                    GroundAtom::new(
                        Tag::new(query.predicate.clone()),
                        terms_to_trace
                            .into_iter()
                            .map(GroundTerm::from)
                            .collect::<Vec<_>>(),
                    )
                }
                TableEntryQuery::Query(query_string) => {
                    // TODO: Support patterns
                    let atom = Atom::parse(&format!("{}({})", query.predicate, query_string))
                        .map_err(|_| {
                            Error::TracingError(TracingError::EmptyFactQuery {
                                predicate: query.predicate.clone(),
                                query: query_string.clone(),
                            })
                        })?;
                    GroundAtom::try_from(atom).map_err(|_| {
                        Error::TracingError(TracingError::EmptyFactQuery {
                            predicate: query.predicate.clone(),
                            query: query_string,
                        })
                    })?
                }
            };

            facts.push(fact);
        }

        if let Some(mut result) = self.trace_tree_recursive(facts, &program).await {
            self.trace_tree_add_negation(&mut result, &program).await;
            Ok(result)
        } else {
            let predicate = Tag::new(query.predicate.clone());

            let possible_rules_above = self
                .program
                .rules_with_body_predicate(&predicate)
                .flat_map(|index| {
                    TraceRule::all_possible_single_head_rules(index, self.program().rule(index))
                })
                .collect::<Vec<_>>();

            let possible_rules_below = self
                .program
                .rules_with_head_predicate(&predicate)
                .flat_map(|index| {
                    TraceRule::possible_rules_for_head_predicate(
                        index,
                        self.program().rule(index),
                        &predicate,
                    )
                })
                .collect::<Vec<_>>();

            Ok(TreeForTableResponse {
                predicate: query.predicate,
                entries: vec![],
                pagination: PaginationResponse {
                    start: query
                        .pagination
                        .map(|pagination| pagination.start)
                        .unwrap_or(0),
                    more: false,
                },
                possible_rules_above,
                possible_rules_below,
                next: None,
            })
        }
    }
}
