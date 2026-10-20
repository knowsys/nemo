//! Stratification: grouping rules so that negated predicates are final.
//!
//! The dependency graph has one node per rule
//! and an edge from a deriving rule to each consumer of its head predicate,
//! labeled by how the consumer reads it: positively, or through negation.
//! Strongly connected components become strata.
//! A negative edge inside a component means a predicate is negated within its own recursion,
//! and the program is not stratified.
//! Components are emitted in dependency order, ties broken by the smallest contained rule
//! index.

use std::collections::HashMap;

use nemo_physical::{diagnostics::Diagnostic, management::TableId};

use crate::{diagnostics::TranslationError, engine::compile::compiled::CompiledRule, model::Span};

/// Returns the strata of the compiled rules, or the diagnostic for an
/// unstratifiable program (labeled with an offending rule's span).
pub(crate) fn stratify(
    rules: &[CompiledRule],
    spans: &[Option<Span>],
) -> Result<Vec<Vec<usize>>, Diagnostic> {
    let count = rules.len();
    if count == 0 {
        return Ok(Vec::new());
    }

    // Deriving rules per predicate: ordinary heads, aggregate heads,
    // and existential insert atoms alike.
    let mut derived_by: HashMap<TableId, Vec<usize>> = HashMap::new();
    for (index, rule) in rules.iter().enumerate() {
        for head in &rule.heads {
            derived_by.entry(head.table).or_default().push(index);
        }
        if let Some(aggregate) = &rule.aggregate {
            derived_by.entry(aggregate.table).or_default().push(index);
        }
        if let Some(existential) = &rule.existential {
            for insert in &existential.inserts {
                derived_by.entry(insert.table).or_default().push(index);
            }
        }
    }

    // Edges deriving → consuming, with negativeness. An aggregate
    // rule's inputs must be finished like negated ones, so its positive
    // dependencies count as negative.
    let mut edges: Vec<Vec<(usize, bool)>> = vec![Vec::new(); count];
    for (consumer, rule) in rules.iter().enumerate() {
        let aggregating = rule.aggregate.is_some();
        for atom in &rule.body {
            for &producer in derived_by.get(&atom.table).into_iter().flatten() {
                edges[producer].push((consumer, aggregating));
            }
        }
        for atom in &rule.negated {
            for &producer in derived_by.get(&atom.table).into_iter().flatten() {
                edges[producer].push((consumer, true));
            }
        }
    }

    let components = condense(&edges);

    // A negative edge within one component negates through recursion.
    let mut component_of = vec![0usize; count];
    for (index, component) in components.iter().enumerate() {
        for &rule in component {
            component_of[rule] = index;
        }
    }
    for (producer, targets) in edges.iter().enumerate() {
        for &(consumer, negative) in targets {
            if negative && component_of[producer] == component_of[consumer] {
                let diagnostic = TranslationError::Unstratified.diagnostic();
                let negates = rules[consumer].negated.iter().any(|atom| {
                    derived_by
                        .get(&atom.table)
                        .is_some_and(|producers| producers.contains(&producer))
                });
                let label = if negates {
                    "this rule negates a predicate of its own recursion"
                } else {
                    "this rule aggregates over its own recursion"
                };
                return Err(match spans[consumer] {
                    Some(span) => diagnostic.with_label(span, label),
                    None => diagnostic,
                });
            }
        }
    }
    Ok(components)
}

/// Returns the strongly connected components of the rule graph,
/// in dependency order (producers before consumers),
/// ties broken by the smallest contained rule index.
///
/// The rules are sorted within each component.
fn condense(edges: &[Vec<(usize, bool)>]) -> Vec<Vec<usize>> {
    let components = tarjan(edges);

    // Kahn's algorithm over the condensation, smallest-rule-first.
    let count = components.len();
    let mut component_of = vec![0usize; edges.len()];
    for (index, component) in components.iter().enumerate() {
        for &rule in component {
            component_of[rule] = index;
        }
    }
    let mut incoming = vec![0usize; count];
    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); count];
    for (producer, targets) in edges.iter().enumerate() {
        for &(consumer, _) in targets {
            let (from, to) = (component_of[producer], component_of[consumer]);
            if from != to && !outgoing[from].contains(&to) {
                outgoing[from].push(to);
                incoming[to] += 1;
            }
        }
    }

    let mut ready: Vec<usize> = (0..count).filter(|&index| incoming[index] == 0).collect();
    let mut ordered = Vec::with_capacity(count);
    while !ready.is_empty() {
        let position = ready
            .iter()
            .enumerate()
            .min_by_key(|&(_, &component)| components[component][0])
            .map(|(position, _)| position)
            .expect("the ready set is not empty");
        let component = ready.swap_remove(position);
        for &next in &outgoing[component] {
            incoming[next] -= 1;
            if incoming[next] == 0 {
                ready.push(next);
            }
        }
        ordered.push(components[component].clone());
    }
    ordered
}

/// Returns the strongly connected components by Tarjan's algorithm, iteratively,
/// the rules sorted within each component.
fn tarjan(edges: &[Vec<(usize, bool)>]) -> Vec<Vec<usize>> {
    let count = edges.len();
    let mut index = vec![usize::MAX; count];
    let mut low = vec![0usize; count];
    let mut on_stack = vec![false; count];
    let mut stack = Vec::new();
    let mut next_index = 0usize;
    let mut components = Vec::new();

    for root in 0..count {
        if index[root] != usize::MAX {
            continue;
        }
        // The explicit DFS frame: node and next edge position.
        let mut frames: Vec<(usize, usize)> = vec![(root, 0)];
        while let Some(&mut (node, ref mut position)) = frames.last_mut() {
            if *position == 0 {
                index[node] = next_index;
                low[node] = next_index;
                next_index += 1;
                stack.push(node);
                on_stack[node] = true;
            }
            if let Some(&(target, _)) = edges[node].get(*position) {
                *position += 1;
                if index[target] == usize::MAX {
                    frames.push((target, 0));
                } else if on_stack[target] {
                    low[node] = low[node].min(index[target]);
                }
                continue;
            }

            // The node is finished: close its component at the root.
            if low[node] == index[node] {
                let mut component = Vec::new();
                loop {
                    let member = stack.pop().expect("the component is on the stack");
                    on_stack[member] = false;
                    component.push(member);
                    if member == node {
                        break;
                    }
                }
                component.sort_unstable();
                components.push(component);
            }
            frames.pop();
            if let Some(&(parent, _)) = frames.last() {
                low[parent] = low[parent].min(low[node]);
            }
        }
    }
    components
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns the components of a graph given by its edge list alone.
    fn components(count: usize, edge_list: &[(usize, usize)]) -> Vec<Vec<usize>> {
        let mut edges: Vec<Vec<(usize, bool)>> = vec![Vec::new(); count];
        for &(from, to) in edge_list {
            edges[from].push((to, false));
        }
        condense(&edges)
    }

    #[test]
    fn components_come_in_dependency_order() {
        // 2 → 0 → 1 with a cycle 0 ↔ 3.
        let strata = components(4, &[(2, 0), (0, 1), (0, 3), (3, 0)]);
        assert_eq!(strata, vec![vec![2], vec![0, 3], vec![1]]);
    }

    #[test]
    fn independent_components_order_by_smallest_rule() {
        let strata = components(4, &[(3, 1)]);
        assert_eq!(strata, vec![vec![0], vec![2], vec![3], vec![1]]);
    }
}
