//! The grouping of a rule's body for the rewrite.
//!
//! A *remote atom* is an atom over a candidate import, and every other literal is *local*.
//! The local literals form groups by shared variables,
//! and the remote atoms form *components*, each of which one query replaces.

use std::collections::HashSet;

use nemo_io::sparql::compose::Binding;

use crate::{
    engine::transform::sparql_rewrite::{
        context::{CandidateImport, RewriteContext},
        variables::{extend_unique, private_name, shares, universal},
    },
    model::{Literal, Operator, Rule, Term, TermKind, VariableKind},
};

/// One atom of a rule over a candidate import, resolved for composing queries.
pub(super) struct RemoteAtom {
    /// The atom's position in the body.
    pub(super) position: usize,
    /// The name of the candidate import that the atom reads.
    pub(super) candidate: String,
    /// The binding of each argument: a variable or a constant.
    pub(super) bindings: Vec<Binding>,
    /// The universal variables that the atom mentions, in order of first mention,
    /// with the fresh variables of its computed arguments.
    pub(super) variables: Vec<String>,
    /// Whether the atom's arguments are only distinct variables, anonymous ones included.
    ///
    /// Such an atom reads the candidate import's one fetch,
    /// whose unread columns the import plan prunes, rather than a fetch of its own.
    pub(super) plain: bool,
    /// Whether the rule reads the atom negatively.
    pub(super) negated: bool,
    /// The arguments computed from rule variables.
    pub(super) computed: Vec<ComputedArgument>,
}

/// An argument of a remote atom computed from rule variables, such as `STRLANG(?org, "en")`.
///
/// The query sees a fresh variable in its place.
/// A local group that binds the term's variables computes the argument,
/// and so binds the import.
#[derive(Debug, Clone)]
pub(super) struct ComputedArgument {
    /// The argument's position in the atom.
    pub(super) position: usize,
    /// The fresh variable that stands for the argument.
    pub(super) variable: String,
    /// The written term.
    pub(super) term: Term,
}

/// A group of local body literals that share variables,
/// which becomes a binding predicate if it shares a variable with a component.
pub(super) struct LocalGroup {
    /// The literals' body positions.
    pub(super) positions: Vec<usize>,
    /// The computed arguments that the group computes, after its literals.
    pub(super) computed: Vec<ComputedArgument>,
    /// The universal variables that the group mentions or computes, in order of first mention.
    pub(super) variables: Vec<String>,
}

impl LocalGroup {
    /// Groups the local literals at `positions`, and the `computed` arguments.
    ///
    /// The positive atoms are grouped by shared variables.
    /// Then each other literal and each computed argument joins the first group that binds
    /// everything it reads, repeatedly, until no more of them join.
    /// An assignment, which is an equality with a variable that no positive local atom binds,
    /// reads only its other side, and its group then binds its variable too.
    /// A computed argument is assigned to its fresh variable in the same way.
    /// A literal that no group binds fully belongs to no group, and stays in the rule.
    pub(super) fn all(
        rule: &Rule,
        positions: &[usize],
        computed: &[ComputedArgument],
    ) -> Vec<LocalGroup> {
        let mut groups = Self::positive(rule, positions);
        let bound: HashSet<String> = groups
            .iter()
            .flat_map(|group| group.variables.iter().cloned())
            .collect();
        let mut literals: Vec<usize> = positions
            .iter()
            .copied()
            .filter(|&position| !matches!(&rule.body[position], Literal::Positive(_)))
            .collect();
        let mut computed: Vec<&ComputedArgument> = computed.iter().collect();
        loop {
            let before = literals.len() + computed.len();
            literals.retain(|&position| {
                let literal = &rule.body[position];
                let (reads, assigned) = match assignment(literal, &bound) {
                    Some((variable, term)) => (universal(term), Some(variable)),
                    None => (universal(literal), None),
                };
                let Some(group) = Self::binding(&mut groups, &reads) else {
                    return true;
                };
                group.positions.push(position);
                extend_unique(&mut group.variables, assigned.map(str::to_owned));
                false
            });
            computed.retain(|argument| {
                let Some(group) = Self::binding(&mut groups, &universal(&argument.term)) else {
                    return true;
                };
                group.computed.push((*argument).clone());
                extend_unique(&mut group.variables, [argument.variable.clone()]);
                false
            });
            if literals.len() + computed.len() == before {
                break;
            }
        }
        for group in &mut groups {
            group.positions.sort_unstable();
        }
        groups
    }

    /// Returns the first group that binds every one of the variables.
    fn binding<'a>(groups: &'a mut [LocalGroup], variables: &[String]) -> Option<&'a mut Self> {
        groups.iter_mut().find(|group| {
            variables
                .iter()
                .all(|variable| group.variables.contains(variable))
        })
    }

    /// Groups the positive local atoms by shared variables.
    fn positive(rule: &Rule, positions: &[usize]) -> Vec<LocalGroup> {
        let mut groups: Vec<LocalGroup> = Vec::new();
        for &position in positions {
            let Literal::Positive(atom) = &rule.body[position] else {
                continue;
            };
            let variables = universal(atom);
            // Every group that shares a variable with the atom is merged with it.
            let mut merged = LocalGroup {
                positions: vec![position],
                computed: Vec::new(),
                variables: Vec::new(),
            };
            extend_unique(&mut merged.variables, variables.iter().cloned());
            let mut rest = Vec::new();
            for group in groups {
                if shares(&group.variables, &variables) {
                    merged.positions.extend(group.positions);
                    extend_unique(&mut merged.variables, group.variables);
                } else {
                    rest.push(group);
                }
            }
            rest.push(merged);
            groups = rest;
        }
        groups
    }
}

/// Returns the variable that an equality assigns, and the term that it assigns to it.
///
/// The variable is one side of the equality:
/// a universal variable outside `bound` that the other side does not mention.
fn assignment<'a>(literal: &'a Literal, bound: &HashSet<String>) -> Option<(&'a str, &'a Term)> {
    let Literal::Operation(operation) = literal else {
        return None;
    };
    let [left, right] = operation.arguments.as_slice() else {
        return None;
    };
    if operation.operator != Operator::Equal {
        return None;
    }
    [(left, right), (right, left)]
        .into_iter()
        .find_map(|(target, term)| match &target.kind {
            TermKind::Variable(variable)
                if variable.kind == VariableKind::Universal
                    && !bound.contains(&variable.name)
                    && !universal(term).contains(&variable.name) =>
            {
                Some((variable.name.as_str(), term))
            }
            _ => None,
        })
}

/// The remote atoms that one query replaces.
///
/// A component is one of these:
/// - positive atoms over one endpoint, connected by shared variables,
///   with the negated atoms that they absorbed as `FILTER NOT EXISTS` conditions;
/// - a single negated atom that no component absorbed, which the rule reads negatively.
pub(super) struct RemoteComponent {
    /// The positive atoms, as indices into the rule's remote atoms.
    pub(super) members: Vec<usize>,
    /// The absorbed negated atoms, as indices into the rule's remote atoms.
    pub(super) absorbed: Vec<usize>,
}

impl RemoteComponent {
    /// Returns the component's atoms: its members, then the negated atoms that it absorbed.
    pub(super) fn atoms<'a>(&self, remote: &'a [RemoteAtom]) -> Vec<&'a RemoteAtom> {
        self.members
            .iter()
            .chain(&self.absorbed)
            .map(|&index| &remote[index])
            .collect()
    }

    /// Returns whether the rule reads the component negatively,
    /// which is the case for a single negated atom that no component absorbed.
    pub(super) fn negative(&self) -> bool {
        self.members.is_empty()
    }

    /// Returns the variables of the component's atoms, in order of first mention.
    pub(super) fn variables(&self, remote: &[RemoteAtom]) -> Vec<String> {
        let mut variables = Vec::new();
        for atom in self.atoms(remote) {
            extend_unique(&mut variables, atom.variables.iter().cloned());
        }
        variables
    }

    /// Returns whether the component is a single atom whose arguments are distinct variables,
    /// with nothing absorbed.
    pub(super) fn plain(&self, remote: &[RemoteAtom]) -> bool {
        matches!(self.members.as_slice(), [member] if remote[*member].plain)
            && self.absorbed.is_empty()
    }
}

impl RewriteContext {
    /// Returns the candidate a remote atom reads.
    pub(super) fn candidate(&self, atom: &RemoteAtom) -> &CandidateImport {
        &self.candidates[&atom.candidate]
    }

    /// Returns whether two remote atoms read the same endpoint with the same `on_error` setting.
    ///
    /// Only such atoms can be merged into one query.
    fn same_endpoint(&self, left: &RemoteAtom, right: &RemoteAtom) -> bool {
        let (left, right) = (self.candidate(left), self.candidate(right));
        left.endpoint() == right.endpoint() && left.on_error() == right.on_error()
    }

    /// Resolves the atoms of a rule over candidate imports, in body order.
    ///
    /// An operation with variables as the argument of a positive atom becomes a
    /// [computed argument](ComputedArgument).
    /// `rule_variables` contains the rule's variables, which the fresh variables avoid.
    ///
    /// Returns `None` if one of the atoms cannot be composed:
    /// if its arity is not the query's, which translation reports,
    /// if one of its constants has no SPARQL form,
    /// or if it is negated and has an argument that is neither a variable nor ground.
    /// The rule then goes to translation as written.
    pub(super) fn remote_atoms(
        &self,
        rule: &Rule,
        rule_variables: &HashSet<String>,
    ) -> Option<Vec<RemoteAtom>> {
        let mut remote = Vec::new();
        let mut anonymous = 0;
        let mut fresh = 0;
        for (position, literal) in rule.body.iter().enumerate() {
            let (atom, negated) = match literal {
                Literal::Positive(atom) => (atom, false),
                Literal::Negative(atom) => (atom, true),
                Literal::Operation(_) => continue,
            };
            let Ok(name) = self.scope.resolve(&atom.predicate, atom.span) else {
                continue;
            };
            let Some(candidate) = self.candidates.get(&name) else {
                continue;
            };
            if atom.terms.len() != candidate.query.arity() {
                return None;
            }
            let mut bindings = Vec::with_capacity(atom.terms.len());
            let mut variables = Vec::new();
            let mut computed = Vec::new();
            let mut plain = true;
            for (index, term) in atom.terms.iter().enumerate() {
                let binding = match &term.kind {
                    TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                        if variables.contains(&variable.name) {
                            plain = false;
                        } else {
                            variables.push(variable.name.clone());
                        }
                        Binding::Variable(variable.name.clone())
                    }
                    TermKind::Variable(variable) if variable.kind == VariableKind::Anonymous => {
                        Binding::Variable(private_name(rule_variables, "anonymous", &mut anonymous))
                    }
                    TermKind::Operation(_) if !negated && !term.is_ground() => {
                        plain = false;
                        let variable = private_name(rule_variables, "computed", &mut fresh);
                        variables.push(variable.clone());
                        computed.push(ComputedArgument {
                            position: index,
                            variable: variable.clone(),
                            term: term.clone(),
                        });
                        Binding::Variable(variable)
                    }
                    other => {
                        plain = false;
                        Binding::Constant(self.ground_terms(other)?)
                    }
                };
                bindings.push(binding);
            }
            remote.push(RemoteAtom {
                position,
                candidate: name,
                bindings,
                variables,
                plain,
                negated,
                computed,
            });
        }
        Some(remote)
    }

    /// Groups the remote atoms into components, each of which one query replaces.
    ///
    /// The positive atoms of one endpoint that are connected by shared variables form a
    /// component.
    /// The components that share variables with one negated atom are merged,
    /// and each negated atom is absorbed by a component that binds all its variables.
    /// `groups` are the rule's local groups.
    pub(super) fn components(
        &self,
        remote: &[RemoteAtom],
        groups: &[LocalGroup],
    ) -> Vec<RemoteComponent> {
        let mut components = self.connected(remote);
        self.bridge(&mut components, remote);
        self.absorb(components, remote, groups)
    }

    /// Returns the positive atoms grouped by endpoint and shared variables,
    /// with each component's members in the order in which they were reached.
    ///
    /// An atom whose query [stands alone](CandidateImport::stands_alone) forms a component of
    /// its own.
    fn connected(&self, remote: &[RemoteAtom]) -> Vec<RemoteComponent> {
        let positive: Vec<usize> = (0..remote.len())
            .filter(|&index| !remote[index].negated)
            .collect();
        let mut components = Vec::new();
        let mut assigned: HashSet<usize> = HashSet::new();
        for &seed in &positive {
            if !assigned.insert(seed) {
                continue;
            }
            let mut component = RemoteComponent {
                members: vec![seed],
                absorbed: Vec::new(),
            };
            if self.candidate(&remote[seed]).stands_alone() {
                components.push(component);
                continue;
            }
            let mut frontier = vec![seed];
            while let Some(current) = frontier.pop() {
                for &other in &positive {
                    if assigned.contains(&other)
                        || !self.same_endpoint(&remote[other], &remote[current])
                        || self.candidate(&remote[other]).stands_alone()
                        || !shares(&remote[other].variables, &remote[current].variables)
                    {
                        continue;
                    }
                    assigned.insert(other);
                    component.members.push(other);
                    frontier.push(other);
                }
            }
            components.push(component);
        }
        components
    }

    /// Merges the components that share variables with one negated atom of their endpoint.
    ///
    /// The query that contains a `FILTER NOT EXISTS` must bind every variable it refers to.
    /// In a query of its own, the negated atom could only be fetched unrestricted.
    fn bridge(&self, components: &mut Vec<RemoteComponent>, remote: &[RemoteAtom]) {
        loop {
            let bridged = remote.iter().filter(|atom| atom.negated).find_map(|atom| {
                let connected: Vec<usize> = components
                    .iter()
                    .enumerate()
                    .filter(|(_, component)| {
                        component.members.iter().any(|&member| {
                            self.same_endpoint(&remote[member], atom)
                                && !self.candidate(&remote[member]).stands_alone()
                                && shares(&remote[member].variables, &atom.variables)
                        })
                    })
                    .map(|(index, _)| index)
                    .collect();
                (connected.len() > 1).then_some(connected)
            });
            let Some(connected) = bridged else {
                break;
            };
            let target = connected[0];
            for &other in connected[1..].iter().rev() {
                let merged = components.remove(other);
                components[target].members.extend(merged.members);
                components[target].absorbed.extend(merged.absorbed);
            }
        }
    }

    /// Absorbs each negated atom into the first component of its endpoint that binds all its
    /// variables, through its members or the local groups that share variables with them.
    ///
    /// A negated atom that no component absorbs becomes a component of its own,
    /// which the rule reads negatively.
    fn absorb(
        &self,
        mut components: Vec<RemoteComponent>,
        remote: &[RemoteAtom],
        groups: &[LocalGroup],
    ) -> Vec<RemoteComponent> {
        let mut absorbed: HashSet<usize> = HashSet::new();
        for component in &mut components {
            let members = component.variables(remote);
            let mut bound: HashSet<&str> = members.iter().map(String::as_str).collect();
            for group in groups {
                if shares(&group.variables, &members) {
                    bound.extend(group.variables.iter().map(String::as_str));
                }
            }
            for (index, atom) in remote.iter().enumerate() {
                if !atom.negated
                    || absorbed.contains(&index)
                    || !self.same_endpoint(atom, &remote[component.members[0]])
                    || self.candidate(atom).stands_alone()
                    || !atom
                        .variables
                        .iter()
                        .all(|variable| bound.contains(variable.as_str()))
                {
                    continue;
                }
                absorbed.insert(index);
                component.absorbed.push(index);
            }
        }
        for (index, atom) in remote.iter().enumerate() {
            if atom.negated && !absorbed.contains(&index) {
                components.push(RemoteComponent {
                    members: Vec::new(),
                    absorbed: vec![index],
                });
            }
        }
        components
    }

    /// Returns the local groups that bind a component: those that share a variable with it.
    ///
    /// `variables` are the component's variables.
    /// A component that the rule reads negatively is not bound by a group that depends on the
    /// rule's head,
    /// since the fetch would then be in a negative cycle with the rule,
    /// and the program could not be stratified.
    /// Such a group keeps its variables local.
    pub(super) fn overlapping<'a>(
        &self,
        groups: &'a [LocalGroup],
        component: &RemoteComponent,
        variables: &[String],
        rule: &Rule,
    ) -> Vec<&'a LocalGroup> {
        groups
            .iter()
            .filter(|group| shares(&group.variables, variables))
            .filter(|group| !component.negative() || !self.recurs_through(&group.positions, rule))
            .collect()
    }
}
