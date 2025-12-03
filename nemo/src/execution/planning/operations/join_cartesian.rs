//! This module defines [GeneratorJoinCartesian].

use std::collections::HashSet;

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning::{
        RuntimeInformation,
        analysis::variable_order::VariableOrder,
        normalization::{atom::body::BodyAtom, operation::Operation},
        operations::{
            function_filter_negation::GeneratorFunctionFilterNegation,
            join_seminaive::GeneratorJoinSeminaive,
        },
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// The rule body is divided into multiple independent factors.
/// Those can either be [BodyAtom]s
/// or [Operation]s assigning constants to a variable.
#[derive(Debug)]
enum Factor {
    /// Factor consisting of body atoms
    Atoms(Vec<BodyAtom>),
    /// Factor consisting of operations
    Operations(Vec<Operation>),
}

/// Generator for execution plan nodes
/// representing multiple disjoint seminaive join of body atoms
#[derive(Debug)]
pub struct GeneratorJoinCartesian {
    /// Distinct joins
    joins: Vec<GeneratorJoinSeminaive>,

    /// List of filters applied to `joins`
    filters: Vec<Option<GeneratorFunctionFilterNegation>>,
}

impl GeneratorJoinCartesian {
    /// Create a new [GeneratorJoinCartesian].
    pub fn new(
        order: &VariableOrder,
        atoms: &[BodyAtom],
        operations: &mut Vec<Operation>,
        atoms_negation: &mut Vec<BodyAtom>,
    ) -> Self {
        let factors = Self::partition_atoms(atoms, operations);

        let mut joins = Vec::default();
        let mut filters = Vec::default();

        for factor in factors {
            let (join, filter) = match factor {
                Factor::Atoms(atoms) => {
                    let join = GeneratorJoinSeminaive::new(atoms, order);
                    let filter = GeneratorFunctionFilterNegation::new(
                        join.output_variables(),
                        operations,
                        atoms_negation,
                    )
                    .or_none();

                    (join, filter)
                }
                Factor::Operations(mut operations) => {
                    let join = GeneratorJoinSeminaive::new(Vec::default(), order);
                    let filter = GeneratorFunctionFilterNegation::new(
                        Vec::default(),
                        &mut operations,
                        atoms_negation,
                    )
                    .or_none();

                    (join, filter)
                }
            };

            joins.push(join);
            filters.push(filter);
        }

        Self { joins, filters }
    }

    /// Return a list of variables used in each of the cartesian products.
    pub fn output_variables(&self) -> Vec<Vec<Variable>> {
        self.joins
            .iter()
            .zip(self.filters.iter())
            .map(|(join, filter)| {
                if let Some(filter) = filter {
                    filter.output_variables()
                } else {
                    join.output_variables()
                }
            })
            .collect::<Vec<_>>()
    }

    /// Partition the given [BodyAtom]s and (constant operations)
    /// based on whether they are connected by shared variables.
    ///
    /// For example:
    /// atoms: [a(x, y), b(y, z), c(z), r(w), t(w)]
    /// operations: [q = 3, s = 7]
    /// result: [[a(x, y), b(y, z), c(z)], [r(w), t(w)], [q = 3, s = 7]]
    fn partition_atoms(atoms: &[BodyAtom], operations: &mut Vec<Operation>) -> Vec<Factor> {
        #[derive(Clone)]
        struct Partition {
            pub variables: HashSet<Variable>,
            pub origins: Vec<usize>,
        }

        impl std::fmt::Display for Partition {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let variables = self
                    .variables
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let origins = format!("{:?}", self.origins);

                f.write_fmt(format_args!(
                    "Paritition {{ variables: {variables}, origins: {origins} }}"
                ))
            }
        }

        impl Partition {
            /// Combine two [Partition]s into one.
            pub fn combine(&self, others: Vec<Self>) -> Self {
                let mut variables = self.variables.clone();
                let mut origins = self.origins.clone();

                for other in others {
                    variables.extend(other.variables);
                    origins.extend(other.origins);
                }

                Self { variables, origins }
            }

            /// Check whether this and another [Partition]
            /// share variables.
            pub fn compatible(&self, other: &Self) -> bool {
                self.variables
                    .intersection(&other.variables)
                    .next()
                    .is_some()
            }
        }

        #[derive(Default)]
        struct Partitions(Vec<Partition>);

        impl Partitions {
            /// Add a new [Partition] and combines compatible partitions.
            pub fn add(&mut self, partition: Partition) {
                let mut compatible = Vec::<Partition>::default();

                self.0.retain(|other| {
                    if other.compatible(&partition) {
                        compatible.push(other.clone());
                        return false;
                    }

                    true
                });

                self.0.push(partition.combine(compatible));
            }
        }

        let mut partitions = Partitions::default();

        for (atom_index, atom) in atoms.iter().enumerate() {
            let partition = Partition {
                variables: atom.terms().cloned().collect::<HashSet<_>>(),
                origins: vec![atom_index],
            };

            partitions.add(partition);
        }

        let mut result = Vec::default();

        for partition in partitions.0 {
            if partition.origins.is_empty() {
                continue;
            }

            let factor_atoms = partition
                .origins
                .into_iter()
                .map(|origin| atoms[origin].clone())
                .collect::<Vec<_>>();

            result.push(Factor::Atoms(factor_atoms));
        }

        let body_variables = atoms
            .iter()
            .flat_map(|atom| atom.terms())
            .collect::<HashSet<_>>();

        let mut constant_operations = Vec::<Operation>::default();
        let mut constant_operations_len = constant_operations.len();
        let mut constant_operations_variables = HashSet::<Variable>::default();

        loop {
            operations.retain(|operation| {
                if let Some((left, right)) = operation.variable_assignment()
                    && !body_variables.contains(left)
                    && right
                        .variables()
                        .all(|variable| constant_operations_variables.contains(variable))
                {
                    constant_operations.push(operation.clone());
                    constant_operations_variables.insert(left.clone());

                    return false;
                }

                true
            });

            if constant_operations_len == constant_operations.len() {
                break;
            }

            constant_operations_len = constant_operations.len();
        }

        if !constant_operations.is_empty() {
            result.push(Factor::Operations(constant_operations));
        }

        result
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        runtime: &RuntimeInformation,
    ) -> Vec<ExecutionNodeRef> {
        let mut result = Vec::default();

        for (join, filter) in self.joins.iter().zip(self.filters.iter()) {
            let mut node = join.create_plan(plan, runtime);

            if let Some(filter) = filter {
                node = filter.create_plan(plan, node, runtime);
            }

            plan.add_temporary_table(node.clone(), "Cartesian Factor");

            result.push(node);
        }

        result
    }

    /// Return whether this the body atoms were partitioned into only one group.
    pub fn is_single_join(&self) -> bool {
        self.joins.iter().filter(|join| !join.is_empty()).count() == 1
    }
}
