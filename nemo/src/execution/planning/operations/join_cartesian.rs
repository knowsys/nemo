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
        let partitioned_atoms = Self::partition_atoms(atoms, operations);

        let mut joins = Vec::default();
        let mut filters = Vec::default();

        for partition in partitioned_atoms {
            let join = GeneratorJoinSeminaive::new(partition, order);
            let filter = GeneratorFunctionFilterNegation::new(
                join.output_variables(),
                operations,
                atoms_negation,
            )
            .or_none();

            joins.push(join);
            filters.push(filter);
        }

        Self { joins, filters }
    }

    /// Return a list of variables used in each of the cartesian products.
    pub fn output_variables(&self) -> Vec<Vec<Variable>> {
        self.joins
            .iter()
            .map(|join| join.output_variables())
            .collect::<Vec<_>>()
    }

    /// Partition the given [BodyAtom]s based on whether they are connected by shared variables
    /// (or [Operation]s).
    ///
    /// For example:
    /// atoms: [a(x, y), b(y, z), c(v), r(w), t(w)], operations: [v < z]
    /// result: [[a(x, y), b(y, z), c(v)], [r(w), t(w)]]
    fn partition_atoms(atoms: &[BodyAtom], operations: &[Operation]) -> Vec<Vec<BodyAtom>> {
        #[derive(Clone)]
        struct Partition {
            pub variables: HashSet<Variable>,
            pub origins: Vec<usize>,
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

        for operation in operations {
            let partition = Partition {
                variables: operation.variables().cloned().collect::<HashSet<_>>(),
                origins: Vec::default(),
            };

            partitions.add(partition);
        }

        let mut result = Vec::default();

        for partition in partitions.0 {
            let partitioned_atoms = partition
                .origins
                .into_iter()
                .map(|origin| atoms[origin].clone())
                .collect::<Vec<_>>();
            result.push(partitioned_atoms);
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

            result.push(node);
        }

        result
    }
}
