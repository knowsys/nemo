//! This module defines [GeneratorProjectionHead].

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    datavalues::AnyDataValue, function::tree::FunctionTree,
    management::execution_plan::ExecutionNodeRef,
};

use crate::{
    execution::planning::{RuntimeInformation, normalization::atom::head::HeadAtom},
    rule_model::components::{
        tag::Tag,
        term::primitive::{Primitive, variable::Variable},
    },
    table_manager::SubtableExecutionPlan,
};

/// Marker of a position in a head atom
/// indicating which operation should be used to obtain it
#[derive(Debug)]
enum HeadInstruction {
    /// Projected variable
    Variable(Variable),
    /// Repetition of an earlier variabe
    Repeat(Variable),
    /// Constant value
    Constant(AnyDataValue),
}

/// Operational representation of an [HeadAtom]
#[derive(Debug)]
struct ProjectionHeadAtom {
    /// Marked columns
    terms: Vec<HeadInstruction>,

    /// Predicate
    predicate: Tag,
}

impl ProjectionHeadAtom {
    /// Return an iterator all variables in this atom.
    pub fn projection_variables(&self) -> impl Iterator<Item = &Variable> {
        self.terms.iter().filter_map(|instruction| {
            if let HeadInstruction::Variable(variable) = instruction {
                Some(variable)
            } else {
                None
            }
        })
    }

    /// Return the predicate.
    pub fn predicate(&self) -> Tag {
        self.predicate.clone()
    }
}

impl From<HeadAtom> for ProjectionHeadAtom {
    fn from(atom: HeadAtom) -> Self {
        let mut variables = HashSet::<Variable>::default();
        let mut instructions = Vec::<HeadInstruction>::default();

        for term in atom.terms() {
            let instruction = match term {
                Primitive::Variable(variable) => {
                    if variables.insert(variable.clone()) {
                        HeadInstruction::Variable(variable.clone())
                    } else {
                        HeadInstruction::Repeat(variable.clone())
                    }
                }
                Primitive::Ground(ground) => HeadInstruction::Constant(ground.value()),
            };

            instructions.push(instruction);
        }

        ProjectionHeadAtom {
            terms: instructions,
            predicate: atom.predicate(),
        }
    }
}

/// Generator of a project node in execution plans
/// representing head atoms
#[derive(Debug)]
pub struct GeneratorProjectionHead {
    /// Head atom
    atom: ProjectionHeadAtom,
}

impl GeneratorProjectionHead {
    /// Create a new [GeneratorProjectionHead].
    pub fn new(atom: HeadAtom) -> Self {
        Self {
            atom: ProjectionHeadAtom::from(atom),
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let markers_projection = runtime
            .translation
            .operation_table(self.atom.projection_variables());

        let node_projection = plan
            .plan_mut()
            .projectreorder(markers_projection.clone(), input_node);

        let mut markers_append = markers_projection;
        let mut functions = HashMap::default();

        for (term_index, term) in self.atom.terms.iter().enumerate() {
            match term {
                HeadInstruction::Variable(_) => {}
                HeadInstruction::Repeat(variable) => {
                    let marker_variable = runtime
                        .translation
                        .get(variable)
                        .expect("all variables are known");

                    let marker = *markers_append.push_new_at(term_index);
                    let tree = FunctionTree::reference(*marker_variable);

                    functions.insert(marker, tree);
                }
                HeadInstruction::Constant(constant) => {
                    let marker = *markers_append.push_new_at(term_index);
                    let tree = FunctionTree::constant(constant.clone());

                    functions.insert(marker, tree);
                }
            }
        }

        plan.plan_mut()
            .function(markers_append, node_projection, functions)
    }

    /// Return the predicate.
    pub fn predicate(&self) -> Tag {
        self.atom.predicate()
    }
}
