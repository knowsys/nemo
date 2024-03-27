//! This module contains functionality
//! for handling constants and repeated variables in head atoms.

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    datavalues::AnyDataValue,
    function::tree::FunctionTree,
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
};

use crate::{
    execution::rule_execution::VariableTranslation,
    model::{
        chase_model::{ChaseAtom, PrimitiveAtom, VariableAtom},
        PrimitiveTerm, Variable,
    },
};

/// Encodes an instruction for appending either a constant or repeating the value of a variable
#[derive(Debug)]
pub(crate) enum AppendInstruction {
    Repeat(Variable),
    Constant(AnyDataValue),
}

/// Derived from head atoms which may contain duplicate (non-existential) variables or constants.
/// Represents a normal form which only contains non-duplicate universal variables and
/// the respective [AppendInstruction]s to obtain the intended result.
#[derive(Debug)]
pub(crate) struct HeadInstruction {
    /// Reduced form of the the atom which only contains duplicate variables, constants or existential variables.
    pub reduced_atom: VariableAtom,
    /// The [AppendInstruction]s to get the original atom.
    pub append_instructions: Vec<Vec<AppendInstruction>>,
    /// Arity of the non reduced atom
    pub arity: usize,
}

/// Given an atom, bring compute the corresponding [HeadInstruction].
/// TODO: This needs to be revised once the Type System on the logical layer has been implemented.
pub(crate) fn head_instruction_from_atom(atom: &PrimitiveAtom) -> HeadInstruction {
    let arity = atom.terms().len();
    let mut reduced_terms = Vec::<Variable>::with_capacity(arity);

    let mut append_instructions = Vec::<Vec<AppendInstruction>>::new();
    append_instructions.push(vec![]);
    let mut current_append_vector = &mut append_instructions[0];

    let mut variable_set = HashSet::new();

    for term in atom.terms() {
        match term {
            PrimitiveTerm::Variable(variable) => {
                if !variable_set.insert(variable) {
                    let instruction = AppendInstruction::Repeat(variable.clone());
                    current_append_vector.push(instruction);
                } else {
                    reduced_terms.push(variable.clone());

                    append_instructions.push(vec![]);
                    current_append_vector = append_instructions.last_mut().unwrap();
                }
            }
            PrimitiveTerm::GroundTerm(datavalue) => {
                let instruction = AppendInstruction::Constant(datavalue.clone());
                current_append_vector.push(instruction);
            }
        }
    }

    let reduced_atom = VariableAtom::new(atom.predicate(), reduced_terms);

    HeadInstruction {
        reduced_atom,
        append_instructions,
        arity,
    }
}

/// Compute a node in an execution plan from a [HeadInstruction].
pub(crate) fn node_head_instruction(
    plan: &mut ExecutionPlan,
    variable_translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    instruction: &HeadInstruction,
) -> ExecutionNodeRef {
    let project_markers =
        variable_translation.operation_table(instruction.reduced_atom.get_variables().iter());
    let project_node = plan.projectreorder(project_markers.clone(), subnode);

    let mut append_markers = project_markers;
    let mut insert_index = 0;
    let mut functions = HashMap::new();
    for append_instructions in &instruction.append_instructions {
        for append_instruction in append_instructions {
            let new_maker = append_markers.push_new_at(insert_index);
            insert_index += 1;

            let tree = match append_instruction {
                AppendInstruction::Repeat(variable) => {
                    let variable_marker = variable_translation
                        .get(variable)
                        .expect("All variables are known.");

                    FunctionTree::reference(*variable_marker)
                }
                AppendInstruction::Constant(constant) => FunctionTree::constant(constant.clone()),
            };

            functions.insert(*new_maker, tree);
        }

        insert_index += 1;
    }

    plan.function(append_markers, project_node, functions)
}
