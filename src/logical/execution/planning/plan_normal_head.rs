//! Module defining the strategies used to derive the new facts for a rule application.

use std::collections::HashMap;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Identifier, NumericLiteral, Rule, Term, Variable},
        program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
        table_manager::{ColumnOrder, TableKey},
        types::LogicalTypeCollection,
        TableManager,
    },
    physical::{
        datatypes::DataValueT,
        dictionary::Dictionary,
        management::execution_plan::{ExecutionNodeRef, ExecutionResult, ExecutionTree},
        tabular::operations::triescan_append::AppendInstruction,
        util::Reordering,
    },
};

use super::plan_util::{join_binding, BODY_JOIN};

/// Strategies for calculating the newly derived tables.
pub trait HeadStrategy<Dict: Dictionary, LogicalTypes: LogicalTypeCollection> {
    /// Do preparation work for the planning phase.
    fn initialize(rule: &Rule<LogicalTypes>, analysis: &RuleAnalysis) -> Self;

    /// Calculate the concrete plan given a variable order.
    fn execution_tree(
        &self,
        table_manager: &TableManager<Dict, LogicalTypes>,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> Vec<ExecutionTree<TableKey>>;
}

/// Derived from head atoms which may contain duplicate variables or constants.
/// Represents a normal form which only contains non-duplicate variables and
/// the respective [`AppendInstruction`]s to obtain the intended result.
#[derive(Debug)]
struct HeadInstruction {
    reduced_atom: Atom,
    append_instructions: Vec<Vec<AppendInstruction>>,
    arity: usize,
}

/// Strategy for computing the results for a datalog (non-existential) rule.
#[derive(Debug)]
pub struct DatalogStrategy {
    predicate_to_atoms: HashMap<Identifier, Vec<HeadInstruction>>,
    num_body_variables: usize,
}

impl DatalogStrategy {
    /// TODO: This needs to be revised once the Type System on the logical layer has been implemented.
    fn head_instruction_from_atom(atom: &Atom) -> HeadInstruction {
        let arity = atom.terms().len();
        let mut reduced_terms = Vec::<Term>::with_capacity(arity);
        let mut append_instructions = Vec::<Vec<AppendInstruction>>::new();

        append_instructions.push(vec![]);
        let mut current_append_vector = &mut append_instructions[0];

        let mut variable_map = HashMap::<Identifier, usize>::new();

        for (term_index, term) in atom.terms().iter().enumerate() {
            match term {
                Term::NumericLiteral(nl) => match nl {
                    NumericLiteral::Integer(i) => {
                        let instruction = AppendInstruction::Constant(
                            DataValueT::U64((*i).try_into().unwrap()),
                            false,
                        );
                        current_append_vector.push(instruction);
                    }
                    _ => unimplemented!(),
                },
                Term::Constant(identifier) => {
                    let instruction = AppendInstruction::Constant(
                        DataValueT::U64(identifier.to_constant_u64()),
                        false,
                    );
                    current_append_vector.push(instruction);
                }
                Term::Variable(variable) => {
                    if let Variable::Universal(universal_variable) = variable {
                        if let Some(repeat_index) = variable_map.get(universal_variable) {
                            let instruction = AppendInstruction::RepeatColumn(*repeat_index);
                            current_append_vector.push(instruction);
                        } else {
                            reduced_terms
                                .push(Term::Variable(Variable::Universal(*universal_variable)));

                            variable_map.insert(*universal_variable, term_index);

                            append_instructions.push(vec![]);
                            current_append_vector = append_instructions.last_mut().unwrap();
                        }
                    } else {
                        panic!("This class provides a strategy only for datalog rules.");
                    }
                }
                Term::RdfLiteral(_) => unimplemented!(),
            }
        }

        let reduced_atom = Atom::new(atom.predicate(), reduced_terms);

        HeadInstruction {
            reduced_atom,
            append_instructions,
            arity,
        }
    }
}

impl<Dict: Dictionary, LogicalTypes: LogicalTypeCollection> HeadStrategy<Dict, LogicalTypes>
    for DatalogStrategy
{
    fn initialize(rule: &Rule<LogicalTypes>, analysis: &RuleAnalysis) -> Self {
        let mut predicate_to_atoms = HashMap::<Identifier, Vec<HeadInstruction>>::new();

        for head_atom in rule.head() {
            let atoms = predicate_to_atoms
                .entry(head_atom.predicate())
                .or_insert(Vec::new());

            atoms.push(Self::head_instruction_from_atom(head_atom));
        }

        let num_body_variables = analysis.body_variables.len();

        Self {
            predicate_to_atoms,
            num_body_variables,
        }
    }

    fn execution_tree(
        &self,
        table_manager: &TableManager<Dict, LogicalTypes>,
        _rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> Vec<ExecutionTree<TableKey>> {
        let mut trees = Vec::<ExecutionTree<TableKey>>::new();

        for (&predicate, head_instructions) in self.predicate_to_atoms.iter() {
            let predicate_arity = head_instructions[0].arity;
            // We just pick the default order
            // TODO: Is there a better pick?
            let head_order = ColumnOrder::default(predicate_arity);

            let head_table_name =
                table_manager.get_table_name(predicate, step_number..step_number + 1);
            let head_table_key =
                TableKey::from_name(head_table_name, ColumnOrder::default(predicate_arity));
            let mut head_tree = ExecutionTree::<TableKey>::new(
                "Datalog Head".to_string(),
                ExecutionResult::Save(head_table_key),
            );

            let mut project_append_nodes =
                Vec::<ExecutionNodeRef<TableKey>>::with_capacity(head_instructions.len());
            for head_instruction in head_instructions {
                let head_binding = join_binding(
                    &head_instruction.reduced_atom,
                    &Reordering::default(head_instruction.reduced_atom.terms().len()),
                    &variable_order,
                );
                let head_reordering =
                    Reordering::new(head_binding.clone(), self.num_body_variables);

                let fetch_node = head_tree.fetch_temp(BODY_JOIN);
                let project_node = head_tree.project(fetch_node, head_reordering);
                let append_node = head_tree
                    .append_columns(project_node, head_instruction.append_instructions.clone());

                project_append_nodes.push(append_node);
            }

            let new_tables_union = head_tree.union(project_append_nodes);

            let old_tables_keys: Vec<TableKey> = table_manager
                .cover_whole_table(predicate)
                .into_iter()
                .map(|r| TableKey::new(predicate, r, head_order.clone()))
                .collect();
            let old_table_nodes: Vec<ExecutionNodeRef<TableKey>> = old_tables_keys
                .into_iter()
                .map(|k| head_tree.fetch_table(k))
                .collect();
            let old_table_union = head_tree.union(old_table_nodes);

            let remove_duplicate_node = head_tree.minus(new_tables_union, old_table_union);
            head_tree.set_root(remove_duplicate_node);

            trees.push(head_tree);
        }

        trees
    }
}
