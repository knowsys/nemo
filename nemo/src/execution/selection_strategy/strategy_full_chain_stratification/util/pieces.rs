use std::collections::HashSet;

use crate::execution::selection_strategy::strategy_full_chain_stratification::util::ordered_atoms::GetRuleMem; // should be somewhere else...
use crate::{
    execution::planning::normalization::{atom::head::HeadAtom, rule::NormalizedRule},
    rule_model::components::term::primitive::variable::Variable,
};

#[derive(Debug, Clone)]
pub(crate) struct Piece {
    pub(crate) existentials: HashSet<Variable>,
    pub(crate) atoms: Vec<HeadAtom>,
}

impl<'a> GetRuleMem<'a> for Vec<Piece> {
    /// Decompose the head into pieces.
    fn compute(rule: &'a NormalizedRule) -> Vec<Piece> {
        let vars_exists = rule.existentials();
        let mut head_pieces = Vec::new();

        let mut atom_existentials: Vec<(_, _)> = rule
            .head()
            .iter()
            .map(|atom| {
                (
                    atom,
                    atom.variables()
                        .filter(|var| vars_exists.contains(var))
                        .collect::<HashSet<_>>(),
                )
            })
            .collect();

        while let Some((atom, mut existentials)) = atom_existentials.pop() {
            let mut head_piece = vec![atom.clone()];
            if existentials.len() > 0 {
                let mut remaining = true;
                while remaining {
                    let mut removed: Vec<(&HeadAtom, HashSet<&Variable>)> = Vec::new();
                    atom_existentials.retain(|(other_atom, other_existentials)| {
                        if !other_existentials.is_disjoint(&existentials) {
                            removed.push((other_atom, other_existentials.clone()));
                            false
                        } else {
                            true
                        }
                    });
                    remaining = removed.len() > 0;
                    if remaining {
                        let (further_atoms, further_existentials): (Vec<_>, Vec<_>) =
                            removed.into_iter().unzip();
                        head_piece.extend(further_atoms.into_iter().cloned());
                        existentials.extend(further_existentials.into_iter().flatten());
                    }
                }
            }
            head_pieces.push(Piece {
                existentials: head_piece
                    .iter()
                    .flat_map(|atom| atom.variables())
                    .collect::<HashSet<_>>()
                    .intersection(&vars_exists)
                    .copied()
                    .cloned()
                    .collect(),
                atoms: head_piece,
            });
        }
        head_pieces
    }
}
