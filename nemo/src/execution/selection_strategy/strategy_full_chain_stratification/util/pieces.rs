use std::collections::HashSet;

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atom, Rule, Var,
};

#[derive(Debug, Clone)]
pub(crate) struct Piece {
    pub(crate) existentials: HashSet<Var>,
    pub(crate) atoms: Vec<Atom>,
}

/// Decompose `rule`'s head into pieces: maximal groups of head atoms transitively connected via
/// shared existential variables.
pub(crate) fn compute_pieces(rule: &Rule) -> Vec<Piece> {
    let vars_exists = rule.existentials();
    let mut head_pieces = Vec::new();

    let mut atom_existentials: Vec<(Atom, HashSet<Var>)> = rule
        .head()
        .atoms()
        .map(|atom| {
            let existentials = atom
                .variables()
                .filter(|var| vars_exists.contains(var))
                .collect::<HashSet<_>>();
            (atom, existentials)
        })
        .collect();

    while let Some((atom, mut existentials)) = atom_existentials.pop() {
        let mut head_piece = vec![atom];
        if !existentials.is_empty() {
            let mut remaining = true;
            while remaining {
                let mut removed: Vec<(Atom, HashSet<Var>)> = Vec::new();
                atom_existentials.retain(|(other_atom, other_existentials)| {
                    if !other_existentials.is_disjoint(&existentials) {
                        removed.push((other_atom.clone(), other_existentials.clone()));
                        false
                    } else {
                        true
                    }
                });
                remaining = !removed.is_empty();
                if remaining {
                    for (further_atom, further_existentials) in removed {
                        head_piece.push(further_atom);
                        existentials.extend(further_existentials);
                    }
                }
            }
        }
        head_pieces.push(Piece {
            existentials: head_piece
                .iter()
                .flat_map(|atom| atom.variables())
                .filter(|v| vars_exists.contains(v))
                .collect(),
            atoms: head_piece,
        });
    }
    head_pieces
}
