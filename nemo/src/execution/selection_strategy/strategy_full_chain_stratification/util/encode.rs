use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atom, Operation, Predicate, Rule, Var,
};

use strum::IntoEnumIterator;

use crate::rule_model::components::term::operation::operation_kind::OperationKind;

#[derive(Clone, Debug)]
pub(crate) struct RuleEncoding {
    pred_stream: Vec<Predicate>,
    arg_stream: Vec<usize>,
}

#[derive(Debug)]
struct EncodeState {
    pred_stream: Vec<Predicate>,
    arg_stream: Vec<usize>,
    v: usize,
    var_map: HashMap<Var, usize>,
    consts: HashMap<Var, u64>,
}

impl EncodeState {
    fn encode_slice<T>(&mut self, slice: &[T], f: fn(&mut EncodeState, &T)) {
        self.arg_stream.push(slice.len());
        for elem in slice {
            f(self, elem);
        }
    }

    /// Encode a term slot: constants hash to a value derived from their constant id, and
    /// variables get a canonical index in first-seen order.
    fn encode_var(&mut self, var: &Var) {
        if let Some(&hash) = self.consts.get(var) {
            self.arg_stream.push(hash as usize); // may truncate high bits of hash
            return;
        }
        let v = self.v;
        let next = *self.var_map.entry(*var).or_insert_with(|| {
            self.v += 1;
            v
        });
        self.arg_stream.push(next);
    }

    fn encode_atom(&mut self, atom: &Atom) {
        self.pred_stream.push(atom.predicate());
        self.encode_slice(atom.terms(), Self::encode_var);
    }

    fn encode_operation(&mut self, operation: &Operation) {
        match operation {
            Operation::Primitive(var) => self.encode_var(var),
            Operation::Operation { kind, subterms } => {
                let kind_index = OperationKind::iter().position(|k| k == *kind).unwrap();
                self.arg_stream.push(kind_index);
                self.encode_slice(subterms, Self::encode_operation);
            }
        }
    }
}

pub(crate) fn encode_rule(rule: &Rule) -> RuleEncoding {
    let consts = rule
        .consts()
        .iter()
        .map(|(&var, constant)| {
            let mut hasher = DefaultHasher::new();
            constant.hash(&mut hasher);
            (var, hasher.finish())
        })
        .collect();

    let mut state = EncodeState {
        pred_stream: vec![],
        arg_stream: vec![],
        v: 0,
        var_map: HashMap::new(),
        consts,
    };

    for atoms in [rule.positive(), rule.negative_atoms(), rule.head()] {
        state.arg_stream.push(atoms.len());
        for atom in atoms.atoms() {
            state.encode_atom(&atom);
        }
    }
    state.encode_slice(rule.operations(), EncodeState::encode_operation);

    RuleEncoding {
        pred_stream: state.pred_stream,
        arg_stream: state.arg_stream,
    }
}

pub(crate) fn encode_rule_pair(enc1: &RuleEncoding, enc2: &RuleEncoding) -> Vec<usize> {
    let mut key = Vec::with_capacity(
        enc1.pred_stream.len()
            + enc1.arg_stream.len()
            + enc2.pred_stream.len()
            + enc2.arg_stream.len(),
    );
    let mut pred_map = HashMap::new();
    let mut p = 0;
    for pred in enc1.pred_stream.iter().chain(enc2.pred_stream.iter()) {
        key.push(*pred_map.entry(*pred).or_insert_with(|| {
            let prev_p = p;
            p += 1;
            prev_p
        }));
    }
    key.extend(enc1.arg_stream.iter().chain(enc2.arg_stream.iter()));
    key
}
