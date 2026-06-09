use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::execution::planning::normalization::{
    aggregate::Aggregation,
    atom::{body::BodyAtom, head::HeadAtom, import::ImportAtom},
    operation::Operation,
    rule::NormalizedRule,
};
use crate::rule_model::components::term::{
    aggregate::AggregateKind,
    operation::operation_kind::OperationKind,
    primitive::{Primitive, variable::Variable},
};

use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::Predicate;

use strum::IntoEnumIterator;

#[derive(Clone, Debug)]
pub struct RuleEncoding {
    pred_stream: Vec<Predicate>,
    arg_stream: Vec<usize>,
}

#[derive(Debug)]
struct EncodeState {
    pred_stream: Vec<Predicate>,
    arg_stream: Vec<usize>,
    v: usize,
    var_map: HashMap<String, usize>,
}

impl EncodeState {
    fn encode_iter<'a, T: 'a>(
        &mut self,
        len: usize,
        it: impl Iterator<Item = &'a T>,
        f: fn(&mut EncodeState, &T),
    ) {
        self.arg_stream.push(len);
        for elem in it {
            f(self, elem);
        }
    }
    fn encode_vec<T>(&mut self, vec: &Vec<T>, f: fn(&mut EncodeState, &T)) {
        self.encode_iter(vec.len(), vec.iter(), f);
    }
    fn encode_variable(&mut self, var: &Variable) {
        let name = match var {
            Variable::Universal(u_var) => u_var.name(),
            Variable::Existential(e_var) => Some(e_var.name()),
            Variable::Global(g_var) => Some(g_var.name()),
        };
        match name {
            Some(name) => {
                self.arg_stream
                    .push(*self.var_map.entry(name.to_string()).or_insert_with(|| {
                        let v = self.v;
                        self.v += 1;
                        v
                    }));
            }
            None => {
                // anonymous variable
                self.arg_stream.push(self.v);
                self.v += 1;
            }
        }
    }
    fn encode_primitive(&mut self, prim: &Primitive) {
        match prim {
            Primitive::Variable(var) => self.encode_variable(var),
            Primitive::Ground(g) => {
                let mut hasher = DefaultHasher::new();
                g.value().hash(&mut hasher);
                let hash = hasher.finish();
                self.arg_stream.push(hash as usize); // may truncate high bits of hash
            }
        }
    }
    fn encode_body_atom(&mut self, body_atom: &BodyAtom) {
        self.pred_stream
            .push(body_atom.predicate().name().to_owned());
        self.encode_iter(body_atom.arity(), body_atom.terms(), Self::encode_variable);
    }
    fn encode_import_atom(&mut self, import_atom: &ImportAtom) {
        self.pred_stream
            .push(import_atom.predicate().name().to_owned());
        self.encode_iter(
            import_atom.arity(),
            import_atom.variables(),
            Self::encode_variable,
        );
    }
    fn encode_operation(&mut self, operation: &Operation) {
        match operation {
            Operation::Primitive(prim) => self.encode_primitive(prim),
            Operation::Operation { kind, subterms } => {
                let kind_index = OperationKind::iter().position(|k| k == *kind).unwrap();
                self.arg_stream.push(kind_index);
                self.encode_vec(subterms, Self::encode_operation);
            }
        }
    }
    fn encode_head_atom(&mut self, head_atom: &HeadAtom) {
        self.pred_stream
            .push(head_atom.predicate().name().to_owned());
        self.encode_iter(head_atom.arity(), head_atom.terms(), Self::encode_primitive);
    }
    fn encode_aggregation(&mut self, aggregation: &Aggregation, aggregate_index: usize) {
        let kind = aggregation.aggregate_kind();
        let kind_index = AggregateKind::iter().position(|k| k == kind).unwrap();
        self.arg_stream.push(kind_index);
        self.encode_variable(aggregation.input_variable());
        self.encode_variable(aggregation.output_variable());
        self.encode_vec(aggregation.distinct_variables(), Self::encode_variable);
        self.encode_vec(aggregation.group_by_variables(), Self::encode_variable);

        self.arg_stream.push(aggregate_index);
    }
}

// TODO: how to make sure that atom mappings stay valid with reordered atoms?
pub fn encode_rule(rule: &NormalizedRule) -> RuleEncoding {
    let mut state = EncodeState {
        pred_stream: vec![],
        arg_stream: vec![],
        v: 0,
        var_map: HashMap::new(),
    };

    state.encode_vec(rule.positive(), EncodeState::encode_body_atom);
    state.encode_vec(rule.negative_atoms(), |enc, neg_body_atom| {
        enc.encode_body_atom(neg_body_atom.as_body_atom())
    });
    state.encode_vec(rule.positive_imports(), EncodeState::encode_import_atom);
    state.encode_vec(rule.negative_imports(), EncodeState::encode_import_atom);
    state.encode_vec(rule.operations(), EncodeState::encode_operation);
    state.encode_vec(rule.head(), EncodeState::encode_head_atom);
    if let Some(aggregate) = rule.aggregate() {
        state.encode_aggregation(
            aggregate,
            rule.aggregate_index()
                .expect("rule with aggregate should have aggregation_index"),
        );
    }

    RuleEncoding {
        pred_stream: state.pred_stream,
        arg_stream: state.arg_stream,
    }
}

pub fn encode_rule_pair(enc1: &RuleEncoding, enc2: &RuleEncoding) -> Vec<usize> {
    let mut key = Vec::with_capacity(
        enc1.pred_stream.len()
            + enc1.arg_stream.len()
            + enc2.pred_stream.len()
            + enc2.arg_stream.len(),
    );
    let mut pred_map = HashMap::new();
    let mut p = 0;
    for pred in enc1.pred_stream.iter().chain(enc2.pred_stream.iter()) {
        key.push(*pred_map.entry(pred).or_insert_with(|| {
            let prev_p = p;
            p += 1;
            prev_p
        }));
    }
    key
}
