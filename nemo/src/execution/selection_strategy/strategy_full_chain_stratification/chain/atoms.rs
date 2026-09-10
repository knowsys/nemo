use std::collections::HashMap;

use super::tuples::Tuples;

pub struct Predicate(usize);
pub struct Constant(usize);

pub struct Atoms {
    tuples: Box<[Tuples]>,
    preds: HashMap<Predicate, usize>,
}


pub struct Rule {
    body: Atoms,
    head: Atoms,
    consts: HashMap<usize, Constant>,
}
