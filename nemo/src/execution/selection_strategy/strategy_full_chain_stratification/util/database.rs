use std::collections::{HashMap, HashSet};

use crate::rule_model::components::term::primitive::Primitive;

use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::Predicate;

struct RepresentativeDatabase(HashMap<Predicate, HashSet<Vec<Primitive>>>);
