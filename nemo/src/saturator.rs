#![feature(btree_cursors)]
#![feature(hash_set_entry)]

use nemo::rule_model::components::{
    atom::Atom,
    literal::Literal,
    rule::Rule,
    term::{
        primitive::{
            ground,
            variable::{universal::UniversalVariable, Variable},
            Primitive,
        },
        Term,
    },
    IterableVariables, ProgramComponent,
};

use nemo_physical::{datatypes::StorageValueT, dictionary::DvDict, management::database::Dict};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    ThreadPoolBuilder,
};
use std::{
    collections::{btree_set, BTreeSet, HashMap, HashSet, VecDeque},
    i32,
    iter::repeat_n,
    ops::{Bound, Index},
    sync::{mpsc, Arc},
    thread::current,
};

type VariableIdx = u16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum BodyTerm {
    Constant(StorageValueT),
    Variable(VariableIdx),
    Ignore,
}

#[derive(Debug, Default, Clone)]
struct SaturationSubstitution(Vec<Option<StorageValueT>>);

impl Index<VariableIdx> for SaturationSubstitution {
    type Output = Option<StorageValueT>;

    fn index(&self, index: VariableIdx) -> &Self::Output {
        if self.0.len() <= usize::from(index) {
            &None
        } else {
            &self.0[usize::from(index)]
        }
    }
}

impl SaturationSubstitution {
    fn insert(&mut self, var: VariableIdx, value: StorageValueT) -> Option<StorageValueT> {
        if self.0.len() <= usize::from(var) {
            self.0.resize_with(usize::from(var + 1), || None);
            self.0[usize::from(var)] = Some(value);
            None
        } else {
            let prev = self.0[usize::from(var)];
            self.0[usize::from(var)] = Some(value);
            prev
        }
    }

    fn bind(&self, terms: &[BodyTerm]) -> Row {
        terms
            .iter()
            .map(|term| match term {
                BodyTerm::Constant(constant) => RowElement::Value(*constant),
                BodyTerm::Variable(var) => self[*var]
                    .map(RowElement::Value)
                    .unwrap_or(RowElement::Bottom),
                BodyTerm::Ignore => RowElement::Bottom,
            })
            .collect()
    }

    fn update(&mut self, terms: &[BodyTerm], row: &[RowElement]) {
        for (term, value) in terms.iter().zip(row) {
            let BodyTerm::Variable(var) = term else {
                continue;
            };

            self.insert(*var, value.value());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SaturationAtom {
    predicate: Arc<str>,
    terms: Box<[BodyTerm]>,
}

// impl Clone for SaturationAtom {
//     fn clone(&self) -> Self {
//         todo!()
//     }
// }

impl SaturationAtom {
    fn match_fact(&self, fact: &SaturationFact) -> Option<SaturationSubstitution> {
        if fact.predicate != self.predicate {
            return None;
        }

        let mut res = SaturationSubstitution::default();
        debug_assert_eq!(self.terms.len(), fact.values.len());

        for (term, value) in self.terms.iter().zip(&fact.values) {
            match term {
                BodyTerm::Constant(constant) => {
                    if value != constant {
                        return None;
                    }
                }
                BodyTerm::Variable(idx) => {
                    if let Some(prev) = res.insert(*idx, *value) {
                        if prev != *value {
                            return None;
                        }
                    }
                }
                BodyTerm::Ignore => {}
            }
        }

        Some(res)
    }

    fn variables(&self) -> impl Iterator<Item = VariableIdx> + use<'_> {
        self.terms.iter().flat_map(|term| match term {
            BodyTerm::Variable(var) => Some(*var),
            _ => None,
        })
    }
}

type JoinOrder = Arc<[JoinOp]>;

enum JoinOp {
    Join(SaturationAtom),
    Filter(SaturationAtom),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum HeadTerm {
    Existential(VariableIdx),
    Universal(VariableIdx),
    Constant(StorageValueT),
}

struct HeadAtom {
    predicate: Arc<str>,
    terms: Box<[HeadTerm]>,
}

enum Head {
    Datalog(Box<[SaturationAtom]>),
    Existential(Box<[HeadTerm]>),
}

struct SaturationRule {
    body_atoms: Box<[SaturationAtom]>,
    join_orders: Box<[JoinOrder]>,
    head: Head,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SaturationFact {
    predicate: Arc<str>,
    values: Box<[StorageValueT]>,
}

impl SaturationRule {
    fn trigger<'a, 'b>(
        &'a self,
        fact: &'b SaturationFact,
    ) -> impl Iterator<Item = (SaturationSubstitution, JoinOrder)> + use<'a, 'b> {
        self.body_atoms
            .iter()
            .zip(&self.join_orders)
            .filter_map(|(atom, order)| Some((atom.match_fact(fact)?, order.clone())))
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum RowElement {
    Value(StorageValueT),
    Bottom,
    Top,
}

type Row = Box<[RowElement]>;

impl PartialOrd for RowElement {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowElement {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (RowElement::Bottom, RowElement::Bottom) => std::cmp::Ordering::Equal,
            (RowElement::Top, RowElement::Top) => std::cmp::Ordering::Equal,
            (RowElement::Value(a), RowElement::Value(b)) => a.cmp(b),

            (_, RowElement::Bottom) => std::cmp::Ordering::Greater,
            (_, RowElement::Top) => std::cmp::Ordering::Less,
            (RowElement::Bottom, _) => std::cmp::Ordering::Less,
            (RowElement::Top, _) => std::cmp::Ordering::Greater,
        }
    }
}

enum MatchResult {
    Matches,
    InBounds,
    OutOfBounds,
}

impl RowElement {
    fn value(self) -> StorageValueT {
        match self {
            RowElement::Value(inner) => inner,
            RowElement::Top | RowElement::Bottom => panic!("called value() on RowElement::Ghost"),
        }
    }
}

fn match_rows(pattern: &[RowElement], row: &[RowElement]) -> MatchResult {
    let mut index = 0;

    while index < pattern.len() {
        let RowElement::Value(value) = pattern[index] else {
            break;
        };

        match value.cmp(&row[index].value()) {
            std::cmp::Ordering::Less => return MatchResult::OutOfBounds,
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => panic!("pattern must always be a lower bound"),
        }

        index += 1;
    }

    // only here if pattern[index] == Ghost || index >= pattern.len()
    index += 1;

    while index < pattern.len() {
        let RowElement::Value(value) = pattern[index] else {
            index += 1;
            continue;
        };

        if value != row[index].value() {
            return MatchResult::InBounds;
        }

        index += 1;
    }

    MatchResult::Matches
}

struct RowIterator<'a> {
    lower_cursor: btree_set::Cursor<'a, Row>,
    upper_cursor: btree_set::Cursor<'a, Row>,
    pattern: Row,
}

impl<'a> Iterator for RowIterator<'a> {
    type Item = &'a [RowElement];

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(row) = self.lower_cursor.next() {
            if Some(row) == self.upper_cursor.peek_next() {
                return None;
            }

            match match_rows(&self.pattern, row) {
                MatchResult::Matches => return Some(row),
                MatchResult::InBounds => continue,
                MatchResult::OutOfBounds => unreachable!("this should have been caught early"),
            }
        }

        None
    }
}

trait GhostBound {
    fn invert_bound(&self) -> Self;
}

impl GhostBound for Row {
    fn invert_bound(&self) -> Self {
        self.iter()
            .map(|elem| match elem {
                RowElement::Bottom => RowElement::Top,
                RowElement::Top => RowElement::Bottom,
                value => *value,
            })
            .collect()
    }
}

fn find_all_matches<'a>(pattern: Row, table: &'a BTreeSet<Row>) -> RowIterator<'a> {
    let lower_cursor = table.lower_bound(Bound::Excluded(&pattern));
    let upper_cursor = table.upper_bound(Bound::Excluded(&pattern.invert_bound()));
    RowIterator {
        lower_cursor,
        upper_cursor,
        pattern,
    }
}

fn join<'a, 'b, 'c>(
    subst: &'a SaturationSubstitution,
    terms: &'b [BodyTerm],
    table: &'c BTreeSet<Row>,
) -> impl Iterator<Item = SaturationSubstitution> + use<'a, 'b, 'c> {
    find_all_matches(subst.bind(terms), table).map(|row| {
        let mut subst = subst.clone();
        subst.update(terms, row);
        subst
    })
}

#[test]
fn find_all_matches_works() {
    macro_rules! table {
        [ $([ $($v:expr),* ],)* ] => {
            BTreeSet::from([ $( Box::from([ $(RowElement::Value(StorageValueT::Id32($v))),* ]), )* ])
        };
    }

    let table: BTreeSet<Row> = table![
        [0, 0, 0, 1, 0],
        [0, 1, 0, 0, 0],
        [0, 1, 0, 1, 2],
        [0, 1, 1, 0, 0],
        [0, 1, 2, 1, 2],
        [1, 0, 0, 0, 0],
        [1, 1, 0, 1, 2],
        [2, 1, 0, 0, 0],
    ];

    let pattern1: Row = Box::from([
        RowElement::Value(StorageValueT::Id32(0)),
        RowElement::Bottom,
        RowElement::Value(StorageValueT::Id32(0)),
        RowElement::Value(StorageValueT::Id32(1)),
        RowElement::Bottom,
    ]);

    let matches: Vec<_> = find_all_matches(pattern1, &table).collect();
    let expected: Vec<&[RowElement]> = vec![
        &[
            RowElement::Value(StorageValueT::Id32(0)),
            RowElement::Value(StorageValueT::Id32(0)),
            RowElement::Value(StorageValueT::Id32(0)),
            RowElement::Value(StorageValueT::Id32(1)),
            RowElement::Value(StorageValueT::Id32(0)),
        ],
        &[
            RowElement::Value(StorageValueT::Id32(0)),
            RowElement::Value(StorageValueT::Id32(1)),
            RowElement::Value(StorageValueT::Id32(0)),
            RowElement::Value(StorageValueT::Id32(1)),
            RowElement::Value(StorageValueT::Id32(2)),
        ],
    ];

    assert_eq!(matches, expected);

    let pattern = Box::from([
        RowElement::Value(StorageValueT::Id32(1)),
        RowElement::Bottom,
        RowElement::Value(StorageValueT::Id32(0)),
        RowElement::Value(StorageValueT::Id32(0)),
        RowElement::Bottom,
    ]);

    let mut iter = find_all_matches(pattern, &table);
    let expected: &[RowElement] = &[
        RowElement::Value(StorageValueT::Id32(1)),
        RowElement::Value(StorageValueT::Id32(0)),
        RowElement::Value(StorageValueT::Id32(0)),
        RowElement::Value(StorageValueT::Id32(0)),
        RowElement::Value(StorageValueT::Id32(0)),
    ];
    assert_eq!(
        iter.lower_cursor.peek_next().map(|row| {
            let row: &[RowElement] = row;
            row
        }),
        Some(expected)
    );
    assert_eq!(iter.next(), Some(expected));
    assert_eq!(iter.next(), None);
}

fn bench_rules(n: usize) -> Vec<SaturationRule> {
    let one = BodyTerm::Constant(StorageValueT::Int64(1));
    let zero = BodyTerm::Constant(StorageValueT::Int64(0));

    let rules: Vec<_> = (0..n)
        .map(|i| {
            let head = (0..VariableIdx::try_from(i).unwrap())
                .map(BodyTerm::Variable)
                .chain(Some(one))
                .chain(repeat_n(zero, n - i - 1));

            let head = SaturationAtom {
                predicate: Arc::from("p"),
                terms: head.collect(),
            };

            let body = (0..VariableIdx::try_from(i).unwrap())
                .map(BodyTerm::Variable)
                .chain(Some(zero))
                .chain(repeat_n(one, n - i - 1));

            let body = SaturationAtom {
                predicate: Arc::from("p"),
                terms: body.collect(),
            };

            SaturationRule {
                body_atoms: Box::from([body]),
                join_orders: Box::from([Arc::from([])]),
                head: Head::Datalog(Box::from([head])),
            }
        })
        .collect();

    rules
}

trait Set {
    type Item: ?Sized;

    fn contains(&self, it: &Self::Item) -> bool;
}

impl Set for BTreeSet<Row> {
    type Item = [RowElement];

    fn contains(&self, it: &Self::Item) -> bool {
        self.contains(it)
    }
}

struct LoopJoin<'a, 'b> {
    rows: RowIterator<'a>,
    terms: &'b [BodyTerm],
}

struct Filter<'a, 'b> {
    table: &'a dyn Set<Item = [RowElement]>,
    terms: &'b [BodyTerm],
}

enum JoinStep<'a, 'b> {
    LoopJoin(LoopJoin<'a, 'b>),
    Filter(Filter<'a, 'b>),
}

enum Cases<A, B> {
    A(A),
    B(B),
}

impl<A, B> Iterator for Cases<A, B>
where
    A: Iterator,
    B: Iterator<Item = A::Item>,
{
    type Item = A::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Cases::A(a) => a.next(),
            Cases::B(b) => b.next(),
        }
    }
}

fn join_step<'a, 'b, 'c>(
    step: JoinStep<'a, 'b>,
    subst: &'c SaturationSubstitution,
) -> impl Iterator<Item = SaturationSubstitution> + use<'a, 'b, 'c> {
    match step {
        JoinStep::LoopJoin(LoopJoin { rows, terms }) => Cases::A(rows.map(|row| {
            let mut subst = subst.clone();
            subst.update(terms, row);
            subst
        })),
        JoinStep::Filter(Filter { table, terms }) => {
            let row = subst.bind(terms);
            match table.contains(&row) {
                true => Cases::B(Some(subst.clone()).into_iter()),
                false => Cases::B(None.into_iter()),
            }
        }
    }
}

struct JoinPlan<'a, 'b> {
    inputs: Vec<SaturationSubstitution>,
    steps: Vec<JoinStep<'a, 'b>>,
}

struct Variables(HashMap<String, u16>);

impl Variables {
    fn get(&mut self, var: String) -> u16 {
        let len = self.0.len().try_into().unwrap();
        *self.0.entry(var).or_insert(len)
    }
}

fn convert_term(term: Term, dict: &mut Dict, vars: &mut Variables) -> Result<BodyTerm, ()> {
    match term.try_into_ground(&Default::default()) {
        Ok(ground) => {
            let value = ground.value().to_storage_value_t_dict(dict);
            Ok(BodyTerm::Constant(value))
        }
        Err(term) => {
            let Term::Primitive(Primitive::Variable(var)) = term else {
                return Err(());
            };

            let Variable::Universal(var) = var else {
                return Err(());
            };

            match var.name() {
                Some(name) => Ok(BodyTerm::Variable(vars.get(name))),
                None => Ok(BodyTerm::Ignore),
            }
        }
    }
}

struct Interner(HashSet<Arc<str>>);

impl Interner {
    fn create(&mut self, input: &str) -> Arc<str> {
        self.0.get_or_insert_with(input, |s| Arc::from(s)).clone()
    }
}

fn convert_atom(atom: &Atom, dict: &mut Dict, vars: &mut Variables) -> Result<SaturationAtom, ()> {
    let predicate = Arc::from(atom.predicate().name());

    let terms: Box<[BodyTerm]> = atom
        .arguments()
        .map(|term| convert_term(term.clone(), dict, vars))
        .collect::<Result<_, ()>>()?;

    Ok(SaturationAtom { predicate, terms })
}

fn convert_literal(
    lit: &Literal,
    dict: &mut Dict,
    vars: &mut Variables,
) -> Result<SaturationAtom, ()> {
    match lit {
        Literal::Positive(atom) => convert_atom(atom, dict, vars),
        Literal::Negative(_) => Err(()),
        Literal::Operation(_) => Err(()),
    }
}

fn filter_index(variables: &HashSet<VariableIdx>, atom: &SaturationAtom) -> (i32, i32) {
    let mut other_variables = 0;
    let mut overlapping_variables = 0;

    for var in atom.variables() {
        if variables.contains(&var) {
            overlapping_variables += 1;
        } else {
            other_variables += 1;
        }
    }

    (other_variables, overlapping_variables)
}

fn compute_join_order(
    mut variables: HashSet<VariableIdx>,
    body: &[SaturationAtom],
    mask: &mut [bool],
) -> JoinOrder {
    let mut operations = Vec::new();

    loop {
        let mut index = None;
        let mut min_new_variables = i32::MAX;
        let mut max_overlapping = 0;

        for (current_index, atom) in body
            .iter()
            .enumerate()
            .zip(&mut *mask)
            .filter_map(|(atom, flag)| flag.then_some(atom))
        {
            let (other, overlap) = filter_index(&variables, atom);

            if other < min_new_variables
                || (other == min_new_variables && max_overlapping < overlap)
            {
                min_new_variables = other;
                max_overlapping = overlap;
                index = Some(current_index);
            }
        }

        let Some(index) = index else {
            break JoinOrder::from(operations);
        };

        mask[index] = false;
        if min_new_variables == 0 {
            operations.push(JoinOp::Filter(body[index].clone()));
        } else {
            operations.push(JoinOp::Join(body[index].clone()));
            variables.extend(body[index].variables());
        }
    }
}

fn convert(rule: Rule, dict: &mut Dict) -> Result<SaturationRule, ()> {
    let mut vars = Variables(HashMap::new());

    let body: Box<[SaturationAtom]> = rule
        .body()
        .iter()
        .map(|lit| convert_literal(lit, dict, &mut vars))
        .collect::<Result<_, ()>>()?;

    let join_orders: Box<[JoinOrder]> = body
        .iter()
        .enumerate()
        .map(|(idx, atom)| {
            let variables: HashSet<_> = atom.variables().collect();
            let mut mask = vec![true; body.len()];
            mask[idx] = false;

            compute_join_order(variables, &body, &mut mask)
        })
        .collect();

    let head = if rule.variables().any(Variable::is_existential) {
        todo!()
    } else {
        Head::Datalog(
            rule.head()
                .iter()
                .map(|atom| convert_atom(atom, dict, &mut vars))
                .collect::<Result<_, ()>>()?,
        )
    };

    Ok(SaturationRule {
        body_atoms: body,
        join_orders,
        head,
    })
}

fn run() {
    let n = 20;
    let mut todo = VecDeque::from([(
        SaturationFact {
            predicate: Arc::from("p"),
            values: repeat_n(StorageValueT::Int64(0), n).collect(),
        },
        repeat_n(RowElement::Value(StorageValueT::Int64(0)), n).collect::<Row>(),
    )]);

    let rules = bench_rules(n);

    let mut closure = HashMap::<Arc<str>, BTreeSet<Row>>::new();

    loop {
        if todo.is_empty() {
            break;
        }

        let mut ops = Vec::new();

        while let Some((fact, tuple)) = todo.pop_front() {
            if !closure
                .entry(fact.predicate.clone())
                .or_default()
                .insert(tuple)
            {
                continue;
            }

            ops.extend(rules.iter().enumerate().flat_map(|(index, rule)| {
                rule.trigger(&fact)
                    .map(move |(substitution, join_order)| (index, substitution, join_order))
            }))
        }

        // join phase
        todo = ops
            .into_par_iter()
            .map(|(rule_index, substitution, join_order)| {
                let rule = &rules[rule_index];

                // let iter = join_order
                //     .iter()
                //     .fold(JoinNode::Leaf(substitution), |input, &index| {
                //         JoinNode::LoopJoin {
                //             table: closure.get(&rule.body_atoms[index].predicate).unwrap(),
                //             terms: &rule.body_atoms[index].terms,
                //             input: Box::new(input),
                //         }
                //     });

                let tuple = substitution.bind(&rule.head.terms);

                let fact = SaturationFact {
                    predicate: rule.head.predicate.clone(),
                    values: tuple.iter().cloned().map(RowElement::value).collect(),
                };

                (fact, tuple)
            })
            .collect();
    }
}

fn main() {
    let tp = ThreadPoolBuilder::new().build().unwrap();
    tp.install(|| run())
}
