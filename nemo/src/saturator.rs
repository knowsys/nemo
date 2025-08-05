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
