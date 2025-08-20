use std::{
    collections::{btree_set, BTreeSet, HashMap},
    iter::repeat_n,
    ops::{Bound, Index},
    sync::Arc,
};

use nemo_physical::datatypes::StorageValueT;

use crate::{
    execution::saturation::model::{Head, JoinOp},
    rule_model::substitution::Substitution,
};

use super::model::{
    BodyTerm, JoinOrder, SaturationAtom, SaturationFact, SaturationRule, VariableIdx,
};

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

impl SaturationAtom {
    fn match_fact(&self, fact: &SaturationFact) -> Option<SaturationSubstitution> {
        if fact.predicate != self.predicate {
            return None;
        }

        let mut res = SaturationSubstitution::default();
        debug_assert_eq!(self.terms.len(), fact.values.len());

        for (term, value) in self.terms.iter().zip(fact.values.iter()) {
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
}

struct Triggers<'a, 'b> {
    rule: &'a mut SaturationRule,
    fact: &'b SaturationFact,
    index: usize,
}

impl Iterator for Triggers<'_, '_> {
    type Item = ExecutionTree;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.rule.body_atoms.len() {
            let Some(substitution) = self.rule.body_atoms[self.index].match_fact(self.fact) else {
                self.index += 1;
                continue;
            };

            let ops = self.rule.join_order(self.index);
            let index = ops.len();

            self.index += 1;

            return Some(ExecutionTree {
                init: substitution,
                ops,
                index,
            });
        }

        None
    }
}

impl SaturationRule {
    fn trigger<'a, 'b>(
        &'a mut self,
        fact: &'b SaturationFact,
    ) -> impl Iterator<Item = ExecutionTree> + use<'a, 'b> {
        Triggers {
            rule: self,
            fact,
            index: 0,
        }
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

struct RowMatcher<'a> {
    substitution: SaturationSubstitution,
    atom: SaturationAtom,
    cursor: RowIterator<'a>,
}

impl Iterator for RowMatcher<'_> {
    type Item = SaturationSubstitution;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.cursor.next()?;
        let mut subst = self.substitution.clone();
        subst.update(&self.atom.terms, row);
        Some(subst)
    }
}

fn join<'a>(
    substitution: SaturationSubstitution,
    atom: SaturationAtom,
    table: &'a BTreeSet<Row>,
) -> RowMatcher<'a> {
    let cursor = find_all_matches(substitution.bind(&atom.terms), table);

    RowMatcher {
        substitution,
        atom,
        cursor,
    }
}
struct ExecutionTree {
    init: SaturationSubstitution,
    ops: Arc<[JoinOp]>,
    index: usize,
}

enum JoinIter<'a> {
    Done,
    NoOp(SaturationSubstitution),
    Join {
        inner: Box<JoinIter<'a>>,
        atom: SaturationAtom,
        table: &'a BTreeSet<Row>,
        current: Option<RowMatcher<'a>>,
    },
}

type DataBase = HashMap<Arc<str>, BTreeSet<Row>>;

impl ExecutionTree {
    fn pop(&mut self) -> Option<&JoinOp> {
        if self.index > 0 {
            self.index -= 1;
            Some(&self.ops[self.index])
        } else {
            None
        }
    }

    fn execute<'a>(mut self, tables: &'a DataBase) -> JoinIter<'a> {
        let Some(op) = self.pop() else {
            return JoinIter::NoOp(self.init);
        };

        match op {
            JoinOp::Join(atom) => {
                let table = tables.get(&atom.predicate).unwrap();
                let atom = atom.clone();
                let inner = Box::new(self.execute(&tables));

                JoinIter::Join {
                    inner,
                    atom,
                    table,
                    current: None,
                }
            }
            // todo: more efficient implementation?
            JoinOp::Filter(atom) => {
                let table = tables.get(&atom.predicate).unwrap();
                let atom = atom.clone();
                let inner = Box::new(self.execute(&tables));

                JoinIter::Join {
                    inner,
                    atom,
                    table,
                    current: None,
                }
            }
        }
    }
}

impl Iterator for JoinIter<'_> {
    type Item = SaturationSubstitution;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            JoinIter::NoOp(saturation_substitution) => {
                let res = saturation_substitution.clone();
                *self = Self::Done;
                Some(res)
            }
            JoinIter::Join {
                inner,
                atom,
                table,
                current,
            } => loop {
                if let Some(current) = current {
                    if let Some(next) = current.next() {
                        return Some(next);
                    }
                }

                let substitution = inner.next()?;
                *current = Some(join(substitution, atom.clone(), table));
            },
            JoinIter::Done => None,
        }
    }
}

fn fact_from_row(row: &Row, predicate: Arc<str>) -> SaturationFact {
    let values = row
        .iter()
        .map(|element| match element {
            RowElement::Value(value) => Some(*value),
            _ => None,
        })
        .collect::<Option<_>>()
        .unwrap();

    SaturationFact { predicate, values }
}

fn saturate(db: &mut DataBase, mut rules: Vec<SaturationRule>) {
    let mut todo = Vec::new();

    for (predicate, table) in db.iter() {
        for row in table.iter() {
            todo.push(fact_from_row(row, predicate.clone()));
        }
    }

    while !todo.is_empty() {
        let mut matches = Vec::new();

        for (rule_index, rule) in rules.iter_mut().enumerate() {
            for fact in &todo {
                for trigger in rule.trigger(&fact) {
                    matches.extend(trigger.execute(&db).map(|row| (row, rule_index)));
                }
            }
        }

        todo.clear();

        for (substitution, rule_index) in matches {
            let rule = &rules[rule_index];

            match &rule.head {
                Head::Datalog(atoms) => {
                    for atom in atoms {
                        let row = substitution.bind(&atom.terms);
                        let table = db.entry(atom.predicate.clone()).or_default();

                        let mut cursor = table.lower_bound_mut(Bound::Included(&row));

                        if cursor.peek_next() != Some(&row) {
                            let fact = fact_from_row(&row, atom.predicate.clone());

                            cursor.insert_after(row).unwrap();
                            todo.push(fact);
                        }
                    }
                }
            }
        }
    }
}

mod test {
    use std::{
        collections::{BTreeSet, HashMap},
        iter::repeat_n,
    };

    use nemo_physical::datatypes::StorageValueT;

    use crate::execution::saturation::{
        execution::{find_all_matches, saturate, Row, RowElement},
        model::bench_rules,
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

    #[test]
    fn saturate_bench_rules() {
        let (rules, predicate) = bench_rules(5);
        let row: Row = repeat_n(RowElement::Value(StorageValueT::Int64(0)), 5).collect();

        let mut db = HashMap::from([(predicate.clone(), BTreeSet::from([row]))]);

        saturate(&mut db, rules);

        assert_eq!(db.get(&predicate).unwrap().len(), 32);
    }
}
