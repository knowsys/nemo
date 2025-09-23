//! Executing a set of saturation rules

use core::panic;
use std::{
    collections::{BTreeMap, HashMap, btree_map},
    ops::{Bound, Deref, DerefMut, Index},
    sync::Arc,
};

use nemo_physical::datatypes::StorageValueT;
#[cfg(not(test))]
use nemo_physical::meta::timing::TimedCode;

use super::model::{
    BodyTerm, Head, JoinOp, SaturationAtom, SaturationFact, SaturationRule, VariableIdx,
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

    #[must_use]
    fn update(&mut self, terms: &[BodyTerm], row: &[RowElement]) -> bool {
        for (term, value) in terms.iter().zip(row) {
            let BodyTerm::Variable(var) = term else {
                continue;
            };

            if let Some(prev) = self.insert(*var, value.value())
                && prev != value.value()
            {
                return false;
            }
        }

        true
    }

    fn satisfies(&self, equality: (VariableIdx, VariableIdx)) -> bool {
        self.0[equality.0 as usize].unwrap() == self.0[equality.1 as usize].unwrap()
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
                    if let Some(prev) = res.insert(*idx, *value)
                        && prev != *value
                    {
                        return None;
                    }
                }
                BodyTerm::Ignore => {}
            }
        }

        if let Some(equality) = self.equality
            && !res.satisfies(equality)
        {
            return None;
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

#[derive(Debug)]
struct RowIterator<'a> {
    lower_cursor: btree_map::Cursor<'a, Row, Age>,
    upper_cursor_next: Option<&'a Row>,
    pattern: Row,
}

impl<'a> Iterator for RowIterator<'a> {
    type Item = &'a [RowElement];

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((row, _)) = self.lower_cursor.next() {
            if let Some(other_row) = self.upper_cursor_next
                && other_row == row
            {
                return None;
            }

            match match_rows(&self.pattern, row) {
                MatchResult::Matches => return Some(row),
                MatchResult::InBounds => continue,
                MatchResult::OutOfBounds => {
                    log::trace!("OutOfBounds {row:?}, {:?}", self.pattern);
                    log::trace!("upper cursor next {:?}", self.upper_cursor_next);
                    unreachable!("this should have been caught early")
                }
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

fn find_all_matches<'a>(pattern: Row, table: &'a BTreeMap<Row, Age>) -> RowIterator<'a> {
    let lower_cursor = table.lower_bound(Bound::Included(&pattern));
    let upper_cursor = table.upper_bound(Bound::Included(&pattern.invert_bound()));
    let upper_cursor_next = upper_cursor.peek_next().map(|(r, _)| r);
    RowIterator {
        lower_cursor,
        upper_cursor_next,
        pattern,
    }
}

#[derive(Debug)]
struct RowMatcher<'a> {
    substitution: SaturationSubstitution,
    atom: SaturationAtom,
    cursor: RowIterator<'a>,
}

impl Iterator for RowMatcher<'_> {
    type Item = SaturationSubstitution;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let row = self.cursor.next()?;
            let mut subst = self.substitution.clone();
            if subst.update(&self.atom.terms, row) {
                if let Some(equality) = &self.atom.equality
                    && !subst.satisfies(*equality)
                {
                    continue;
                }

                return Some(subst);
            }
        }
    }
}

fn join<'a>(
    substitution: SaturationSubstitution,
    atom: SaturationAtom,
    table: &'a BTreeMap<Row, Age>,
) -> RowMatcher<'a> {
    let cursor = find_all_matches(substitution.bind(&atom.terms), table);

    RowMatcher {
        substitution,
        atom,
        cursor,
    }
}

#[derive(Debug)]
struct ExecutionTree {
    init: SaturationSubstitution,
    ops: Arc<[JoinOp]>,
    index: usize,
}

#[derive(Debug)]
enum JoinIter<'a> {
    Done,
    NoOp(SaturationSubstitution),
    Join {
        inner: Box<JoinIter<'a>>,
        atom: SaturationAtom,
        table: &'a BTreeMap<Row, Age>,
        current: Option<RowMatcher<'a>>,
    },
}

#[derive(Debug, Clone, Copy)]
enum Age {
    Old,
    New,
}

#[derive(Debug)]
enum Singular<'a, T> {
    #[allow(unused)]
    Ref(&'a mut T),
    Owned(T),
}

impl<T: Default> Default for Singular<'_, T> {
    fn default() -> Self {
        Self::Owned(T::default())
    }
}

impl<'a, T> Deref for Singular<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Singular::Ref(r) => &*r,
            Singular::Owned(o) => o,
        }
    }
}

impl<'a, T> DerefMut for Singular<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        match self {
            Singular::Ref(r) => r,
            Singular::Owned(o) => o,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct DataBase<'a>(HashMap<Arc<str>, Singular<'a, BTreeMap<Row, Age>>>);

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
                let Some(table) = tables.0.get(&atom.predicate) else {
                    return JoinIter::Done;
                };

                let atom = atom.clone();
                let inner = Box::new(self.execute(tables));

                JoinIter::Join {
                    inner,
                    atom,
                    table,
                    current: None,
                }
            }
            // todo: more efficient implementation?
            JoinOp::Filter(atom) => {
                let Some(table) = tables.0.get(&atom.predicate) else {
                    return JoinIter::Done;
                };

                let atom = atom.clone();
                let inner = Box::new(self.execute(tables));

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
                if let Some(current) = current
                    && let Some(next) = current.next()
                {
                    return Some(next);
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
        .unwrap_or_else(|| panic!("{row:?}"));

    SaturationFact { predicate, values }
}

pub(crate) fn saturate(db: &mut DataBase, rules: &mut [SaturationRule]) {
    let mut matches = Vec::new();

    #[cfg(not(test))]
    TimedCode::instance()
        .sub("Reasoning/Saturation/update")
        .start();

    for (rule_index, rule) in rules.iter_mut().enumerate() {
        let predicate = rule
            .input_predicates()
            .min_by_key(|p| db.0.get(p).map(|t| t.len()).unwrap_or(0))
            .unwrap();

        for (row, _) in db.0.get(&predicate).iter().flat_map(|table| table.iter()) {
            let fact = fact_from_row(row, predicate.clone());

            for trigger in rule.trigger(&fact) {
                matches.extend(trigger.execute(db).map(|row| (row, rule_index)));
            }
        }
    }

    #[cfg(not(test))]
    TimedCode::instance()
        .sub("Reasoning/Saturation/update")
        .stop();
    #[cfg(not(test))]
    TimedCode::instance()
        .sub("Reasoning/Saturation/loop")
        .start();

    let mut todo = Vec::new();
    while !matches.is_empty() {
        todo.clear();

        for (substitution, rule_index) in matches.drain(..) {
            let rule = &rules[rule_index];

            match &rule.head {
                Head::Datalog(atoms) => {
                    for atom in atoms {
                        let row = substitution.bind(&atom.terms);
                        let table = db.0.entry(atom.predicate.clone()).or_default();

                        let mut cursor = table.lower_bound_mut(Bound::Included(&row));

                        if let Some((other_row, _)) = cursor.peek_next()
                            && other_row == &row
                        {
                            continue;
                        }

                        let fact = fact_from_row(&row, atom.predicate.clone());

                        cursor.insert_after(row, Age::New).unwrap();
                        todo.push(fact);
                    }
                }
            }
        }

        for fact in &todo {
            for (rule_index, rule) in rules.iter_mut().enumerate() {
                for trigger in rule.trigger(fact) {
                    matches.extend(trigger.execute(db).map(|row| (row, rule_index)));
                }
            }
        }
    }

    #[cfg(not(test))]
    TimedCode::instance()
        .sub("Reasoning/Saturation/loop")
        .stop();
}

impl DataBase<'_> {
    pub fn add_table(
        &mut self,
        predicate: Arc<str>,
        table: impl Iterator<Item = Vec<StorageValueT>>,
    ) {
        let table = Singular::Owned(
            table
                .map(|row| (row.into_iter().map(RowElement::Value).collect(), Age::Old))
                .collect(),
        );

        self.0.insert(predicate, table);
    }

    pub fn new_facts(&self, predicate: &str) -> impl Iterator<Item = StorageValueT> + use<'_> {
        self.0.get(predicate).into_iter().flat_map(|table| {
            table.iter().flat_map(|(row, age)| {
                (matches!(age, Age::New))
                    .then_some(row.iter().map(|v| match v {
                        RowElement::Value(storage_value_t) => *storage_value_t,
                        RowElement::Bottom => unreachable!("sentinel elements are never written"),
                        RowElement::Top => unreachable!("sentinel elements are never written"),
                    }))
                    .into_iter()
                    .flatten()
            })
        })
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeMap, HashMap},
        iter::repeat_n,
        sync::Arc,
    };

    use nemo_physical::datatypes::StorageValueT;

    use super::Age;

    use crate::execution::saturation::{
        execution::{DataBase, Row, RowElement, Singular, find_all_matches, saturate},
        model::{BodyTerm, Head, SaturationAtom, SaturationRule, bench_rules},
    };

    macro_rules! table {
        [ $([ $($v:expr),* ],)* ] => {
            BTreeMap::from([ $( (Box::from([ $(RowElement::Value(StorageValueT::Id32($v))),* ]), Age::Old), )* ])
        };
    }

    #[test]
    fn find_all_matches_works() {
        let table: BTreeMap<Row, Age> = table![
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
            iter.lower_cursor.peek_next().map(|(row, _)| {
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
        let n = 10;
        let (mut rules, predicate) = bench_rules(n);
        let row: Row = repeat_n(RowElement::Value(StorageValueT::Int64(0)), n).collect();

        let mut db = DataBase(HashMap::from([(
            predicate.clone(),
            Singular::Owned(BTreeMap::from([(row, Age::Old)])),
        )]));

        saturate(&mut db, &mut rules);

        assert_eq!(db.0.get(&predicate).unwrap().len(), 2_usize.pow(n as u32));

        let new_len = db.new_facts(&predicate).count() / n;
        assert_eq!(new_len, 2_usize.pow(n as u32) - 1);
    }

    #[test]
    fn saturate_multi_join() {
        let p1: Arc<str> = Arc::from("p1");
        let p2: Arc<str> = Arc::from("p2");
        let p3: Arc<str> = Arc::from("p3");
        let p4: Arc<str> = Arc::from("p4");

        let x = BodyTerm::Variable(0);
        let y = BodyTerm::Variable(1);
        let z = BodyTerm::Variable(2);

        let head = Head::Datalog(Box::from([SaturationAtom {
            predicate: p1.clone(),
            terms: Box::from([x.clone(), y.clone(), z.clone()]),
            equality: Default::default(),
        }]));

        let p2_atom = SaturationAtom {
            predicate: p2.clone(),
            terms: Box::from([x.clone(), y.clone()]),
            equality: Default::default(),
        };

        let p2_table: BTreeMap<Row, Age> = table![[0, 0],];

        let p3_atom = SaturationAtom {
            predicate: p3.clone(),
            terms: Box::from([x.clone(), y.clone()]),
            equality: Default::default(),
        };

        let p3_table: BTreeMap<Row, Age> = table![[0, 0],];

        let p4_atom = SaturationAtom {
            predicate: p4.clone(),
            terms: Box::from([x.clone(), y.clone(), z.clone()]),
            equality: Default::default(),
        };

        let p4_table: BTreeMap<Row, Age> = table![[0, 0, 0], [0, 0, 1],];

        let mut db = HashMap::new();
        db.insert(p2.clone(), Singular::Owned(p2_table));
        db.insert(p3.clone(), Singular::Owned(p3_table));
        db.insert(p4.clone(), Singular::Owned(p4_table.clone()));

        let rule = SaturationRule {
            body_atoms: Arc::new([p2_atom, p3_atom, p4_atom]),
            join_orders: Box::from([None, None, None]),
            head,
        };

        let mut db = DataBase(db);
        let mut rules = vec![rule];

        saturate(&mut db, &mut rules);

        assert_eq!(
            db.0.get(&p1)
                .unwrap_or(&Default::default())
                .keys()
                .collect::<Vec<_>>(),
            p4_table.keys().collect::<Vec<_>>()
        );
    }
}
