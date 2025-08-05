use std::{
    collections::{btree_set, BTreeSet},
    ops::{Bound, Index},
};

use nemo_physical::datatypes::StorageValueT;

use crate::execution::planning::operations::join;

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
    type Item = (SaturationSubstitution, JoinOrder);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.rule.body_atoms.len() {
            let Some(substitution) = self.rule.body_atoms[self.index].match_fact(self.fact) else {
                self.index += 1;
                continue;
            };

            let join_order = self.rule.join_order(self.index);
            self.index += 1;

            return Some((substitution, join_order));
        }

        None
    }
}

impl SaturationRule {
    fn trigger<'a, 'b>(
        &'a mut self,
        fact: &'b SaturationFact,
    ) -> impl Iterator<Item = (SaturationSubstitution, JoinOrder)> + use<'a, 'b> {
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
