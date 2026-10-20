//! Signatures: the domains an instruction accepts and yields.
//!
//! A signature lists the ways an instruction can succeed.
//! Each way, an overload, has one parameter per operand:
//! the domains that operand must lie in.
//! It also names the domains the result then lies in.
//! Operands come in operand order, the order they were pushed in.
//!
//! A signature over-approximates.
//! Operands that fit an overload may still fail on their values,
//! as a malformed string fails a cast.
//! Operands that fit no overload never succeed.
//!
//! [OpCode::signature](super::OpCode::signature) holds the signature of every instruction,
//! written in the named sets of [DomainSet].
//! Inference applies it to the domains the operands may hold,
//! narrowing reads it backwards from the result,
//! and debug builds hold every evaluation to it.

use crate::value::{DomainSet, ValueDomain};

/// The domains an instruction accepts and yields.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Signature {
    /// Pushes a value without popping any: a constant or an input.
    ///
    /// The program knows what it pushes, not the instruction,
    /// so the signature allows any domain.
    Push,
    /// Pops a fixed number of operands,
    /// and can succeed only if they fit one of the overloads.
    ///
    /// There is at least one overload, and each has one parameter per operand.
    Fixed(&'static [Overload]),
    /// Pops leading operands and then a run of operands,
    /// and can succeed only if every operand fits.
    Each {
        /// The domains each leading operand must lie in.
        leading: &'static [DomainSet],
        /// The number of operands in the run.
        count: usize,
        /// The domains each operand of the run must lie in.
        operand: DomainSet,
        /// The domains the result lies in.
        result: DomainSet,
    },
    /// Pops a run of numbers and yields one of them,
    /// promoted to the smallest numeric type that holds all of them.
    Promoting {
        /// The number of operands.
        count: usize,
    },
    /// Pops a run of operands,
    /// and can succeed wherever one of them fits, whatever the others hold.
    ///
    /// This is a connective of three-valued logic:
    /// one operand can decide it while the others are errors.
    Lenient {
        /// The number of operands.
        count: usize,
        /// The domains a deciding operand must lie in.
        operand: DomainSet,
        /// The domains the result lies in.
        result: DomainSet,
    },
}

/// One way an instruction can succeed:
/// operands in the domains of the parameters, one parameter per operand,
/// yield a result in the domains the overload names.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Overload {
    /// Yields a result in the given domains.
    Plain(&'static [DomainSet], DomainSet),
    /// Yields a result in the given domains,
    /// if the operands also share one domain.
    Shared(&'static [DomainSet], DomainSet),
    /// Yields a result in the domain of the operand at the given position.
    Keeps(&'static [DomainSet], usize),
    /// Takes two numbers and yields a number of the domain they promote to:
    /// a double beside a double, a float beside a float or an exact number,
    /// and an exact number for two exact numbers,
    /// an integer for two integers if `integral` holds.
    ///
    /// An exact result that is integral is an integer, so it may be one in any case.
    Promoted {
        /// Whether two integers yield an integer.
        integral: bool,
    },
}

/// The parameters of a [Promoted](Overload::Promoted) overload.
const NUMBERS: [DomainSet; 2] = [DomainSet::NUMERIC, DomainSet::NUMERIC];

impl Signature {
    /// Returns how many operands the instruction pops.
    pub(crate) fn pops(self) -> usize {
        match self {
            Signature::Push => 0,
            Signature::Fixed(overloads) => {
                let pops = overloads[0].parameters().len();
                debug_assert!(
                    overloads
                        .iter()
                        .all(|overload| overload.parameters().len() == pops),
                    "the overloads of an instruction pop alike"
                );
                pops
            }
            Signature::Each { leading, count, .. } => leading.len() + count,
            Signature::Promoting { count } | Signature::Lenient { count, .. } => count,
        }
    }

    /// Returns the domains the result can lie in,
    /// for operands in the given domains, one set per operand.
    ///
    /// Empty proves that the instruction fails on every such input.
    pub(crate) fn apply(self, operands: &[DomainSet]) -> DomainSet {
        debug_assert_eq!(operands.len(), self.pops(), "one domain set per operand");

        match self {
            Signature::Push => DomainSet::ANY,
            Signature::Fixed(overloads) => {
                overloads.iter().fold(DomainSet::EMPTY, |sum, overload| {
                    sum | overload.apply(operands)
                })
            }
            Signature::Each {
                leading,
                operand,
                result,
                ..
            } => {
                let (first, run) = operands.split_at(leading.len());
                if fits(leading, first) && run.iter().all(|kinds| kinds.intersects(operand)) {
                    result
                } else {
                    DomainSet::EMPTY
                }
            }
            Signature::Promoting { .. } => {
                if operands
                    .iter()
                    .all(|kinds| kinds.intersects(DomainSet::NUMERIC))
                {
                    promoted(operands)
                } else {
                    DomainSet::EMPTY
                }
            }
            Signature::Lenient {
                operand, result, ..
            } => {
                if operands.iter().any(|kinds| kinds.intersects(operand)) {
                    result
                } else {
                    DomainSet::EMPTY
                }
            }
        }
    }
}

impl Signature {
    /// Returns the domains each operand can lie in
    /// if the instruction succeeds with a result in the given domains,
    /// for operands in the given domains, one set per operand.
    ///
    /// This is [apply](Self::apply) read backwards:
    /// only the overloads that can yield such a result count,
    /// and each operand keeps the domains their parameters accept.
    /// A connective can succeed whatever one operand holds, so it narrows none.
    pub(crate) fn narrow(self, operands: &[DomainSet], result: DomainSet) -> Vec<DomainSet> {
        debug_assert_eq!(operands.len(), self.pops(), "one domain set per operand");

        let nothing = || vec![DomainSet::EMPTY; operands.len()];
        match self {
            Signature::Push | Signature::Lenient { .. } => operands.to_vec(),
            Signature::Fixed(overloads) => overloads
                .iter()
                .filter_map(|overload| overload.narrow(operands, result))
                .fold(nothing(), |sum, within| {
                    sum.into_iter()
                        .zip(within)
                        .map(|(sum, within)| sum | within)
                        .collect()
                }),
            Signature::Each {
                leading, operand, ..
            } => {
                if !self.apply(operands).intersects(result) {
                    return nothing();
                }
                operands
                    .iter()
                    .enumerate()
                    .map(|(index, &kinds)| kinds & leading.get(index).copied().unwrap_or(operand))
                    .collect()
            }
            Signature::Promoting { .. } => {
                if !self.apply(operands).intersects(result) {
                    return nothing();
                }
                operands
                    .iter()
                    .map(|&kinds| kinds & DomainSet::NUMERIC)
                    .collect()
            }
        }
    }
}

impl Overload {
    /// Returns the domains each operand must lie in, one set per operand.
    fn parameters(self) -> &'static [DomainSet] {
        match self {
            Overload::Plain(parameters, _)
            | Overload::Shared(parameters, _)
            | Overload::Keeps(parameters, _) => parameters,
            Overload::Promoted { .. } => &NUMBERS,
        }
    }

    /// Returns the domains the result can lie in through this overload,
    /// for operands in the given domains, one set per operand.
    fn apply(self, operands: &[DomainSet]) -> DomainSet {
        let parameters = self.parameters();
        if !fits(parameters, operands) {
            return DomainSet::EMPTY;
        }

        match self {
            Overload::Plain(_, result) => result,
            Overload::Shared(_, result) => {
                let shared = parameters
                    .iter()
                    .zip(operands)
                    .fold(DomainSet::ANY, |common, (&parameter, &kinds)| {
                        common & parameter & kinds
                    });
                if shared.is_empty() {
                    DomainSet::EMPTY
                } else {
                    result
                }
            }
            Overload::Keeps(_, position) => parameters[position] & operands[position],
            Overload::Promoted { integral } => arithmetic(operands, integral),
        }
    }
}

impl Overload {
    /// Returns the domains each operand can lie in
    /// if the instruction succeeds through this overload with a result in the given domains,
    /// or `None` if it cannot.
    fn narrow(self, operands: &[DomainSet], result: DomainSet) -> Option<Vec<DomainSet>> {
        if !self.apply(operands).intersects(result) {
            return None;
        }

        let parameters = self.parameters();
        let within = parameters
            .iter()
            .zip(operands)
            .map(|(&parameter, &kinds)| parameter & kinds);
        Some(match self {
            Overload::Plain(..) => within.collect(),
            // A double result admits any operand, a float one no double,
            // and an exact one only exact operands.
            Overload::Promoted { .. } => {
                let allowed = if result.contains(ValueDomain::Double) {
                    DomainSet::NUMERIC
                } else if result.contains(ValueDomain::Float) {
                    DomainSet::EXACT | DomainSet::FLOAT
                } else {
                    DomainSet::EXACT
                };
                within.map(|kinds| kinds & allowed).collect()
            }
            Overload::Shared(..) => {
                let shared = within.fold(DomainSet::ANY, |common, kinds| common & kinds);
                vec![shared; operands.len()]
            }
            Overload::Keeps(_, position) => within
                .enumerate()
                .map(|(index, kinds)| {
                    if index == position {
                        kinds & result
                    } else {
                        kinds
                    }
                })
                .collect(),
        })
    }
}

/// Returns whether every operand can lie in the domains of its parameter.
fn fits(parameters: &[DomainSet], operands: &[DomainSet]) -> bool {
    parameters
        .iter()
        .zip(operands)
        .all(|(parameter, kinds)| parameter.intersects(*kinds))
}

/// Returns the domains the result of an arithmetic operation can lie in,
/// for numeric operands in the given domains;
/// see [Promoted](Overload::Promoted).
fn arithmetic(operands: &[DomainSet], integral: bool) -> DomainSet {
    let some_may = |domains| operands.iter().any(|kinds| kinds.intersects(domains));
    let each_may = |domains| operands.iter().all(|kinds| kinds.intersects(domains));
    let mut result = DomainSet::EMPTY;

    if each_may(DomainSet::EXACT) {
        let integers = operands
            .iter()
            .all(|&kinds| (kinds & DomainSet::EXACT).is_subset(DomainSet::INTEGER));
        result |= if integral && integers {
            DomainSet::INTEGER
        } else {
            DomainSet::EXACT
        };
    }
    if some_may(DomainSet::FLOAT) && each_may(DomainSet::EXACT | DomainSet::FLOAT) {
        result |= DomainSet::FLOAT;
    }
    if some_may(DomainSet::DOUBLE) {
        result |= DomainSet::DOUBLE;
    }
    result
}

/// Returns the domains a number can lie in
/// once promoted to the smallest numeric type that holds every operand,
/// for numeric operands in the given domains:
/// - a double, if some operand may be one;
/// - a float, if some operand may be one and each may be below a double;
/// - an exact domain of an operand, if each may be exact.
fn promoted(operands: &[DomainSet]) -> DomainSet {
    let some_may = |domains| operands.iter().any(|kinds| kinds.intersects(domains));
    let each_may = |domains| operands.iter().all(|kinds| kinds.intersects(domains));
    let mut promoted = DomainSet::EMPTY;

    if each_may(DomainSet::EXACT) {
        let union = operands
            .iter()
            .fold(DomainSet::EMPTY, |sum, kinds| sum | *kinds);
        promoted |= union & DomainSet::EXACT;
    }
    if some_may(DomainSet::FLOAT) && each_may(DomainSet::EXACT | DomainSet::FLOAT) {
        promoted |= DomainSet::FLOAT;
    }
    if some_may(DomainSet::DOUBLE) {
        promoted |= DomainSet::DOUBLE;
    }
    promoted
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::value::DomainSet as D;

    #[test]
    fn plain_overloads_need_every_operand_to_fit() {
        let add = Overload::Plain(&[D::NUMERIC, D::NUMERIC], D::NUMERIC);
        assert_eq!(add.apply(&[D::INTEGER | D::STRING, D::DOUBLE]), D::NUMERIC);
        assert!(add.apply(&[D::INTEGER, D::STRING]).is_empty());
    }

    #[test]
    fn shared_overloads_need_one_domain_for_all_operands() {
        let distance = Overload::Shared(&[D::TEMPORAL, D::TEMPORAL], D::DAY_TIME_DURATION);
        assert_eq!(
            distance.apply(&[D::DATE | D::INTEGER, D::DATE]),
            D::DAY_TIME_DURATION
        );
        assert!(distance.apply(&[D::DATE, D::TIME]).is_empty());
        // A shared domain outside the parameters does not count.
        assert!(distance.apply(&[D::INTEGER, D::INTEGER]).is_empty());
    }

    #[test]
    fn keeping_overloads_yield_the_operand_domain() {
        let shift = Overload::Keeps(&[D::TEMPORAL, D::DURATION], 0);
        assert_eq!(
            shift.apply(&[D::DATE | D::INTEGER, D::DAY_TIME_DURATION]),
            D::DATE
        );
        assert!(shift.apply(&[D::DATE, D::INTEGER]).is_empty());
    }

    #[test]
    fn fixed_signatures_unite_their_overloads() {
        let signature = Signature::Fixed(&[
            Overload::Plain(&[D::STRING, D::STRING], D::STRING),
            Overload::Plain(&[D::LANGUAGE_STRING, D::STRINGLIKE], D::STRINGLIKE),
        ]);
        assert_eq!(signature.pops(), 2);
        assert_eq!(signature.apply(&[D::STRINGLIKE, D::STRING]), D::STRINGLIKE);
        assert_eq!(signature.apply(&[D::STRING, D::STRING]), D::STRING);
        assert!(signature.apply(&[D::STRING, D::LANGUAGE_STRING]).is_empty());
    }

    #[test]
    fn runs_need_every_operand_to_fit() {
        let labeled = Signature::Each {
            leading: &[D::IRI],
            count: 2,
            operand: D::ANY,
            result: D::TUPLE,
        };
        assert_eq!(labeled.pops(), 3);
        assert_eq!(labeled.apply(&[D::IRI, D::INTEGER, D::MAP]), D::TUPLE);
        assert!(labeled.apply(&[D::INTEGER, D::INTEGER, D::MAP]).is_empty());
        // An operand without a value starves the run.
        assert!(labeled.apply(&[D::IRI, D::INTEGER, D::EMPTY]).is_empty());
    }

    #[test]
    fn arithmetic_promotes_and_keeps_integers() {
        let add = Overload::Promoted { integral: true };
        assert_eq!(add.apply(&[D::INTEGER, D::INTEGER]), D::INTEGER);
        assert_eq!(add.apply(&[D::INTEGER, D::DECIMAL]), D::EXACT);
        assert_eq!(add.apply(&[D::INTEGER, D::FLOAT]), D::FLOAT);
        assert_eq!(add.apply(&[D::FLOAT, D::DOUBLE]), D::DOUBLE);
        assert_eq!(
            add.apply(&[D::INTEGER | D::DOUBLE, D::INTEGER]),
            D::INTEGER | D::DOUBLE
        );
        // Two integers divide to an exact number.
        let divide = Overload::Promoted { integral: false };
        assert_eq!(divide.apply(&[D::INTEGER, D::INTEGER]), D::EXACT);
        // An integer result takes exact operands.
        assert_eq!(
            add.narrow(&[D::ANY, D::ANY], D::INTEGER),
            Some(vec![D::EXACT, D::EXACT])
        );
    }

    #[test]
    fn extrema_promote_their_operands() {
        let maximum = Signature::Promoting { count: 2 };
        assert_eq!(maximum.apply(&[D::INTEGER, D::EXACT]), D::EXACT);
        assert_eq!(maximum.apply(&[D::INTEGER, D::FLOAT]), D::FLOAT);
        assert_eq!(maximum.apply(&[D::FLOAT, D::DOUBLE]), D::DOUBLE);
        assert!(maximum.apply(&[D::INTEGER, D::STRING]).is_empty());
    }

    #[test]
    fn narrowing_reads_the_overloads_backwards() {
        let compare = Signature::Fixed(&[
            Overload::Plain(&[D::NUMERIC, D::NUMERIC], D::BOOLEAN),
            Overload::Shared(&[D::STRINGLIKE, D::STRINGLIKE], D::BOOLEAN),
        ]);
        // A comparison with an integer holds only for a number.
        assert_eq!(
            compare.narrow(&[D::ANY, D::INTEGER], D::BOOLEAN),
            vec![D::NUMERIC, D::INTEGER]
        );
        // With either operand open, both overloads remain.
        assert_eq!(
            compare.narrow(&[D::ANY, D::ANY], D::BOOLEAN),
            vec![D::NUMERIC | D::STRINGLIKE; 2]
        );
        // A result outside every overload's leaves no operand a domain.
        assert_eq!(
            compare.narrow(&[D::ANY, D::ANY], D::INTEGER),
            vec![D::EMPTY; 2]
        );
        // A kept domain is the result's.
        let shift = Signature::Fixed(&[Overload::Keeps(&[D::TEMPORAL, D::DURATION], 0)]);
        assert_eq!(
            shift.narrow(&[D::ANY, D::ANY], D::DATE),
            vec![D::DATE, D::DURATION]
        );
        // A connective narrows none.
        let and = Signature::Lenient {
            count: 2,
            operand: D::TRUTHY,
            result: D::BOOLEAN,
        };
        assert_eq!(
            and.narrow(&[D::ANY, D::IRI], D::BOOLEAN),
            vec![D::ANY, D::IRI]
        );
    }

    #[test]
    fn connectives_need_one_operand_to_fit() {
        let and = Signature::Lenient {
            count: 2,
            operand: D::TRUTHY,
            result: D::BOOLEAN,
        };
        assert_eq!(and.apply(&[D::EMPTY, D::BOOLEAN]), D::BOOLEAN);
        assert!(and.apply(&[D::EMPTY, D::IRI]).is_empty());
    }
}
