use std::fmt;

use super::bitset::BitSet;

/// some variables may be assigned to a fixed value, and others may have multiple possible values, represented by bits
#[cfg_attr(test, derive(Clone))]
pub enum Domain {
    Unassigned(BitSet),
    Assigned(usize),
}

impl Domain {
    pub fn as_bitset(self) -> Option<BitSet> {
        match self {
            Self::Unassigned(bitset) => Some(bitset),
            Self::Assigned(_) => None,
        }
    }

    pub fn as_bitset_ref(&self) -> Option<&BitSet> {
        match self {
            Self::Unassigned(bitset) => Some(bitset),
            Self::Assigned(_) => None,
        }
    }

    pub fn as_bitset_mut(&mut self) -> Option<&mut BitSet> {
        match self {
            Self::Unassigned(bitset) => Some(bitset),
            Self::Assigned(_) => None,
        }
    }

    pub fn count_ones(&self) -> usize {
        match self {
            Self::Unassigned(bitset) => bitset.count_ones(),
            Self::Assigned(_) => 1,
        }
    }

    pub fn first_set(&self) -> Option<usize> {
        match self {
            Self::Unassigned(bitset) => bitset.first_set(),
            Self::Assigned(value) => Some(*value),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Unassigned(bitset) => bitset.len(),
            Self::Assigned(_) => 1,
        }
    }
}

pub(super) struct Domains(pub(super) Box<[Domain]>);

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Domain::Unassigned(bitset) => write!(f, "{}", bitset),
            Domain::Assigned(value) => write!(f, "={}", value),
        }
    }
}

impl fmt::Display for Domains {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Domains(domains) = self;
        let num_vars = domains.iter().map(|d| d.len()).max().unwrap_or(0);
        write!(
            f,
            "  G {}\nH   {}\n",
            (0..num_vars)
                .map(|var| format!("{}", var))
                .collect::<String>(),
            "-".repeat(num_vars)
        )?;
        for (var, domain) in domains.iter().enumerate() {
            write!(f, "{} | {}\n", var, domain)?;
        }
        Ok(())
    }
}
