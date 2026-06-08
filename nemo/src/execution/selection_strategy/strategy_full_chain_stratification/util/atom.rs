use crate::execution::planning::normalization::atom::{body::BodyAtom, head::HeadAtom};
use crate::rule_model::components::term::primitive::{Primitive, variable::Variable};

use crate::execution::selection_strategy::strategy_full_chain_stratification::util::ordered_atoms::{Mem, ReorderedAtoms, ReorderAtoms};

use delegate::delegate;

// TODO: possibly performance issue: the Tag contains the actual string... should be interned
pub type Predicate = String;

pub trait Atom {
    type Item<'a>: AsRef<Primitive>
    where
        Self: 'a;

    fn pred(&self) -> Predicate;
    fn primitives<'a>(&'a self) -> impl Iterator<Item = Self::Item<'a>>;
    fn variables<'a>(&'a self) -> impl Iterator<Item = &'a Variable>;

    fn reorder<'b, 'a: 'b>(
        reordered_atoms: &'b mut ReorderAtoms<'a>,
    ) -> &'b mut Mem<ReorderedAtoms<'a, Self>>;
}

impl Atom for HeadAtom {
    type Item<'a> = &'a Primitive;

    fn pred(&self) -> Predicate {
        self.predicate().name().to_owned()
    }

    fn primitives<'a>(&'a self) -> impl Iterator<Item = Self::Item<'a>> {
        self.terms()
    }

    fn variables<'a>(&'a self) -> impl Iterator<Item = &'a Variable> {
        self.variables()
    }

    fn reorder<'b, 'a: 'b>(
        reordered_atoms: &'b mut ReorderAtoms<'a>,
    ) -> &'b mut Mem<ReorderedAtoms<'a, HeadAtom>> {
        &mut reordered_atoms.reordered_head_atoms
    }
}

impl Atom for BodyAtom {
    type Item<'a> = Primitive;

    fn pred(&self) -> Predicate {
        self.predicate().name().to_owned()
    }

    /// Return an iterator over all terms contained in this atom as primitives.
    fn primitives<'a>(&'a self) -> impl Iterator<Item = Self::Item<'a>> {
        self.terms().map(|var| Primitive::Variable(var.clone()))
    }

    fn variables<'a>(&'a self) -> impl Iterator<Item = &'a Variable> {
        self.terms()
    }

    fn reorder<'b, 'a: 'b>(
        reordered_atoms: &'b mut ReorderAtoms<'a>,
    ) -> &'b mut Mem<ReorderedAtoms<'a, BodyAtom>> {
        &mut reordered_atoms.reordered_body_atoms
    }
}

#[repr(transparent)]
pub struct NegBodyAtom(BodyAtom);

impl NegBodyAtom {
    /// Cast a reference to a [BodyAtom] to a reference to a [NegBodyAtom].
    /// This is safe, since the layout of the transparent newtype is compatible.
    /// This is analogous to deriving `bytemuck::TransparentWrapper` or `ref_cast::RefCast`.
    pub fn wrap_ref(r: &BodyAtom) -> &Self {
        //let tmp: *const BodyAtom = r;
        //unsafe { &*(tmp as *const NegBodyAtom) }
        unsafe { &*std::ptr::from_ref(r).cast::<NegBodyAtom>() }
    }
}

impl Atom for NegBodyAtom {
    type Item<'a>
        = <BodyAtom as Atom>::Item<'a>
    where
        Self: 'a;
    delegate! {
        to self.0 {
            fn pred(&self) -> Predicate;
            fn primitives<'a>(&'a self) -> impl Iterator<Item = Self::Item<'a>>;
            fn variables<'a>(&'a self) -> impl Iterator<Item = &'a Variable>;
        }
    }

    fn reorder<'b, 'a: 'b>(
        reordered_atoms: &'b mut ReorderAtoms<'a>,
    ) -> &'b mut Mem<ReorderedAtoms<'a, NegBodyAtom>> {
        &mut reordered_atoms.reordered_negative_body_atoms
    }
}
