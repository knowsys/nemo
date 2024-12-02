//! General traits and global constants for dictionaries that work for datavalues.

use crate::{datavalues::AnyDataValue, management::bytesized::ByteSized};
use std::fmt::Debug;

use super::meta_dv_dict::MetaDvDictionary;

/// Fake id that dictionaries use to indicate that an entry has no id.
pub const NONEXISTING_ID_MARK: usize = usize::MAX;
/// Fake id that dictionaries use for marked entries.
pub const KNOWN_ID_MARK: usize = usize::MAX - 1;

/// A marker id reserved for playing the role of [KNOWN_ID_MARK] while we are workin
/// with 32bit ids. This is internal and always replaced by [KNOWN_ID_MARK] to the outside.
#[cfg(not(test))]
pub(crate) const SMALL_KNOWN_ID_MARK: u32 = u32::MAX;
#[cfg(test)]
// Note: A smaller value is used to exercise the relevant code in the unit tests as well.
pub(crate) const SMALL_KNOWN_ID_MARK: u32 = 3;

pub(crate) const SMALL_KNOWN_ID_MARK_AS_USIZE: usize = SMALL_KNOWN_ID_MARK as usize;

/// Result of adding new values to a dictionary.
/// It indicates if the operation was successful, and whether the value was previously present or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddResult {
    /// Element was new and has been freshly assinged the given id.
    Fresh(usize),
    /// Element was already known and has the given id.
    Known(usize),
    /// Element not supported by dictionary.
    Rejected,
}

impl AddResult {
    /// Returns the actual index.
    /// In case of [AddResult::Rejected], a fake id is returned ([usize::MAX]).
    pub fn value(&self) -> usize {
        match self {
            AddResult::Fresh(value) => *value,
            AddResult::Known(value) => *value,
            AddResult::Rejected => NONEXISTING_ID_MARK,
        }
    }
}

/// A [DvDict] represents a dictionary for datavalues, i.e., a bijective (invertible) mapping from
/// [crate::datavalues::DataValue]s to numeric ids (`usize`).
///
/// In addition, to this bijection, dictionaries maintain
/// a set of *marked* datavalues. For these, the dictionary will always return the virtual id [KNOWN_ID_MARK],
/// which cannot be used to retrieve datavalues.
///
/// The id values are provided when the dictionary is used, whereas the ids are newly
/// assigned by the dictionary itself.
pub trait DvDict: Debug + ByteSized {
    /// Adds a new [AnyDataValue] to the dictionary. If the value is not known yet, it will
    /// be assigned a new id. Unsupported datavalues can also be rejected, which specialized
    /// dictionary implementations might do.
    ///
    /// The result is an [AddResult] that indicates if the string was newly added,
    /// previoulsy present, or rejected. In the first two cases, the result yields
    /// the id.
    ///
    /// When adding values that have previously been marked (see [DvDict::mark_dv]),
    /// the dictionary will *not* assign a fresh id, but simply return [AddResult::Known]
    /// with [KNOWN_ID_MARK].
    fn add_datavalue(&mut self, dv: AnyDataValue) -> AddResult;

    /// Returns a function pointer to a function that takes a (parent) [MetaDvDictionary] and a dictionary id
    /// that identifies this dictionary rather than `&self`. Mutable access to a parent dictionary is required
    /// to resolve nested terms recursively, but making this a method (that takes `&self`) would offend the Rust
    /// borrow checker even if self is never accessed directly within the method. Returning a function pointer
    /// replaces the native dynamic dispatch without requiring `&self` to be present.
    ///
    /// The default implementation of this method returns a pointer to [add_datavalue_with_parent],
    /// which simply calls [DvDict::add_datavalue] after retrieving the identified dictionary from the parent.
    /// There is no major performance penalty in this double indirection since [MetaDvDictionary] can cache the
    /// function pointer, and would have to retrieve the sub-dictionary anyway to call it.
    fn add_datavalue_with_parent_fn(
        &self,
    ) -> fn(&mut MetaDvDictionary, dict_id: usize, dv: AnyDataValue) -> AddResult {
        add_datavalue_with_parent
    }

    /// Creates a fresh null [AnyDataValue] and assigns an id to it. Both the new null and
    /// the id are returned.
    fn fresh_null(&mut self) -> (AnyDataValue, usize);

    /// Creates a fresh null [AnyDataValue] and returns its id.
    fn fresh_null_id(&mut self) -> usize;

    /// Looks up the given [AnyDataValue] and returns `Some(id)` if it is in the dictionary, and `None` otherwise.
    /// For marked datavalues, this returns [KNOWN_ID_MARK] as an id.
    fn datavalue_to_id(&self, dv: &AnyDataValue) -> Option<usize>;

    /// Returns a function pointer to a function that takes a (parent) [MetaDvDictionary] and a dictionary id
    /// that identifies this dictionary rather than `&self`. Immutable access to a parent dictionary is required
    /// to handle nested terms recursively, but making this a method (that takes `&self`) would offend the Rust
    /// borrow checker even if self is never accessed directly within the method. Returning a function pointer
    /// replaces the native dynamic dispatch without requiring `&self` to be present.
    ///
    /// The default implementation of this method returns a pointer to [datavalue_to_id_with_parent],
    /// which simply calls [DvDict::datavalue_to_id] after retrieving the identified dictionary from the parent.
    /// There is no major performance penalty in this double indirection since [MetaDvDictionary] can cache the
    /// function pointer, and would have to retrieve the sub-dictionary anyway to call it.
    fn datavalue_to_id_with_parent_fn(
        &self,
    ) -> fn(&MetaDvDictionary, dict_id: usize, dv: &AnyDataValue) -> Option<usize> {
        datavalue_to_id_with_parent
    }

    /// Returns the [AnyDataValue] associated with the `id`, or None if the `id` is not associated with any datavalue.
    /// In particular, this occurs if the datavalue was marked (and has virtual id [KNOWN_ID_MARK]).
    fn id_to_datavalue(&self, id: usize) -> Option<AnyDataValue>;

    /// Returns a function pointer to a function that takes a (parent) [MetaDvDictionary] and a dictionary id
    /// that identifies this dictionary rather than `&self`. Immutable access to a parent dictionary is required
    /// to resolve nested terms recursively, but making this a method (that takes `&self`) would offend the Rust
    /// borrow checker even if self is never accessed directly within the method. Returning a function pointer
    /// replaces the native dynamic dispatch without requiring `&self` to be present.
    ///
    /// The default implementation of this method returns a pointer to [id_to_datavalue_with_parent],
    /// which simply calls [DvDict::id_to_datavalue] after retrieving the identified dictionary from the parent.
    /// There is no major performance penalty in this double indirection since [MetaDvDictionary] can cache the
    /// function pointer, and would have to retrieve the sub-dictionary anyway to call it.
    fn id_to_datavalue_with_parent_fn(
        &self,
    ) -> fn(&MetaDvDictionary, dict_id: usize, id: usize) -> Option<AnyDataValue> {
        id_to_datavalue_with_parent
    }

    /// Returns the number of values in the dictionary. Databalues that were merely marked are not counted,
    /// only those that have a unique id through which they can be retrieved.
    fn len(&self) -> usize;

    /// Returns true if the dictionary is empty. False otherwise
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if the given ID is associated with an IRI, and false
    /// if it is associated with anything else or nothing at all.
    fn is_iri(&self, id: usize) -> bool;

    /// Returns true if the given ID is associated with a plain string, and false
    /// if it is associated with anything else or nothing at all.
    fn is_plain_string(&self, id: usize) -> bool;

    /// Returns true if the given ID is associated with a language-tagged string, and false
    /// if it is associated with anything else or nothing at all.
    fn is_lang_string(&self, id: usize) -> bool;

    /// Returns true if the given ID is associated with a null, and false
    /// if it is associated with anything else or nothing at all.
    fn is_null(&self, id: usize) -> bool;

    /// Marks the given datavalue as being known, without assigning an own id to it.
    /// If the entry exists already, the existing id will be kept and returned. Otherwise,
    /// the virtual id [KNOWN_ID_MARK] is assigned.
    ///
    /// Implementations may return [AddResult::Rejected] to indicate that the dictionary
    /// does not support marking of the given value.
    fn mark_dv(&mut self, dv: AnyDataValue) -> AddResult;

    /// Returns true if the dictionary contains any marked elements (see [DvDict::mark_dv]).
    fn has_marked(&self) -> bool;

    /// Returns a mutable object that implements this trait as a [std::any::Any]. Dynamic
    /// trait objects cannot be used as [std::any::Any], but concrete implementations
    /// can provide a (dynamic) method that accomplishes this. This is a pattern that
    /// can then be used for casting the dynamic object into a concrete type.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;

    /// Returns an unmutable object that implements this trait as a [std::any::Any]. Dynamic
    /// trait objects cannot be used as [std::any::Any], but concrete implementations
    /// can provide a (dynamic) method that accomplishes this. This is a pattern that
    /// can then be used for casting the dynamic object into a concrete type.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Default implementation for [DvDict::add_datavalue_with_parent_fn]. The function simply
/// fetches the [DvDict] and calls the local [DvDict::add_datavalue].
fn add_datavalue_with_parent(
    parent_dict: &mut MetaDvDictionary,
    dict_id: usize,
    dv: AnyDataValue,
) -> AddResult {
    parent_dict
        .sub_dictionary_mut_unchecked(dict_id)
        .add_datavalue(dv)
}

/// Default implementation for [DvDict::datavalue_to_id_with_parent_fn]. The function simply
/// fetches the [DvDict] and calls the local [DvDict::datavalue_to_id].
fn datavalue_to_id_with_parent(
    parent_dict: &MetaDvDictionary,
    dict_id: usize,
    dv: &AnyDataValue,
) -> Option<usize> {
    parent_dict
        .sub_dictionary_unchecked(dict_id)
        .datavalue_to_id(dv)
}

/// Default implementation for [DvDict::id_to_datavalue_with_parent_fn]. The function simply
/// fetches the [DvDict] and calls the local [DvDict::id_to_datavalue].
fn id_to_datavalue_with_parent(
    parent_dict: &MetaDvDictionary,
    dict_id: usize,
    id: usize,
) -> Option<AnyDataValue> {
    parent_dict
        .sub_dictionary_unchecked(dict_id)
        .id_to_datavalue(id)
}
