//! A [DvDict] implementation that manages tuple datavalues.

use crate::datavalues::{AnyDataValue, IriDataValue, MapDataValue, NullDataValue};
use crate::dictionary::tuple_dv_dict::{
    TUPLE_TYPE_BOOL, TUPLE_TYPE_DICTID32, TUPLE_TYPE_DICTID64, TUPLE_TYPE_F32, TUPLE_TYPE_F64,
    TUPLE_TYPE_I64, TUPLE_TYPE_LABELID32, TUPLE_TYPE_LABELID64, TUPLE_TYPE_NOTYPE,
    TUPLE_TYPE_NULLID32, TUPLE_TYPE_NULLID64, TUPLE_TYPE_U64,
};
use crate::management::bytesized::ByteSized;

use super::tuple_dv_dict::{tuple_bytes, tuple_bytes_mut};
use super::{
    AddResult, DvDict, meta_dv_dict::MetaDvDictionary, tuple_dv_dict::TupleBytesPairDictionary,
};

/// Dictionary for storing IDs of Map. This is not a subdictionary like the others, since
/// we need mut access to the parent [MetaDvDictionary] to recursively create IDs for subterms
/// of a Map.
#[derive(Debug, Default)]
pub(crate) struct MapDvDict {
    /// Internal dictionary to store map term types and parameter
    /// values. This reuses the encoding for tuples, treating the Map
    /// as a tuple alternating between keys and values.
    dict: TupleBytesPairDictionary,
}

impl MapDvDict {
    /// Construct a new and empty dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl DvDict for MapDvDict {
    /// This method always rejects.
    fn add_datavalue(&mut self, _dv: AnyDataValue) -> AddResult {
        AddResult::Rejected
    }

    fn add_datavalue_with_parent_fn(
        &self,
    ) -> fn(&mut MetaDvDictionary, dict_id: usize, dv: AnyDataValue) -> AddResult {
        add_map_datavalue_with_parent
    }

    fn fresh_null(&mut self) -> (AnyDataValue, usize) {
        unimplemented!("map dictionaries cannot make fresh nulls");
    }

    fn fresh_null_id(&mut self) -> usize {
        unimplemented!("map dictionaries cannot make fresh nulls");
    }

    fn datavalue_to_id(&self, _dv: &AnyDataValue) -> Option<usize> {
        unimplemented!(
            "map dictionaries do not support local `datavalue_to_id`; use the callback `datavalue_to_id_with_parent_fn`"
        );
    }

    fn datavalue_to_id_with_parent_fn(
        &self,
    ) -> fn(&MetaDvDictionary, dict_id: usize, dv: &AnyDataValue) -> Option<usize> {
        map_datavalue_to_id_with_parent
    }

    fn id_to_datavalue(&self, _id: usize) -> Option<AnyDataValue> {
        unimplemented!(
            "map dictionaries do not support local `id_to_datavalue`; use the callback `id_to_datavalue_with_parent_fn`"
        );
    }

    fn id_to_datavalue_with_parent_fn(
        &self,
    ) -> fn(&MetaDvDictionary, dict_id: usize, id: usize) -> Option<AnyDataValue> {
        id_to_map_datavalue_with_parent
    }

    fn len(&self) -> usize {
        self.dict.len()
    }

    fn is_iri(&self, _id: usize) -> bool {
        false
    }

    fn is_plain_string(&self, _id: usize) -> bool {
        false
    }

    fn is_lang_string(&self, _id: usize) -> bool {
        false
    }

    fn is_null(&self, _id: usize) -> bool {
        false
    }

    /// This method always rejects.
    fn mark_dv(&mut self, _dv: AnyDataValue) -> AddResult {
        AddResult::Rejected
    }

    fn has_marked(&self) -> bool {
        self.dict.has_marked()
    }
}

impl ByteSized for MapDvDict {
    fn size_bytes(&self) -> u64 {
        let size = self.dict.size_bytes();
        log::debug!(
            "MapDvDictionary with {} entries: {} bytes",
            self.len(),
            size
        );
        size
    }
}

/// Adds a tuple value to the dictionary. Subvalues that are used inside the tuple are
/// added to the dictionary recursively, if necessary. The same applies to the label, if any.
///
/// Following the general callback scheme of [DvDict::add_datavalue_with_parent_fn], the
/// function fetches the "self" tuple dict from its parent based on the given dictionary id.
fn add_map_datavalue_with_parent(
    parent_dict: &mut MetaDvDictionary,
    dict_id: usize,
    dv: AnyDataValue,
) -> AddResult {
    let (tuple_type, tuple_content) = tuple_bytes_mut(parent_dict, dv);

    let myself = parent_dict.sub_dictionary_mut_unchecked(dict_id);
    if let Some(map_dict) = myself.as_any_mut().downcast_mut::<MapDvDict>() {
        map_dict.dict.add_pair(&tuple_type, &tuple_content)
    } else {
        panic!(
            "map dictionary called with a subdictionary id that does not belong to a tuple dict; this is a bug"
        )
    }
}

/// Converts a tuple value to a dictionary id, if that tuple is known to the dictionary.
/// Identifiers of subvalues that are used inside the tuple resolved recursively, if necessary.
/// The same applies to the label, if any.
///
/// Following the general callback scheme of [DvDict::datavalue_to_id_with_parent_fn], the
/// function fetches the "self" tuple dict from its parent based on the given dictionary id.
fn map_datavalue_to_id_with_parent(
    parent_dict: &MetaDvDictionary,
    dict_id: usize,
    dv: &AnyDataValue,
) -> Option<usize> {
    let (tuple_type, tuple_content) = tuple_bytes(parent_dict, dv)?;

    let myself = parent_dict.sub_dictionary_unchecked(dict_id);
    if let Some(map_dict) = myself.as_any().downcast_ref::<MapDvDict>() {
        map_dict.dict.pair_to_id(&tuple_type, &tuple_content)
    } else {
        panic!(
            "map dictionary called with a subdictionary id that does not belong to a tuple dict; this is a bug"
        )
    }
}

/// Converts a dictionary id to a tuple value, if that id is known to the tuple value dictionary.
/// Datavalues of subvalues that are used inside the recovered recursively, if necessary. It is
/// an error if any of the subvalues used in the tuple do not have a dictionary entry.
///
/// Following the general callback scheme of [DvDict::datavalue_to_id_with_parent_fn], the
/// function fetches the "self" tuple dict from its parent based on the given dictionary id.
///
/// # Panics
/// If the label or any of the dictinary-managed subvalues of this tuple cannot be found in the
/// dictionary. Also, the function will panic if the given `dict_id` is not affiliated with a
/// [TupleDvDict] in the given `parent_dict`.
fn id_to_map_datavalue_with_parent(
    parent_dict: &MetaDvDictionary,
    dict_id: usize,
    id: usize,
) -> Option<AnyDataValue> {
    /// Macro for copying some bytes from a byte array while advancing a position counter.
    macro_rules! get_value_bytes {
        ($number:literal, $source_pos: ident, $source: ident, $bytes:ident) => {
            let mut $bytes: [u8; $number] = [0; $number];
            $bytes.copy_from_slice(&$source[$source_pos..=$source_pos + $number - 1]);
            $source_pos += $number;
        };
    }

    let tuple_type: Vec<u8>;
    let tuple_content: Vec<u8>;
    let myself = parent_dict.sub_dictionary_unchecked(dict_id);
    if let Some(tuple_dict) = myself.as_any().downcast_ref::<MapDvDict>() {
        (tuple_type, tuple_content) = tuple_dict.dict.id_to_pair(id)?;
    } else {
        panic!(
            "map dictionary called with a subdictionary id that does not belong to a tuple dict; this is a bug"
        )
    }

    // read label, if any
    let label: Option<IriDataValue>;
    let mut pos_types: usize = 0;
    // Note: the case is_empty() can occur when storing an empty tuple that has no label
    if !tuple_type.is_empty() && tuple_type[0] == TUPLE_TYPE_LABELID32 {
        pos_types += 1; // consume initial type byte
        get_value_bytes!(4, pos_types, tuple_type, label_bytes);
        if let Ok(label_id) = usize::try_from(u32::from_be_bytes(label_bytes)) {
            label = Some(parent_dict.id_to_datavalue(label_id).expect(
            "failed to find specified label of a tuple value; dictionaries seem to be corrupted",
        ).try_into().expect("label recovered from dictionary is not an IRI; this is a bug"));
        } else {
            panic!(
                "failed to convert u32 to usize; platforms with less than 32bit are not supported"
            );
        }
    } else if !tuple_type.is_empty() && tuple_type[0] == TUPLE_TYPE_LABELID64 {
        pos_types += 1; // consume initial type byte
        get_value_bytes!(8, pos_types, tuple_type, label_bytes);
        if let Ok(label_id) = usize::try_from(u64::from_be_bytes(label_bytes)) {
            label = Some(parent_dict.id_to_datavalue(label_id).expect(
            "failed to find specified label of a tuple value; dictionaries seem to be corrupted",
        ).try_into().expect("label recovered from dictionary is not an IRI; this is a bug"));
        } else {
            panic!(
                "failed to convert u64 to usize; dictionary surpasses the capabilities of this platform"
            );
        }
        pos_types += 9;
    } else {
        label = None;
    }

    // read values
    let mut pos_content: usize = 0;
    let mut values: Vec<AnyDataValue> = vec![];

    let mut cur_type: u8 = TUPLE_TYPE_NOTYPE;
    while pos_content < tuple_content.len() {
        // automatically keeps last type when reaching end of type list before end of content:
        if pos_types < tuple_type.len() {
            cur_type = tuple_type[pos_types];
            pos_types += 1;
        }
        match cur_type {
            TUPLE_TYPE_DICTID64 => {
                get_value_bytes!(8, pos_content, tuple_content, value_bytes);
                let id = u64::from_be_bytes(value_bytes);
                if let Ok(u_id) = usize::try_from(id) {
                    values.push(
                        parent_dict
                            .id_to_datavalue(u_id)
                            .expect("value used within tuple not found in dictionary"),
                    );
                } else {
                    panic!(
                        "failed to convert u64 to usize; dictionary surpasses the capabilities of this platform"
                    );
                }
            }
            TUPLE_TYPE_DICTID32 => {
                get_value_bytes!(4, pos_content, tuple_content, value_bytes);
                let id = u32::from_be_bytes(value_bytes);
                if let Ok(u_id) = usize::try_from(id) {
                    values.push(
                        parent_dict
                            .id_to_datavalue(u_id)
                            .expect("value used within tuple not found in dictionary"),
                    );
                } else {
                    panic!(
                        "failed to convert u32 to usize; platforms with less that 32bits are not supported"
                    );
                }
            }
            TUPLE_TYPE_NULLID64 => {
                get_value_bytes!(8, pos_content, tuple_content, value_bytes);
                let id = u64::from_be_bytes(value_bytes);
                if let Ok(u_id) = usize::try_from(id) {
                    values.push(NullDataValue::new(u_id).into());
                } else {
                    panic!(
                        "failed to convert u64 to usize; dictionary surpasses the capabilities of this platform"
                    );
                }
            }
            TUPLE_TYPE_NULLID32 => {
                get_value_bytes!(4, pos_content, tuple_content, value_bytes);
                let id = u32::from_be_bytes(value_bytes);
                if let Ok(u_id) = usize::try_from(id) {
                    values.push(NullDataValue::new(u_id).into());
                } else {
                    panic!(
                        "failed to convert u32 to usize; platforms with less that 32bits are not supported"
                    );
                }
            }
            TUPLE_TYPE_I64 => {
                get_value_bytes!(8, pos_content, tuple_content, value_bytes);
                values.push(AnyDataValue::new_integer_from_i64(i64::from_be_bytes(
                    value_bytes,
                )));
            }
            TUPLE_TYPE_U64 => {
                get_value_bytes!(8, pos_content, tuple_content, value_bytes);
                values.push(AnyDataValue::new_integer_from_u64(u64::from_be_bytes(
                    value_bytes,
                )));
            }
            TUPLE_TYPE_F32 => {
                get_value_bytes!(4, pos_content, tuple_content, value_bytes);
                values.push(
                    AnyDataValue::new_float_from_f32(f32::from_be_bytes(value_bytes))
                        .expect("stored f32 value could not be recovered"),
                );
            }
            TUPLE_TYPE_F64 => {
                get_value_bytes!(8, pos_content, tuple_content, value_bytes);
                values.push(
                    AnyDataValue::new_double_from_f64(f64::from_be_bytes(value_bytes))
                        .expect("stored f64 value could not be recovered"),
                );
            }
            TUPLE_TYPE_BOOL => {
                get_value_bytes!(1, pos_content, tuple_content, value_bytes);
                if value_bytes[0] == 0 {
                    values.push(AnyDataValue::new_boolean(false));
                } else {
                    values.push(AnyDataValue::new_boolean(true));
                }
            }
            _ => panic!("unexpected type byte {cur_type} for tuple value"),
        }
    }

    let chunks = values.into_iter().array_chunks().collect::<Vec<[_; 2]>>();
    Some(MapDataValue::from_chunks(label, chunks.iter()).into())
}
