//! A [DvDict] implementation that manages tuple datavalues.

use crate::datavalues::{
    AnyDataValue, DataValue, IriDataValue, NullDataValue, TupleDataValue, ValueDomain,
};
use crate::management::bytesized::ByteSized;

use super::bytes_buffer::{BytesBuffer, GlobalBytesBuffer};
use super::meta_dv_dict::MetaDvDictionary;
use super::DvDict;
use super::{ranked_pair_dictionary::GenericRankedPairDictionary, AddResult};

/// Type constant that signifies no type in a tuple
const TUPLE_TYPE_NOTYPE: u8 = 0;
/// Type constant to signify a 64bit dictionary id in a tuple
const TUPLE_TYPE_DICTID64: u8 = 1;
/// Type constant to signify a 32bit dictionary id in a tuple
const TUPLE_TYPE_DICTID32: u8 = 2;
/// Type constant to signify a 64bit null id in a tuple
const TUPLE_TYPE_NULLID64: u8 = 3;
/// Type constant to signify a 32bit null id in a tuple
const TUPLE_TYPE_NULLID32: u8 = 4;
/// Type constant to signify an i64 value in a tuple
const TUPLE_TYPE_I64: u8 = 5;
/// Type constant to signify an u64 value in a tuple
const TUPLE_TYPE_U64: u8 = 6;
/// Type constant to signify an f32 value in a tuple
const TUPLE_TYPE_F32: u8 = 7;
/// Type constant to signify an f64 value in a tuple
const TUPLE_TYPE_F64: u8 = 8;
/// Type constant to signify a boolean value in a tuple
const TUPLE_TYPE_BOOL: u8 = 9;
/// Type constant to signify a 32bit dictionary id of a label (at the start of tuples)
const TUPLE_TYPE_LABELID32: u8 = 10;
/// Type constant to signify a 64bit dictionary id of a label (at the start of tuples)
const TUPLE_TYPE_LABELID64: u8 = 11;

crate::dictionary::bytes_buffer::declare_bytes_buffer!(
    TupleBytesPairDictBytesBuffer,
    TUPLE_BYTES_PAIR_DICT_BYTES_BUFFER
);
/// Type for byte pairs dictionary used for encoding tuples
pub(crate) type TupleBytesPairDictionary =
    GenericRankedPairDictionary<TupleBytesPairDictBytesBuffer>;

/// Dictionary for storing IDs of tuples. This is not a subdictionary like the others, since
/// we need mut access to the parent [MetaDvDictionary] to recursively create IDs for subterms
/// of a tuple.
#[derive(Debug)]
pub(crate) struct TupleDvDict {
    /// Internal dictionary to store tuple term types and parameter values.
    ///
    /// Tuple values are encoded in two byte sequences, *type* and *values*, as follows.
    /// The *values* are simply a sequence of binary encodings of all parameter values,
    /// each in some format. Values of some basic types, such as numbers, are encoded directly,
    /// whereas values of other types are represented by dictionary ids. To decode this
    /// blob of bytes (with non-uniform boundaries between the bytes of values), the *type*
    /// information is needed.
    ///
    /// The *type* encodes the encoding types of each value, and the label of the tuple (if any).
    /// If a label is present, then the first byte in *types* is [TUPLE_TYPE_LABELID32] or
    /// [TUPLE_TYPE_LABELID64], depending on how large the label's dictionary ID is. This is followed
    /// by the (4 or 8) bytes of the label. Thereafter is a list of bytes that encode the types of
    /// the values at the corresponding position. In many cases, there are as many type bytes as
    /// values in the *values* sequence. However, repeated occurrences of the same type at the very
    /// end of the sequence are omitted. This allows in particular to encode uniformly typed lists of
    /// any length to share the same *type*.
    dict: TupleBytesPairDictionary,
}

impl TupleDvDict {
    /// Construct a new and empty dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl Default for TupleDvDict {
    fn default() -> Self {
        TupleDvDict {
            dict: TupleBytesPairDictionary::default(),
        }
    }
}

impl DvDict for TupleDvDict {
    /// This method always rejects.
    fn add_datavalue(&mut self, _dv: AnyDataValue) -> AddResult {
        AddResult::Rejected
    }

    fn add_datavalue_with_parent_fn(
        &self,
    ) -> fn(&mut MetaDvDictionary, dict_id: usize, dv: AnyDataValue) -> AddResult {
        add_tuple_datavalue_with_parent
    }

    fn fresh_null(&mut self) -> (AnyDataValue, usize) {
        panic!("tuple dictionaries cannot make fresh nulls");
    }

    fn fresh_null_id(&mut self) -> usize {
        panic!("tuple dictionaries cannot make fresh nulls");
    }

    fn datavalue_to_id(&self, _dv: &AnyDataValue) -> Option<usize> {
        panic!("tuple dictionaries do not support local `datavalue_to_id`; use the callback `datavalue_to_id_with_parent_fn`");
    }

    fn datavalue_to_id_with_parent_fn(
        &self,
    ) -> fn(&MetaDvDictionary, dict_id: usize, dv: &AnyDataValue) -> Option<usize> {
        tuple_datavalue_to_id_with_parent
    }

    fn id_to_datavalue(&self, _id: usize) -> Option<AnyDataValue> {
        panic!("tuple dictionaries do not support local `id_to_datavalue`; use the callback `id_to_datavalue_with_parent_fn`");
    }

    fn id_to_datavalue_with_parent_fn(
        &self,
    ) -> fn(&MetaDvDictionary, dict_id: usize, id: usize) -> Option<AnyDataValue> {
        id_to_tuple_datavalue_with_parent
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

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ByteSized for TupleDvDict {
    fn size_bytes(&self) -> u64 {
        let size = self.dict.size_bytes();
        log::debug!(
            "TupleDvDictionary with {} entries: {} bytes",
            self.len(),
            size
        );
        size
    }
}

/// Create a pair of type bytes and content bytes to encode the given tuple data value.
/// Missing sub-values are added to the dictionary in the process.
///
/// For the schema used in this conversion, see the documentation of [TupleDvDict::dict].
///
/// TODO: This function is very similar to [tuple_bytes]. Maybe consider (efficient) options
/// for code sharing.
fn tuple_bytes_mut(parent_dict: &mut MetaDvDictionary, dv: AnyDataValue) -> (Vec<u8>, Vec<u8>) {
    let mut tuple_type: Vec<u8> = Vec::default(); // maybe define a low capacity here
    let mut tuple_content: Vec<u8> = Vec::default(); // maybe define a low capacity here

    // write label id, if any
    if let Some(label) = dv.label() {
        let label_id = parent_dict.add_datavalue(label.clone().into()).value();

        if let Ok(i32id) = u32::try_from(label_id) {
            tuple_type.push(TUPLE_TYPE_LABELID32);
            tuple_type.extend_from_slice(&i32id.to_be_bytes());
        } else if let Ok(i64id) = u64::try_from(label_id) {
            tuple_type.push(TUPLE_TYPE_LABELID64);
            tuple_type.extend_from_slice(&i64id.to_be_bytes());
        } else {
            panic!("Nemo does not support platforms with more than 64bit.");
        }
    }

    // process parameters to write type and content bytes
    let mut cur_type: u8;
    let mut prev_type: u8 = TUPLE_TYPE_NOTYPE;
    let mut last_type_change: usize = 0;
    for i in 0..dv.len_unchecked() {
        let dvi = dv.tuple_element_unchecked(i);
        match dvi.value_domain() {
            ValueDomain::PlainString
            | ValueDomain::LanguageTaggedString
            | ValueDomain::Iri
            | ValueDomain::Other
            | ValueDomain::Tuple
            | ValueDomain::Map => {
                let term_id = parent_dict.add_datavalue(dvi.clone()).value();

                if let Ok(i32id) = u32::try_from(term_id) {
                    cur_type = TUPLE_TYPE_DICTID32;
                    tuple_content.extend_from_slice(&i32id.to_be_bytes());
                } else if let Ok(i64id) = u64::try_from(term_id) {
                    cur_type = TUPLE_TYPE_DICTID64;
                    tuple_content.extend_from_slice(&i64id.to_be_bytes());
                } else {
                    panic!("Nemo does not support platforms with more than 64bit.");
                }
            }
            ValueDomain::Float => {
                cur_type = TUPLE_TYPE_F32;
                tuple_content.extend_from_slice(&dvi.to_f32_unchecked().to_be_bytes());
            }
            ValueDomain::Double => {
                cur_type = TUPLE_TYPE_F64;
                tuple_content.extend_from_slice(&dvi.to_f64_unchecked().to_be_bytes());
            }
            ValueDomain::Long
            | ValueDomain::Int
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::NonNegativeLong => {
                cur_type = TUPLE_TYPE_I64;
                tuple_content.extend_from_slice(&dvi.to_i64_unchecked().to_be_bytes());
            }
            ValueDomain::UnsignedLong => {
                cur_type = TUPLE_TYPE_U64;
                tuple_content.extend_from_slice(&dvi.to_u64_unchecked().to_be_bytes());
            }
            ValueDomain::Boolean => {
                cur_type = TUPLE_TYPE_BOOL;
                if dvi.to_boolean_unchecked() {
                    tuple_content.push(1);
                } else {
                    tuple_content.push(0);
                }
            }
            ValueDomain::Null => {
                let null_id = dvi.null_id_unchecked();

                if let Ok(i32id) = u32::try_from(null_id) {
                    cur_type = TUPLE_TYPE_NULLID32;
                    tuple_content.extend_from_slice(&i32id.to_be_bytes());
                } else if let Ok(i64id) = u64::try_from(null_id) {
                    cur_type = TUPLE_TYPE_NULLID64;
                    tuple_content.extend_from_slice(&i64id.to_be_bytes());
                } else {
                    panic!("Nemo does not support platforms with more than 64bit.");
                }
            }
        }
        tuple_type.push(cur_type);
        if cur_type != prev_type {
            last_type_change = tuple_type.len();
            prev_type = cur_type;
        }
    }
    tuple_type.truncate(last_type_change);

    (tuple_type, tuple_content)
}

/// Create a pair of type bytes and content bytes to encode the given tuple data value.
/// If relevant sub-values are missing, the method returns [None].
///
/// For the schema used in this conversion, see the documentation of [TupleDvDict::dict].
///
/// TODO: This function is very similar to [tuple_bytes_mut]. Maybe consider (efficient) options
/// for code sharing.
fn tuple_bytes(parent_dict: &MetaDvDictionary, dv: &AnyDataValue) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut tuple_type: Vec<u8> = Vec::default(); // maybe define a low capacity here
    let mut tuple_content: Vec<u8> = Vec::default(); // maybe define a low capacity here

    // write label id, if any
    if let Some(label) = dv.label() {
        if let Some(label_id) = parent_dict.datavalue_to_id(&label.clone().into()) {
            if let Ok(i32id) = u32::try_from(label_id) {
                tuple_type.push(TUPLE_TYPE_LABELID32);
                tuple_type.extend_from_slice(&i32id.to_be_bytes());
            } else if let Ok(i64id) = u64::try_from(label_id) {
                tuple_type.push(TUPLE_TYPE_LABELID64);
                tuple_type.extend_from_slice(&i64id.to_be_bytes());
            } else {
                panic!("Nemo does not support platforms with more than 64bit.");
            }
        } else {
            return None;
        }
    }

    // process parameters to write type and content bytes
    let mut cur_type: u8;
    let mut prev_type: u8 = TUPLE_TYPE_NOTYPE;
    let mut last_type_change: usize = 0;
    for i in 0..dv.len_unchecked() {
        let dvi = dv.tuple_element_unchecked(i);
        match dvi.value_domain() {
            ValueDomain::PlainString
            | ValueDomain::LanguageTaggedString
            | ValueDomain::Iri
            | ValueDomain::Other
            | ValueDomain::Tuple
            | ValueDomain::Map => {
                if let Some(term_id) = parent_dict.datavalue_to_id(&dvi) {
                    if let Ok(i32id) = u32::try_from(term_id) {
                        cur_type = TUPLE_TYPE_DICTID32;
                        tuple_content.extend_from_slice(&i32id.to_be_bytes());
                    } else if let Ok(i64id) = u64::try_from(term_id) {
                        cur_type = TUPLE_TYPE_DICTID64;
                        tuple_content.extend_from_slice(&i64id.to_be_bytes());
                    } else {
                        panic!("Nemo does not support platforms with more than 64bit.");
                    }
                } else {
                    return None;
                }
            }
            ValueDomain::Float => {
                cur_type = TUPLE_TYPE_F32;
                tuple_content.extend_from_slice(&dvi.to_f32_unchecked().to_be_bytes());
            }
            ValueDomain::Double => {
                cur_type = TUPLE_TYPE_F64;
                tuple_content.extend_from_slice(&dvi.to_f64_unchecked().to_be_bytes());
            }
            ValueDomain::Long
            | ValueDomain::Int
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::NonNegativeLong => {
                cur_type = TUPLE_TYPE_I64;
                tuple_content.extend_from_slice(&dvi.to_i64_unchecked().to_be_bytes());
            }
            ValueDomain::UnsignedLong => {
                cur_type = TUPLE_TYPE_U64;
                tuple_content.extend_from_slice(&dvi.to_u64_unchecked().to_be_bytes());
            }
            ValueDomain::Boolean => {
                cur_type = TUPLE_TYPE_BOOL;
                if dvi.to_boolean_unchecked() {
                    tuple_content.push(1);
                } else {
                    tuple_content.push(0);
                }
            }
            ValueDomain::Null => {
                let null_id = dvi.null_id_unchecked();

                if let Ok(i32id) = u32::try_from(null_id) {
                    cur_type = TUPLE_TYPE_NULLID32;
                    tuple_content.extend_from_slice(&i32id.to_be_bytes());
                } else if let Ok(i64id) = u64::try_from(null_id) {
                    cur_type = TUPLE_TYPE_NULLID64;
                    tuple_content.extend_from_slice(&i64id.to_be_bytes());
                } else {
                    panic!("Nemo does not support platforms with more than 64bit.");
                }
            }
        }
        tuple_type.push(cur_type);
        if cur_type != prev_type {
            last_type_change = tuple_type.len();
            prev_type = cur_type;
        }
    }

    tuple_type.truncate(last_type_change);

    Some((tuple_type, tuple_content))
}

fn add_tuple_datavalue_with_parent(
    parent_dict: &mut MetaDvDictionary,
    dict_id: usize,
    dv: AnyDataValue,
) -> AddResult {
    let (tuple_type, tuple_content) = tuple_bytes_mut(parent_dict, dv);

    let myself = parent_dict.sub_dictionary_mut_unchecked(dict_id);
    if let Some(tuple_dict) = myself.as_any_mut().downcast_mut::<TupleDvDict>() {
        tuple_dict.dict.add_pair(&tuple_type, &tuple_content)
    } else {
        panic!("tuple dictionary called with a subdictionary id that does not belong to a tuple dict; this is a bug")
    }
}

fn tuple_datavalue_to_id_with_parent(
    parent_dict: &MetaDvDictionary,
    dict_id: usize,
    dv: &AnyDataValue,
) -> Option<usize> {
    let (tuple_type, tuple_content) = tuple_bytes(parent_dict, dv)?;

    let myself = parent_dict.sub_dictionary_unchecked(dict_id);
    if let Some(tuple_dict) = myself.as_any().downcast_ref::<TupleDvDict>() {
        tuple_dict.dict.pair_to_id(&tuple_type, &tuple_content)
    } else {
        panic!("tuple dictionary called with a subdictionary id that does not belong to a tuple dict; this is a bug")
    }
}

fn id_to_tuple_datavalue_with_parent(
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
    if let Some(tuple_dict) = myself.as_any().downcast_ref::<TupleDvDict>() {
        (tuple_type, tuple_content) = tuple_dict.dict.id_to_pair(id)?;
    } else {
        panic!("tuple dictionary called with a subdictionary id that does not belong to a tuple dict; this is a bug")
    }

    // read label, if any
    let label: Option<IriDataValue>;
    let mut pos_types: usize = 0;
    // Note: the len==0 case can occur when storing an empty tuple that has no label
    if tuple_type.len() > 0 && tuple_type[0] == TUPLE_TYPE_LABELID32 {
        pos_types += 1; // consume initial type byte
        get_value_bytes!(4, pos_types, tuple_type, label_bytes);
        if let Ok(label_id) = usize::try_from(u32::from_be_bytes(label_bytes)) {
            label = Some(parent_dict.id_to_datavalue(label_id).expect(
            "failed to find specified label of a tuple value; dictionaries seem to be corrupted",
        ).into());
        } else {
            panic!(
                "failed to convert u32 to usize; platforms with less than 32bit are not supported"
            );
        }
    } else if tuple_type.len() > 0 && tuple_type[0] == TUPLE_TYPE_LABELID64 {
        pos_types += 1; // consume initial type byte
        get_value_bytes!(8, pos_types, tuple_type, label_bytes);
        if let Ok(label_id) = usize::try_from(u64::from_be_bytes(label_bytes)) {
            label = Some(parent_dict.id_to_datavalue(label_id).expect(
            "failed to find specified label of a tuple value; dictionaries seem to be corrupted",
        ).into());
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
                    values.push(parent_dict.id_to_datavalue(u_id)?); // maybe fail harder here if None is returned?
                } else {
                    panic!("failed to convert u64 to usize; dictionary surpasses the capabilities of this platform");
                }
            }
            TUPLE_TYPE_DICTID32 => {
                get_value_bytes!(4, pos_content, tuple_content, value_bytes);
                let id = u32::from_be_bytes(value_bytes);
                if let Ok(u_id) = usize::try_from(id) {
                    values.push(parent_dict.id_to_datavalue(u_id)?); // maybe fail harder here if None is returned?
                } else {
                    panic!("failed to convert u32 to usize; platforms with less that 32bits are not supported");
                }
            }
            TUPLE_TYPE_NULLID64 => {
                get_value_bytes!(8, pos_content, tuple_content, value_bytes);
                let id = u64::from_be_bytes(value_bytes);
                if let Ok(u_id) = usize::try_from(id) {
                    values.push(NullDataValue::new(u_id).into());
                } else {
                    panic!("failed to convert u64 to usize; dictionary surpasses the capabilities of this platform");
                }
            }
            TUPLE_TYPE_NULLID32 => {
                get_value_bytes!(4, pos_content, tuple_content, value_bytes);
                let id = u32::from_be_bytes(value_bytes);
                if let Ok(u_id) = usize::try_from(id) {
                    values.push(NullDataValue::new(u_id).into());
                } else {
                    panic!("failed to convert u32 to usize; platforms with less that 32bits are not supported");
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
            _ => panic!("unexpected type byte {} for tuple value", cur_type),
        }
    }

    Some(TupleDataValue::new(label, values).into())
}

// tests for tuple_dv_dict are in meta_dv_dict
