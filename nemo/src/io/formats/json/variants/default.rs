use serde::{Deserialize, Serialize};

use nemo_physical::datavalues::{
    any_datavalue::AnyDataValueEnum, AnyDataValue, BooleanDataValue, DoubleDataValue,
    FloatDataValue, IriDataValue, LangStringDataValue, LongDataValue, MapDataValue, NullDataValue,
    OtherDataValue, StringDataValue, TupleDataValue, UnsignedLongDataValue,
};

use crate::io::formats::json::datavalues;

#[derive(Deserialize, Serialize)]
#[serde(remote = "AnyDataValueEnum")]
pub(crate) enum AnyDataValueRef {
    #[serde(with = "datavalues::plain_string::default")]
    PlainString(StringDataValue),
    #[serde(with = "datavalues::other::default")]
    LanguageTaggedString(LangStringDataValue),
    #[serde(with = "datavalues::other::default")]
    Iri(IriDataValue),
    #[serde(with = "datavalues::other::default")]
    Float(FloatDataValue),
    #[serde(with = "datavalues::other::default")]
    Double(DoubleDataValue),
    #[serde(with = "datavalues::other::default")]
    UnsignedLong(UnsignedLongDataValue),
    #[serde(with = "datavalues::other::default")]
    Long(LongDataValue),
    #[serde(with = "datavalues::other::default")]
    Boolean(BooleanDataValue),
    #[serde(with = "datavalues::other::default")]
    Null(NullDataValue),
    #[serde(with = "datavalues::other::default")]
    Tuple(TupleDataValue),
    #[serde(with = "datavalues::other::default")]
    Map(MapDataValue),
    #[serde(with = "datavalues::other::default")]
    Other(OtherDataValue),
}

impl From<AnyDataValueRef> for AnyDataValueEnum {
    fn from(value: AnyDataValueRef) -> Self {
        use AnyDataValueRef::*;
        match value {
            PlainString(value) => Self::PlainString(value),
            LanguageTaggedString(value) => Self::LanguageTaggedString(value),
            Iri(value) => Self::Iri(value),
            Float(value) => Self::Float(value),
            Double(value) => Self::Double(value),
            UnsignedLong(value) => Self::UnsignedLong(value),
            Long(value) => Self::Long(value),
            Boolean(value) => Self::Boolean(value),
            Null(value) => Self::Null(value),
            Tuple(value) => Self::Tuple(value),
            Map(value) => Self::Map(value),
            Other(value) => Self::Other(value),
        }
    }
}

impl From<AnyDataValueRef> for AnyDataValue {
    fn from(value: AnyDataValueRef) -> Self {
        Self::from_enum(AnyDataValueEnum::from(value))
    }
}
