/// A helper macro to set up the boilerplate for a JSON variant.
///
/// `$name` is the variant name, the macro will define a wrapper type
/// `JsonAnyDataValue` that is used for serializing and deserializing
/// an [AnyDataValue](nemo_physical::datavalues::AnyDataValue).
///
/// For each variant in
/// [AnyDataValue](nemo_physical::datavalues::AnyDataValue), it takes
/// a specification of the form `$variant($type) => "$policy", where
/// `$variant` is the name of the variant, `$type` is the underlying
/// data value type, and `$policy` is a string of the form
/// `"datavalues::type::variant"`, which points to a module in the
/// [datavalues](crate::io::formats::json::datavalues) module
/// indicating which type and which variant to use for
/// serialization/deserialization.
macro_rules! json_variant {
    ($name:ident; $($variant:ident($type:ty) => $policy:expr),+) => {
        pub(crate) mod $name {
            use serde::{Deserialize, Serialize};

            use nemo_physical::datavalues::{any_datavalue::AnyDataValueEnum, *};
            use crate::io::formats::json::datavalues;

            #[derive(Deserialize, Serialize)]
            pub(crate) enum JsonAnyDataValue {
                $(
                    #[serde(with = $policy)]
                    $variant($type)
                ),+
            }

            // this is mostly to ensure that we exhaustively match all variants
            impl From<AnyDataValueEnum> for JsonAnyDataValue {
                fn from(value: AnyDataValueEnum) -> Self {
                    use AnyDataValueEnum::*;

                    match value {
                        $( $variant(value) => Self::$variant(value) ),+
                    }
                }
            }

            impl From<AnyDataValue> for JsonAnyDataValue {
                fn from(value: AnyDataValue) -> Self {
                    Self::from(value.into_inner())
                }
            }

            impl From<JsonAnyDataValue> for AnyDataValueEnum {
                fn from(value: JsonAnyDataValue) -> Self {
                use JsonAnyDataValue::*;

                    match value {
                        $( $variant(value) => Self::$variant(value) ),+
                    }
                }
            }

            impl From<JsonAnyDataValue> for AnyDataValue {
                fn from(value: JsonAnyDataValue) -> Self {
                    Self::from_enum(AnyDataValueEnum::from(value))
                }
            }
        }
    };
}

// Order is important here: deserializing will take the first
// successful variant, so we need to try, e.g., values serialized into
// strings (such as IRIs or language strings) before plain strings.
json_variant!(default;
              Null(NullDataValue) => "datavalues::any::default",
              Tuple(TupleDataValue) => "datavalues::other::default",
              Map(MapDataValue) => "datavalues::other::default",
              LanguageTaggedString(LangStringDataValue) => "datavalues::any::default",
              Iri(IriDataValue) => "datavalues::any::default",
              PlainString(StringDataValue) => "datavalues::plain_string::default",
              UnsignedLong(UnsignedLongDataValue) => "datavalues::other::default",
              Long(LongDataValue) => "datavalues::other::default",
              Float(FloatDataValue) => "datavalues::float::default",
              Double(DoubleDataValue) => "datavalues::double::default",
              Boolean(BooleanDataValue) => "datavalues::other::default",
              Other(OtherDataValue) => "datavalues::other::default"
);
