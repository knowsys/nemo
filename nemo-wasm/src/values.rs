//! The value bridge: one conversion table, both directions.
//!
//! Lossless primitives cross bare:
//! strings, booleans, doubles as `number`, integers as `number` within ±2⁵³ and `BigInt`
//! beyond.
//! Everything else crosses as a **plain tagged object** with a `kind` discriminant,
//! plain because results cross worker boundaries via `postMessage`,
//! and plain objects are structured-cloneable while class instances are not.
//! The tags keep term identity honest: an IRI never collides with the string of its characters.
//!
//! Values passed to the engine take the same forms, with the one asymmetry JS forces:
//! a bare `number` with an integral value is an *integer* term
//! (an integral double is written `{kind: "double", value}`),
//! since JS cannot tell `5` from `5.0`.

use js_sys::{Array, BigInt, Object, Reflect};
use wasm_bindgen::prelude::*;

use nemo::values::{
    DataValue, Date, DecimalRef, Duration, MapRef, MapValue, NumberRef, Time, TupleRef, TupleValue,
    ValueRef,
};

/// The largest integer magnitude a JS `number` holds exactly.
const SAFE_INTEGER: i64 = 1 << 53;

/// Builds a tagged object from key–value pairs.
fn tagged(kind: &str, fields: &[(&str, JsValue)]) -> JsValue {
    let object = Object::new();
    let set = |key: &str, value: &JsValue| {
        Reflect::set(&object, &JsValue::from_str(key), value)
            .expect("setting a fresh object's property succeeds");
    };
    set("kind", &JsValue::from_str(kind));
    for (key, value) in fields {
        set(key, value);
    }
    object.into()
}

/// Converts an engine value into its JavaScript form, per the table.
pub(crate) fn to_js(value: ValueRef<'_>) -> JsValue {
    match value {
        ValueRef::String(text) => JsValue::from_str(text),
        ValueRef::Boolean(value) => JsValue::from_bool(value),
        ValueRef::Number(NumberRef::Decimal(value)) if value.is_integral() => integer_to_js(value),
        ValueRef::Number(NumberRef::Double(value)) => JsValue::from_f64(value.get()),
        ValueRef::Iri(iri) => tagged("iri", &[("value", JsValue::from_str(&iri.to_string()))]),
        ValueRef::LanguageString(value) => tagged(
            "lang",
            &[
                ("text", JsValue::from_str(value.text())),
                ("language", JsValue::from_str(value.language())),
            ],
        ),
        ValueRef::Number(NumberRef::Float(value)) => {
            tagged("float", &[("value", JsValue::from_f64(value.to_f64()))])
        }
        ValueRef::Number(value) => tagged(
            "decimal",
            &[("lexical", JsValue::from_str(&value.to_string()))],
        ),
        ValueRef::DateTime(value) => tagged(
            "datetime",
            &[("lexical", JsValue::from_str(&value.to_string()))],
        ),
        ValueRef::Date(value) => tagged(
            "date",
            &[("lexical", JsValue::from_str(&value.to_string()))],
        ),
        ValueRef::Time(value) => tagged(
            "time",
            &[("lexical", JsValue::from_str(&value.to_string()))],
        ),
        ValueRef::Duration(value) => tagged(
            "duration",
            &[("lexical", JsValue::from_str(&value.to_string()))],
        ),
        ValueRef::Null(_) => tagged(
            "null",
            &[(
                "id",
                JsValue::from_str(&value.null_number().unwrap_or_default().to_string()),
            )],
        ),
        ValueRef::Tuple(tuple) => tuple_to_js(tuple),
        ValueRef::Map(map) => map_to_js(map),
        ValueRef::Other(literal) => tagged(
            "literal",
            &[
                ("lexical", JsValue::from_str(literal.lexical())),
                ("datatype", JsValue::from_str(literal.datatype())),
            ],
        ),
    }
}

/// Converts an integer into a `number` within ±2⁵³, and a `BigInt` beyond.
fn integer_to_js(value: DecimalRef<'_>) -> JsValue {
    match value.to_i64() {
        // A range test: `abs` overflows on `i64::MIN`.
        Some(integer) if (-SAFE_INTEGER..=SAFE_INTEGER).contains(&integer) =>
        {
            #[allow(clippy::cast_precision_loss)]
            JsValue::from_f64(integer as f64)
        }
        Some(integer) => BigInt::from(integer).into(),
        None => BigInt::new(&JsValue::from_str(&value.to_string()))
            .expect("a canonical integer lexical is a BigInt")
            .into(),
    }
}

/// Converts a tuple into an array, or a tagged object if it carries a label.
fn tuple_to_js(tuple: TupleRef<'_>) -> JsValue {
    let elements: Array = tuple.elements().map(to_js).collect();
    match tuple.label() {
        None => elements.into(),
        Some(label) => tagged(
            "tuple",
            &[
                ("label", JsValue::from_str(label)),
                ("elements", elements.into()),
            ],
        ),
    }
}

/// Converts a map into a tagged object of its pairs, and its label if it carries one.
fn map_to_js(map: MapRef<'_>) -> JsValue {
    let pairs: Array = map
        .pairs()
        .map(|(key, value)| Array::of2(&to_js(key), &to_js(value)).into())
        .collect::<Vec<JsValue>>()
        .into_iter()
        .collect();
    let mut fields = vec![("pairs", JsValue::from(pairs))];
    if let Some(label) = map.label() {
        fields.insert(0, ("label", JsValue::from_str(label)));
    }
    tagged("map", &fields)
}

/// Returns a conversion failure, described for a thrown error.
fn conversion_error(what: &str) -> JsValue {
    js_sys::Error::new(&format!("{what} does not convert to a Nemo term")).into()
}

/// Returns a field of a tagged object, as text.
fn field(object: &JsValue, key: &str) -> Result<String, JsValue> {
    Reflect::get(object, &JsValue::from_str(key))?
        .as_string()
        .ok_or_else(|| conversion_error(&format!("an object without a string `{key}`")))
}

/// Converts a JavaScript value into an engine value, per the table.
pub(crate) fn to_value(any: &JsValue) -> Result<DataValue, JsValue> {
    if let Some(text) = any.as_string() {
        return Ok(DataValue::string(text.as_str()));
    }
    if let Some(value) = any.as_bool() {
        return Ok(DataValue::boolean(value));
    }
    if let Some(value) = any.as_f64() {
        // A JS number cannot tell 5 from 5.0: integral values are
        // integer terms, and an integral double is written
        // `{kind: "double", value}`.
        #[allow(clippy::cast_possible_truncation)]
        if value.fract() == 0.0 && value.abs() <= SAFE_INTEGER as f64 {
            return Ok(DataValue::integer(value as i64));
        }
        return Ok(DataValue::double(value));
    }
    if let Ok(big) = any.clone().dyn_into::<BigInt>() {
        let lexical = big
            .to_string(10)
            .map_err(|_| conversion_error("a BigInt"))?
            .as_string()
            .ok_or_else(|| conversion_error("a BigInt"))?;
        return DataValue::decimal_from_lexical(&lexical)
            .ok_or_else(|| conversion_error("an unrepresentable BigInt"));
    }
    if let Ok(elements) = any.clone().dyn_into::<Array>() {
        let elements: Vec<DataValue> = elements
            .iter()
            .map(|element| to_value(&element))
            .collect::<Result<_, _>>()?;
        return Ok(DataValue::tuple(TupleValue::new(elements)));
    }
    if any.is_object() {
        return tagged_to_value(any);
    }
    Err(conversion_error("this value"))
}

/// Converts a plain tagged object, by its `kind`.
fn tagged_to_value(any: &JsValue) -> Result<DataValue, JsValue> {
    let kind = Reflect::get(any, &JsValue::from_str("kind"))?;
    let Some(kind) = kind.as_string() else {
        return Err(conversion_error("an object without a `kind`"));
    };
    match kind.as_str() {
        "iri" => Ok(DataValue::iri(field(any, "value")?)),
        "lang" => {
            let text = field(any, "text")?;
            let language = field(any, "language")?;
            DataValue::language_string(text.as_str(), language.as_str())
                .ok_or_else(|| conversion_error(&format!("the language tag `{language}`")))
        }
        "float" =>
        {
            #[allow(clippy::cast_possible_truncation)]
            Ok(DataValue::float(number_field(any, "a float")? as f32))
        }
        "double" => Ok(DataValue::double(number_field(any, "a double")?)),
        "decimal" => lexical_value(any, "decimal", DataValue::decimal_from_lexical),
        "datetime" => lexical_value(any, "dateTime", DataValue::datetime_from_lexical),
        "date" => lexical_value(any, "date", |lexical| {
            Date::from_lexical(lexical).map(DataValue::date)
        }),
        "time" => lexical_value(any, "time", |lexical| {
            Time::from_lexical(lexical).map(DataValue::time)
        }),
        "duration" => lexical_value(any, "duration", |lexical| {
            Duration::from_lexical(lexical).map(DataValue::duration)
        }),
        "literal" => {
            let lexical = field(any, "lexical")?;
            let datatype = field(any, "datatype")?;
            DataValue::typed_literal(&datatype, &lexical)
                .ok_or_else(|| conversion_error(&format!("the `{datatype}` lexical `{lexical}`")))
        }
        "tuple" => labeled_tuple_to_value(any),
        "map" => map_to_value(any),
        "null" => Err(conversion_error("a null")),
        other => Err(conversion_error(&format!("a `{other}` object"))),
    }
}

/// Returns the numeric `value` field of a tagged number, which `what` names in the error.
fn number_field(any: &JsValue, what: &str) -> Result<f64, JsValue> {
    Reflect::get(any, &JsValue::from_str("value"))?
        .as_f64()
        .ok_or_else(|| conversion_error(&format!("{what} without a numeric `value`")))
}

/// Converts the `lexical` field of a tagged value of the kind `what` names in the error.
fn lexical_value(
    any: &JsValue,
    what: &str,
    convert: impl FnOnce(&str) -> Option<DataValue>,
) -> Result<DataValue, JsValue> {
    let lexical = field(any, "lexical")?;
    convert(&lexical).ok_or_else(|| conversion_error(&format!("the {what} lexical `{lexical}`")))
}

/// Converts a tagged labeled tuple.
fn labeled_tuple_to_value(any: &JsValue) -> Result<DataValue, JsValue> {
    let label = field(any, "label")?;
    let elements = Reflect::get(any, &JsValue::from_str("elements"))?
        .dyn_into::<Array>()
        .map_err(|_| conversion_error("a tuple without an `elements` array"))?;
    let elements: Vec<DataValue> = elements
        .iter()
        .map(|element| to_value(&element))
        .collect::<Result<_, _>>()?;
    Ok(DataValue::tuple(TupleValue::labeled(
        label.as_str(),
        elements,
    )))
}

/// Converts a tagged map, labeled if it carries a label.
fn map_to_value(any: &JsValue) -> Result<DataValue, JsValue> {
    let pairs = Reflect::get(any, &JsValue::from_str("pairs"))?
        .dyn_into::<Array>()
        .map_err(|_| conversion_error("a map without a `pairs` array"))?;
    let pairs: Vec<(DataValue, DataValue)> = pairs
        .iter()
        .map(|pair| {
            let pair = pair
                .dyn_into::<Array>()
                .map_err(|_| conversion_error("a map pair that is not an array"))?;
            Ok::<_, JsValue>((to_value(&pair.get(0))?, to_value(&pair.get(1))?))
        })
        .collect::<Result<_, _>>()?;
    let label = Reflect::get(any, &JsValue::from_str("label"))?.as_string();
    match label {
        None => MapValue::new(pairs).map(DataValue::map),
        Some(label) => MapValue::labeled(label.as_str(), pairs).map(DataValue::map),
    }
    .ok_or_else(|| conversion_error("a map holding one key twice"))
}
