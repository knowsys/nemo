//! The conversion of values between the engine and JavaScript, in both directions.
//!
//! Strings and booleans become plain JavaScript values, a double becomes a `number`,
//! and an integer becomes a `number` within ±2⁵³ and a `BigInt` beyond.
//! Every other value becomes a **plain tagged object** with a `kind` field.
//! The objects are plain because results are sent between workers with `postMessage`,
//! which can clone plain objects but not class instances.
//! The tags keep different terms apart: an IRI is never the string with its characters.
//!
//! Values passed to the engine use the same forms, with one difference that JavaScript forces:
//! a `number` with an integral value within ±2⁵³ is an *integer*,
//! since JavaScript does not distinguish `5` from `5.0`,
//! and an integral double is written `{kind: "double", value}`.

use js_sys::{Array, BigInt, Object, Reflect};
use wasm_bindgen::prelude::*;

use nemo::values::{
    DataValue, Date, DecimalRef, Duration, MapRef, MapValue, NumberRef, Time, TupleRef, TupleValue,
    ValueRef,
};

/// The magnitude up to which a JavaScript `number` holds every integer exactly.
const SAFE_INTEGER: i64 = 1 << 53;

/// Returns a tagged object of the kind `kind`, with the given fields.
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

/// Converts an engine value into its JavaScript form.
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
        // A range test rather than `abs`, which overflows on `i64::MIN`.
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

/// Converts a tuple into an array, or into a tagged object if it has a label.
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

/// Converts a map into a tagged object with its pairs, and with its label if it has one.
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

/// Returns the error to throw for `what`, a value that does not convert.
fn conversion_error(what: &str) -> JsValue {
    js_sys::Error::new(&format!("{what} does not convert to a Nemo term")).into()
}

/// Returns the string field `key` of a tagged object, or an error if it has none.
fn field(object: &JsValue, key: &str) -> Result<String, JsValue> {
    Reflect::get(object, &JsValue::from_str(key))?
        .as_string()
        .ok_or_else(|| conversion_error(&format!("an object without a string `{key}`")))
}

/// Converts a JavaScript value into an engine value.
///
/// Returns the error to throw for a value that does not convert.
pub(crate) fn to_value(any: &JsValue) -> Result<DataValue, JsValue> {
    if let Some(text) = any.as_string() {
        return Ok(DataValue::string(text.as_str()));
    }
    if let Some(value) = any.as_bool() {
        return Ok(DataValue::boolean(value));
    }
    if let Some(value) = any.as_f64() {
        // JavaScript does not distinguish 5 from 5.0, so an integral number becomes an
        // integer, and an integral double must be written `{kind: "double", value}`.
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

/// Converts a plain tagged object according to its `kind`.
///
/// A null does not convert, since no term denotes it.
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

/// Returns the numeric `value` field of a tagged number.
///
/// `what` names the kind of number in the error.
fn number_field(any: &JsValue, what: &str) -> Result<f64, JsValue> {
    Reflect::get(any, &JsValue::from_str("value"))?
        .as_f64()
        .ok_or_else(|| conversion_error(&format!("{what} without a numeric `value`")))
}

/// Converts the `lexical` field of a tagged value with `convert`.
///
/// `what` names the kind of value in the error.
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

/// Converts a tagged map, with its label if it has one.
///
/// Fails for a map with a repeated key.
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
