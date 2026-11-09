//! The conversion of values between the engine and Python, in both directions.
//!
//! The conversion keeps term identity: two values are equal in Python, and hash equally,
//! exactly if they are the same term for the engine.
//! The exception is Python's own equality across builtin types,
//! under which `True`, `1` and `1.0` are equal although they are different terms.
//! A value that a Python builtin type holds without loss is converted to that type.
//! Every other kind of value becomes an instance of a small wrapper class, never a `str`
//! subclass, since `Iri("a") == "a"` would make two different terms equal.

use std::sync::OnceLock;

use pyo3::{
    IntoPyObjectExt,
    exceptions::PyTypeError,
    prelude::*,
    types::{PyDict, PyString, PyTuple, PyType},
};

use nemo::values::{
    DataValue, Date, DateTime, DecimalRef, Duration, Float, MapRef, MapValue, NumberRef, Time,
    TupleRef, TupleValue, ValueRef,
};

use crate::errors::value_error;

/// The `decimal.Decimal` type, imported once.
static DECIMAL: OnceLock<Py<PyType>> = OnceLock::new();

/// The `datetime.datetime` type, imported once.
static DATETIME: OnceLock<Py<PyType>> = OnceLock::new();

/// The `datetime.date` type, imported once.
static DATE: OnceLock<Py<PyType>> = OnceLock::new();

/// The `datetime.time` type, imported once.
static TIME: OnceLock<Py<PyType>> = OnceLock::new();

/// The `datetime.timedelta` type, imported once.
static TIMEDELTA: OnceLock<Py<PyType>> = OnceLock::new();

/// The `datetime.timezone` type, imported once.
static TIMEZONE: OnceLock<Py<PyType>> = OnceLock::new();

/// The number of attoseconds in a microsecond, the precision of Python's times and
/// timedeltas.
const ATTOSECONDS_PER_MICROSECOND: i128 = 10i128.pow(12);

/// The number of attoseconds in a second.
const ATTOSECONDS_PER_SECOND: i128 = 10i128.pow(18);

/// Returns the type `name` of the standard library module `module`,
/// imported once and cached in `cell`.
fn stdlib_type<'py>(
    py: Python<'py>,
    cell: &OnceLock<Py<PyType>>,
    module: &str,
    name: &str,
) -> PyResult<Bound<'py, PyType>> {
    if let Some(cached) = cell.get() {
        return Ok(cached.bind(py).clone());
    }
    let looked = py
        .import(module)?
        .getattr(name)?
        .cast_into::<PyType>()?
        .unbind();
    Ok(cell.get_or_init(|| looked).bind(py).clone())
}

/// An IRI, which is a term different from the string with the same characters.
#[pyclass(frozen, eq, hash, str, skip_from_py_object, module = "nmo")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Iri {
    /// The IRI text, without angle brackets.
    #[pyo3(get)]
    pub value: String,
}

#[pymethods]
impl Iri {
    /// Creates an IRI from its text.
    #[new]
    fn new(value: String) -> Self {
        Self { value }
    }

    /// Returns the IRI as `Iri("…")`.
    fn __repr__(&self) -> String {
        format!("Iri({:?})", self.value)
    }
}

impl std::fmt::Display for Iri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", DataValue::iri(self.value.as_str()))
    }
}

/// A language-tagged string like `"hallo"@de`.
#[pyclass(frozen, eq, hash, str, skip_from_py_object, module = "nmo")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LanguageString {
    /// The text.
    #[pyo3(get)]
    pub text: String,
    /// The BCP 47 language tag, in lowercase and without the `@`.
    #[pyo3(get)]
    pub language: String,
}

#[pymethods]
impl LanguageString {
    /// Creates a language-tagged string.
    ///
    /// Raises `ValueError` if `language` is not a valid language tag.
    #[new]
    fn new(text: String, language: String) -> PyResult<Self> {
        // Creating the engine's value checks the tag and lowercases it.
        let value = DataValue::language_string(text.as_str(), language.as_str())
            .ok_or_else(|| value_error(format!("`{language}` is not a valid language tag")))?;
        let ValueRef::LanguageString(value) = value.as_ref() else {
            unreachable!("a language string stays one");
        };
        Ok(Self {
            text: value.text().to_owned(),
            language: value.language().to_owned(),
        })
    }

    /// Returns the string as `LanguageString("text", "language")`.
    fn __repr__(&self) -> String {
        format!("LanguageString({:?}, {:?})", self.text, self.language)
    }
}

impl std::fmt::Display for LanguageString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match DataValue::language_string(self.text.as_str(), self.language.as_str()) {
            Some(value) => write!(f, "{value}"),
            None => write!(f, "{:?}@{}", self.text, self.language),
        }
    }
}

/// A 32-bit floating-point number.
///
/// It is a term different from the 64-bit double with the same value,
/// which is converted to a plain Python `float`.
#[pyclass(name = "Float", frozen, str, skip_from_py_object, module = "nmo")]
#[derive(Debug, Clone)]
pub struct Float32 {
    /// The value.
    pub value: Float,
}

#[pymethods]
impl Float32 {
    /// Creates a 32-bit float, rounding `value` to 32 bits.
    #[new]
    fn new(value: f64) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        Self {
            value: Float::new(value as f32),
        }
    }

    /// Returns the value, widened to a Python float.
    #[getter]
    fn value(&self) -> f64 {
        self.value.to_f64()
    }

    /// Returns the number as `Float(…)`.
    fn __repr__(&self) -> String {
        format!("Float({})", self.value)
    }

    /// Returns whether `other` is a `Float` that is the same term.
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        // Equality is term identity: a NaN equals itself, -0.0 and 0.0 differ,
        // and a `Float` never equals a plain `float`, which is a double.
        other
            .cast::<Self>()
            .is_ok_and(|other| self.value == other.get().value)
    }

    /// Returns a hash of the number's bits.
    fn __hash__(&self) -> u64 {
        self.value.get().to_bits().into()
    }
}

impl std::fmt::Display for Float32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", ValueRef::Number(NumberRef::Float(self.value)))
    }
}

/// A labeled null, an anonymous value that the engine created.
///
/// It can be read from the engine but not passed to it, since no term denotes it.
/// Passing one to the engine raises `ValueError`.
#[pyclass(frozen, eq, hash, str, skip_from_py_object, module = "nmo")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Null {
    /// The null's number, which identifies it within one engine.
    #[pyo3(get)]
    pub id: u64,
}

#[pymethods]
impl Null {
    /// Creates a null with the number `id`, which is meaningful only within one engine.
    #[new]
    fn new(id: u64) -> Self {
        Self { id }
    }

    /// Returns the null as `Null(id)`.
    fn __repr__(&self) -> String {
        format!("Null({})", self.id)
    }
}

impl std::fmt::Display for Null {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "_:{}", self.id)
    }
}

/// A typed literal whose value no Python type holds, given by its lexical form and its
/// datatype IRI.
#[pyclass(frozen, eq, hash, str, skip_from_py_object, module = "nmo")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Literal {
    /// The lexical form.
    #[pyo3(get)]
    pub lexical: String,
    /// The datatype IRI.
    #[pyo3(get)]
    pub datatype: String,
}

#[pymethods]
impl Literal {
    /// Creates a typed literal.
    ///
    /// Passing it to the engine raises `ValueError` if `lexical` is not a valid lexical form
    /// of `datatype`.
    #[new]
    fn new(lexical: String, datatype: String) -> Self {
        Self { lexical, datatype }
    }

    /// Returns the literal as `Literal("lexical", "datatype")`.
    fn __repr__(&self) -> String {
        format!("Literal({:?}, {:?})", self.lexical, self.datatype)
    }
}

impl std::fmt::Display for Literal {
    /// Writes the term that the literal denotes,
    /// or the literal itself if its lexical form is not valid for its datatype.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match DataValue::typed_literal(&self.datatype, &self.lexical) {
            Some(value) => write!(f, "{value}"),
            None => {
                let lexical = self.lexical.replace('\\', "\\\\").replace('"', "\\\"");
                write!(f, "\"{lexical}\"^^<{}>", self.datatype)
            }
        }
    }
}

/// A labeled structure: a label, and the `tuple` or `dict` that it labels.
///
/// For example, `point(1, 2)` is the label `point` with a tuple,
/// and `config{port = 80}` is the label `config` with a dict.
#[pyclass(frozen, str, module = "nmo")]
#[derive(Debug)]
pub struct Labeled {
    /// The label.
    #[pyo3(get)]
    pub label: String,
    /// The labeled content: a `tuple` or a `dict`.
    #[pyo3(get)]
    pub value: Py<PyAny>,
}

#[pymethods]
impl Labeled {
    /// Creates a labeled structure of `label` and `value`.
    ///
    /// Passing it to the engine raises `TypeError` if `value` is not a `tuple`, a `list`
    /// or a `dict`.
    #[new]
    fn new(label: String, value: Py<PyAny>) -> Self {
        Self { label, value }
    }

    /// Returns the structure as `Labeled("label", value)`.
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Labeled({:?}, {})",
            self.label,
            self.value.bind(py).repr()?
        ))
    }

    /// Returns whether `other` is a `Labeled` with the same label and equal content.
    fn __eq__(&self, other: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<bool> {
        let Ok(other) = other.cast::<Self>() else {
            return Ok(false);
        };
        let other = other.get();
        Ok(self.label == other.label && self.value.bind(py).eq(other.value.bind(py))?)
    }

    /// Returns a hash of the label and the content.
    fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        // The hash starts from the content's hash, so a `Labeled` of a dict cannot be
        // hashed, just like the dict itself.
        let mut hash = self.value.bind(py).hash()?;
        for byte in self.label.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as isize);
        }
        Ok(hash)
    }
}

impl std::fmt::Display for Labeled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Python::attach(|py| match to_value(self.value.bind(py)) {
            Ok(value) => match relabel(&self.label, value) {
                Ok(labeled) => write!(f, "{labeled}"),
                Err(_) => write!(f, "{}(…)", self.label),
            },
            Err(_) => write!(f, "{}(…)", self.label),
        })
    }
}

/// Attaches a label to a converted tuple or map value.
///
/// Raises `ValueError` for a map with a repeated key, and `TypeError` for any other value.
fn relabel(label: &str, value: DataValue) -> Result<DataValue, PyErr> {
    match value.as_ref() {
        ValueRef::Tuple(tuple) => Ok(DataValue::tuple(TupleValue::labeled(
            label,
            tuple.elements().map(ValueRef::to_owned).collect::<Vec<_>>(),
        ))),
        ValueRef::Map(map) => MapValue::labeled(
            label,
            map.pairs()
                .map(|(key, value)| (key.to_owned(), value.to_owned())),
        )
        .map(DataValue::map)
        .ok_or_else(|| value_error("duplicate key in map")),
        _ => Err(PyTypeError::new_err(
            "`Labeled` takes a tuple, a list, or a dict",
        )),
    }
}

/// Returns `value` as a [Literal] with the lexical form `lexical`,
/// for a value that its usual Python type cannot hold.
fn typed_literal(py: Python<'_>, value: ValueRef<'_>, lexical: String) -> PyResult<Py<PyAny>> {
    let datatype = value
        .datatype_iri()
        .expect("a value of a native datatype has one");
    Literal {
        lexical,
        datatype: datatype.to_owned(),
    }
    .into_py_any(py)
}

/// Converts an engine value into its Python form.
///
/// Raises `ValueError` for a map whose keys convert to the same dict key.
pub(crate) fn to_python(py: Python<'_>, value: ValueRef<'_>) -> PyResult<Py<PyAny>> {
    match value {
        ValueRef::Iri(iri) => Iri {
            value: iri.to_string(),
        }
        .into_py_any(py),
        ValueRef::String(text) => text.into_py_any(py),
        ValueRef::LanguageString(value) => LanguageString {
            text: value.text().to_owned(),
            language: value.language().to_owned(),
        }
        .into_py_any(py),
        ValueRef::Boolean(value) => value.into_py_any(py),
        ValueRef::Number(NumberRef::Float(value)) => Float32 { value }.into_py_any(py),
        ValueRef::Number(NumberRef::Double(value)) => value.get().into_py_any(py),
        ValueRef::Number(NumberRef::Decimal(value)) => decimal_to_python(py, value),
        ValueRef::DateTime(value) => date_time_to_python(py, value),
        ValueRef::Date(date) => date_to_python(py, date),
        ValueRef::Time(time) => time_to_python(py, time),
        ValueRef::Duration(duration) => duration_to_python(py, duration),
        ValueRef::Null(_) => Null {
            id: value.null_number().unwrap_or_default(),
        }
        .into_py_any(py),
        ValueRef::Tuple(tuple) => tuple_to_python(py, tuple),
        ValueRef::Map(map) => map_to_python(py, map),
        ValueRef::Other(literal) => Literal {
            lexical: literal.lexical().to_owned(),
            datatype: literal.datatype().to_owned(),
        }
        .into_py_any(py),
    }
}

/// Converts a decimal into an `int` if it is integral, and a `decimal.Decimal` otherwise.
fn decimal_to_python(py: Python<'_>, value: DecimalRef<'_>) -> PyResult<Py<PyAny>> {
    match value.to_i64() {
        Some(integer) => integer.into_py_any(py),
        // Any other integer becomes an `int` too, made from its canonical lexical form,
        // which keeps its exact value.
        None if value.is_integral() => {
            let int = py.import("builtins")?.getattr("int")?;
            Ok(int.call1((value.to_string(),))?.unbind())
        }
        None => {
            let decimal = stdlib_type(py, &DECIMAL, "decimal", "Decimal")?;
            Ok(decimal.call1((value.to_string(),))?.unbind())
        }
    }
}

/// Converts a dateTime into a `datetime.datetime`.
///
/// `fromisoformat` covers years 1–9999 at microsecond precision,
/// and a dateTime outside that comes back as its typed literal instead.
fn date_time_to_python(py: Python<'_>, value: DateTime) -> PyResult<Py<PyAny>> {
    let lexical = value.to_string();
    let datetime = stdlib_type(py, &DATETIME, "datetime", "datetime")?;
    let parsed = datetime.call_method1("fromisoformat", (lexical.replace('Z', "+00:00"),));
    match parsed {
        Ok(datetime) => Ok(datetime.unbind()),
        Err(_) => typed_literal(py, ValueRef::DateTime(value), lexical),
    }
}

/// Converts a date into a `datetime.date`.
///
/// That type has no time zone and holds the years 1 to 9999,
/// so a date with a time zone offset or in another year becomes a [Literal].
fn date_to_python(py: Python<'_>, date: Date) -> PyResult<Py<PyAny>> {
    if date.offset().is_some() || !(1..=9999).contains(&date.year()) {
        return typed_literal(py, ValueRef::Date(date), date.to_string());
    }
    let class = stdlib_type(py, &DATE, "datetime", "date")?;
    Ok(class
        .call1((date.year(), date.month(), date.day()))?
        .unbind())
}

/// Converts a time into a `datetime.time`, with a time zone if the time has an offset.
///
/// That type holds microseconds, so a finer time becomes a [Literal].
fn time_to_python(py: Python<'_>, time: Time) -> PyResult<Py<PyAny>> {
    let attosecond = i128::from(time.attosecond());
    if attosecond % ATTOSECONDS_PER_MICROSECOND != 0 {
        return typed_literal(py, ValueRef::Time(time), time.to_string());
    }
    let tzinfo = match time.offset() {
        Some(offset) => {
            let timedelta = stdlib_type(py, &TIMEDELTA, "datetime", "timedelta")?;
            let timezone = stdlib_type(py, &TIMEZONE, "datetime", "timezone")?;
            let shift = timedelta.call1((0, i32::from(offset.minutes()) * 60))?;
            timezone.call1((shift,))?.unbind()
        }
        None => py.None(),
    };
    let class = stdlib_type(py, &TIME, "datetime", "time")?;
    let microsecond = attosecond / ATTOSECONDS_PER_MICROSECOND;
    Ok(class
        .call1((
            time.hour(),
            time.minute(),
            time.second(),
            i64::try_from(microsecond).expect("below a million"),
            tzinfo,
        ))?
        .unbind())
}

/// Converts a duration into a `datetime.timedelta`.
///
/// That type holds a duration without months, to the microsecond and within a billion days.
/// Any other duration becomes a [Literal].
fn duration_to_python(py: Python<'_>, duration: Duration) -> PyResult<Py<PyAny>> {
    let attoseconds = duration.total_attoseconds();
    let microseconds = (duration.is_day_time() && attoseconds % ATTOSECONDS_PER_MICROSECOND == 0)
        .then(|| i64::try_from(attoseconds / ATTOSECONDS_PER_MICROSECOND).ok())
        .flatten();
    let Some(microseconds) = microseconds else {
        return typed_literal(py, ValueRef::Duration(duration), duration.to_string());
    };
    let timedelta = stdlib_type(py, &TIMEDELTA, "datetime", "timedelta")?;
    match timedelta.call1((0, 0, microseconds)) {
        Ok(timedelta) => Ok(timedelta.unbind()),
        Err(_) => typed_literal(py, ValueRef::Duration(duration), duration.to_string()),
    }
}

/// Converts a tuple into a Python `tuple`, wrapped as [Labeled] if it has a label.
fn tuple_to_python(py: Python<'_>, tuple: TupleRef<'_>) -> PyResult<Py<PyAny>> {
    let elements: Vec<Py<PyAny>> = tuple
        .elements()
        .map(|element| to_python(py, element))
        .collect::<PyResult<_>>()?;
    let content = PyTuple::new(py, elements)?;
    match tuple.label() {
        None => Ok(content.unbind().into_any()),
        Some(label) => Labeled {
            label: label.to_owned(),
            value: content.unbind().into_any(),
        }
        .into_py_any(py),
    }
}

/// Converts a map into a Python `dict`, wrapped as [Labeled] if it has a label.
///
/// Two different terms can convert to equal dict keys, such as `1` and `true`,
/// or `1` and `1.0d`.
/// A map with both cannot be converted without losing a pair, so it raises `ValueError`.
fn map_to_python(py: Python<'_>, map: MapRef<'_>) -> PyResult<Py<PyAny>> {
    let content = PyDict::new(py);
    for (index, (key, value)) in map.pairs().enumerate() {
        let converted = to_python(py, key)?;
        if content.contains(&converted)? {
            let earlier = collided(py, map.pairs().take(index), converted.bind(py))?;
            return Err(value_error(format!(
                "map keys `{earlier}` and `{key}` convert to the same dict key"
            )));
        }
        content.set_item(converted, to_python(py, value)?)?;
    }
    match map.label() {
        None => Ok(content.unbind().into_any()),
        Some(label) => Labeled {
            label: label.to_owned(),
            value: content.unbind().into_any(),
        }
        .into_py_any(py),
    }
}

/// Returns the key among `pairs` whose Python form equals `converted`,
/// which is the other key of a collision, for the error message.
fn collided<'a>(
    py: Python<'_>,
    pairs: impl Iterator<Item = (ValueRef<'a>, ValueRef<'a>)>,
    converted: &Bound<'_, PyAny>,
) -> PyResult<ValueRef<'a>> {
    for (earlier, _) in pairs {
        if to_python(py, earlier)?.bind(py).eq(converted)? {
            return Ok(earlier);
        }
    }
    Err(value_error("two map keys convert to the same dict key"))
}

/// One step of [to_value], which converts some kinds of Python values and returns `None`
/// for the others.
type Step = fn(&Bound<'_, PyAny>) -> PyResult<Option<DataValue>>;

/// Converts a Python value into an engine value.
///
/// Raises `TypeError` for a value of a type that does not convert,
/// and `ValueError` for a value that the conversion of its type rejects.
pub(crate) fn to_value(any: &Bound<'_, PyAny>) -> PyResult<DataValue> {
    // The wrapper classes come first, since each of them converts in exactly one way.
    let steps: [Step; 4] = [
        wrapper_to_value,
        builtin_to_value,
        standard_to_value,
        container_to_value,
    ];
    for step in steps {
        if let Some(value) = step(any)? {
            return Ok(value);
        }
    }
    Err(PyTypeError::new_err(format!(
        "`{}` values do not convert to Nemo terms",
        any.get_type().name()?
    )))
}

/// Converts an instance of one of the module's wrapper classes,
/// or returns `None` for any other value.
///
/// A [Null] raises `ValueError`, since no term denotes it.
fn wrapper_to_value(any: &Bound<'_, PyAny>) -> PyResult<Option<DataValue>> {
    if let Ok(iri) = any.cast::<Iri>() {
        return Ok(Some(DataValue::iri(iri.get().value.as_str())));
    }
    if let Ok(tagged) = any.cast::<LanguageString>() {
        let tagged = tagged.get();
        return DataValue::language_string(tagged.text.as_str(), tagged.language.as_str())
            .map(Some)
            .ok_or_else(|| {
                value_error(format!("`{}` is not a valid language tag", tagged.language))
            });
    }
    if let Ok(float) = any.cast::<Float32>() {
        return Ok(Some(DataValue::float(float.get().value)));
    }
    if let Ok(literal) = any.cast::<Literal>() {
        let literal = literal.get();
        // A literal of a datatype with native values becomes its canonical value,
        // so `Literal("42", …integer)` and `42` are the same term.
        return DataValue::typed_literal(&literal.datatype, &literal.lexical)
            .map(Some)
            .ok_or_else(|| {
                value_error(format!(
                    "`{}` is not a valid `{}` lexical",
                    literal.lexical, literal.datatype
                ))
            });
    }
    if let Ok(labeled) = any.cast::<Labeled>() {
        let labeled = labeled.get();
        return relabel(&labeled.label, to_value(labeled.value.bind(any.py()))?).map(Some);
    }
    if any.cast::<Null>().is_ok() {
        return Err(value_error(
            "nulls cannot be passed to the engine, since no term denotes them",
        ));
    }
    Ok(None)
}

/// Converts a value of a builtin scalar type (`bool`, `int`, `str`, `float`),
/// or returns `None` for any other value.
///
/// The types are checked exactly, so an instance of a subclass is not converted here.
fn builtin_to_value(any: &Bound<'_, PyAny>) -> PyResult<Option<DataValue>> {
    // `bool` is a subclass of `int`, but the exact checks keep a `bool` from converting as
    // an `int`.
    if let Ok(value) = any.cast_exact::<pyo3::types::PyBool>() {
        return Ok(Some(DataValue::boolean(value.is_true())));
    }
    if any.cast_exact::<pyo3::types::PyInt>().is_ok() {
        if let Ok(value) = any.extract::<i64>() {
            return Ok(Some(DataValue::integer(value)));
        }
        // An integer beyond 64 bits is converted through its decimal text.
        let lexical = any.str()?.to_string();
        return DataValue::decimal_from_lexical(&lexical)
            .map(Some)
            .ok_or_else(|| value_error(format!("`{lexical}` is not a representable integer")));
    }
    if let Ok(value) = any.cast_exact::<PyString>() {
        return Ok(Some(DataValue::string(value.extract::<String>()?.as_str())));
    }
    if let Ok(value) = any.cast_exact::<pyo3::types::PyFloat>() {
        return Ok(Some(DataValue::double(value.value())));
    }
    Ok(None)
}

/// Converts a value of a standard library type, a `decimal.Decimal` or a type of the
/// `datetime` module, or returns `None` for any other value.
fn standard_to_value(any: &Bound<'_, PyAny>) -> PyResult<Option<DataValue>> {
    let py = any.py();
    if any.is_instance(stdlib_type(py, &DECIMAL, "decimal", "Decimal")?.as_any())? {
        // The decimal is written in plain notation, not in scientific notation, and then
        // parsed.
        let lexical: String = any.call_method1("__format__", ("f",))?.extract()?;
        return DataValue::decimal_from_lexical(&lexical)
            .map(Some)
            .ok_or_else(|| value_error(format!("`{lexical}` is not a representable decimal")));
    }
    if any.is_instance(stdlib_type(py, &DATETIME, "datetime", "datetime")?.as_any())? {
        let lexical: String = any.call_method0("isoformat")?.extract()?;
        return DataValue::datetime_from_lexical(&lexical)
            .map(Some)
            .ok_or_else(|| value_error(format!("`{lexical}` is not an xsd:dateTime lexical")));
    }
    // This check comes after `datetime.datetime`, which is a subclass of `datetime.date`.
    if any.is_instance(stdlib_type(py, &DATE, "datetime", "date")?.as_any())? {
        let lexical: String = any.call_method0("isoformat")?.extract()?;
        return Date::from_lexical(&lexical)
            .map(|date| Some(DataValue::date(date)))
            .ok_or_else(|| value_error(format!("`{lexical}` is not an xsd:date lexical")));
    }
    if any.is_instance(stdlib_type(py, &TIME, "datetime", "time")?.as_any())? {
        let lexical: String = any.call_method0("isoformat")?.extract()?;
        return Time::from_lexical(&lexical)
            .map(|time| Some(DataValue::time(time)))
            .ok_or_else(|| value_error(format!("`{lexical}` is not an xsd:time lexical")));
    }
    if any.is_instance(stdlib_type(py, &TIMEDELTA, "datetime", "timedelta")?.as_any())? {
        let days: i64 = any.getattr("days")?.extract()?;
        let seconds: i64 = any.getattr("seconds")?.extract()?;
        let microseconds: i64 = any.getattr("microseconds")?.extract()?;
        let attoseconds = (i128::from(days) * 86_400 + i128::from(seconds))
            * ATTOSECONDS_PER_SECOND
            + i128::from(microseconds) * ATTOSECONDS_PER_MICROSECOND;
        let duration =
            Duration::new(0, attoseconds).expect("a duration without months has one sign");
        return Ok(Some(DataValue::duration(duration)));
    }
    Ok(None)
}

/// Converts a container, a `tuple`, `list` or `dict`, with its contents converted
/// recursively, or returns `None` for any other value.
///
/// A `tuple` and a `list` both become an unlabeled tuple.
/// A `dict` whose keys convert to the same term raises `ValueError`.
fn container_to_value(any: &Bound<'_, PyAny>) -> PyResult<Option<DataValue>> {
    if let Ok(value) = any.cast::<PyTuple>() {
        let elements: Vec<DataValue> = value
            .iter()
            .map(|item| to_value(&item))
            .collect::<PyResult<_>>()?;
        return Ok(Some(DataValue::tuple(TupleValue::new(elements))));
    }
    if let Ok(value) = any.cast::<pyo3::types::PyList>() {
        let elements: Vec<DataValue> = value
            .iter()
            .map(|item| to_value(&item))
            .collect::<PyResult<_>>()?;
        return Ok(Some(DataValue::tuple(TupleValue::new(elements))));
    }
    if let Ok(value) = any.cast::<PyDict>() {
        let pairs: Vec<(DataValue, DataValue)> = value
            .iter()
            .map(|(key, value)| Ok((to_value(&key)?, to_value(&value)?)))
            .collect::<PyResult<_>>()?;
        return MapValue::new(pairs)
            .map(|map| Some(DataValue::map(map)))
            .ok_or_else(|| value_error("two dict keys convert to the same term"));
    }
    Ok(None)
}
