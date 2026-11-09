//! Numbers.
//!
//! The types here are the XSD numeric datatypes
//! [`decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal),
//! its derived [`integer`](https://www.w3.org/TR/xmlschema11-2/#integer),
//! [`float`](https://www.w3.org/TR/xmlschema11-2/#float) and
//! [`double`](https://www.w3.org/TR/xmlschema11-2/#double).
//!
//! [Number] is the owned form of a number, and [NumberRef] is the borrowed view.
//! They store an exact number as a [Decimal] or a [DecimalRef],
//! and a floating-point number as a [Float] or a [Double].
//!
//! Integers and decimals are *exact*.
//! They share one value space, in which the integers are the integral values,
//! and their arithmetic is exact up to the [length limit](Decimal) of numbers.
//! Floats and doubles are *approximate*.
//! Each is an IEEE 754 value space with a single NaN.
//! An operation on two numbers first promotes both to the first type that can hold them,
//! in the order integer, decimal, float, double, as XPath's `op:numeric-*` functions do.
//!
//! The numeric domains are distinct, so `1`, `1.0f` and `1.0d` are three values.
//! XSD's value comparison, [value_eq](NumberRef::value_eq), finds them equal.
//! `1.0` is `1`, since an integral value is always an integer.
//!
//! # Example
//!
//! ```
//! use nemo_physical::value::{DataValue, NumberRef, ValueRef};
//!
//! let price = DataValue::decimal_from_lexical("1.50").unwrap();
//! let count = DataValue::decimal_from_lexical("2.0").unwrap();
//! assert_eq!(price.to_string(), "1.5");
//! assert_eq!(count, DataValue::integer(2));
//!
//! let double = DataValue::double(1.5);
//! assert_ne!(price, double);
//!
//! let (ValueRef::Number(price), ValueRef::Number(count), ValueRef::Number(double)) =
//!     (price.as_ref(), count.as_ref(), double.as_ref())
//! else {
//!     unreachable!()
//! };
//!
//! assert_eq!((price * count).unwrap().to_string(), "3");
//! assert!(price.value_eq(double));
//! ```

pub(crate) mod arithmetic;
pub(crate) mod decimal;
pub(crate) mod double;
pub(crate) mod float;
pub(crate) mod lexical;
pub(crate) mod owned;
pub(crate) mod reference;

pub use decimal::{Decimal, DecimalRef};
pub use double::Double;
pub use float::Float;
pub use owned::Number;
pub use reference::NumberRef;
