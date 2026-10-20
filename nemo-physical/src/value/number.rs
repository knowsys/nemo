//! Numbers.
//!
//! The types here are the XSD numeric datatypes
//! [`decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal),
//! its derived [`integer`](https://www.w3.org/TR/xmlschema11-2/#integer),
//! [`float`](https://www.w3.org/TR/xmlschema11-2/#float) and
//! [`double`](https://www.w3.org/TR/xmlschema11-2/#double).
//!
//! [Number] is the owned form of a number, and [NumberRef] is the borrowed view.
//! Either holds an exact number as a [Decimal] or a [DecimalRef],
//! and a floating one as a [Float] or a [Double].
//!
//! Integers and decimals are *exact*: they share one value space,
//! the integers being its integral values,
//! and their arithmetic is exact and (practically) unbounded.
//! Floats and doubles are *approximate*: each is an IEEE value space with one NaN.
//! An operation on two numbers promotes them along the ladder
//! integer, decimal, float, double to the smallest type holding both,
//! as XPath's `op:numeric-*` functions do.
//!
//! The domains are distinct, so `1`, `1.0f` and `1.0d` are three values,
//! which XSD's value comparison [value_eq](NumberRef::value_eq) finds equal.
//! `1.0` is `1`: an integral value is canonically an integer.
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
