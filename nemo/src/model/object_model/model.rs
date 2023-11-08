use nemo_physical::datavalues::{DataValue, Long, ValueDomain};

#[derive(Debug)]
pub enum PrimitiveConstant {
    String(String),
    Long(Long),
    // ...
}

impl DataValue for PrimitiveConstant {
    fn datatype_iri(&self) -> String {
        match self {
            PrimitiveConstant::String(_) => todo!(),
            PrimitiveConstant::Long(_) => todo!(),
        }
    }

    fn lexical_value(&self) -> String {
        todo!()
    }

    fn value_domain(&self) -> ValueDomain {
        todo!()
    }
}
#[derive(Debug)]
pub struct Tag(String);

#[derive(Debug)]
pub struct TupleConstant {
    fields: Vec<Constant>,
    tag: Option<Tag>,
}

impl DataValue for TupleConstant {
    fn datatype_iri(&self) -> String {
        String::from("https://www.nemo.sh/schema#tuple")
    }

    fn lexical_value(&self) -> String {
        todo!()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Tuple
    }

    fn tuple_element_unchecked(&self, index: usize) -> &dyn DataValue {
        &self.fields[index]
    }
}

#[derive(Debug)]
pub enum Constant {
    Primitive(PrimitiveConstant),
    Tuple(TupleConstant),
}

impl DataValue for Constant {
    fn datatype_iri(&self) -> String {
        match self {
            Constant::Primitive(_) => todo!(),
            Constant::Tuple(_) => todo!(),
        }
    }

    fn lexical_value(&self) -> String {
        todo!()
    }

    fn value_domain(&self) -> ValueDomain {
        todo!()
    }
}

#[derive(Debug)]
pub enum Variable {
    Universal(String),
    Existential(String),
}

#[derive(Debug)]
pub struct TuplePattern {
    fields: Vec<Pattern>,
    tag: Option<Tag>,
}

#[derive(Debug, Copy, Clone)]
pub enum UnaryNumericOperation {
    AbsoluteValue,
    Squareroot,
    Inverse,
}

#[derive(Debug)]
pub struct UnaryNumericExpression {
    operation: UnaryNumericOperation,
    input: Box<Pattern>,
}

#[derive(Debug, Copy, Clone)]
pub enum BinayNumericOperation {
    Addition,
    Subtraction,
    Multiplication,
    Division,
    Exponentiation,
}

#[derive(Debug)]
pub struct BinaryNumericExpression {
    operation: BinayNumericOperation,
    left: Box<Pattern>,
    right: Box<Pattern>,
}

#[derive(Debug)]
pub enum NumericExpression {
    Unary(UnaryNumericExpression),
    Binary(BinaryNumericExpression),
}

#[derive(Debug, Copy, Clone)]
pub enum StringOperation {
    Concatanate,
    Prefix,
    Suffix,
    Contains,
}

#[derive(Debug)]
pub struct StringExpression {
    operation: StringOperation,
    base: Box<Pattern>,
    other: Box<Pattern>,
}

#[derive(Debug, Copy, Clone)]
pub enum AggregationOpeation {
    Max,
    Min,
    Count,
    Sum,
}

#[derive(Debug)]
pub struct Aggregation {
    operation: AggregationOpeation,
    inputs: Vec<Pattern>,
}

#[derive(Debug)]
pub enum Operation {
    Numeric(NumericExpression),
    String(StringExpression),
}

#[derive(Debug)]
pub enum Pattern {
    Constant(Constant),
    Variable(Variable),
    Tuple(TuplePattern),
    Operation(Operation),
    Aggregation(Aggregation),
}

#[derive(Debug)]
pub struct Rule {
    body: Vec<Pattern>,
    head: Vec<Pattern>,
}
