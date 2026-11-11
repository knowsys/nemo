//! Stack programs and their interpreter.
//!
//! The module also defines the [values](StackValue) on the stack,
//! the [context](EvaluationContext) that a program is evaluated in,
//! and the [bindings](StackProgram::binding) that a program can express.

mod binding;
mod context;
mod value;

pub use self::{context::EvaluationContext, value::StackValue};

use crate::{
    function::{
        IdentityCondition, IntegerProgram, OpCode, Outcome,
        infer::{self, Inferred},
        narrow,
        operation::{
            boolean, compare, generic, hashing, numeric, pattern, random, string, structure,
            temporal::{self, Component},
        },
    },
    value::{DataValue, EvaluationError, LabelTypes, Type, ValueRef},
};

/// A compiled expression, a flat sequence of [instructions](OpCode) that work on a stack of values.
///
/// A program reads one row of input values and its own constants, and leaves one result.
/// [new](Self::new) checks that the instructions use the stack correctly
/// and refer only to constants and inputs that exist, so that evaluation needs no checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackProgram {
    /// The instructions, in evaluation order.
    operations: Vec<OpCode>,
    /// The constants that the [Constant](OpCode::Constant) instructions refer to.
    constants: Vec<DataValue>,
    /// The number of inputs that the program reads from a row.
    inputs: usize,
    /// The largest stack depth that an evaluation reaches.
    stack_bound: usize,
}

impl StackProgram {
    /// The largest number of constants, and of inputs, that one program can refer to.
    ///
    /// An instruction refers to a constant or an input by a 16-bit index.
    pub const OPERAND_LIMIT: usize = u16::MAX as usize + 1;

    /// Creates a program from its instructions, its constants and its number of inputs.
    ///
    /// # Panics
    ///
    /// Panics if an instruction pops more values than the stack has,
    /// if the instructions do not leave exactly one result,
    /// or if they refer to a constant or an input that does not exist.
    pub fn new(operations: Vec<OpCode>, constants: Vec<DataValue>, inputs: usize) -> Self {
        let mut depth = 0usize;
        let mut stack_bound = 0;
        for &operation in &operations {
            match operation {
                OpCode::Constant(index) => assert!(
                    usize::from(index) < constants.len(),
                    "a stack program references a constant it does not have"
                ),
                OpCode::Input(index) => assert!(
                    usize::from(index) < inputs,
                    "a stack program references an input beyond its arity"
                ),
                _ => {}
            }

            let pops = operation.pops();
            assert!(depth >= pops, "a stack program underflows its stack");
            depth = depth - pops + 1;
            stack_bound = stack_bound.max(depth);
        }
        assert_eq!(depth, 1, "a stack program must leave exactly one result");

        Self {
            operations,
            constants,
            inputs,
            stack_bound,
        }
    }

    /// Returns the number of inputs that the program reads from a row.
    pub fn inputs(&self) -> usize {
        self.inputs
    }

    /// Returns whether the program's result is not determined by its inputs.
    ///
    /// This is the case if the program draws a random number or a UUID,
    /// or reads the instant of its [context](EvaluationContext).
    /// The results of such a program can neither be memoized nor folded into constants.
    pub fn is_nondeterministic(&self) -> bool {
        self.operations
            .iter()
            .any(|operation| operation.is_nondeterministic())
    }

    /// Returns the largest stack depth that an evaluation reaches.
    ///
    /// A stack of this capacity is never reallocated by [evaluate](Self::evaluate).
    pub fn stack_bound(&self) -> usize {
        self.stack_bound
    }

    /// Returns the constant with the given index, as a stack value that borrows it.
    fn constant(&self, index: u16) -> StackValue<'_> {
        self.constants[usize::from(index)].as_ref().into()
    }

    /// Evaluates the program on one row of inputs, and returns its result.
    ///
    /// `inputs` must have as many values as [inputs](Self::inputs) says.
    /// `context` supplies the instant that [Now](OpCode::Now) returns.
    /// `stack` is cleared first, so that one stack can serve many evaluations.
    ///
    /// The result is an [error value](StackValue::Error) if the evaluation fails.
    ///
    /// In debug builds, the result of every instruction is checked against what
    /// [inference](Self::infer) predicts from its operands, and a mismatch panics.
    pub fn evaluate<'a>(
        &'a self,
        context: EvaluationContext,
        stack: &mut Vec<StackValue<'a>>,
        inputs: &[ValueRef<'a>],
    ) -> StackValue<'a> {
        debug_assert_eq!(inputs.len(), self.inputs, "the row must match the program");
        stack.clear();

        // One flat match over the instruction set, so that the loop branches once per instruction.
        // Each arm names the function of an operation family,
        // and the helpers (`unary`, `binary`, `fold`, ...) pop the operands and push the result.
        for &operation in &self.operations {
            #[cfg(debug_assertions)]
            let promise = infer::Promise::of(operation, stack);

            match operation {
                OpCode::Constant(index) => stack.push(self.constant(index)),
                OpCode::Input(index) => stack.push(inputs[usize::from(index)].into()),

                OpCode::Same => binary(stack, compare::same),
                OpCode::Equal => binary(stack, compare::equal),
                OpCode::Less => binary(stack, compare::less),
                OpCode::LessOrEqual => binary(stack, compare::less_or_equal),
                OpCode::Greater => binary(stack, compare::greater),
                OpCode::GreaterOrEqual => binary(stack, compare::greater_or_equal),
                OpCode::Compare => binary(stack, compare::signum),
                OpCode::Maximum(count) => fold(stack, count, compare::maximum),
                OpCode::Minimum(count) => fold(stack, count, compare::minimum),

                OpCode::Add => binary(stack, numeric::add),
                OpCode::Subtract => binary(stack, numeric::subtract),
                OpCode::Multiply => binary(stack, numeric::multiply),
                OpCode::Divide => binary(stack, numeric::divide),
                OpCode::Negate => unary(stack, numeric::negate),
                OpCode::Absolute => unary(stack, numeric::absolute),
                OpCode::Round => unary(stack, numeric::round),
                OpCode::Ceiling => unary(stack, numeric::ceiling),
                OpCode::Floor => unary(stack, numeric::floor),
                OpCode::Power => binary(stack, numeric::power),
                OpCode::Logarithm => binary(stack, numeric::logarithm),
                OpCode::Sine => unary(stack, numeric::sine),
                OpCode::Cosine => unary(stack, numeric::cosine),
                OpCode::Tangent => unary(stack, numeric::tangent),
                OpCode::SquareRoot => unary(stack, numeric::square_root),
                OpCode::Remainder => binary(stack, numeric::remainder),
                OpCode::ShiftLeft => binary(stack, numeric::shift_left),
                OpCode::ShiftRight => binary(stack, numeric::shift_right),
                OpCode::BitAnd(count) => fold(stack, count, numeric::bit_and),
                OpCode::BitOr(count) => fold(stack, count, numeric::bit_or),
                OpCode::BitXor(count) => fold(stack, count, numeric::bit_xor),
                OpCode::Lukasiewicz(count) => fold(stack, count, numeric::lukasiewicz),

                OpCode::And => connective(stack, boolean::and),
                OpCode::Or => connective(stack, boolean::or),
                OpCode::Not => unary(stack, boolean::not),

                OpCode::InDomains(domains) => {
                    unary(stack, |value| generic::in_domains(value, domains))
                }
                OpCode::Datatype => unary(stack, generic::datatype),
                OpCode::TermSyntax => unary(stack, generic::term_syntax),
                OpCode::Lexical => unary(stack, generic::lexical),
                OpCode::Language => unary(stack, generic::language),
                OpCode::StringLanguage => binary(stack, generic::string_language),
                OpCode::LanguageMatches => binary(stack, generic::language_matches),
                OpCode::TypedLiteral => binary(stack, generic::typed_literal),
                OpCode::CastInteger => unary(stack, generic::cast_integer),
                OpCode::CastFloat => unary(stack, generic::cast_float),
                OpCode::CastDouble => unary(stack, generic::cast_double),
                OpCode::CastIri => unary(stack, generic::cast_iri),
                OpCode::CastDateTime => unary(stack, generic::cast_datetime),
                OpCode::CastDate => unary(stack, generic::cast_date),
                OpCode::CastTime => unary(stack, generic::cast_time),
                OpCode::CastDuration => unary(stack, generic::cast_duration),
                OpCode::CastDayTimeDuration => unary(stack, generic::cast_day_time_duration),
                OpCode::CastYearMonthDuration => unary(stack, generic::cast_year_month_duration),

                OpCode::StringLength => unary(stack, string::length),
                OpCode::UpperCase => unary(stack, string::upper_case),
                OpCode::LowerCase => unary(stack, string::lower_case),
                OpCode::Reverse => unary(stack, string::reverse),
                OpCode::Trim => unary(stack, |value| string::trim(value, str::trim)),
                OpCode::TrimStart => unary(stack, |value| string::trim(value, str::trim_start)),
                OpCode::TrimEnd => unary(stack, |value| string::trim(value, str::trim_end)),
                OpCode::Contains => binary(stack, string::contains),
                OpCode::StartsWith => binary(stack, string::starts_with),
                OpCode::EndsWith => binary(stack, string::ends_with),
                OpCode::StringBefore => binary(stack, string::before),
                OpCode::StringAfter => binary(stack, string::after),
                OpCode::Substring => binary(stack, string::substring),
                OpCode::SubstringWith => ternary(stack, string::substring_with),
                OpCode::Concat(count) => fold(stack, count, string::concat),
                OpCode::Levenshtein => binary(stack, string::levenshtein),
                OpCode::UriEncode => unary(stack, string::uri_encode),
                OpCode::UriDecode => unary(stack, string::uri_decode),
                OpCode::Matches { flags } => regex(stack, flags, false),
                OpCode::Replace { flags } => regex(stack, flags, true),
                OpCode::Md5 => unary(stack, hashing::digest::<hashing::Md5>),
                OpCode::Sha1 => unary(stack, hashing::digest::<hashing::Sha1>),
                OpCode::Sha256 => unary(stack, hashing::digest::<hashing::Sha256>),
                OpCode::Sha384 => unary(stack, hashing::digest::<hashing::Sha384>),
                OpCode::Sha512 => unary(stack, hashing::digest::<hashing::Sha512>),

                OpCode::Tuple(count) => {
                    fold(stack, count, |elements| structure::tuple(None, elements))
                }
                OpCode::LabeledTuple(count) => labeled(stack, count, structure::tuple),
                OpCode::Map(count) => {
                    fold(stack, 2 * usize::from(count), |pairs| {
                        structure::map(None, pairs)
                    });
                }
                OpCode::LabeledMap(count) => labeled(stack, 2 * usize::from(count), structure::map),
                OpCode::Get => binary_borrowed(stack, structure::get),
                OpCode::Length => unary(stack, structure::length),
                OpCode::Label => unary_borrowed(stack, structure::label),
                OpCode::Keys => unary(stack, structure::keys),
                OpCode::HasKey => binary(stack, structure::has_key),
                OpCode::JoinStrings => binary(stack, structure::join_strings),
                OpCode::SplitString => binary(stack, structure::split_string),

                OpCode::Year => unary(stack, |value| temporal::component(value, Component::Year)),
                OpCode::Month => unary(stack, |value| temporal::component(value, Component::Month)),
                OpCode::Day => unary(stack, |value| temporal::component(value, Component::Day)),
                OpCode::Hours => unary(stack, |value| temporal::component(value, Component::Hours)),
                OpCode::Minutes => unary(stack, |value| {
                    temporal::component(value, Component::Minutes)
                }),
                OpCode::Seconds => unary(stack, |value| {
                    temporal::component(value, Component::Seconds)
                }),
                OpCode::TimezoneString => unary(stack, temporal::timezone_string),
                OpCode::TimezoneDuration => unary(stack, temporal::timezone_duration),
                OpCode::Adjust => binary(stack, temporal::adjust),

                OpCode::Random => stack.push(random::random()),
                OpCode::UuidIri => stack.push(random::uuid_iri()),
                OpCode::UuidString => stack.push(random::uuid_string()),
                OpCode::Now => stack.push(DataValue::datetime(context.instant()).into()),
            }

            #[cfg(debug_assertions)]
            promise.check(stack);
        }

        stack.pop().expect("a validated program leaves one result")
    }

    /// Evaluates the program as a condition,
    /// and returns the [effective boolean value](ValueRef::effective_boolean_value) of its result.
    ///
    /// The parameters are those of [evaluate](Self::evaluate).
    ///
    /// # Errors
    ///
    /// Returns the error of the evaluation, or a [TypeMismatch](EvaluationError::TypeMismatch)
    /// if the result has no effective boolean value.
    pub fn filter<'a>(
        &'a self,
        context: EvaluationContext,
        stack: &mut Vec<StackValue<'a>>,
        inputs: &[ValueRef<'a>],
    ) -> Result<bool, EvaluationError> {
        let result = self.evaluate(context, stack, inputs);
        result.as_result()?.effective_boolean_value()
    }

    /// Evaluates the program on types instead of values,
    /// and returns what is known of the result of a successful evaluation.
    ///
    /// `inputs` gives the [Type] of each input, and an input beyond the slice may have any value.
    /// `labels` gives the types declared for the labels that these types refer to.
    ///
    /// The result is the exact value if the constants and the input types determine it,
    /// and a bound otherwise.
    /// It over-approximates what [evaluate](Self::evaluate) can produce,
    /// so an empty bound proves that the program fails on every row,
    /// and an exact `false` proves that no row satisfies it as a condition.
    pub fn infer(&self, inputs: &[Type], labels: &LabelTypes) -> Inferred {
        let mut stack: Vec<Inferred> = Vec::with_capacity(self.stack_bound);

        for &operation in &self.operations {
            let result = match operation {
                OpCode::Constant(index) => {
                    Inferred::Exact(self.constants[usize::from(index)].clone())
                }
                OpCode::Input(index) => {
                    Inferred::Bound(inputs.get(usize::from(index)).cloned().unwrap_or(Type::ANY))
                }
                other => {
                    // The program is validated, so the stack has the operands.
                    let arguments = stack.split_off(stack.len() - other.pops());
                    infer::transfer(other, &arguments, labels)
                }
            };
            stack.push(result);
        }

        stack.pop().expect("a validated program leaves one result")
    }

    /// Returns the types of the inputs narrowed to the values for which the program can yield
    /// `outcome`.
    ///
    /// `inputs` and `labels` are as for [infer](Self::infer),
    /// and the result has one type for each type in `inputs`.
    ///
    /// The result over-approximates like the one of [infer](Self::infer),
    /// so an input narrowed to the empty type proves that no row yields the outcome.
    pub fn narrow(&self, inputs: &[Type], labels: &LabelTypes, outcome: &Outcome) -> Vec<Type> {
        narrow::narrow(&self.operations, &self.constants, inputs, labels, outcome)
    }

    /// Compiles the [integer form](IntegerProgram) of this program.
    ///
    /// Returns `None` if an instruction or a constant lies outside the integer fragment.
    pub(super) fn integer_specialization(&self) -> Option<IntegerProgram> {
        IntegerProgram::compile(&self.operations, &self.constants, self.inputs)
    }

    /// Recognizes the [identity form](IdentityCondition) of this program.
    ///
    /// Returns `None` if the program does more than compare two terms for identity.
    pub(super) fn identity_specialization(&self) -> Option<IdentityCondition> {
        IdentityCondition::recognize(&self.operations, &self.constants)
    }
}

/// Pops one operand and pushes the result of the operation on it.
///
/// If the operand is an error, that error is the result.
fn unary<'a>(
    stack: &mut Vec<StackValue<'a>>,
    operation: impl FnOnce(ValueRef<'_>) -> StackValue<'static>,
) {
    let value = stack.pop().expect("the program is validated");
    let result = match value.as_result() {
        Ok(value) => operation(value),
        Err(error) => error.into(),
    };

    stack.push(result);
}

/// Pops two operands and pushes the result of the operation on them.
///
/// If an operand is an error, that error is the result, the left operand's if both are errors.
fn binary<'a>(
    stack: &mut Vec<StackValue<'a>>,
    operation: impl FnOnce(ValueRef<'_>, ValueRef<'_>) -> StackValue<'static>,
) {
    let right = stack.pop().expect("the program is validated");
    let left = stack.pop().expect("the program is validated");

    let result = match (left.as_result(), right.as_result()) {
        (Err(error), _) | (_, Err(error)) => error.into(),
        (Ok(left), Ok(right)) => operation(left, right),
    };

    stack.push(result);
}

/// Pops three operands and pushes the result of the operation on them.
///
/// If operands are errors, the error of the first of them is the result.
fn ternary<'a>(
    stack: &mut Vec<StackValue<'a>>,
    operation: impl FnOnce(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>) -> StackValue<'static>,
) {
    let third = stack.pop().expect("the program is validated");
    let second = stack.pop().expect("the program is validated");
    let first = stack.pop().expect("the program is validated");

    let result = match (first.as_result(), second.as_result(), third.as_result()) {
        (Ok(first), Ok(second), Ok(third)) => operation(first, second, third),
        (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => error.into(),
    };

    stack.push(result);
}

/// Pops two operands and pushes the result of a connective on them.
///
/// The connective receives the operands as they are, errors included,
/// since three-valued logic can decide a result despite an error.
fn connective<'a>(
    stack: &mut Vec<StackValue<'a>>,
    operation: impl FnOnce(&StackValue<'a>, &StackValue<'a>) -> StackValue<'static>,
) {
    let right = stack.pop().expect("the program is validated");
    let left = stack.pop().expect("the program is validated");

    stack.push(operation(&left, &right));
}

/// Pops `operands` values and pushes the result of the operation on them,
/// for the variadic instructions.
///
/// The operation receives the operands as a slice, errors included, and propagates errors itself.
fn fold<'a>(
    stack: &mut Vec<StackValue<'a>>,
    operands: impl Into<usize>,
    operation: impl FnOnce(&[StackValue<'a>]) -> StackValue<'static>,
) {
    let start = stack.len() - operands.into();
    let result = operation(&stack[start..]);

    stack.truncate(start);
    stack.push(result);
}

/// Pops `elements` values and the label below them,
/// and pushes the result of the operation on them, for the labeled structure instructions.
///
/// As in [fold], the operation receives the operands as they are, errors included.
fn labeled<'a>(
    stack: &mut Vec<StackValue<'a>>,
    elements: impl Into<usize>,
    operation: impl FnOnce(Option<&StackValue<'a>>, &[StackValue<'a>]) -> StackValue<'static>,
) {
    let start = stack.len() - elements.into();
    let label = start - 1;
    let result = operation(Some(&stack[label]), &stack[start..]);

    stack.truncate(label);
    stack.push(result);
}

/// Pops one operand and pushes the result of an accessor on it,
/// which may borrow from the operand.
///
/// A component of a borrowed structure is returned borrowed,
/// which the `'static` results of the other helpers cannot express.
/// The accessor receives the operand as it is, error included.
fn unary_borrowed<'a>(
    stack: &mut Vec<StackValue<'a>>,
    operation: impl FnOnce(&StackValue<'a>) -> StackValue<'a>,
) {
    let result = operation(stack.last().expect("the program is validated"));

    stack.pop();
    stack.push(result);
}

/// Pops two operands and pushes the result of an accessor on them,
/// which may borrow from the operands.
///
/// As in [unary_borrowed], the accessor receives the operands as they are, errors included.
fn binary_borrowed<'a>(
    stack: &mut Vec<StackValue<'a>>,
    operation: impl FnOnce(&StackValue<'a>, &StackValue<'a>) -> StackValue<'a>,
) {
    let result = {
        let [left, right] = &stack[stack.len() - 2..] else {
            unreachable!("the program is validated")
        };
        operation(left, right)
    };

    stack.truncate(stack.len() - 2);
    stack.push(result);
}

/// Pops the operands of [Matches](OpCode::Matches) or [Replace](OpCode::Replace),
/// and pushes the result of the match or the replacement.
///
/// `flags` says whether the instruction has a flags operand,
/// and `replace` whether it is a replacement.
/// If operands are errors, the result is the first error
/// in the order subject, pattern, flags, replacement.
fn regex<'a>(stack: &mut Vec<StackValue<'a>>, flags: bool, replace: bool) {
    let flags = flags.then(|| stack.pop().expect("the program is validated"));
    let replacement = replace.then(|| stack.pop().expect("the program is validated"));
    let pattern = stack.pop().expect("the program is validated");
    let subject = stack.pop().expect("the program is validated");

    let operands = (|| {
        let subject = subject.as_result()?;
        let pattern = pattern.as_result()?;
        let flags = flags.as_ref().map(StackValue::as_result).transpose()?;
        let replacement = replacement
            .as_ref()
            .map(StackValue::as_result)
            .transpose()?;
        Ok::<_, EvaluationError>((subject, pattern, flags, replacement))
    })();

    let result = match operands {
        Ok((subject, pattern, flags, Some(replacement))) => {
            pattern::replace(subject, pattern, replacement, flags)
        }
        Ok((subject, pattern, flags, None)) => pattern::matches(subject, pattern, flags),
        Err(error) => error.into(),
    };

    stack.push(result);
}
