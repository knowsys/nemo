//! Stack programs: compiled expressions and their interpreter,
//! with the [values](StackValue) on the stack, the [context](EvaluationContext) of a run,
//! and the [bindings](StackProgram::binding) a program can express.

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

/// A compiled expression:
/// a flat sequence of [instructions](OpCode) evaluated over a value stack,
/// reading a row of input values and leaving one result.
///
/// Construction validates the stack and the index bounds,
/// so evaluation never checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackProgram {
    /// The instructions, in evaluation order.
    operations: Vec<OpCode>,
    /// The constants the instructions reference.
    constants: Vec<DataValue>,
    /// The number of row inputs the program reads.
    inputs: usize,
    /// The largest stack depth evaluation reaches.
    stack_bound: usize,
}

impl StackProgram {
    /// The most constants, and the most inputs, one program may read:
    /// an instruction addresses them by a 16-bit index.
    pub const OPERAND_LIMIT: usize = u16::MAX as usize + 1;

    /// Creates a validated program.
    ///
    /// # Panics
    ///
    /// Panics if the instructions underflow the stack or leave more or less than one result,
    /// or if they reference a constant or an input beyond the given ones.
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

    /// Returns the number of row inputs the program reads.
    pub fn inputs(&self) -> usize {
        self.inputs
    }

    /// Returns whether the program's result is not fixed by its inputs:
    /// it draws randomness or a UUID, or reads the run's instant.
    ///
    /// Its results can then neither be memoized nor folded into constants.
    pub fn is_nondeterministic(&self) -> bool {
        self.operations
            .iter()
            .any(|operation| operation.is_nondeterministic())
    }

    /// Returns the largest stack depth evaluation reaches:
    /// the capacity to give the caller-owned stack.
    pub fn stack_bound(&self) -> usize {
        self.stack_bound
    }

    /// Returns the constant at an index, as a stack value borrowing it.
    fn constant(&self, index: u16) -> StackValue<'_> {
        self.constants[usize::from(index)].as_ref().into()
    }

    /// Evaluates the program over one row of inputs,
    /// which must be as many as [inputs](Self::inputs),
    /// in the [context](EvaluationContext) of the run.
    ///
    /// The result may be an [error value](StackValue::Error):
    /// the caller decides whether that drops the row, and counts it.
    /// The stack is cleared and reused.
    ///
    /// Debug builds also hold every instruction's result
    /// to what [inference](Self::infer) promises for its operands,
    /// and panic if the two disagree.
    pub fn evaluate<'a>(
        &'a self,
        context: EvaluationContext,
        stack: &mut Vec<StackValue<'a>>,
        inputs: &[ValueRef<'a>],
    ) -> StackValue<'a> {
        debug_assert_eq!(inputs.len(), self.inputs, "the row must match the program");
        stack.clear();

        // One flat match over the instruction set, so the loop branches once per instruction.
        // The arms name the operation family's function,
        // and the helpers that pop the operands (`unary`, `binary`, `fold`, ...)
        // hold the stack discipline.
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
                OpCode::Canonical => unary(stack, generic::canonical),
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

    /// Evaluates the program as a condition:
    /// the effective boolean value of its result.
    ///
    /// # Errors
    ///
    /// Returns the error that makes the row undecidable:
    /// the error of the evaluation, or a [TypeMismatch](EvaluationError::TypeMismatch)
    /// for a result without an effective boolean value.
    pub fn filter<'a>(
        &'a self,
        context: EvaluationContext,
        stack: &mut Vec<StackValue<'a>>,
        inputs: &[ValueRef<'a>],
    ) -> Result<bool, EvaluationError> {
        let result = self.evaluate(context, stack, inputs);
        result.as_result()?.effective_boolean_value()
    }

    /// Evaluates the program abstractly:
    /// for inputs holding the given [types](Type), what the result can hold on success,
    /// and the exact value if the constants and shapes pin one down.
    ///
    /// An input beyond the given slice is taken as unknown.
    /// The labels give the types declared for the labels the input types refer to.
    ///
    /// The answer over-approximates what [evaluate](Self::evaluate) can produce,
    /// so an empty bound proves that the program fails on every row,
    /// and an exact `false` that no row passes it as a condition.
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
                    // Construction validated the stack discipline.
                    let arguments = stack.split_off(stack.len() - other.pops());
                    infer::transfer(other, &arguments, labels)
                }
            };
            stack.push(result);
        }

        stack.pop().expect("a validated program leaves one result")
    }

    /// Narrows the types of the inputs to those that can yield an outcome:
    /// for inputs holding the given [types](Type), what they can hold
    /// if the program evaluates to a value of a type, or to a condition that holds.
    ///
    /// An input beyond the given slice is taken as unknown and not narrowed.
    ///
    /// The answer over-approximates, like [infer](Self::infer),
    /// so an input narrowed to the empty type proves that no row yields the outcome.
    pub fn narrow(&self, inputs: &[Type], labels: &LabelTypes, outcome: &Outcome) -> Vec<Type> {
        narrow::narrow(&self.operations, &self.constants, inputs, labels, outcome)
    }

    /// Compiles the integer form of this program,
    /// or returns `None` if an instruction or a constant lies outside the integer fragment.
    /// See [IntegerProgram].
    pub(super) fn integer_specialization(&self) -> Option<IntegerProgram> {
        IntegerProgram::compile(&self.operations, &self.constants, self.inputs)
    }

    /// Recognizes the identity form of this program,
    /// or returns `None` if it does more than compare two terms for identity.
    /// See [IdentityCondition].
    pub(super) fn identity_specialization(&self) -> Option<IdentityCondition> {
        IdentityCondition::recognize(&self.operations, &self.constants)
    }
}

/// Pops one operand, propagates a pending error, applies the operation.
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

/// Pops two operands, propagates a pending error, the left one first,
/// applies the operation.
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

/// Pops three operands, propagates a pending error, the leftmost first,
/// applies the operation.
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

/// Pops two operands and applies a connective to them as they are,
/// errors included, which three-valued logic can decide against.
fn connective<'a>(
    stack: &mut Vec<StackValue<'a>>,
    operation: impl FnOnce(&StackValue<'a>, &StackValue<'a>) -> StackValue<'static>,
) {
    let right = stack.pop().expect("the program is validated");
    let left = stack.pop().expect("the program is validated");

    stack.push(operation(&left, &right));
}

/// Pops `operands` values and applies the operation to them as a slice,
/// for the variadic instructions.
///
/// Each operand's error is the operation's to propagate.
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

/// Pops like [fold], with a label operand beneath the `elements`,
/// for the labeled structure constructors.
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

/// Pops one operand like [unary], for an accessor whose result may borrow from it:
/// a component of a borrowed structure comes back borrowed,
/// which the `'static` results of the other helpers cannot express.
///
/// The operand arrives as it is, error included.
fn unary_borrowed<'a>(
    stack: &mut Vec<StackValue<'a>>,
    operation: impl FnOnce(&StackValue<'a>) -> StackValue<'a>,
) {
    let result = operation(stack.last().expect("the program is validated"));

    stack.pop();
    stack.push(result);
}

/// Pops two operands like [binary], for an accessor whose result may borrow from them;
/// see [unary_borrowed].
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

/// Pops the operands of a pattern instruction,
/// the flags if the instruction carries them, and the replacement for a replacement,
/// propagates a pending error, the subject's first,
/// and applies the match or the replacement.
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
