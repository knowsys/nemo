//! Compiling model expressions into stack programs.

use std::collections::HashMap;

use nemo_physical::{
    diagnostics::Diagnostic,
    function::{OpCode, StackProgram},
    value::{DataValue, DomainSet, ValueDomain},
};

use crate::{
    diagnostics::TranslationError,
    engine::{
        compile::compiled::VariableId,
        compile::translation::{AGGREGATE_RESULT, Translator, terms::is_volatile, unsupported},
    },
    model::{Operation, Operator, Span, Term, TermKind, Variable, VariableKind},
};

/// Returns the diagnostic of a builtin applied to more arguments than a program
/// can pass.
fn too_many_arguments(operator: Operator, span: Option<Span>) -> Diagnostic {
    TranslationError::TooManyArguments {
        operator: operator.name().to_owned(),
    }
    .at(span)
}

/// Returns the argument-count diagnostic of a builtin application,
/// `expected` naming the accepted counts with their noun, as `1 argument`.
fn argument_count(
    operator: Operator,
    expected: &str,
    found: usize,
    span: Option<Span>,
) -> Diagnostic {
    TranslationError::ArgumentCount {
        operator: operator.name().to_owned(),
        expected: expected.to_owned(),
        found,
    }
    .at(span)
}

/// Returns a count of arguments with its noun, as `1 argument` or `2 or 3 arguments`,
/// `last` being the count the noun agrees with.
fn arguments_noun(count: &str, last: usize) -> String {
    if last == 1 {
        format!("{count} argument")
    } else {
        format!("{count} arguments")
    }
}

/// The variable scope a compiled expression reads from.
#[derive(Clone, Copy)]
pub(super) enum VariableScope<'s> {
    /// A rule body or head: variables resolve to bound rule variables.
    Rule(&'s HashMap<String, VariableId>),
    /// A ground context (facts): variables cannot appear.
    Ground,
    /// An aggregate head expression: like [Rule](Self::Rule), with the
    /// aggregate subterm reading as [AGGREGATE_RESULT].
    AggregateHead(&'s HashMap<String, VariableId>),
    /// A directive option value: variables read the guard bindings as
    /// constants.
    Bindings(&'s HashMap<String, DataValue>),
}

/// How a builtin takes its arguments, and the code applying them once
/// they are on the stack.
enum BuiltinForm {
    /// A fixed number of arguments, then the codes.
    Fixed(usize, &'static [OpCode]),
    /// The given number of arguments, or one more as an option, with the code for each count.
    Optional(usize, OpCode, OpCode),
    /// Any number of arguments, at least one, folded pairwise under one code,
    /// as the named forms of the infix operators are.
    ///
    /// A single argument takes the other codes instead.
    Folded(OpCode, &'static [OpCode]),
    /// Any number of arguments, at least the given count, with the code carrying the count.
    Counted(usize, fn(u16) -> OpCode),
}

use BuiltinForm::{Counted, Fixed, Folded, Optional};

/// Returns the table of builtins: how each takes its arguments.
fn builtin_form(operator: Operator) -> BuiltinForm {
    match operator {
        Operator::Equal => Fixed(2, &[OpCode::Same]),
        Operator::Unequal => Fixed(2, &[OpCode::Same, OpCode::Not]),
        Operator::ValueEqual => Fixed(2, &[OpCode::Equal]),
        Operator::ValueUnequal => Fixed(2, &[OpCode::Equal, OpCode::Not]),
        Operator::Greater => Fixed(2, &[OpCode::Greater]),
        Operator::GreaterEq => Fixed(2, &[OpCode::GreaterOrEqual]),
        Operator::Less => Fixed(2, &[OpCode::Less]),
        Operator::LessEq => Fixed(2, &[OpCode::LessOrEqual]),
        // A single number is its own sum and product: the maximum of one
        // operand passes a number and rejects anything else.
        Operator::Add => Folded(OpCode::Add, &[OpCode::Maximum(1)]),
        Operator::Subtract => Fixed(2, &[OpCode::Subtract]),
        Operator::Multiply => Folded(OpCode::Multiply, &[OpCode::Maximum(1)]),
        Operator::Divide => Fixed(2, &[OpCode::Divide]),
        Operator::Negate => Fixed(1, &[OpCode::Negate]),
        Operator::Abs => Fixed(1, &[OpCode::Absolute]),
        Operator::Str => Fixed(1, &[OpCode::Lexical]),
        // The conjunction and the disjunction of one operand are its
        // effective boolean value, which negating twice gives.
        Operator::And => Folded(OpCode::And, &[OpCode::Not, OpCode::Not]),
        Operator::Or => Folded(OpCode::Or, &[OpCode::Not, OpCode::Not]),
        Operator::Not => Fixed(1, &[OpCode::Not]),
        Operator::IsIri => Fixed(1, const { &[in_domain(ValueDomain::Iri)] }),
        Operator::IsNumeric => Fixed(1, &[OpCode::InDomains(DomainSet::NUMERIC)]),
        Operator::StringLength => Fixed(1, &[OpCode::StringLength]),
        Operator::UpperCase => Fixed(1, &[OpCode::UpperCase]),
        Operator::LowerCase => Fixed(1, &[OpCode::LowerCase]),
        Operator::Contains => Fixed(2, &[OpCode::Contains]),
        Operator::StartsWith => Fixed(2, &[OpCode::StartsWith]),
        Operator::EndsWith => Fixed(2, &[OpCode::EndsWith]),
        Operator::IsString => Fixed(1, const { &[in_domain(ValueDomain::String)] }),
        Operator::IsInteger => Fixed(1, const { &[in_domain(ValueDomain::Integer)] }),
        Operator::IsFloat => Fixed(1, const { &[in_domain(ValueDomain::Float)] }),
        Operator::IsDouble => Fixed(1, const { &[in_domain(ValueDomain::Double)] }),
        Operator::IsNull => Fixed(1, const { &[in_domain(ValueDomain::Null)] }),
        // Integers are decimals, as in the declared type `decimal`.
        Operator::IsDecimal => Fixed(1, &[OpCode::InDomains(DomainSet::EXACT)]),
        Operator::IsBoolean => Fixed(1, const { &[in_domain(ValueDomain::Boolean)] }),
        Operator::IsLangString => Fixed(1, const { &[in_domain(ValueDomain::LanguageString)] }),
        Operator::IsDateTime => Fixed(1, const { &[in_domain(ValueDomain::DateTime)] }),
        Operator::IsDate => Fixed(1, const { &[in_domain(ValueDomain::Date)] }),
        Operator::IsTime => Fixed(1, const { &[in_domain(ValueDomain::Time)] }),
        Operator::IsDuration => Fixed(1, &[OpCode::InDomains(DomainSet::DURATION)]),
        Operator::IsDayTimeDuration => {
            Fixed(1, const { &[in_domain(ValueDomain::DayTimeDuration)] })
        }
        Operator::IsYearMonthDuration => {
            Fixed(1, const { &[in_domain(ValueDomain::YearMonthDuration)] })
        }
        Operator::Datatype => Fixed(1, &[OpCode::Datatype]),
        Operator::FullStr => Fixed(1, &[OpCode::Canonical]),
        Operator::Lang => Fixed(1, &[OpCode::Language]),
        Operator::CastInt => Fixed(1, &[OpCode::CastInteger]),
        Operator::CastFloat => Fixed(1, &[OpCode::CastFloat]),
        Operator::CastDouble => Fixed(1, &[OpCode::CastDouble]),
        Operator::CastIri => Fixed(1, &[OpCode::CastIri]),
        Operator::CastDateTime => Fixed(1, &[OpCode::CastDateTime]),
        Operator::CastDate => Fixed(1, &[OpCode::CastDate]),
        Operator::CastTime => Fixed(1, &[OpCode::CastTime]),
        Operator::CastDuration => Fixed(1, &[OpCode::CastDuration]),
        Operator::CastDayTimeDuration => Fixed(1, &[OpCode::CastDayTimeDuration]),
        Operator::CastYearMonthDuration => Fixed(1, &[OpCode::CastYearMonthDuration]),
        Operator::Round => Fixed(1, &[OpCode::Round]),
        Operator::Sin => Fixed(1, &[OpCode::Sine]),
        Operator::Sqrt => Fixed(1, &[OpCode::SquareRoot]),
        Operator::Cos => Fixed(1, &[OpCode::Cosine]),
        Operator::Tan => Fixed(1, &[OpCode::Tangent]),
        Operator::Ceil => Fixed(1, &[OpCode::Ceiling]),
        Operator::Floor => Fixed(1, &[OpCode::Floor]),
        Operator::StrRev => Fixed(1, &[OpCode::Reverse]),
        Operator::Trim => Fixed(1, &[OpCode::Trim]),
        Operator::TrimStart => Fixed(1, &[OpCode::TrimStart]),
        Operator::TrimEnd => Fixed(1, &[OpCode::TrimEnd]),
        Operator::UriEncode => Fixed(1, &[OpCode::UriEncode]),
        Operator::UriDecode => Fixed(1, &[OpCode::UriDecode]),
        Operator::Md5 => Fixed(1, &[OpCode::Md5]),
        Operator::Sha1 => Fixed(1, &[OpCode::Sha1]),
        Operator::Sha256 => Fixed(1, &[OpCode::Sha256]),
        Operator::Sha384 => Fixed(1, &[OpCode::Sha384]),
        Operator::Sha512 => Fixed(1, &[OpCode::Sha512]),
        Operator::Year => Fixed(1, &[OpCode::Year]),
        Operator::Month => Fixed(1, &[OpCode::Month]),
        Operator::Day => Fixed(1, &[OpCode::Day]),
        Operator::Hours => Fixed(1, &[OpCode::Hours]),
        Operator::Minutes => Fixed(1, &[OpCode::Minutes]),
        Operator::Seconds => Fixed(1, &[OpCode::Seconds]),
        Operator::TzString => Fixed(1, &[OpCode::TimezoneString]),
        Operator::Timezone => Fixed(1, &[OpCode::TimezoneDuration]),
        Operator::Adjust => Fixed(2, &[OpCode::Adjust]),
        Operator::StrLang => Fixed(2, &[OpCode::StringLanguage]),
        Operator::LangMatches => Fixed(2, &[OpCode::LanguageMatches]),
        Operator::StrDt => Fixed(2, &[OpCode::TypedLiteral]),
        Operator::Pow => Fixed(2, &[OpCode::Power]),
        Operator::Log => Fixed(2, &[OpCode::Logarithm]),
        Operator::Rem => Fixed(2, &[OpCode::Remainder]),
        Operator::BitShl => Fixed(2, &[OpCode::ShiftLeft]),
        Operator::BitShr => Fixed(2, &[OpCode::ShiftRight]),
        Operator::Compare => Fixed(2, &[OpCode::Compare]),
        Operator::Levenshtein => Fixed(2, &[OpCode::Levenshtein]),
        Operator::StrBefore => Fixed(2, &[OpCode::StringBefore]),
        Operator::StrAfter => Fixed(2, &[OpCode::StringAfter]),
        Operator::Rand => Fixed(0, &[OpCode::Random]),
        Operator::Uuid => Fixed(0, &[OpCode::UuidIri]),
        Operator::StrUuid => Fixed(0, &[OpCode::UuidString]),
        Operator::Now => Fixed(0, &[OpCode::Now]),
        Operator::Get => Fixed(2, &[OpCode::Get]),
        Operator::Length => Fixed(1, &[OpCode::Length]),
        Operator::Label => Fixed(1, &[OpCode::Label]),
        Operator::Keys => Fixed(1, &[OpCode::Keys]),
        Operator::HasKey => Fixed(2, &[OpCode::HasKey]),
        Operator::IsTuple => Fixed(1, const { &[in_domain(ValueDomain::Tuple)] }),
        Operator::IsMap => Fixed(1, const { &[in_domain(ValueDomain::Map)] }),
        Operator::StrJoin => Fixed(2, &[OpCode::JoinStrings]),
        Operator::BitAnd => Counted(1, OpCode::BitAnd),
        Operator::BitOr => Counted(1, OpCode::BitOr),
        Operator::BitXor => Counted(1, OpCode::BitXor),
        Operator::Luka => Counted(1, OpCode::Lukasiewicz),
        Operator::Substr => Optional(2, OpCode::Substring, OpCode::SubstringWith),
        Operator::Regex => Optional(
            2,
            OpCode::Matches { flags: false },
            OpCode::Matches { flags: true },
        ),
        Operator::Replace => Optional(
            3,
            OpCode::Replace { flags: false },
            OpCode::Replace { flags: true },
        ),
        Operator::Concat => Counted(0, OpCode::Concat),
        Operator::Max => Counted(1, OpCode::Maximum),
        Operator::Min => Counted(1, OpCode::Minimum),
    }
}

/// Returns the instruction testing whether a value is of the given domain.
///
/// The table calls it in `const` blocks,
/// since a function call does not become a static slice by itself.
const fn in_domain(domain: ValueDomain) -> OpCode {
    OpCode::InDomains(DomainSet::of(domain))
}

/// Compiles a model operation tree into a stack program, postorder.
#[derive(Default)]
pub(super) struct StackProgramBuilder {
    /// The instructions emitted so far.
    pub(super) operations: Vec<OpCode>,
    /// The constants collected so far.
    pub(super) constants: Vec<DataValue>,
    /// The variables feeding the program, in input order.
    pub(super) inputs: Vec<VariableId>,
    /// Whether the constants or the inputs outgrew what an instruction addresses.
    ///
    /// [finish](Self::finish) then returns an error for the expression.
    overflow: bool,
}

impl StackProgramBuilder {
    /// Emits a constant.
    fn push(&mut self, value: DataValue) {
        let index = self.constants.len();
        self.constants.push(value);
        let operand = self.operand(index);
        self.operations.push(OpCode::Constant(operand));
    }

    /// Returns an operand index as an instruction addresses it.
    ///
    /// An index beyond the limit marks the overflow and is replaced by the largest operand,
    /// and the expression is an error before its program could run.
    fn operand(&mut self, index: usize) -> u16 {
        u16::try_from(index).unwrap_or_else(|_| {
            self.overflow = true;
            u16::MAX
        })
    }

    /// Emits a read of a program input, registered on first sight.
    fn input(&mut self, id: VariableId) {
        let input = match self.inputs.iter().position(|&existing| existing == id) {
            Some(position) => position,
            None => {
                self.inputs.push(id);
                self.inputs.len() - 1
            }
        };
        let operand = self.operand(input);
        self.operations.push(OpCode::Input(operand));
    }

    /// Emits the instruction building a structure of the given size.
    fn structure(
        &mut self,
        make: fn(u16) -> OpCode,
        size: usize,
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        let size = u16::try_from(size).map_err(|_| TranslationError::StructureTooLarge.at(span))?;
        self.operations.push(make(size));
        Ok(())
    }

    /// Emits the code of one term.
    pub(super) fn term(
        &mut self,
        translator: &Translator<'_>,
        scope: VariableScope<'_>,
        term: &Term,
    ) -> Result<(), Diagnostic> {
        match &term.kind {
            TermKind::Variable(variable) => self.variable(translator, scope, variable, term.span),
            TermKind::Operation(operation) => self.operation(translator, scope, operation),
            TermKind::Aggregate(aggregate) => match scope {
                // The result of the (single) aggregate reads as a
                // pseudo-input; the caller remaps it.
                VariableScope::AggregateHead(_) => {
                    self.input(AGGREGATE_RESULT);
                    Ok(())
                }
                _ => Err(TranslationError::AggregateOutsideHead.at(aggregate.span)),
            },
            TermKind::Tuple(_) | TermKind::Map { .. } | TermKind::Function { .. } => {
                self.structured(translator, scope, term)
            }
            _ => {
                self.push(translator.evaluate(term)?);
                Ok(())
            }
        }
    }

    /// Emits the code of a variable: a parameter reads as a constant,
    /// a rule variable as an input, and a guard variable as the
    /// binding it stands for.
    fn variable(
        &mut self,
        translator: &Translator<'_>,
        scope: VariableScope<'_>,
        variable: &Variable,
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        match variable.kind {
            VariableKind::Global => {
                self.push(translator.global(&variable.name, span)?);
                Ok(())
            }
            VariableKind::Universal => match scope {
                VariableScope::Bindings(bindings) => {
                    let Some(value) = bindings.get(&variable.name) else {
                        return Err(TranslationError::GuardUnbound {
                            name: variable.name.clone(),
                        }
                        .at(span)
                        .with_hint(format!(
                            "bind it after the options, as `, ?{} = \"…\"`",
                            variable.name
                        )));
                    };
                    self.push(value.clone());
                    Ok(())
                }
                VariableScope::Rule(names) | VariableScope::AggregateHead(names) => {
                    // Assignment resolution ran first, so a variable still
                    // unbound here cannot be bound at all.
                    let Some(&id) = names.get(&variable.name) else {
                        return Err(TranslationError::UnsafeVariable {
                            role: String::from("variable"),
                            variable: variable.name.clone(),
                        }
                        .at(span));
                    };
                    self.input(id);
                    Ok(())
                }
                VariableScope::Ground => Err(TranslationError::NotGround.at(span)),
            },
            VariableKind::Anonymous => Err(TranslationError::AnonymousAsValue.at(span)),
            VariableKind::Existential => Err(unsupported(
                "an existential variable in an expression",
                span,
            )),
        }
    }

    /// Emits the code of a structured term:
    /// the constant it evaluates to if it is ground,
    /// and code building it per row if it is not,
    /// or if it calls a nondeterministic function (such as `RAND()`).
    fn structured(
        &mut self,
        translator: &Translator<'_>,
        scope: VariableScope<'_>,
        term: &Term,
    ) -> Result<(), Diagnostic> {
        if !is_volatile(term)
            && let Ok(value) = translator.evaluate(term)
        {
            translator.check_value_types(value.as_ref(), term.span)?;
            self.push(value);
            return Ok(());
        }
        match &term.kind {
            TermKind::Tuple(elements) => {
                for element in elements {
                    self.term(translator, scope, element)?;
                }
                self.structure(OpCode::Tuple, elements.len(), term.span)
            }
            TermKind::Function {
                name,
                arguments,
                labels,
            } => {
                if labels.is_some() {
                    return Err(TranslationError::LabeledTupleNamedFields.at(term.span));
                }
                // A function term is a labeled tuple, and its label is
                // pushed before its elements.
                let label = translator.resolve(name, term.span)?;
                translator.check_tuple_construction(&label, arguments, term.span)?;
                self.push(DataValue::iri(label));
                for argument in arguments {
                    self.term(translator, scope, argument)?;
                }
                self.structure(OpCode::LabeledTuple, arguments.len(), term.span)
            }
            TermKind::Map { label, pairs } => {
                // Keys known now are checked now, as for a ground map.
                if let Some(problem) = translator.repeated_key(pairs, term.span) {
                    return Err(problem);
                }
                // A labeled map's label is pushed before its entries.
                if let Some(name) = label {
                    let label = translator.resolve(name, term.span)?;
                    translator.check_map_construction(&label, pairs, term.span)?;
                    self.push(DataValue::iri(label));
                }
                for (key, value) in pairs {
                    self.term(translator, scope, key)?;
                    self.term(translator, scope, value)?;
                }
                let code: fn(u16) -> OpCode = match label {
                    None => OpCode::Map,
                    Some(_) => OpCode::LabeledMap,
                };
                self.structure(code, pairs.len(), term.span)
            }
            _ => unreachable!("only tuples, maps, and function terms are structured"),
        }
    }

    /// Emits the code of one operation: the arguments postorder, then
    /// the operator's code from the [table](builtin_form).
    pub(super) fn operation(
        &mut self,
        translator: &Translator<'_>,
        scope: VariableScope<'_>,
        operation: &Operation,
    ) -> Result<(), Diagnostic> {
        let arguments = &operation.arguments;
        let operator = operation.operator;
        let span = operation.span;
        match builtin_form(operator) {
            Fixed(count, code) => {
                if arguments.len() != count {
                    return Err(argument_count(
                        operator,
                        &arguments_noun(&count.to_string(), count),
                        arguments.len(),
                        span,
                    ));
                }
                self.arguments(translator, scope, arguments)?;
                self.operations.extend_from_slice(code);
            }
            Optional(count, without, with) => {
                let code = match arguments.len() {
                    found if found == count => without,
                    found if found == count + 1 => with,
                    found => {
                        return Err(argument_count(
                            operator,
                            &arguments_noun(&format!("{count} or {}", count + 1), count + 1),
                            found,
                            span,
                        ));
                    }
                };
                self.arguments(translator, scope, arguments)?;
                self.operations.push(code);
            }
            Folded(code, single) => {
                let Some((first, rest)) = arguments.split_first() else {
                    return Err(argument_count(operator, "at least 1 argument", 0, span));
                };
                self.term(translator, scope, first)?;
                if rest.is_empty() {
                    self.operations.extend_from_slice(single);
                }
                for argument in rest {
                    self.term(translator, scope, argument)?;
                    self.operations.push(code);
                }
            }
            Counted(least, make) => {
                let count = u16::try_from(arguments.len())
                    .map_err(|_| too_many_arguments(operator, span))?;
                if arguments.len() < least {
                    return Err(argument_count(
                        operator,
                        &arguments_noun(&format!("at least {least}"), least),
                        arguments.len(),
                        span,
                    ));
                }
                self.arguments(translator, scope, arguments)?;
                self.operations.push(make(count));
            }
        }
        Ok(())
    }

    /// Emits the code of arguments, in order.
    fn arguments(
        &mut self,
        translator: &Translator<'_>,
        scope: VariableScope<'_>,
        arguments: &[Term],
    ) -> Result<(), Diagnostic> {
        for argument in arguments {
            self.term(translator, scope, argument)?;
        }
        Ok(())
    }

    /// Finishes the program of the expression at the given span.
    pub(super) fn finish(self, span: Option<Span>) -> Result<StackProgram, Diagnostic> {
        if self.overflow {
            return Err(TranslationError::ExpressionTooLarge {
                limit: StackProgram::OPERAND_LIMIT,
            }
            .at(span));
        }
        let arity = self.inputs.len();
        Ok(StackProgram::new(self.operations, self.constants, arity))
    }
}
