//! Declarations: parameters, column declarations (`@declare`), user-defined types (`@type`),
//! named arguments, and inputs.

use std::collections::HashMap;

use nemo_physical::{
    diagnostics::Diagnostic,
    function::EvaluationContext,
    value::{
        DataValue, DomainSet, LabelBound, MapShape, Shape, TupleShape, Type, ValueDomain, ValueRef,
    },
};

use crate::{
    diagnostics::{AboutPredicate, ModuleError, TranslationError, TypeError},
    engine::{
        compile::translation::{
            Purpose, StackProgramBuilder, Translator, VariableScope, terms::collect_globals,
        },
        compile::types,
        registered::Registered,
    },
    model::{
        Atom, DeclareDirective, Fact, Identifier, InputDirective, Literal, Operator,
        ParameterDirective, Rule, Span, Term, TermKind, TypeDirective, TypeExpression, Variable,
        VariableKind,
    },
    values::TypeSyntax,
};

impl<'a> Translator<'a> {
    /// Checks a new declaration against the values its predicate's table already stores,
    /// by the table's seeded column types.
    ///
    /// A declaration that excludes stored values is an error,
    /// since those values can no longer be rejected.
    /// The seeds are per stored column, and the declaration per column as written,
    /// so a column the loader leaves out stores nothing to check.
    pub(super) fn check_declaration_against_store(
        &self,
        name: &str,
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        let Some(&Registered { table, arity }) = self.predicates.get(name) else {
            return Ok(());
        };
        let Some(seeds) = self.seeds.get(&table) else {
            return Ok(());
        };
        let Some(declaration) = self.declarations.get(name) else {
            return Ok(());
        };
        let layout = self.plan.stored_layout(name, arity);
        for (column, declared) in declaration.types.iter().enumerate() {
            let Some(seed) = layout
                .delivered_position(column)
                .and_then(|position| seeds.get(position))
            else {
                continue;
            };
            if !seed.is_subset(declared, &self.label_types) {
                return Err(TypeError::StoredValuesExcluded {
                    name: name.to_owned(),
                    column: column + 1,
                    stored: TypeSyntax::of(seed).to_string(),
                }
                .at(span));
            }
        }
        Ok(())
    }

    /// Resolves the parameter declarations to a fixpoint, since one may read another, in any
    /// order.
    pub(super) fn resolve_parameters(
        &mut self,
        mut pending: Vec<&ParameterDirective>,
    ) -> Result<(), Diagnostic> {
        for parameter in &pending {
            if pending
                .iter()
                .filter(|other| other.variable.name == parameter.variable.name)
                .count()
                > 1
            {
                return Err(TranslationError::DuplicateParameter {
                    name: parameter.variable.name.clone(),
                }
                .at(parameter.span));
            }
        }
        for parameter in &pending {
            let Some(value) = &parameter.value else {
                continue;
            };
            let mut reads = Vec::new();
            collect_globals(value, &mut reads);
            let undefined = reads.into_iter().find(|name| {
                !self.globals.contains_key(*name)
                    && !pending.iter().any(|other| other.variable.name == *name)
            });
            if let Some(name) = undefined {
                return Err(TranslationError::UndefinedParameter {
                    name: name.to_owned(),
                }
                .at(parameter_span(value, name).or(parameter.span)));
            }
        }
        loop {
            let mut progressed = false;
            let mut remaining = Vec::with_capacity(pending.len());
            for parameter in pending {
                let Some(value) = &parameter.value else {
                    return Err(TranslationError::ParameterWithoutValue {
                        name: parameter.variable.name.clone(),
                    }
                    .at(parameter.span));
                };
                let mut reads = Vec::new();
                collect_globals(value, &mut reads);
                if reads.iter().all(|name| self.globals.contains_key(*name)) {
                    let grounded = self.evaluate(value)?;
                    self.check_value_types(grounded.as_ref(), value.span.or(parameter.span))?;
                    // A typed parameter whose value is provably outside
                    // its declared type is an error.
                    if let Some(datatype) = &parameter.datatype {
                        let declared = self.declared_type(datatype, parameter.span)?;
                        if !declared.admits(grounded.as_ref(), &self.label_types) {
                            let supplied = Type::of_value(grounded.as_ref());
                            return Err(TypeError::ParameterOutsideType {
                                name: parameter.variable.name.clone(),
                                declared: TypeSyntax::of(&declared).to_string(),
                                supplied: TypeSyntax::of(&supplied).to_string(),
                            }
                            .at(value.span.or(parameter.span)));
                        }
                    }
                    self.globals
                        .insert(parameter.variable.name.clone(), grounded);
                    progressed = true;
                } else {
                    remaining.push(parameter);
                }
            }
            pending = remaining;
            if pending.is_empty() {
                return Ok(());
            }
            if !progressed {
                let parameter = parameter_on_cycle(&pending);
                return Err(TranslationError::ParameterCycle {
                    name: parameter.variable.name.clone(),
                }
                .at(parameter.span));
            }
        }
    }

    /// Returns the value of a global parameter.
    pub(super) fn global(&self, name: &str, span: Option<Span>) -> Result<DataValue, Diagnostic> {
        self.globals.get(name).cloned().ok_or_else(|| {
            TranslationError::UndefinedParameter {
                name: name.to_owned(),
            }
            .at(span)
        })
    }

    /// Evaluates the guard bindings of a directive.
    pub(super) fn guard_bindings(
        &self,
        guards: &[(String, Term)],
    ) -> Result<HashMap<String, DataValue>, Diagnostic> {
        let mut bindings = HashMap::new();
        for (name, term) in guards {
            if bindings.contains_key(name) {
                return Err(TranslationError::GuardBoundTwice { name: name.clone() }.at(term.span));
            }
            bindings.insert(name.clone(), self.evaluate(term)?);
        }
        Ok(bindings)
    }

    /// Evaluates a directive option value to one ground value, with the
    /// guard bindings in scope.
    pub(super) fn option_value(
        &self,
        term: &Term,
        bindings: &HashMap<String, DataValue>,
    ) -> Result<DataValue, Diagnostic> {
        let mut builder = StackProgramBuilder::default();
        builder.term(self, VariableScope::Bindings(bindings), term)?;
        let program = builder.finish(term.span)?;
        let mut stack = Vec::with_capacity(program.stack_bound());
        program
            .evaluate(EvaluationContext::now(), &mut stack, &[])
            .as_result()
            .map(|value| value.to_owned())
            .map_err(|_| TranslationError::OptionUnevaluable.at(term.span))
    }

    /// Returns the type a type expression admits.
    ///
    /// Labels and map keys resolve against the program's prefixes and base,
    /// like the values checked against them.
    /// A `decimal` column includes integers, because integral decimals canonicalize to them.
    /// A name a `@type` declares stands for the values carrying it, by the label,
    /// so a recursive declaration needs no unfolding.
    /// Writing out the shape of such a label is an error, since its name is the type.
    pub(super) fn declared_type(
        &self,
        expression: &TypeExpression,
        span: Option<Span>,
    ) -> Result<Type, Diagnostic> {
        use crate::model::{BaseType, TypeLabel};

        Ok(match expression {
            TypeExpression::Base(BaseType::Any) => Type::ANY,
            TypeExpression::Base(BaseType::String) => Type::of(ValueDomain::String),
            TypeExpression::Base(BaseType::LanguageString) => Type::of(ValueDomain::LanguageString),
            TypeExpression::Base(BaseType::Iri) => Type::of(ValueDomain::Iri),
            TypeExpression::Base(BaseType::Int) => Type::of(ValueDomain::Integer),
            TypeExpression::Base(BaseType::Double) => Type::of(ValueDomain::Double),
            TypeExpression::Base(BaseType::Float) => Type::of(ValueDomain::Float),
            TypeExpression::Base(BaseType::Decimal) => Type::of_domains(DomainSet::EXACT),
            TypeExpression::Base(BaseType::Number) => Type::of_domains(DomainSet::NUMERIC),
            TypeExpression::Base(BaseType::DateTime) => Type::of(ValueDomain::DateTime),
            TypeExpression::Base(BaseType::Date) => Type::of(ValueDomain::Date),
            TypeExpression::Base(BaseType::Time) => Type::of(ValueDomain::Time),
            TypeExpression::Base(BaseType::Duration) => Type::of_domains(DomainSet::DURATION),
            TypeExpression::Base(BaseType::DayTimeDuration) => {
                Type::of(ValueDomain::DayTimeDuration)
            }
            TypeExpression::Base(BaseType::YearMonthDuration) => {
                Type::of(ValueDomain::YearMonthDuration)
            }
            TypeExpression::Base(BaseType::Bool) => Type::of(ValueDomain::Boolean),
            TypeExpression::Base(BaseType::Null) => Type::of(ValueDomain::Null),
            TypeExpression::Base(BaseType::Tuple) => Type::of(ValueDomain::Tuple),
            TypeExpression::Base(BaseType::Map) => Type::of(ValueDomain::Map),
            TypeExpression::Named(identifier) => {
                let name = self.resolve(identifier, span)?;
                let Some((definition, _)) = self.type_definitions.get(&name) else {
                    return Err(TranslationError::UnknownType {
                        name: identifier.to_string(),
                    }
                    .at(span));
                };
                let domain = match definition {
                    TypeExpression::Map { .. } => ValueDomain::Map,
                    _ => ValueDomain::Tuple,
                };
                Type::declared(&name, domain)
            }
            TypeExpression::Tuple { label, .. } | TypeExpression::Map { label, .. }
                if self.is_declared_label(label, span)? =>
            {
                let TypeLabel::Named(name) = label else {
                    unreachable!("only a named label has a `@type`")
                };
                return Err(TypeError::DeclaredLabelWrittenOut {
                    name: name.to_string(),
                }
                .at(span)
                .with_hint(format!("write `{name}`")));
            }
            TypeExpression::Tuple { label, fields } => self.tuple_type(label, fields, span)?,
            TypeExpression::Map { label, fields } => self.map_type(label, fields, span)?,
            TypeExpression::Union(members) => {
                let mut united = Type::EMPTY;
                for member in members {
                    united = united.union(&self.declared_type(member, span)?);
                }
                united
            }
        })
    }

    /// Returns the type a `@type` definition declares for its label:
    /// its shape written out, whose fields name other declared labels by their labels.
    pub(super) fn definition_type(
        &self,
        definition: &TypeExpression,
        span: Option<Span>,
    ) -> Result<Type, Diagnostic> {
        match definition {
            TypeExpression::Tuple { label, fields } => self.tuple_type(label, fields, span),
            TypeExpression::Map { label, fields } => self.map_type(label, fields, span),
            other => self.declared_type(other, span),
        }
    }

    /// Returns whether a structured type's label has a `@type`.
    fn is_declared_label(
        &self,
        label: &crate::model::TypeLabel,
        span: Option<Span>,
    ) -> Result<bool, Diagnostic> {
        let crate::model::TypeLabel::Named(name) = label else {
            return Ok(false);
        };
        Ok(self
            .type_definitions
            .contains_key(&self.resolve(name, span)?))
    }

    /// Returns the type of the tuples of a written-out tuple type.
    fn tuple_type(
        &self,
        label: &crate::model::TypeLabel,
        fields: &[TypeExpression],
        span: Option<Span>,
    ) -> Result<Type, Diagnostic> {
        Ok(Type::tuple(TupleShape {
            label: self.declared_label(label, span)?,
            elements: fields
                .iter()
                .map(|field| self.declared_type(field, span))
                .collect::<Result<_, _>>()?,
        }))
    }

    /// Returns the type of the maps of a written-out map type.
    fn map_type(
        &self,
        label: &crate::model::TypeLabel,
        fields: &[(String, TypeExpression)],
        span: Option<Span>,
    ) -> Result<Type, Diagnostic> {
        let mut pairs = Vec::with_capacity(fields.len());
        for (key, field) in fields {
            let key = DataValue::iri(self.resolve(&Identifier::bare(key.clone()), span)?);
            pairs.push((key, self.declared_type(field, span)?));
        }
        let label = self.declared_label(label, span)?;
        let Some(shape) = MapShape::new(label, pairs) else {
            let repeated = fields
                .iter()
                .enumerate()
                .find(|(position, (key, _))| {
                    fields[..*position]
                        .iter()
                        .any(|(earlier, _)| earlier == key)
                })
                .map(|(_, (key, _))| key.clone())
                .expect("a map type fails to build only for a repeated key");
            return Err(TranslationError::DuplicateMapKey { key: repeated }.at(span));
        };
        Ok(Type::map(shape))
    }

    /// Returns the label bound a declared structured type states.
    fn declared_label(
        &self,
        label: &crate::model::TypeLabel,
        span: Option<Span>,
    ) -> Result<LabelBound, Diagnostic> {
        use crate::model::TypeLabel;

        Ok(match label {
            TypeLabel::Unlabeled => LabelBound::Exactly(None),
            TypeLabel::Anonymous => LabelBound::Unknown,
            TypeLabel::Named(name) => LabelBound::Exactly(Some(self.resolve(name, span)?.into())),
        })
    }

    /// Records a `@type` directive: the element types every value built with the label must
    /// have, and a name for later type expressions.
    ///
    /// Definitions are validated once every `@type` directive is recorded,
    /// so they may reference each other in any order.
    pub(super) fn type_directive(&mut self, directive: &TypeDirective) -> Result<(), Diagnostic> {
        use crate::model::TypeLabel;

        let label = match &directive.definition {
            TypeExpression::Tuple { label, .. } | TypeExpression::Map { label, .. } => label,
            _ => {
                return Err(TranslationError::TypeNotConstructor.at(directive.span));
            }
        };
        let TypeLabel::Named(identifier) = label else {
            return Err(TranslationError::TypeUnnamed.at(directive.span));
        };
        if let Identifier::Bare(name) = identifier {
            if crate::model::BaseType::from_name(name).is_some() {
                return Err(
                    TranslationError::TypeLabelIsBaseType { name: name.clone() }.at(directive.span)
                );
            }
            if Operator::from_name(name.as_str()).is_some() {
                return Err(
                    TranslationError::TypeLabelIsBuiltin { name: name.clone() }.at(directive.span)
                );
            }
        }
        let name = self.resolve(identifier, directive.span)?;
        match self.type_definitions.get(&name) {
            // Every instance of a module declares the module's types again, alike.
            Some((definition, _)) if *definition == directive.definition => Ok(()),
            Some(_) => Err(TranslationError::DuplicateType {
                name: identifier.to_string(),
            }
            .at(directive.span)),
            None => {
                self.type_definitions
                    .insert(name, (directive.definition.clone(), directive.span));
                Ok(())
            }
        }
    }

    /// Returns the resolved label a `@type` directive declares, if it names one.
    pub(super) fn defined_label(&self, directive: &TypeDirective) -> Option<String> {
        use crate::model::TypeLabel;

        match &directive.definition {
            TypeExpression::Tuple {
                label: TypeLabel::Named(identifier),
                ..
            }
            | TypeExpression::Map {
                label: TypeLabel::Named(identifier),
                ..
            } => self.resolve(identifier, directive.span).ok(),
            _ => None,
        }
    }

    /// Returns the type `@type` declares for a label, or `None` if the label has no `@type`.
    fn label_type(&self, label: &str) -> Option<&Type> {
        self.label_types.get(label)
    }

    /// Checks a ground value, recursively, against the element types `@type` declares for its
    /// labels.
    ///
    /// A fact or constant that violates them is an error.
    pub(super) fn check_value_types(
        &self,
        value: ValueRef<'_>,
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        let check = |label: Option<&str>| -> Result<(), Diagnostic> {
            let Some(label) = label else { return Ok(()) };
            let Some(label_type) = self.label_type(label) else {
                return Ok(());
            };
            if !label_type.admits(value, &self.label_types) {
                return Err(TypeError::ValueOutsideType {
                    label: label.to_owned(),
                    declared: TypeSyntax::of(label_type).to_string(),
                    value: TypeSyntax::of(&Type::of_value(value)).to_string(),
                }
                .at(span));
            }
            Ok(())
        };
        // The parts are checked first, so that the innermost value
        // outside its label's type is the one reported.
        match value {
            ValueRef::Tuple(tuple) => {
                for element in tuple.elements() {
                    self.check_value_types(element, span)?;
                }
                check(tuple.label())?;
            }
            ValueRef::Map(map) => {
                for (key, entry) in map.pairs() {
                    self.check_value_types(key, span)?;
                    self.check_value_types(entry, span)?;
                }
                check(map.label())?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Checks what is statically known of a non-ground labeled tuple construction
    /// against the type `@type` declares for its label:
    /// that it is a tuple, its length, and its ground elements.
    ///
    /// Variable elements are checked at the declared columns their values flow into.
    pub(super) fn check_tuple_construction(
        &self,
        label: &str,
        arguments: &[Term],
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        let Some(label_type) = self.label_type(label) else {
            return Ok(());
        };
        let Some(shape) = written_out(label_type.tuple_shapes()) else {
            return Err(TypeError::ConstructionKind {
                label: label.to_owned(),
                declared: TypeSyntax::of(label_type).to_string(),
                builds: String::from("tuple"),
            }
            .at(span));
        };
        if shape.elements.len() != arguments.len() {
            return Err(TypeError::ConstructionSize {
                label: label.to_owned(),
                declared: shape.elements.len().into(),
                built: arguments.len().into(),
            }
            .at(span));
        }
        for (argument, declared) in arguments.iter().zip(&shape.elements) {
            if let Ok(grounded) = self.evaluate(argument)
                && !declared.admits(grounded.as_ref(), &self.label_types)
            {
                return Err(TypeError::TupleFieldOutsideType {
                    label: label.to_owned(),
                    declared: TypeSyntax::of(declared).to_string(),
                    value: TypeSyntax::of(&Type::of_value(grounded.as_ref())).to_string(),
                }
                .at(argument.span.or(span)));
            }
        }
        Ok(())
    }

    /// Checks what is statically known of a non-ground labeled map construction
    /// against the type `@type` declares for its label:
    /// that it is a map, its number of entries, and its ground keys and values.
    pub(super) fn check_map_construction(
        &self,
        label: &str,
        pairs: &[(Term, Term)],
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        let Some(label_type) = self.label_type(label) else {
            return Ok(());
        };
        let Some(shape) = written_out(label_type.map_shapes()) else {
            return Err(TypeError::ConstructionKind {
                label: label.to_owned(),
                declared: TypeSyntax::of(label_type).to_string(),
                builds: String::from("map"),
            }
            .at(span));
        };
        if shape.pairs().len() != pairs.len() {
            return Err(TypeError::ConstructionSize {
                label: label.to_owned(),
                declared: shape.pairs().len().into(),
                built: pairs.len().into(),
            }
            .at(span));
        }
        for (key, value) in pairs {
            let Ok(key_value) = self.evaluate(key) else {
                continue;
            };
            let Some(declared) = shape.value_at(key_value.as_ref()) else {
                return Err(TypeError::UnknownField {
                    label: label.to_owned(),
                    declared: TypeSyntax::of(label_type).to_string(),
                    key: key.to_string(),
                }
                .at(key.span.or(span)));
            };
            if let Ok(grounded) = self.evaluate(value)
                && !declared.admits(grounded.as_ref(), &self.label_types)
            {
                return Err(TypeError::MapFieldOutsideType {
                    key: key.to_string(),
                    label: label.to_owned(),
                    declared: TypeSyntax::of(declared).to_string(),
                    value: TypeSyntax::of(&Type::of_value(grounded.as_ref())).to_string(),
                }
                .at(value.span.or(span)));
            }
        }
        Ok(())
    }

    /// Records a `@declare` directive: the column types the type analysis
    /// checks facts, imports, and rule heads against.
    pub(super) fn declare_directive(
        &mut self,
        declare: &DeclareDirective,
    ) -> Result<String, Diagnostic> {
        let name = self.resolve(&declare.predicate, declare.span)?;
        let types = declare
            .columns
            .iter()
            .map(|column| self.declared_type(&column.datatype, declare.span))
            .collect::<Result<_, _>>()?;
        let declaration = types::PredicateDeclaration {
            types,
            names: declare
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            span: declare.span,
        };
        if self
            .declarations
            .insert(name.clone(), declaration)
            .is_some()
        {
            return Err(TranslationError::DuplicateDeclaration {
                predicate: declare.predicate.to_string(),
            }
            .at(declare.span));
        }
        Ok(name)
    }

    /// Resolves named arguments to their declared positions, returning the terms in
    /// declaration order.
    ///
    /// A column a body atom leaves out becomes an anonymous variable.
    /// Leaving one out anywhere else is an error, since a fact or a head gives every column a
    /// value.
    pub(super) fn positional_terms(
        &self,
        predicate: &Identifier,
        terms: &[Term],
        labels: &[String],
        span: Option<Span>,
        body: bool,
    ) -> Result<Vec<Term>, Diagnostic> {
        let name = self.resolve(predicate, span)?;
        let Some(declaration) = self.declarations.get(&name) else {
            return Err(TranslationError::NamedArgumentsUndeclared {
                predicate: predicate.to_string(),
            }
            .at(span));
        };
        let mut placed: Vec<Option<Term>> = vec![None; declaration.names.len()];
        for (label, term) in labels.iter().zip(terms) {
            let Some(position) = declaration.position(label) else {
                return Err(TranslationError::UnknownColumn {
                    predicate: predicate.to_string(),
                    column: label.clone(),
                }
                .at(term.span.or(span)));
            };
            if placed[position].is_some() {
                return Err(TranslationError::ColumnGivenTwice {
                    column: label.clone(),
                }
                .at(term.span.or(span)));
            }
            placed[position] = Some(term.clone());
        }
        placed
            .into_iter()
            .enumerate()
            .map(|(position, term)| match term {
                Some(term) => Ok(term),
                None if body => Ok(Term {
                    kind: TermKind::Variable(Variable {
                        kind: VariableKind::Anonymous,
                        name: String::new(),
                    }),
                    span,
                }),
                None => {
                    let column = declaration.names[position].as_deref().map_or_else(
                        || format!("column {}", position + 1),
                        |name| format!("column `{name}`"),
                    );
                    Err(TranslationError::ColumnNotGiven {
                        column,
                        predicate: predicate.to_string(),
                    }
                    .at(span))
                }
            })
            .collect()
    }

    /// Returns the fact with named arguments resolved to declared positions,
    /// or `None` if the fact uses no named arguments.
    pub(super) fn positional_fact(&self, fact: &Fact) -> Result<Option<Fact>, Diagnostic> {
        let Some(labels) = &fact.labels else {
            return Ok(None);
        };
        Ok(Some(Fact {
            predicate: fact.predicate.clone(),
            terms: self.positional_terms(&fact.predicate, &fact.terms, labels, fact.span, false)?,
            labels: None,
            span: fact.span,
        }))
    }

    /// Rewrites one atom's named arguments into declaration order.
    fn positional_atom(&self, atom: &mut Atom, body: bool) -> Result<(), Diagnostic> {
        if let Some(labels) = atom.labels.take() {
            atom.terms =
                self.positional_terms(&atom.predicate, &atom.terms, &labels, atom.span, body)?;
        }
        Ok(())
    }

    /// Returns the rule with named arguments resolved to declared positions,
    /// or `None` if the rule uses no named arguments.
    pub(crate) fn positional_rule(&self, rule: &Rule) -> Result<Option<Rule>, Diagnostic> {
        let named = |atom: &Atom| atom.labels.is_some();
        let touched = rule.head.iter().any(named)
            || rule.body.iter().any(|literal| match literal {
                Literal::Positive(atom) | Literal::Negative(atom) => named(atom),
                Literal::Operation(_) => false,
            });
        if !touched {
            return Ok(None);
        }
        let mut rule = rule.clone();
        for atom in &mut rule.head {
            self.positional_atom(atom, false)?;
        }
        for literal in &mut rule.body {
            if let Literal::Positive(atom) | Literal::Negative(atom) = literal {
                self.positional_atom(atom, true)?;
            }
        }
        Ok(Some(rule))
    }

    /// Records an `@input` directive: a predicate that a `@use` of the program binds
    /// to a predicate of the using program.
    ///
    /// Its columns, if given, declare it like a `@declare`.
    /// It is marked open, since it may hold anything its column types allow.
    /// A second `@input` of the same predicate is refused, as linking refuses it in a module.
    pub(super) fn input_directive(
        &mut self,
        input: &InputDirective,
        preloaded: bool,
    ) -> Result<Option<String>, Diagnostic> {
        let name = self.resolve(&input.predicate, input.span)?;
        if self.inputs.contains(&name) {
            return Err(ModuleError::DuplicateInput {
                predicate: input.predicate.to_string(),
            }
            .at(input.span));
        }
        let mut declared = None;
        if let Some(columns) = &input.columns {
            let declaration = types::PredicateDeclaration {
                types: columns
                    .iter()
                    .map(|column| self.declared_type(&column.datatype, input.span))
                    .collect::<Result<_, _>>()?,
                names: columns.iter().map(|column| column.name.clone()).collect(),
                span: input.span,
            };
            if self
                .declarations
                .insert(name.clone(), declaration)
                .is_some()
            {
                return Err(TranslationError::DuplicateDeclaration {
                    predicate: input.predicate.to_string(),
                }
                .at(input.span));
            }
            declared = Some(name.clone());
        }
        // Running the program directly leaves the input unbound, which
        // earns a warning. A check looks at a module on its own, whose
        // inputs are bound later, and a preloaded statement was warned
        // about already, so neither warns.
        if self.purpose != Purpose::Check && !preloaded {
            self.warn(
                TranslationError::InputUnsupplied {
                    predicate: input.predicate.to_string(),
                }
                .at(input.span)
                .with_predicate(name.clone()),
            );
        }
        self.inputs.insert(name.clone());
        self.open.insert(name);
        Ok(declared)
    }
}

/// Returns the source position of the first use of a parameter in a term.
fn parameter_span(term: &Term, name: &str) -> Option<Span> {
    match &term.kind {
        TermKind::Variable(variable)
            if variable.kind == VariableKind::Global && variable.name == name =>
        {
            term.span
        }
        _ => term
            .subterms()
            .find_map(|subterm| parameter_span(subterm, name)),
    }
}

/// Returns a parameter on a cycle among parameters that cannot be resolved,
/// each of which reads another of them.
///
/// # Panics
///
/// Panics if a parameter has no value or reads none of the others.
fn parameter_on_cycle<'p>(pending: &[&'p ParameterDirective]) -> &'p ParameterDirective {
    let mut visited: Vec<&'p str> = Vec::new();
    let mut current = pending[0];
    while !visited.contains(&current.variable.name.as_str()) {
        visited.push(&current.variable.name);
        let value = current
            .value
            .as_ref()
            .expect("a parameter without a value is refused before");
        let mut reads = Vec::new();
        collect_globals(value, &mut reads);
        current = reads
            .into_iter()
            .find_map(|name| {
                pending
                    .iter()
                    .copied()
                    .find(|other| other.variable.name == name)
            })
            .expect("an unresolved parameter reads another unresolved one");
    }
    current
}

/// Returns the one written-out shape a label's declared type has, of tuples or of maps,
/// or `None` if it declares the other kind.
fn written_out<S>(shapes: Option<&[Shape<S>]>) -> Option<&S> {
    match shapes? {
        [Shape::Structural(shape)] => Some(shape),
        _ => None,
    }
}
