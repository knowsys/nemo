use std::collections::HashMap;

use nemo_physical::aggregates::operation::AggregateOperation;

use crate::{
    io::formats::types::ImportSpec,
    model::{
        chase_model::{ChaseAtom, ChaseFact, ChaseRule, AGGREGATE_VARIABLE_PREFIX},
        types::error::TypeError,
        Identifier, PrimitiveTerm, PrimitiveType, Term, TypeConstraint, Variable,
    },
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum TypeRequirement {
    Hard(PrimitiveType),
    Soft(PrimitiveType),
    None,
}

impl TypeRequirement {
    pub(super) fn stricter_requirement(self, other: Self) -> Option<Self> {
        match self {
            Self::Hard(t1) => match other {
                Self::Hard(t2) => (t1 == t2).then_some(self),
                Self::Soft(t2) => (t1 >= t1.max_type(&t2)).then_some(self),
                Self::None => Some(self),
            },
            Self::Soft(t1) => match other {
                Self::Hard(t2) => (t1.max_type(&t2) <= t2).then_some(other),
                Self::Soft(t2) => Some(Self::Soft(t1.max_type(&t2))),
                Self::None => Some(self),
            },
            Self::None => Some(other),
        }
    }

    pub(super) fn replace_with_max_type_if_compatible(self, other: Self) -> Option<Self> {
        match self {
            Self::Hard(t1) => match other {
                Self::Hard(t2) => (t1 >= t2).then_some(self),
                Self::Soft(t2) => {
                    let max = t1.max_type(&t2);
                    // check if the max type is compatible with both types via partial ord
                    (t1 >= max && max >= t2).then_some(self)
                }
                Self::None => Some(self),
            },
            Self::Soft(t1) => match other {
                Self::Hard(t2) | Self::Soft(t2) => {
                    let max = t1.max_type(&t2);
                    // check if the max type is compatible with both types via partial ord
                    (max >= t1 && max >= t2).then_some(Self::Soft(max))
                }
                Self::None => Some(self),
            },
            Self::None => match other {
                Self::Hard(t2) | Self::Soft(t2) => Some(Self::Soft(t2)),
                Self::None => Some(Self::None),
            },
        }
    }
}

impl From<TypeConstraint> for TypeRequirement {
    fn from(value: TypeConstraint) -> Self {
        match value {
            TypeConstraint::None => TypeRequirement::None,
            TypeConstraint::Exact(p) => TypeRequirement::Hard(p),
            TypeConstraint::AtLeast(p) => TypeRequirement::Soft(p),
            TypeConstraint::Tuple(_) => {
                unimplemented!("currently nested type checking is not supported")
            }
        }
    }
}

impl From<TypeRequirement> for Option<PrimitiveType> {
    fn from(source: TypeRequirement) -> Self {
        match source {
            TypeRequirement::Hard(t) => Some(t),
            TypeRequirement::Soft(t) => Some(t),
            TypeRequirement::None => None,
        }
    }
}

pub(super) type PredicateTypeRequirements = HashMap<Identifier, Vec<TypeRequirement>>;

fn add_type_requirements(
    current_reqs: &mut PredicateTypeRequirements,
    pred: Identifier,
    reqs_to_add: Vec<TypeRequirement>,
    // force_use_of_stricter_requirement: bool,
) -> Result<(), TypeError> {
    let mut types_not_in_conflict: Result<_, _> = Ok(());

    current_reqs
        .entry(pred.clone())
        .and_modify(|types| {
            types
                .iter_mut()
                .zip(reqs_to_add.iter())
                .enumerate()
                .for_each(|(index, (a, b))| {
                    let replacement = a.stricter_requirement(*b);

                    match replacement {
                        Some(replacement) => *a = replacement,
                        None => {
                            types_not_in_conflict = Err(TypeError::InvalidRuleConflictingTypes(
                                pred.0.clone(),
                                index,
                                Option::<PrimitiveType>::from(*a)
                                    .expect("if the type requirement is none, there is a maximum"),
                                Option::<PrimitiveType>::from(*b)
                                    .expect("if the type requirement is none, there is a maximum"),
                            ))
                        }
                    }
                });
        })
        .or_insert(reqs_to_add);

    types_not_in_conflict
}

pub(super) fn merge_type_requirements(
    a: &mut PredicateTypeRequirements,
    b: PredicateTypeRequirements,
    // force_use_of_stricter_requirement: bool,
) -> Result<(), TypeError> {
    for (pred, reqs) in b {
        add_type_requirements(a, pred, reqs)?
    }

    Ok(())
}

pub(super) fn override_type_requirements(
    a: &mut PredicateTypeRequirements,
    b: PredicateTypeRequirements,
) {
    a.extend(b)
}

pub(super) fn requirements_from_pred_decls(
    decls: &HashMap<Identifier, Vec<PrimitiveType>>,
) -> Result<PredicateTypeRequirements, TypeError> {
    let mut type_requirements = HashMap::new();

    for (pred, types) in decls {
        add_type_requirements(
            &mut type_requirements,
            pred.clone(),
            types
                .iter()
                .copied()
                .map(TypeRequirement::Hard)
                .collect::<Vec<_>>(),
        )?;
    }

    Ok(type_requirements)
}

pub(super) fn requirements_from_imports<'a, T: Iterator<Item = &'a ImportSpec>>(
    imports: T,
) -> Result<PredicateTypeRequirements, TypeError> {
    let mut type_requirements = HashMap::new();

    for import_spec in imports {
        add_type_requirements(
            &mut type_requirements,
            import_spec.predicate().clone(),
            import_spec
                .type_constraint()
                .iter()
                .cloned()
                .map(TypeRequirement::from)
                .collect(),
        )?;
    }

    Ok(type_requirements)
}

pub(super) fn requirements_from_facts(
    facts: &Vec<ChaseFact>,
) -> Result<PredicateTypeRequirements, TypeError> {
    let mut fact_decls: PredicateTypeRequirements = HashMap::new();
    for fact in facts {
        let reqs_for_fact: Vec<TypeRequirement> = fact
            .terms()
            .iter()
            .map(|c| c.primitive_type())
            .map(|maybe_type| match maybe_type {
                Some(primitive_type) => TypeRequirement::Soft(primitive_type),
                None => TypeRequirement::None,
            })
            .collect();

        add_type_requirements(&mut fact_decls, fact.predicate(), reqs_for_fact)?;
    }

    Ok(fact_decls)
}

pub(super) fn requirements_from_literals_in_rules(
    rules: &Vec<ChaseRule>,
) -> Result<PredicateTypeRequirements, TypeError> {
    let mut literal_decls: PredicateTypeRequirements = HashMap::new();

    for chase_rule in rules {
        let constructors = chase_rule.constructors();
        let constraints: Vec<_> = chase_rule.all_constraints().collect();

        for chase_atom in chase_rule
            .all_body()
            .cloned()
            .map(Into::into)
            .chain(chase_rule.head().iter().cloned())
        {
            let reqs_for_atom: Vec<TypeRequirement> = chase_atom
                .terms()
                .iter()
                .map(|term| match term {
                    // TODO: should we respect other things here?
                    // variables nested in terms in constraints in particular?
                    PrimitiveTerm::Variable(v) => constructors
                        .iter()
                        .filter_map(|c| (c.variable() == v).then_some(c.term()))
                        .chain(constraints.iter().filter_map(|c| {
                            match c.left() {
                                Term::Primitive(PrimitiveTerm::Variable(var)) => (*term
                                    == PrimitiveTerm::Variable(var.clone()))
                                .then_some(c.right()),
                                _ => match c.right() {
                                    Term::Primitive(PrimitiveTerm::Variable(var)) => (*term
                                        == PrimitiveTerm::Variable(var.clone()))
                                    .then_some(c.left()),
                                    _ => None,
                                },
                            }
                        }))
                        .fold(None, |acc, t| {
                            let b = t.primitive_type();
                            acc.map(|a: PrimitiveType| b.map(|b| a.max_type(&b)).unwrap_or(a))
                                .or(b)
                        }),
                    _ => term.primitive_type(),
                })
                .map(|opt_t| {
                    opt_t
                        .map(TypeRequirement::Soft)
                        .unwrap_or(TypeRequirement::None)
                })
                .collect();

            add_type_requirements(&mut literal_decls, chase_atom.predicate(), reqs_for_atom)?;
        }
    }

    Ok(literal_decls)
}

pub(super) fn requirements_from_aggregates_in_rules(
    rules: &[ChaseRule],
) -> Result<PredicateTypeRequirements, TypeError> {
    let mut type_requirements = HashMap::new();

    for (rule, atom) in rules
        .iter()
        .flat_map(|rule| rule.head().iter().map(move |atom| (rule, atom)))
    {
        add_type_requirements(
            &mut type_requirements,
            atom.predicate(),
            atom.terms()
                .iter()
                .map(|term| {
                    if let PrimitiveTerm::Variable(Variable::Universal(identifier)) = term {
                        if identifier.name().starts_with(AGGREGATE_VARIABLE_PREFIX) {
                            let aggregate = rule
                                .aggregates()
                                .iter()
                                .find(|aggregate| aggregate.output_variable.name() == *identifier.0).expect("variable with aggregate prefix is missing an associated aggregate");
                            if
                                aggregate.aggregate_operation ==
                                AggregateOperation::Count
                            {
                                return TypeRequirement::Hard(PrimitiveType::Integer)
                            }
                        }
                    }
                    TypeRequirement::None
                })
                .collect(),
        )?;
    }

    Ok(type_requirements)
}

pub(super) fn requirements_from_existentials_in_rules(
    rules: &[ChaseRule],
) -> Result<PredicateTypeRequirements, TypeError> {
    let mut type_requirements = HashMap::new();

    for atom in rules.iter().flat_map(|r| r.head()) {
        add_type_requirements(
            &mut type_requirements,
            atom.predicate(),
            atom.terms()
                .iter()
                .map(|t| {
                    if matches!(t, PrimitiveTerm::Variable(Variable::Existential(_))) {
                        TypeRequirement::Hard(PrimitiveType::Any)
                    } else {
                        TypeRequirement::None
                    }
                })
                .collect::<Vec<_>>(),
        )?;
    }

    Ok(type_requirements)
}
