use std::collections::HashMap;

use crate::model::{
    chase_model::{ChaseAtom, ChaseProgram},
    types::error::TypeError,
    Identifier, PrimitiveTerm, PrimitiveType, Variable,
};

use self::{
    position_graph::PositionGraph,
    type_requirement::{
        merge_type_requirements, override_type_requirements, requirements_from_aggregates_in_rules,
        requirements_from_existentials_in_rules, requirements_from_facts,
        requirements_from_imports, requirements_from_literals_in_rules,
        requirements_from_pred_decls, TypeRequirement,
    },
};

mod position_graph;
mod type_requirement;

type PredicateTypes = HashMap<Identifier, Vec<PrimitiveType>>;
type VariableTypesForRules = Vec<HashMap<Variable, PrimitiveType>>;

/// The Type Inference Result consists of a pair of predicate types and a list of variable types
/// (for each rule) if successful; otherwise an error is returned
type TypeInferenceResult = Result<(PredicateTypes, VariableTypesForRules), TypeError>;

pub(super) fn infer_types(program: &ChaseProgram) -> TypeInferenceResult {
    let pred_reqs = requirements_from_pred_decls(program.parsed_predicate_declarations());
    let import_reqs = requirements_from_imports(program.imports());
    let fact_reqs = requirements_from_facts(program.facts());
    let literal_reqs = requirements_from_literals_in_rules(program.rules());
    let aggregate_reqs = requirements_from_aggregates_in_rules(program.rules());
    let existential_reqs = requirements_from_existentials_in_rules(program.rules());

    let mut type_requirements = import_reqs;
    merge_type_requirements(&mut type_requirements, fact_reqs)?;
    merge_type_requirements(&mut type_requirements, literal_reqs)?;
    override_type_requirements(&mut type_requirements, pred_reqs);
    merge_type_requirements(&mut type_requirements, aggregate_reqs)?;
    merge_type_requirements(&mut type_requirements, existential_reqs)?;

    for (predicate, arity) in program.get_all_predicates() {
        type_requirements
            .entry(predicate)
            .or_insert(vec![TypeRequirement::None; arity]);
    }

    let position_graph = PositionGraph::from_rules(program.rules());

    let type_requirements = position_graph.propagate_type_requirements(type_requirements)?;

    // All the types that are not set will be mapped to a default type
    let pred_types = type_requirements
        .into_iter()
        .map(|(predicate, types)| {
            (
                predicate,
                types
                    .into_iter()
                    .map(|t| Option::<PrimitiveType>::from(t).unwrap_or_default())
                    .collect(),
            )
        })
        .collect();

    // we check that everything is consistent
    position_graph.check_type_requirement_compatibility(&pred_types)?;

    let rule_var_types = get_rule_variable_types(program, &pred_types);

    check_for_incompatible_constant_types(program, &rule_var_types, &pred_types)?;
    check_for_nonnumeric_arithmetic(program, &rule_var_types)?;
    check_aggregate_types(program, &rule_var_types)?;

    Ok((pred_types, rule_var_types))
}

fn get_rule_variable_types(
    program: &ChaseProgram,
    pred_types: &PredicateTypes,
) -> VariableTypesForRules {
    program
        .rules()
        .iter()
        .map(|rule| {
            let mut variable_types: HashMap<Variable, PrimitiveType> = HashMap::new();

            for atom in rule.head() {
                for (term_position, term) in atom.terms().iter().enumerate() {
                    if let PrimitiveTerm::Variable(variable) = term {
                        variable_types.entry(variable.clone()).or_insert(
                            pred_types
                                .get(&atom.predicate())
                                .expect("Every predicate should have received type information.")
                                [term_position],
                        );
                    }
                }
            }
            for atom in rule.all_body() {
                for (term_position, variable) in atom.terms().iter().enumerate() {
                    variable_types.entry(variable.clone()).or_insert(
                        pred_types
                            .get(&atom.predicate())
                            .expect("Every predicate should have received type information.")
                            [term_position],
                    );
                }
            }
            variable_types
        })
        .collect()
}

fn check_for_incompatible_constant_types(
    program: &ChaseProgram,
    rule_var_types: &VariableTypesForRules,
    predicate_types: &PredicateTypes,
) -> Result<(), TypeError> {
    for fact in program.facts() {
        let predicate_types = predicate_types
            .get(&fact.predicate())
            .expect("Previous analysis should have assigned a type vector to each predicate.");

        for (term_index, constant) in fact.terms().iter().enumerate() {
            let logical_type = predicate_types[term_index];
            logical_type.ground_term_to_data_value_t(constant.clone())?;
        }
    }

    for (rule, var_types) in program.rules().iter().zip(rule_var_types) {
        for constraint in rule.all_constraints() {
            if let Some(example_variable) = constraint.variables().next() {
                let variable_type = var_types
                    .get(example_variable)
                    .expect("Previous analysis should have assigned a type to each variable.");

                for term in [constraint.left(), constraint.right()] {
                    for primitive_term in term.primitive_terms() {
                        if let PrimitiveTerm::Constant(constant) = primitive_term {
                            variable_type.ground_term_to_data_value_t(constant.clone())?;
                        }
                    }
                }
            }
        }

        for constructor in rule.constructors() {
            let variable_type = var_types
                .get(constructor.variable())
                .expect("Previous analysis should have assigned a type to each variable.");

            for term in constructor.term().primitive_terms() {
                if let PrimitiveTerm::Constant(constant) = term {
                    variable_type.ground_term_to_data_value_t(constant.clone())?;
                }
            }
        }
    }

    Ok(())
}

fn check_for_nonnumeric_arithmetic(
    program: &ChaseProgram,
    rule_var_types: &VariableTypesForRules,
) -> Result<(), TypeError> {
    for (rule, var_types) in program.rules().iter().zip(rule_var_types) {
        for constraint in rule.all_constraints() {
            if let Some(example_variable) = constraint.variables().next() {
                let variable_type = var_types
                    .get(example_variable)
                    .expect("Previous analysis should have assigned a type to each variable.");

                // Note: For now, separating this two checks doesn't make much sense
                // because every type that allows for instance an addition, also allows comparison such as <

                if constraint.is_numeric() && !variable_type.allows_numeric_operations() {
                    return Err(TypeError::InvalidRuleNonNumericComparison);
                }

                if (!constraint.left().is_primitive() || !constraint.right().is_primitive())
                    && !variable_type.allows_numeric_operations()
                {
                    return Err(TypeError::InvalidRuleNonNumericArithmetic);
                }
            }
        }

        for constructor in rule.constructors() {
            let variable_type = var_types
                .get(constructor.variable())
                .expect("Previous analysis should have assigned a type to each variable.");

            if !constructor.term().is_primitive() && !variable_type.allows_numeric_operations() {
                return Err(TypeError::InvalidRuleNonNumericArithmetic);
            }
        }
    }

    Ok(())
}

fn check_aggregate_types(
    program: &ChaseProgram,
    rule_var_types: &VariableTypesForRules,
) -> Result<(), TypeError> {
    for (rule, var_types) in program.rules().iter().zip(rule_var_types) {
        for aggregate in rule.aggregates() {
            let variable_type = var_types.get(&aggregate.input_variables[0]).expect(
                "Previous analysis should have assigned a type to each aggregate output variable.",
            );

            aggregate
                .logical_aggregate_operation
                .check_input_type(&aggregate.input_variables[0].name(), *variable_type)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        io::formats::dsv::DSVFormat,
        model::{
            chase_model::{
                ChaseFact, ChaseProgram, ChaseRule, Constructor, PrimitiveAtom, VariableAtom,
            },
            Constant, Constraint, Identifier, NumericLiteral, PrimitiveTerm, PrimitiveType, Term,
            TupleConstraint, Variable,
        },
        program_analysis::{analysis::get_fresh_rule_predicate, type_inference::infer_types},
    };

    type TestData = (
        (ChaseRule, ChaseRule, ChaseRule),
        (ChaseFact, ChaseFact, ChaseFact),
        (
            Identifier,
            Identifier,
            Identifier,
            Identifier,
            Identifier,
            Identifier,
            Identifier,
        ),
    );

    fn get_test_rules_and_facts_and_predicates() -> TestData {
        let a = Identifier("a".to_string());
        let b = Identifier("b".to_string());
        let c = Identifier("c".to_string());
        let r = Identifier("r".to_string());
        let s = Identifier("s".to_string());
        let t = Identifier("t".to_string());
        let q = Identifier("q".to_string());

        let x = Variable::Universal(Identifier("x".to_string()));
        let headop = Variable::Universal(Identifier("headop".to_string()));
        let z = Variable::Existential(Identifier("z".to_string()));

        let tx = PrimitiveTerm::Variable(x.clone());
        let theadop = PrimitiveTerm::Variable(headop.clone());
        let tz = PrimitiveTerm::Variable(z);

        let v42 = Variable::Universal(Identifier("PLACEHOLDER_42".to_string()));
        let c42 = Constant::NumericLiteral(NumericLiteral::Integer(42));
        let t42 = PrimitiveTerm::Constant(c42.clone());
        let tt42 = Term::Primitive(t42.clone());

        let c3 = Constant::NumericLiteral(NumericLiteral::Integer(3));

        let v7 = Variable::Universal(Identifier("PLACEHOLDER_7".to_string()));
        let c7 = Constant::NumericLiteral(NumericLiteral::Integer(7));
        let t7 = PrimitiveTerm::Constant(c7.clone());
        let tt7 = Term::Primitive(t7.clone());

        let tt55 = Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
            NumericLiteral::Integer(55),
        )));

        // A(x) :- B(x), C(x).
        let basic_rule = ChaseRule::new(
            vec![PrimitiveAtom::new(a.clone(), vec![tx.clone()])],
            vec![],
            vec![],
            vec![
                VariableAtom::new(b.clone(), vec![x.clone()]),
                VariableAtom::new(c.clone(), vec![x.clone()]),
            ],
            vec![],
            vec![],
            vec![],
        );

        // R(x, !z) :- A(x).
        let exis_rule = ChaseRule::new(
            vec![PrimitiveAtom::new(r.clone(), vec![tx.clone(), tz])],
            vec![],
            vec![],
            vec![VariableAtom::new(a.clone(), vec![x.clone()])],
            vec![],
            vec![],
            vec![],
        );

        // S(x, 55) :- T(42, x), Q(7).
        let rule_with_constant = ChaseRule::new(
            vec![PrimitiveAtom::new(s.clone(), vec![tx.clone(), theadop])],
            vec![Constructor::new(headop, tt55)],
            vec![],
            vec![
                VariableAtom::new(t.clone(), vec![v42.clone(), x]),
                VariableAtom::new(q.clone(), vec![v7.clone()]),
            ],
            vec![
                Constraint::Equals(Term::Primitive(PrimitiveTerm::Variable(v42)), tt42),
                Constraint::Equals(Term::Primitive(PrimitiveTerm::Variable(v7)), tt7),
            ],
            vec![],
            vec![],
        );

        let fact1 = ChaseFact::new(t.clone(), vec![c42.clone(), c3.clone()]);
        let fact2 = ChaseFact::new(t.clone(), vec![c3, c7.clone()]);
        let fact3 = ChaseFact::new(t.clone(), vec![c7, c42]);

        (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        )
    }

    #[test]
    fn infer_types_no_decl() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let no_decl = ChaseProgram::builder()
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .build();

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Any]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::Any, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&no_decl).unwrap().0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_a_string_decl() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let a_string_decl = ChaseProgram::builder()
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(a.clone(), vec![PrimitiveType::String])
            .build();

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::String]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::String, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&a_string_decl).unwrap().0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_a_int_decl() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let a_int_decl = ChaseProgram::builder()
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(a.clone(), vec![PrimitiveType::Integer])
            .build();

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Integer]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::Integer, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&a_int_decl).unwrap().0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_b_string_decl() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let b_string_decl = ChaseProgram::builder()
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(b.clone(), vec![PrimitiveType::String])
            .build();
        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::String]),
            (b, vec![PrimitiveType::String]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::String, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&b_string_decl).unwrap().0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_b_and_c_int_decl() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let b_integer_decl = ChaseProgram::builder()
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(b.clone(), vec![PrimitiveType::Integer])
            .predicate_declaration(c.clone(), vec![PrimitiveType::Integer])
            .build();

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Integer]),
            (b, vec![PrimitiveType::Integer]),
            (c, vec![PrimitiveType::Integer]),
            (r, vec![PrimitiveType::Integer, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&b_integer_decl).unwrap().0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_b_source_decl() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let b_source_decl = ChaseProgram::builder()
            .import(
                DSVFormat::csv()
                    .try_into_import(String::new(), b.clone(), TupleConstraint::from_arity(1))
                    .unwrap(),
            )
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .build();

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::String]),
            (b, vec![PrimitiveType::String]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::String, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&b_source_decl).unwrap().0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_c_explicit_decl_overrides_source_type() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let c_explicit_decl_overrides_source_type = ChaseProgram::builder()
            .import(
                DSVFormat::csv()
                    .try_into_import(String::new(), c.clone(), TupleConstraint::from_arity(1))
                    .unwrap(),
            )
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(b.clone(), vec![PrimitiveType::Integer])
            .predicate_declaration(c.clone(), vec![PrimitiveType::Integer])
            .build();

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Integer]),
            (b, vec![PrimitiveType::Integer]),
            (c, vec![PrimitiveType::Integer]),
            (r, vec![PrimitiveType::Integer, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&c_explicit_decl_overrides_source_type)
            .unwrap()
            .0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_a_and_c_conflict_with_implicit_source_decl() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, _b, c, _r, _s, _t, _q),
        ) = get_test_rules_and_facts_and_predicates();

        let a_and_c_conflict_with_implicit_source_decl = ChaseProgram::builder()
            .import(
                DSVFormat::csv()
                    .try_into_import(String::new(), c, TupleConstraint::from_arity(1))
                    .unwrap(),
            )
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(a, vec![PrimitiveType::Integer])
            .build();

        let inferred_types_res = infer_types(&a_and_c_conflict_with_implicit_source_decl);
        assert!(inferred_types_res.is_err());
    }

    #[test]
    fn infer_types_a_and_c_conflict_with_explicit_source_decl_that_would_be_compatible_the_other_way_around(
    ) {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, _b, c, _r, _s, _t, _q),
        ) = get_test_rules_and_facts_and_predicates();

        let a_and_c_conflict_with_explicit_source_decl_that_would_be_compatible_the_other_way_around =
            ChaseProgram::builder()
                .import(
                    DSVFormat::csv()
                        .try_into_import(
                            String::new(),
                            c,
                            TupleConstraint::at_least([PrimitiveType::Any]),
                        )
                        .unwrap(),
                )
                .rule(basic_rule)
                .rule(exis_rule)
                .rule(rule_with_constant)
                .fact(fact1)
                .fact(fact2)
                .fact(fact3)
                .predicate_declaration(a, vec![PrimitiveType::String])
                .build();

        let inferred_types_res =
            infer_types(&a_and_c_conflict_with_explicit_source_decl_that_would_be_compatible_the_other_way_around);
        assert!(inferred_types_res.is_err());
    }

    #[test]
    fn infer_types_a_and_b_source_decl_resolvable_conflict() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let a_and_b_source_decl_resolvable_conflict = ChaseProgram::builder()
            .import(
                DSVFormat::csv()
                    .try_into_import(String::new(), b.clone(), TupleConstraint::from_arity(1))
                    .unwrap(),
            )
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(a.clone(), vec![PrimitiveType::Any])
            .build();

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Any]),
            (b, vec![PrimitiveType::String]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::Any, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&a_and_b_source_decl_resolvable_conflict)
            .unwrap()
            .0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_r_source_decl_resolvable_conflict_with_exis() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let r_source_decl_resolvable_conflict_with_exis = ChaseProgram::builder()
            .import(
                DSVFormat::csv()
                    .try_into_import(String::new(), r.clone(), TupleConstraint::from_arity(2))
                    .unwrap(),
            )
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .build();

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Any]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::String, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&r_source_decl_resolvable_conflict_with_exis)
            .unwrap()
            .0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_b_and_c_conflict_decl() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (_a, b, c, _r, _s, _t, _q),
        ) = get_test_rules_and_facts_and_predicates();

        let b_and_c_conflict_decl = ChaseProgram::builder()
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(b, vec![PrimitiveType::Integer])
            .predicate_declaration(c, vec![PrimitiveType::String])
            .build();

        let inferred_types_res = infer_types(&b_and_c_conflict_decl);
        assert!(inferred_types_res.is_err());
    }

    #[test]
    fn infer_types_b_anc_c_conflict_decl_resolvable() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, r, s, t, q),
        ) = get_test_rules_and_facts_and_predicates();

        let b_and_c_conflict_decl_resolvable = ChaseProgram::builder()
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(b.clone(), vec![PrimitiveType::Any])
            .predicate_declaration(c.clone(), vec![PrimitiveType::String])
            .build();

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Any]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::String]),
            (r, vec![PrimitiveType::Any, PrimitiveType::Any]),
            (s, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (t, vec![PrimitiveType::Integer, PrimitiveType::Integer]),
            (q, vec![PrimitiveType::Integer]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = infer_types(&b_and_c_conflict_decl_resolvable).unwrap().0;
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_a_unresolvable_conflict_with_source_decls_of_b_and_c() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (a, b, c, _r, _s, _t, _q),
        ) = get_test_rules_and_facts_and_predicates();

        let a_unresolvable_conflict_with_source_decls_of_b_and_c = ChaseProgram::builder()
            .import(
                DSVFormat::csv()
                    .try_into_import(
                        String::new(),
                        b,
                        TupleConstraint::at_least([PrimitiveType::Integer]),
                    )
                    .unwrap(),
            )
            .import(
                DSVFormat::csv()
                    .try_into_import(
                        String::new(),
                        c,
                        TupleConstraint::at_least([PrimitiveType::Integer]),
                    )
                    .unwrap(),
            )
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(a, vec![PrimitiveType::Any])
            .build();

        let inferred_types_res = infer_types(&a_unresolvable_conflict_with_source_decls_of_b_and_c);
        assert!(inferred_types_res.is_err());
    }

    #[test]
    fn infer_types_s_decl_unresolvable_conflict_with_fact_values() {
        let (
            (basic_rule, exis_rule, rule_with_constant),
            (fact1, fact2, fact3),
            (_a, _b, _c, _r, s, _t, _q),
        ) = get_test_rules_and_facts_and_predicates();

        let s_decl_unresolvable_conflict_with_fact_values = ChaseProgram::builder()
            .rule(basic_rule)
            .rule(exis_rule)
            .rule(rule_with_constant)
            .fact(fact1)
            .fact(fact2)
            .fact(fact3)
            .predicate_declaration(s, vec![PrimitiveType::Any, PrimitiveType::Integer])
            .build();

        let inferred_types_res = infer_types(&s_decl_unresolvable_conflict_with_fact_values);
        assert!(inferred_types_res.is_err());
    }
}
