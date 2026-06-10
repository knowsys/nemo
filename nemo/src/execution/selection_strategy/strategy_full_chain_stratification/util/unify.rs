use crate::rule_model::{components::term::primitive::Primitive, substitution::Substitution};

/// this is needed because BodyAtoms will have iterator of owned Primitives (converted from Variables) and HeadAtoms will have
impl AsRef<Primitive> for Primitive {
    fn as_ref(&self) -> &Primitive {
        self
    }
}

/// Extend MGU eta to also unify the given iterators of terms
/// terms_a and terms_b should correspond to atoms wih the same predicate symbol, i.e. the same arity
/// thus, unify(.) assumes that both iterators have the same number of values
///
/// in cas body atoms are to be unified, their terms: Vec<Variable> have to be converted
/// maybe look for euqlities in operations to get back at the constants in the body?
///
/// Try to preserve variables of atom_a by mapping variables of atom_b onto them
/// We will produce an MGU with $dom(\eta) \cap ran(\eta) = \varnothing$, i.e. A->B means that B is not replaced
/// maybe resuse Nemo's substitution for VariableMapping?
pub fn unify<'a, T, U>(
    terms_a: impl Iterator<Item = T>,
    terms_b: impl Iterator<Item = U>,
    mut eta: Substitution,
) -> Option<Substitution>
where
    T: AsRef<Primitive>,
    U: AsRef<Primitive>,
{
    for (term_a, term_b) in terms_a.zip(terms_b) {
        let (term_a, term_b) = (term_a.as_ref(), term_b.as_ref());
        if term_a != term_b {
            let mapped_term_a_opt = eta.get_primitive(term_a).cloned();
            let mapped_term_b_opt = eta.get_primitive(term_b).cloned();
            match mapped_term_a_opt {
                Some(mapped_term_a) => match mapped_term_a {
                    Primitive::Ground(_) => match mapped_term_b_opt {
                        Some(mapped_term_b) => match mapped_term_b {
                            Primitive::Ground(_) => {
                                if mapped_term_a != mapped_term_b {
                                    log::trace!(
                                        "cannot assign constant {mapped_term_a} to constant {mapped_term_b}"
                                    );
                                    return None;
                                }
                            }
                            Primitive::Variable(_) => {
                                log::trace!(
                                    "remap {term_b} from {mapped_term_b} to constant {mapped_term_a}"
                                );
                                eta.remap(mapped_term_b, mapped_term_a);
                            }
                        },
                        None => {
                            log::trace!("map {term_b} to constant {mapped_term_a}");
                            eta.remap(term_b.clone(), mapped_term_a);
                        }
                    },
                    Primitive::Variable(_) => {
                        match mapped_term_b_opt {
                            Some(mapped_term_b) => {
                                match mapped_term_b {
                                    Primitive::Ground(_) => {
                                        log::trace!(
                                            "remap {term_a} from {mapped_term_a} to constant {mapped_term_b}"
                                        );
                                        eta.remap(mapped_term_a, mapped_term_b);
                                    }
                                    Primitive::Variable(_) => {
                                        if mapped_term_a != mapped_term_b {
                                            log::trace!(
                                                "remap {term_b} from {mapped_term_b} to variable {mapped_term_a}"
                                            );
                                            // have choice, so leave mapping for term_a in place and change it for term_b!
                                            eta.remap(mapped_term_b, mapped_term_a);
                                        }
                                    }
                                }
                            }
                            None => {
                                if *term_b != mapped_term_a {
                                    log::trace!("map {term_b} to variable {mapped_term_a}");
                                    eta.remap(term_b.clone(), mapped_term_a);
                                }
                            }
                        }
                    }
                },
                None => {
                    match mapped_term_b_opt {
                        Some(mapped_term_b) => {
                            if *term_a != mapped_term_b {
                                log::trace!(
                                    "map {term_a} to {} {mapped_term_b}",
                                    if matches!(mapped_term_b, Primitive::Ground(_)) {
                                        "constant"
                                    } else {
                                        "variable"
                                    }
                                );
                                eta.remap(term_a.clone(), mapped_term_b);
                            }
                        }
                        None => {
                            if term_b != term_a {
                                log::trace!("map {term_b} to variable {term_a}");
                                // have choice, so map term_b to term_a!
                                eta.remap(term_b.clone(), term_a.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    Some(eta)
}
