//! Walking components: every term a component holds, nested ones
//! included, and every variable it mentions.
//!
//! A term's subterms are an operation's arguments, the arguments of a
//! function term, a tuple's elements, a map's keys and values, and an
//! aggregate's input and value. An option key of an import or an export
//! names the option, not a value of the program, so walks leave it out.

use crate::model::{
    Aggregate, Atom, Attribute, Directive, Fact, Literal, Operation, Program, Rule, Statement,
    Term, TermKind, Variable, VariableKind,
};

/// The terms of a component, visited in place.
///
/// # Example
///
/// ```
/// use nemo::model::{Rule, Walk};
///
/// let rule = Rule::parse("p(?x) :- q(?x, ?y), ?y > 3 .").expect("the rule parses");
/// let variables: Vec<String> = rule
///     .variables()
///     .iter()
///     .map(|variable| variable.to_string())
///     .collect();
/// assert_eq!(variables, ["?x", "?x", "?y", "?y"]);
/// ```
pub trait Walk {
    /// Calls `visit` on every term of the component, each before its
    /// subterms.
    fn walk<'a>(&'a self, visit: &mut impl FnMut(&'a Term));

    /// Calls `visit` on every term of the component, mutably, each after
    /// its subterms.
    ///
    /// A term that `visit` replaces is not walked again.
    fn walk_mut(&mut self, visit: &mut impl FnMut(&mut Term));

    /// Returns whether some term of the component, nested ones
    /// included, passes the test.
    fn any_term(&self, mut test: impl FnMut(&Term) -> bool) -> bool {
        let mut found = false;
        self.walk(&mut |term| found = found || test(term));
        found
    }

    /// Returns every variable the component mentions, repetitions
    /// included: the variables of its terms, and the variables an
    /// aggregate's scope names.
    fn variables(&self) -> Vec<&Variable> {
        let mut variables = Vec::new();
        self.walk(&mut |term| match &term.kind {
            TermKind::Variable(variable) => variables.push(variable),
            TermKind::Aggregate(Aggregate { scope, .. }) => variables.extend(scope),
            _ => {}
        });
        variables
    }
}

impl Term {
    /// Returns the term's immediate subterms.
    pub fn subterms(&self) -> Box<dyn Iterator<Item = &Term> + '_> {
        match &self.kind {
            TermKind::Operation(operation) => Box::new(operation.arguments.iter()),
            TermKind::Function { arguments, .. } | TermKind::Tuple(arguments) => {
                Box::new(arguments.iter())
            }
            TermKind::Map { pairs, .. } => {
                Box::new(pairs.iter().flat_map(|(key, value)| [key, value]))
            }
            TermKind::Aggregate(aggregate) => Box::new(
                aggregate
                    .input
                    .iter()
                    .chain(aggregate.value.iter())
                    .map(|term| &**term),
            ),
            _ => Box::new(std::iter::empty()),
        }
    }

    /// Returns whether the term contains no aggregate and no variable other than a global one.
    ///
    /// Global variables are parameters, so they have a fixed value.
    pub fn is_ground(&self) -> bool {
        !self.any_term(|term| match &term.kind {
            TermKind::Variable(variable) => variable.kind != VariableKind::Global,
            TermKind::Aggregate(_) => true,
            _ => false,
        })
    }

    /// Returns the term's immediate subterms, mutably.
    pub fn subterms_mut(&mut self) -> Box<dyn Iterator<Item = &mut Term> + '_> {
        match &mut self.kind {
            TermKind::Operation(operation) => Box::new(operation.arguments.iter_mut()),
            TermKind::Function { arguments, .. } | TermKind::Tuple(arguments) => {
                Box::new(arguments.iter_mut())
            }
            TermKind::Map { pairs, .. } => {
                Box::new(pairs.iter_mut().flat_map(|(key, value)| [key, value]))
            }
            TermKind::Aggregate(aggregate) => Box::new(
                aggregate
                    .input
                    .iter_mut()
                    .chain(aggregate.value.iter_mut())
                    .map(|term| &mut **term),
            ),
            _ => Box::new(std::iter::empty()),
        }
    }
}

impl Walk for Term {
    fn walk<'a>(&'a self, visit: &mut impl FnMut(&'a Term)) {
        visit(self);
        for subterm in self.subterms() {
            subterm.walk(visit);
        }
    }

    fn walk_mut(&mut self, visit: &mut impl FnMut(&mut Term)) {
        for subterm in self.subterms_mut() {
            subterm.walk_mut(visit);
        }
        visit(self);
    }
}

/// Implements [Walk] for a component by walking the listed parts.
macro_rules! walk_parts {
    ($type:ty, $component:ident => $parts:expr, $parts_mut:expr) => {
        impl Walk for $type {
            fn walk<'a>(&'a self, visit: &mut impl FnMut(&'a Term)) {
                let $component = self;
                for part in $parts {
                    part.walk(visit);
                }
            }

            fn walk_mut(&mut self, visit: &mut impl FnMut(&mut Term)) {
                let $component = self;
                for part in $parts_mut {
                    part.walk_mut(visit);
                }
            }
        }
    };
}

walk_parts!(Operation, operation => &operation.arguments, &mut operation.arguments);
walk_parts!(Atom, atom => &atom.terms, &mut atom.terms);
walk_parts!(Fact, fact => &fact.terms, &mut fact.terms);
walk_parts!(Attribute, attribute => &attribute.arguments, &mut attribute.arguments);
walk_parts!(Program, program => &program.statements, &mut program.statements);

impl Walk for Aggregate {
    fn walk<'a>(&'a self, visit: &mut impl FnMut(&'a Term)) {
        for term in self.input.iter().chain(self.value.iter()) {
            term.walk(visit);
        }
    }

    fn walk_mut(&mut self, visit: &mut impl FnMut(&mut Term)) {
        for term in self.input.iter_mut().chain(self.value.iter_mut()) {
            term.walk_mut(visit);
        }
    }
}

impl Walk for Literal {
    fn walk<'a>(&'a self, visit: &mut impl FnMut(&'a Term)) {
        match self {
            Literal::Positive(atom) | Literal::Negative(atom) => atom.walk(visit),
            Literal::Operation(operation) => operation.walk(visit),
        }
    }

    fn walk_mut(&mut self, visit: &mut impl FnMut(&mut Term)) {
        match self {
            Literal::Positive(atom) | Literal::Negative(atom) => atom.walk_mut(visit),
            Literal::Operation(operation) => operation.walk_mut(visit),
        }
    }
}

impl Walk for Rule {
    fn walk<'a>(&'a self, visit: &mut impl FnMut(&'a Term)) {
        for attribute in &self.attributes {
            attribute.walk(visit);
        }
        for atom in &self.head {
            atom.walk(visit);
        }
        for literal in &self.body {
            literal.walk(visit);
        }
    }

    fn walk_mut(&mut self, visit: &mut impl FnMut(&mut Term)) {
        for attribute in &mut self.attributes {
            attribute.walk_mut(visit);
        }
        for atom in &mut self.head {
            atom.walk_mut(visit);
        }
        for literal in &mut self.body {
            literal.walk_mut(visit);
        }
    }
}

impl Walk for Directive {
    fn walk<'a>(&'a self, visit: &mut impl FnMut(&'a Term)) {
        match self {
            Directive::Import(import) => {
                for (_, value) in &import.options {
                    value.walk(visit);
                }
                for (_, value) in &import.guards {
                    value.walk(visit);
                }
                for atom in &import.bindings {
                    atom.walk(visit);
                }
            }
            Directive::Export(export) => {
                for (_, value) in &export.options {
                    value.walk(visit);
                }
                for (_, value) in &export.guards {
                    value.walk(visit);
                }
            }
            Directive::Parameter(parameter) => {
                if let Some(value) = &parameter.value {
                    value.walk(visit);
                }
            }
            Directive::Use(using) => {
                for (_, value) in &using.values {
                    value.walk(visit);
                }
            }
            Directive::Define(define) => define.body.walk(visit),
            Directive::Base(_)
            | Directive::Prefix(_)
            | Directive::Output(_)
            | Directive::Input(_)
            | Directive::Declare(_)
            | Directive::Type(_)
            | Directive::Extern(_) => {}
        }
    }

    fn walk_mut(&mut self, visit: &mut impl FnMut(&mut Term)) {
        match self {
            Directive::Import(import) => {
                for (_, value) in &mut import.options {
                    value.walk_mut(visit);
                }
                for (_, value) in &mut import.guards {
                    value.walk_mut(visit);
                }
                for atom in &mut import.bindings {
                    atom.walk_mut(visit);
                }
            }
            Directive::Export(export) => {
                for (_, value) in &mut export.options {
                    value.walk_mut(visit);
                }
                for (_, value) in &mut export.guards {
                    value.walk_mut(visit);
                }
            }
            Directive::Parameter(parameter) => {
                if let Some(value) = &mut parameter.value {
                    value.walk_mut(visit);
                }
            }
            Directive::Use(using) => {
                for (_, value) in &mut using.values {
                    value.walk_mut(visit);
                }
            }
            Directive::Define(define) => define.body.walk_mut(visit),
            Directive::Base(_)
            | Directive::Prefix(_)
            | Directive::Output(_)
            | Directive::Input(_)
            | Directive::Declare(_)
            | Directive::Type(_)
            | Directive::Extern(_) => {}
        }
    }
}

impl Walk for Statement {
    fn walk<'a>(&'a self, visit: &mut impl FnMut(&'a Term)) {
        match self {
            Statement::Fact(fact) => fact.walk(visit),
            Statement::Rule(rule) => rule.walk(visit),
            Statement::Directive(directive) => directive.walk(visit),
        }
    }

    fn walk_mut(&mut self, visit: &mut impl FnMut(&mut Term)) {
        match self {
            Statement::Fact(fact) => fact.walk_mut(visit),
            Statement::Rule(rule) => rule.walk_mut(visit),
            Statement::Directive(directive) => directive.walk_mut(visit),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::model::VariableKind;

    #[test]
    fn walks_reach_into_every_nesting() {
        let rule =
            Rule::parse("p(?a, #sum(?b per ?c)) :- q(?d, (?e, f(?g)), {k = ?h}), ?i > abs(?j) .")
                .expect("the rule parses");
        let mut names: Vec<String> = rule
            .variables()
            .iter()
            .filter(|variable| variable.kind == VariableKind::Universal)
            .map(|variable| variable.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, ["a", "b", "c", "d", "e", "g", "h", "i", "j"]);
    }

    #[test]
    fn walks_visit_terms_before_their_subterms_and_mutably_after() {
        let term = Term::parse("f(?x, g(?y))").expect("the term parses");
        let mut order = Vec::new();
        term.walk(&mut |term| order.push(term.to_string()));
        assert_eq!(order, ["f(?x, g(?y))", "?x", "g(?y)", "?y"]);

        let mut term = term;
        let mut order = Vec::new();
        term.walk_mut(&mut |term| order.push(term.to_string()));
        assert_eq!(order, ["?x", "?y", "g(?y)", "f(?x, g(?y))"]);
    }

    #[test]
    fn directives_walk_their_values_but_not_their_option_keys() {
        let program = Program::parse(
            "@import p from csv { resource = \"p.csv\" } .",
            crate::model::FileId::new(0),
        )
        .0;
        let mut seen = Vec::new();
        program.walk(&mut |term| seen.push(term.to_string()));
        assert_eq!(seen, ["\"p.csv\""]);
    }
}
