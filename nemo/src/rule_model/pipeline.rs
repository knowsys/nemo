//! This module defines [ProgramPipeline].

use commit::ProgramCommit;
use id::ProgramComponentId;
use state::{ExtendStatementValidity, ProgramState};
use transformations::ProgramTransformation;

use crate::{
    error::warned::Warned, parser::Parser, rule_file::RuleFile,
    rule_model::translation::ASTProgramTranslation,
};

use super::{
    components::{
        atom::Atom,
        fact::Fact,
        import_export::{ExportDirective, ImportDirective},
        output::Output,
        parameter::ParameterDeclaration,
        rule::Rule,
        IterableComponent, ProgramComponent,
    },
    error::ValidationReport,
    program::{Program, ProgramRead, ProgramWrite},
    translation::ProgramParseReport,
};

pub mod commit;
pub mod id;
pub mod state;
pub mod transformations;

/// Big manager object
#[derive(Debug)]
pub struct ProgramPipeline {
    /// Contains state of the program at every commit
    state: ProgramState,

    /// Current id
    current_id: ProgramComponentId,
}

impl Default for ProgramPipeline {
    fn default() -> Self {
        Self {
            state: ProgramState::new(),
            current_id: ProgramComponentId::start(),
        }
    }
}

impl ProgramPipeline {
    /// Initilaize a [ProgramPipeline] with the contents of a given [RuleFile].
    pub fn file(file: &RuleFile) -> Result<Warned<Self, ProgramParseReport>, ProgramParseReport> {
        let parser = Parser::initialize(file.content());
        let ast = parser.parse().map_err(|(_tail, report)| report)?;

        let translation = ASTProgramTranslation::default();
        Ok(translation.translate::<Self>(&ast)?.into())
    }
}

impl ProgramPipeline {
    /// Search for the [ProgramComponent] with a given [ProgramComponentId].
    ///
    /// Returns `None` if there is no  [ProgramComponent] with that [ProgramComponentId].
    fn find_child_component(
        component: &dyn IterableComponent,
        id: ProgramComponentId,
    ) -> Option<&dyn ProgramComponent> {
        let mut child_iterator = component.children();
        let mut previous_component = child_iterator.next()?;
        for current_component in child_iterator {
            if current_component.id() > id {
                break;
            }

            previous_component = current_component;
        }

        if previous_component.id() == id {
            Some(previous_component)
        } else if previous_component.id() < id {
            Self::find_child_component(previous_component, id)
        } else {
            None
        }
    }

    /// Search for the [ProgramComponent] with a given [ProgramComponentId].
    ///
    /// Returns `None` if there is no  [ProgramComponent] with that [ProgramComponentId].
    pub fn find_component(&self, id: ProgramComponentId) -> Option<&dyn ProgramComponent> {
        Self::find_child_component(&self.state, id)
    }

    /// Given a [ProgramComponentId] return the corresponding [Rule],
    /// if it exists.
    pub fn rule_by_id(&self, id: ProgramComponentId) -> Option<&Rule> {
        self.find_component(id)
            .and_then(|component| component.try_as_ref())
    }

    /// Given a [ProgramComponentId] return the corresponding [Atom],
    /// if it exists.
    pub fn atom_by_id(&self, id: ProgramComponentId) -> Option<&Atom> {
        self.find_component(id)
            .and_then(|component| component.try_as_ref())
    }
}

impl ProgramPipeline {
    /// Assigns a [ProgramComponentId] to every (sub) [ProgramComponent].
    fn register_component(&mut self, component: &mut dyn ProgramComponent) -> ProgramComponentId {
        let id = self.current_id.increment();
        component.set_id(id);

        for sub in component.children_mut() {
            self.register_component(sub);
        }

        id
    }

    /// Add a [Rule] to the current program.
    pub fn add_rule(&mut self, mut rule: Rule) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut rule;
        let id = self.register_component(component);

        self.state.add_rule(rule);

        id
    }

    /// Add a [Fact] to the current program.
    pub fn add_fact(&mut self, mut fact: Fact) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut fact;
        let id = self.register_component(component);

        self.state.add_fact(fact);

        id
    }

    /// Add a [ImportDirective] to the current program.
    pub fn add_import(&mut self, mut import: ImportDirective) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut import;
        let id = self.register_component(component);

        self.state.add_import(import);

        id
    }

    /// Add a [ImportDirective] to the current program.
    pub fn add_export(&mut self, mut export: ExportDirective) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut export;
        let id = self.register_component(component);

        self.state.add_export(export);

        id
    }

    /// Add a [Output] to the current program.
    pub fn add_output(&mut self, mut output: Output) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut output;
        let id = self.register_component(component);

        self.state.add_output(output);

        id
    }

    /// Add a [ParameterDeclaration] to the current program.
    pub fn add_parameter(&mut self, mut parameter: ParameterDeclaration) -> ProgramComponentId {
        let component: &mut dyn ProgramComponent = &mut parameter;
        let id = self.register_component(component);

        self.state.add_parameter(parameter);

        id
    }

    /// Prepare a new commit.
    pub fn prepare(&mut self, extend: ExtendStatementValidity) {
        self.state.prepare(extend);
    }

    /// Apply a commit.
    pub fn commit(&mut self, commit: ProgramCommit) {
        for id in commit.deleted {
            self.state.delete(id);
        }

        for id in commit.keep {
            self.state.keep(id);
        }

        for rule in commit.rules {
            self.add_rule(rule);
        }

        for fact in commit.facts {
            self.add_fact(fact);
        }

        for import in commit.imports {
            self.add_import(import);
        }

        for export in commit.exports {
            self.add_export(export);
        }

        for output in commit.outputs {
            self.add_output(output);
        }

        for parameter in commit.parameters {
            self.add_parameter(parameter);
        }
    }

    /// Apply a [ProgramTransformation].
    pub fn apply_transformation<Transformation: ProgramTransformation>(
        &mut self,
        report: &mut ValidationReport,
        transformation: Transformation,
    ) {
        self.prepare(transformation.keep());

        let mut commit = ProgramCommit::default();
        transformation.apply(&mut commit, report, self);

        self.commit(commit);
    }

    /// Return the currently valid program.
    pub fn finalize(self) -> Program {
        self.state.finalize()
    }
}

impl ProgramWrite for ProgramPipeline {
    fn add_export(&mut self, export: ExportDirective) {
        self.add_export(export);
    }

    fn add_import(&mut self, import: ImportDirective) {
        self.add_import(import);
    }

    fn add_output(&mut self, output: Output) {
        self.add_output(output);
    }

    fn add_parameter_declaration(&mut self, parameter: ParameterDeclaration) {
        self.add_parameter(parameter);
    }

    fn add_rule(&mut self, rule: Rule) {
        self.add_rule(rule);
    }

    fn add_fact(&mut self, fact: Fact) {
        self.add_fact(fact);
    }
}

impl ProgramRead for ProgramPipeline {
    fn imports(&self) -> impl Iterator<Item = &ImportDirective> {
        self.state.imports()
    }

    fn exports(&self) -> impl Iterator<Item = &ExportDirective> {
        self.state.exports()
    }

    fn rules(&self) -> impl Iterator<Item = &Rule> {
        self.state.rules()
    }

    fn facts(&self) -> impl Iterator<Item = &Fact> {
        self.state.facts()
    }

    fn outputs(&self) -> impl Iterator<Item = &Output> {
        self.state.outputs()
    }

    fn parameters(&self) -> impl Iterator<Item = &ParameterDeclaration> {
        self.state.parameters()
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::components::{
        atom::Atom, literal::Literal, rule::Rule, ComponentIdentity,
    };

    use super::ProgramPipeline;

    #[test]
    fn find_atom() {
        let mut pipeline = ProgramPipeline::default();

        let head_atom = Atom::new("head".into(), vec![]);
        let body_atom = Atom::new("body".into(), vec![]);

        let rule = Rule::new(vec![head_atom], vec![Literal::Positive(body_atom)]);
        let rule_id = pipeline.add_rule(rule);
        let rule = pipeline.rule_by_id(rule_id).unwrap();

        let head = &rule.head()[0];
        let head = pipeline.atom_by_id(head.id()).unwrap();

        assert_eq!(head.predicate().to_string(), "head".to_owned());
    }
}
