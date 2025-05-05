//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

use std::{
    collections::{hash_map, HashMap},
    fmt::{self, Debug},
};

use crate::{
    rule_model::{error::ValidationErrorBuilder, origin::Origin, pipeline::id::ProgramComponentId},
    syntax,
};

use super::{
    tag::Tag, term::primitive::ground::GroundTerm, ComponentBehavior, ComponentIdentity,
    IterableComponent, ProgramComponent, ProgramComponentKind,
};

/// Direction of import/export activities.
/// We often share code for the two directions, and a direction
/// is then used to enable smaller distinctions where needed.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    /// Processing input.
    Import,
    /// Processing output.
    Export,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Import => f.write_str("import"),
            Direction::Export => f.write_str("export"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ImportExportSpec {
    format: Tag,
    keys: HashMap<String, (Origin, usize)>,
    values: Vec<GroundTerm>,
    origin: Origin,
}

impl ImportExportSpec {
    pub(crate) fn new(origin: Origin, format: Tag) -> Self {
        ImportExportSpec {
            format,
            origin,
            keys: Default::default(),
            values: Default::default(),
        }
    }

    pub(crate) fn format_tag(&self) -> &Tag {
        &self.format
    }

    pub(crate) fn push_attribute(
        &mut self,
        key: (String, Origin),
        value: GroundTerm,
    ) -> Option<(Origin, &GroundTerm)> {
        let index = self.values.len();
        self.values.push(value);

        match self.keys.entry(key.0) {
            hash_map::Entry::Occupied(mut entry) => {
                let (origin, index) = entry.insert((key.1, index));
                Some((origin, &self.values[index]))
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert((key.1, index));
                None
            }
        }
    }

    pub(crate) fn origin(&self) -> &Origin {
        &self.origin
    }

    pub(crate) fn key_value(&self) -> impl Iterator<Item = (Tag, &GroundTerm)> {
        // self.keys
        //     .iter()
        //     .map(|(k, (origin, idx))| (Tag::new(k.into()).set_origin(*origin), &self.values[*idx]))

        std::iter::empty()
    }
}

impl fmt::Display for ImportExportSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{} {{", self.format.name()))?;

        for (idx, (key, value)) in self.key_value().enumerate() {
            f.write_fmt(format_args!("{}: {}", key, value))?;

            if idx < self.keys.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str("}")
    }
}

/// An import/export specification. This object captures all information that is typically
/// present in an import or export directive in a Nemo program, including the main format,
/// optional attributes that define additional parameters, and an indentifier to map the data
/// to or from (i.e., a predicate name).
#[derive(Clone)]
pub(crate) struct ImportExportDirective {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// The predicate we're handling.
    predicate: Tag,
    /// The specified format and import/export attributes
    spec: ImportExportSpec,
}

impl Debug for ImportExportDirective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportExportDirective")
            .field("origin", &self.origin)
            .field("predicate", &self.predicate)
            .field("spec", &self.spec)
            .finish()
    }
}

/// An import specification.
#[derive(Debug, Clone)]
pub struct ImportDirective(pub(crate) ImportExportDirective);

impl ImportDirective {
    /// Create a new [ImportDirective].
    pub(crate) fn new(predicate: Tag, spec: ImportExportSpec) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            predicate,
            spec,
        })
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the attributes.
    pub(crate) fn attributes(&self) -> &ImportExportSpec {
        &self.0.spec
    }
}

impl From<ImportExportDirective> for ImportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl fmt::Display for ImportDirective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "@import {} :- {}.",
            self.predicate(),
            self.attributes()
        ))
    }
}

impl ComponentBehavior for ImportDirective {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Import
    }

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()> {
        // ImportExportBuilder::new(self.0.spec.clone(), Direction::Import, builder)
        todo!()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for ImportDirective {
    fn id(&self) -> ProgramComponentId {
        self.0.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.0.id = id;
    }

    fn origin(&self) -> &Origin {
        &self.0.origin
    }

    fn set_origin(&mut self, origin: Origin) {
        self.0.origin = origin;
    }
}

impl IterableComponent for ImportDirective {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        Box::new(std::iter::empty()) // TODO: ?
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        Box::new(std::iter::empty())
    }
}

/// An export specification.
#[derive(Debug, Clone)]
pub struct ExportDirective(pub(crate) ImportExportDirective);

impl ExportDirective {
    pub(crate) fn new(predicate: Tag, spec: ImportExportSpec) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            predicate,
            spec,
        })
    }

    pub fn new_csv(predicate: Tag) -> Self {
        let spec = ImportExportSpec::new(
            Origin::Created,
            Tag::new(syntax::import_export::file_format::CSV.into()),
        );

        Self::new(predicate, spec)
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the attributes.
    pub(crate) fn attributes(&self) -> &ImportExportSpec {
        &self.0.spec
    }
}

impl ComponentBehavior for ExportDirective {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Export
    }

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()> {
        // ImportExportBuilder::new(self.0.spec.clone(), Direction::Export, builder)
        todo!()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for ExportDirective {
    fn id(&self) -> ProgramComponentId {
        self.0.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.0.id = id;
    }

    fn origin(&self) -> &Origin {
        &self.0.origin
    }

    fn set_origin(&mut self, origin: Origin) {
        self.0.origin = origin;
    }
}

impl IterableComponent for ExportDirective {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        Box::new(std::iter::empty())
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        Box::new(std::iter::empty())
    }
}

impl From<ImportExportDirective> for ExportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl fmt::Display for ExportDirective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "@export {} :- {}.",
            self.predicate(),
            self.attributes()
        ))
    }
}

// impl ProgramComponent for ExportDirective {
//     type ValidationResult = ImportExportBuilder;

//     fn origin(&self) -> &Origin {
//         &self.0.origin
//     }

//     fn set_origin(mut self, origin: Origin) -> Self
//     where
//         Self: Sized,
//     {
//         self.0.origin = origin;
//         self
//     }

//     fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<ImportExportBuilder>
//     where
//         Self: Sized,
//     {
//         ImportExportBuilder::new(self.0.spec.clone(), Direction::Export, builder)
//     }

//     fn kind(&self) -> ProgramComponentKind {
//         ProgramComponentKind::Export
//     }
// }
