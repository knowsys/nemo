//! The module files: how a `@use` finds its module's text,
//! how the text is read once per link and registered under the file's path,
//! how each instance parses it under an id of its own,
//! and how a module's relative import paths resolve against the module's directory.

use std::{
    fs,
    path::{Path, PathBuf},
};

use nemo_physical::diagnostics::{Diagnostic, Severity};

use crate::{
    diagnostics::{AboutResource, EngineError, ModuleError},
    engine::link::{
        Linker,
        location::{Location, root_of},
    },
    model::{Directive, FileId, Identifier, Program, Statement, TermKind, UseDirective},
};

/// Resolves the relative resource of a module's `@import` against the directory
/// the module was read from, so a module can ship the data it reads.
///
/// The rewritten path is *absolute*.
/// The linked program is translated against the import directory of the using program,
/// which would otherwise be joined onto a relative path a second time,
/// reading a file next to the using program rather than next to the module, or none at all.
/// An absolute path is unchanged by that join.
///
/// Absolute paths and URLs are left unchanged.
pub(super) fn anchor_import_resources(statement: &mut Statement, directory: &Path) {
    let Statement::Directive(Directive::Import(import)) = statement else {
        return;
    };
    for (key, value) in &mut import.options {
        let TermKind::Constant(Identifier::Bare(name)) = &key.kind else {
            continue;
        };
        if name != "resource" {
            continue;
        }
        // A resource is written as a string or as an IRI, and the
        // engine takes both.
        let resource = match &value.kind {
            TermKind::String(resource) => resource.clone(),
            TermKind::Constant(Identifier::Iri(resource)) => resource.clone(),
            _ => continue,
        };
        if Path::new(&resource).is_absolute() || resource.contains("://") {
            continue;
        }
        let joined = directory.join(&resource);
        let Ok(absolute) = std::path::absolute(&joined) else {
            continue;
        };
        value.kind = TermKind::String(identity(&absolute));
    }
}

/// Returns the directory a file's relative paths resolve against:
/// its parent, or the current directory if it has none.
pub(in crate::engine) fn directory_of(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

impl Linker<'_> {
    /// Resolves the module a `@use` written in the directory `from` names.
    ///
    /// `program` is the directory of the program containing the outermost `@use`.
    /// Returns an error for a name leading out of a root,
    /// and for a name beginning with a root while a directory of the same name lies in `from`.
    pub(super) fn locate(
        &self,
        directive: &UseDirective,
        from: &Location,
        program: &Path,
    ) -> Result<Location, Diagnostic> {
        let location = Location::resolve(&directive.module, from, self.roots)
            .map_err(|problem| problem.at(directive.span))?;
        if let Some(root) = root_of(&directive.module, self.roots)
            && self.is_directory(from, root, program)
        {
            return Err(ModuleError::AmbiguousRoot {
                root: root.to_owned(),
            }
            .at(directive.span));
        }
        Ok(location)
    }

    /// Returns whether a directory of the given name lies in the directory `from`:
    /// on disk, or holding a registered module text.
    fn is_directory(&self, from: &Location, name: &str, program: &Path) -> bool {
        let mut location = from.clone();
        location.segments.push(name.to_owned());
        let registered = self.texts.keys().any(|text| {
            text.root == location.root
                && text.parents == location.parents
                && text.segments.len() > location.segments.len()
                && text.segments.starts_with(&location.segments)
        });
        let directory = from.path(from.tree(program, self.roots));
        registered
            || (matches!(entry(&directory, name), Entry::Exact) && directory.join(name).is_dir())
    }

    /// Reads a module's text once per link, and returns the id it is registered under.
    ///
    /// A text registered with [add_module](crate::engine::Engine::add_module)
    /// is used instead of the file.
    /// The source is named by the module file's path either way.
    pub(super) fn read(
        &mut self,
        location: &Location,
        tree: &Path,
        directive: &UseDirective,
    ) -> Result<FileId, Diagnostic> {
        let path = location.file(tree);
        let name = identity(&path);
        if let Some(&file) = self.files.get(&name) {
            return Ok(file);
        }
        let text = match self.texts.get(location) {
            Some(text) => text.to_string(),
            None => self.read_file(location, tree, &path, directive)?,
        };
        // Spans address a source by 32-bit offsets, as for the program itself.
        if u32::try_from(text.len()).is_err() {
            return Err(EngineError::SourceTooLarge { name: name.clone() }
                .at(directive.span)
                .with_resource(name));
        }
        let file = self.pipeline.add_source(name.clone(), text);
        self.files.insert(name, file);
        Ok(file)
    }

    /// Reads a module's file from disk, after checking that every part of the module's name
    /// matches its directory or file name exactly.
    fn read_file(
        &self,
        location: &Location,
        tree: &Path,
        path: &Path,
        directive: &UseDirective,
    ) -> Result<String, Diagnostic> {
        if let Err(missing) = find_exactly(location, tree) {
            // The file does not exist, so the problem is not *in* it.
            let problem = ModuleError::NoSuchModule {
                module: directive.module.to_string(),
            }
            .at(directive.span)
            .with_note(format!("looked for `{}`", identity(path)));
            return Err(match self.missing_note(missing, directive) {
                Some(note) => problem.with_note(note),
                None => problem,
            });
        }
        fs::read_to_string(path).map_err(|error| {
            ModuleError::ModuleUnreadable {
                module: directive.module.to_string(),
                reason: error.to_string(),
            }
            .at(directive.span)
            .with_resource(identity(path))
        })
    }

    /// Returns what explains a missing module file, if anything does.
    fn missing_note(&self, missing: Missing, directive: &UseDirective) -> Option<String> {
        let module = &directive.module;
        match missing {
            Missing::Case(found) => Some(format!(
                "`{found}` differs in case, and module names match file names exactly"
            )),
            Missing::Directory(name) => Some(format!(
                "`{name}` is a directory, whose modules are named as in `{module}::name`"
            )),
            Missing::Nothing => match module.segments.as_slice() {
                [root] if module.parents == 0 && self.roots.contains_key(root) => Some(format!(
                    "`{root}` is a module root, whose modules are named as in `{root}::name`"
                )),
                _ => None,
            },
        }
    }

    /// Parses a module file's text under the id of one of its instances,
    /// so the statements' spans identify the instance they belong to.
    ///
    /// Parser warnings are reported for the first instance only.
    pub(super) fn parse(
        &mut self,
        file: FileId,
        instance: FileId,
    ) -> Result<Vec<Statement>, Diagnostic> {
        let text = self.pipeline.text(file).unwrap_or_default().to_owned();
        let (module, diagnostics) = Program::parse(&text, instance);
        let first = self.reported.insert(file);
        // The first error ends linking, and the parser's other
        // diagnostics are reported with it.
        let mut failure = None;
        for diagnostic in diagnostics {
            if failure.is_none() && diagnostic.severity() == Severity::Error {
                failure = Some(diagnostic);
            } else if first {
                self.pipeline.report(diagnostic);
            }
        }
        match failure {
            Some(problem) => Err(problem),
            None => Ok(module.statements),
        }
    }
}

/// Returns the name a module file is registered under:
/// its path with `.` steps dropped and `/` between the parts.
///
/// The same file thus has one name however the path to it was joined.
pub(super) fn identity(path: &Path) -> String {
    use std::path::Component;

    let mut name = String::new();
    for component in path.components() {
        match component {
            // A step that leads nowhere is not part of the name.
            Component::CurDir => {}
            Component::RootDir => {
                if !name.ends_with('/') {
                    name.push('/');
                }
            }
            other => {
                if !name.is_empty() && !name.ends_with('/') {
                    name.push('/');
                }
                name.push_str(&other.as_os_str().to_string_lossy());
            }
        }
    }
    if name.is_empty() {
        String::from(".")
    } else {
        name
    }
}

/// How a name was found among the entries of a directory.
enum Entry {
    /// An entry has exactly the name.
    Exact,
    /// No entry has the name, but one differs from it in case only: that entry's name.
    Differs(String),
    /// No entry has the name.
    Missing,
}

/// Looks a name up among the entries of a directory.
///
/// Comparing with the entries, rather than opening the joined path,
/// makes a case-insensitive filesystem tell names apart by case as other filesystems do.
fn entry(directory: &Path, name: &str) -> Entry {
    let Ok(entries) = fs::read_dir(directory) else {
        return Entry::Missing;
    };
    let mut differs = None;
    for entry in entries.flatten() {
        let found = entry.file_name();
        let Some(found) = found.to_str() else {
            continue;
        };
        if found == name {
            return Entry::Exact;
        }
        if found.eq_ignore_ascii_case(name) {
            differs = Some(found.to_owned());
        }
    }
    differs.map_or(Entry::Missing, Entry::Differs)
}

/// Why no module file lies at a location.
enum Missing {
    /// A directory or the file differs in case from the name's part: its name on disk.
    Case(String),
    /// A directory of the module's name lies where its file was expected: the directory's name.
    Directory(String),
    /// Nothing lies there.
    Nothing,
}

/// Checks that a module file lies at a location whose tree lies in `tree`,
/// every part of the name matching a directory or the file exactly.
fn find_exactly(location: &Location, tree: &Path) -> Result<(), Missing> {
    let Some((name, directories)) = location.segments.split_last() else {
        return Err(Missing::Nothing);
    };
    let mut directory = Location {
        segments: Vec::new(),
        ..location.clone()
    }
    .path(tree);
    for part in directories {
        match entry(&directory, part) {
            Entry::Exact => directory.push(part),
            Entry::Differs(found) => return Err(Missing::Case(found)),
            Entry::Missing => return Err(Missing::Nothing),
        }
    }
    match entry(&directory, &format!("{name}.rls")) {
        Entry::Exact => Ok(()),
        Entry::Differs(found) => Err(Missing::Case(found)),
        Entry::Missing if directory.join(name).is_dir() => Err(Missing::Directory(name.clone())),
        Entry::Missing => Err(Missing::Nothing),
    }
}
