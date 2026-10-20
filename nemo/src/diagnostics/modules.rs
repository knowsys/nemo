//! The kinds of problems module instantiation reports.

use nemo_physical::{diagnostic_kinds, diagnostics::Count};

diagnostic_kinds! {
    /// A problem with `@use`: the modules a program composes, their
    /// interfaces, and the names passed between them.
    pub enum ModuleError {
        OwnedName { name: String, alias: String }
            => OWNED_NAME = 400, error, "`{name}` belongs to module instance `{alias}`",
               explain "A name in the program resolves to a name owned by a module \
                        instantiation, for example through a prefix or `@base`. The names \
                        of a module can only be reached as `alias::name`, and only if the \
                        module offers them.",
               hint "write `alias::name` to use a name the module offers with `@output`";
        DuplicateAlias { alias: String }
            => DUPLICATE_ALIAS = 401, error, "alias `{alias}` is used by two `@use` directives",
               explain "Two `@use` directives have the same alias. Each alias names one \
                        module instantiation.";
        UnknownModule { module: String }
            => UNKNOWN_MODULE = 402, error, "no module is used under the alias `{module}`",
               explain "A qualified name (`alias::name`) refers to an alias that no `@use` \
                        directive introduces.",
               hint "refer to a module by the alias of its `@use`";
        NotOffered { module: String, name: String }
            => NOT_OFFERED = 403, error, "module instance `{module}` does not offer `{name}`",
               explain "A qualified name asks a module for a name that the module does not \
                        offer.",
               hint "offer the name with `@output` in the module";
        PassedInCycle { module: String, name: String }
            => PASSED_IN_CYCLE = 404, error,
               "`{module}::{name}` is passed between modules in a cycle",
               explain "A name of a module is passed on to other modules in a way that \
                        leads back to the module itself.";
        UsedInCycle { module: String }
            => USED_IN_CYCLE = 405, error, "module `{module}` uses itself",
               explain "A module uses itself, directly or through the modules it uses.";
        TooDeep { limit: usize }
            => MODULES_TOO_DEEP = 406, error,
               "modules are nested more than {limit} levels deep",
               explain "Modules use other modules in a chain longer than instantiation \
                        allows.";
        TooManyInstantiations { limit: usize }
            => TOO_MANY_INSTANTIATIONS = 407, error,
               "the program expands into more than {limit} module instantiations",
               explain "Instantiating every `@use` would produce more module copies than a \
                        program may hold. Each `@use` makes a copy of its module, so a \
                        module that uses another one twice doubles the copies at every \
                        level.";
        NoSuchParameter { module: String, name: String }
            => NO_SUCH_PARAMETER = 408, error,
               "module `{module}` has no parameter `${name}`",
               explain "A `@use` gives a value to a parameter that the module does not \
                        declare.";
        ParameterWithoutValue { module: String, name: String }
            => MODULE_PARAMETER_WITHOUT_VALUE = 409, error,
               "parameter `${name}` of module `{module}` has no value",
               explain "A parameter of a module has no value: the module declares none, \
                        and the `@use` gives none.";
        UsedBelow { module: String }
            => USED_BELOW = 410, error,
               "the `@use` of `{module}` comes further down this file",
               explain "A `@use` passes on a name of a module whose `@use` comes later in \
                        the same file.",
               hint "move that `@use` above this one";
        NoSuchModule { module: String }
            => NO_SUCH_MODULE = 411, error, "no module `{module}`",
               explain "A `@use` names a module that is neither a file nor a text \
                        registered with the engine. A module name is relative to the \
                        directory of the file containing the `@use`: `lib::tc` is the file \
                        `lib/tc.rls` there, and each leading `super` goes one directory up. \
                        A name of two or more parts whose first part is a module root names \
                        a file inside the root's directory instead. Every part must match \
                        its directory or file name exactly, case included.";
        AmbiguousRoot { root: String }
            => AMBIGUOUS_ROOT = 412, error,
               "`{root}` is both a module root and a directory next to this file",
               explain "A module name begins with the name of a module root, and a \
                        directory of the same name lies next to the file containing the \
                        `@use`. The name could mean a module in either.",
               hint "rename the directory, or declare the root under another name";
        ModuleUnreadable { module: String, reason: String }
            => MODULE_UNREADABLE = 413, error, "could not read module `{module}`: {reason}",
               explain "The file of a module that a `@use` names could not be read.";
        DuplicateInput { predicate: String }
            => DUPLICATE_INPUT = 414, error, "`{predicate}` is declared as an input twice",
               explain "A program declares the same `@input` twice, whether it runs on its \
                        own or as a module.";
        InterfaceNameNotPlain { name: String }
            => INTERFACE_NAME_NOT_PLAIN = 415, error,
               "`{name}` cannot be offered, since it is not a plain name",
               explain "A module offers a name that is not a plain name, such as an IRI or \
                        a prefixed name. Other files refer to offered names as \
                        `alias::name`, which only fits a plain name.";
        NoSuchInput { input: String }
            => NO_SUCH_INPUT = 416, error, "the module has no input `{input}`",
               explain "A `@use` binds an input that the module does not declare.";
        InputBoundTwice { input: String }
            => INPUT_BOUND_TWICE = 417, error, "input `{input}` is bound twice",
               explain "A `@use` binds the same input twice.";
        InputsUnbound { count: Count, inputs: String }
            => INPUTS_UNBOUND = 418, error, "unbound module input{count:#}: {inputs}",
               explain "A `@use` leaves some inputs of the module unbound.",
               hint "bind every input to a predicate, as in `@use lib::tc { p = q } as a .`";
        QualifiedUnresolvable { module: String, name: String }
            => QUALIFIED_UNRESOLVABLE = 419, error,
               "`{module}::{name}` cannot be used here",
               explain "A qualified name (`alias::name`) appears where only names of the \
                        current file are allowed.";
        OffersUnwritten { module: String, count: Count, names: String }
            => OFFERS_UNWRITTEN = 420, warning,
               "module `{module}` offers {count} name{count:#} it never writes: {names}",
               explain "The `@output` of a module offers names that no rule, fact, import, \
                        or input of the module ever writes, so they are always empty.";
        LeavesRoot { module: String, root: String }
            => LEAVES_ROOT = 421, error, "`{module}` leads out of module root `{root}`",
               explain "A module inside a module root names a module with more `super` \
                        steps than lead back to the root's directory. A root can be placed \
                        anywhere, so its modules can only name modules inside it.",
               hint "name a module outside the root through a root of its own";
        NotARootName { name: String }
            => NOT_A_ROOT_NAME = 422, error, "`{name}` cannot name a module root",
               explain "A module root is named by one plain name other than `super`, such \
                        as `shared`. A `@use` then names the module `tc.rls` in the root's \
                        directory as `shared::tc`.";
    }
}
