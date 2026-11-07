//! The kinds of problems the engine's entry points report.

use nemo_physical::{diagnostic_kinds, diagnostics::Count};

diagnostic_kinds! {
    /// A problem with how the engine is used: its program, its
    /// sources, its cursors, and the updates between runs.
    pub enum EngineError {
        CursorInvalidated
            => CURSOR_INVALIDATED = 206, error,
               "the cursor's predicate changed after the cursor was opened",
               explain "A result cursor was used after the table it reads from changed, so \
                        its remaining rows can no longer be delivered consistently.",
               hint "open a new cursor to read the current rows";
        Cancelled
            => CANCELLED = 207, error,
               "reasoning was cancelled before reaching the fixpoint",
               explain "The run was stopped through its cancel token before reaching the \
                        fixpoint. The stored data is consistent, and the work done so far \
                        is kept.",
               hint "call `reason()` again to continue";
        SourceUnreadable { name: String, reason: String }
            => SOURCE_UNREADABLE = 208, error, "could not read `{name}`: {reason}",
               explain "A program source could not be read.";
        SourceTooLarge { name: String }
            => SOURCE_TOO_LARGE = 480, error, "`{name}` is too large: the limit is 4 GiB",
               explain "A program source is longer than 4 GiB, more than source positions \
                        can address.";
        AlreadyLoaded
            => ALREADY_LOADED = 481, error, "the engine already holds a program",
               explain "A program was loaded or checked in an engine that already holds \
                        one.";
        NoProgram
            => NO_PROGRAM = 482, error, "no program is loaded",
               explain "An operation needs a loaded program, but none is loaded.";
        ExtensionDeclaresBase
            => EXTENSION_DECLARES_BASE = 488, error, "an extension cannot change `@base`",
               explain "An extension declares a `@base` other than the loaded program's. \
                        The names of the loaded program were already resolved against the \
                        old base, or without one, so the extension cannot change it.",
               hint "use `reload()` to replace the program instead";
        ExtensionRebindsPrefix
            => EXTENSION_REBINDS_PREFIX = 483, error, "an extension cannot redefine a prefix",
               explain "An extension declares a prefix that the loaded program declared \
                        differently. Names of the loaded program were already resolved \
                        through the old definition.",
               hint "use `reload()` to replace the program instead";
        ExtensionRedefinesParameter { name: String }
            => EXTENSION_REDEFINES_PARAMETER = 484, error,
               "an extension cannot redefine parameter `${name}`",
               explain "An extension declares a parameter that the loaded program already \
                        declares. The loaded rules were compiled with its value.",
               hint "use `reload()` to replace the program instead";
        ImportedRowsDropped { predicate: String }
            => IMPORTED_ROWS_DROPPED = 485, warning,
               "rows imported into `{predicate}` were dropped, since the reload rebuilds it",
               explain "A reload rebuilds the predicates whose statements changed, and every \
                        predicate if a directive such as `@parameter`, `@prefix` or `@type` \
                        changed. The rows imported into a rebuilt predicate cannot be carried \
                        over.",
               hint "import the rows again if they still apply";
        UnknownPredicate { predicate: String }
            => UNKNOWN_PREDICATE = 486, error, "the program has no predicate `{predicate}`",
               explain "An operation names a predicate that the program does not have.";
        NullInRow { predicate: String }
            => NULL_IN_ROW = 487, error, "a row imported into `{predicate}` contains a null",
               explain "A row given to the engine contains a null. Nulls are only created by \
                        reasoning.";
        NotLinked
            => NOT_LINKED = 490, error, "the program uses modules and is not linked",
               explain "A program with a `@use` directive was loaded, extended, reloaded or \
                        checked before it was linked. Linking instantiates the modules it \
                        uses.",
               hint "call `Engine::link` on the program first";
        AwaitingRemote { count: Count, first: String }
            => AWAITING_REMOTE = 491, error,
               "the method needs {count} remote request{count:#}, one of them to `{first}`, \
                but the engine has no client to send them",
               explain "The method needs data from the network. The engine sends such \
                        requests through its client, which a build with the `http` feature \
                        has unless `set_client(None)` removed it. Without a client, the \
                        method changes nothing. The `start_*` methods send no request: they \
                        return the requests, and `resume()` takes the responses.",
               hint "set a client with `set_client`, or call the `start_*` method and pass \
                     the responses to its requests to `resume()`";
        WaitCancelled
            => WAIT_CANCELLED = 492, error, "the wait for remote responses was cancelled",
               explain "The engine's cancel token stopped a method while its client waited \
                        for the responses to its remote requests. A load, extension or reload \
                        changes nothing. A reasoning run stays suspended, with the tables \
                        consistent, and the next `reason()` continues it.",
               hint "call the method again";
        MethodSuspended { refused: String, suspended: String, count: Count }
            => METHOD_SUSPENDED = 493, error,
               "`{refused}` is refused while `{suspended}` is suspended for {count} remote \
                request{count:#}",
               explain "A method that needs data from the network is suspended from the moment \
                        it returns its requests, which `pending()` lists, until `resume()` \
                        completes it or `discard_pending()` discards it. Meanwhile, the methods \
                        that would change the program or the stored data are refused, rather \
                        than drop what the suspended method has done.",
               hint "pass the responses to `resume()`, or call `discard_pending()`";
        NotStoredWhole { predicate: String, missing: String }
            => NOT_STORED_WHOLE = 489, error,
               "`{predicate}` is not stored whole: {missing}",
               explain "The facts of a predicate were requested, but the engine does not \
                        store all of them. By default, an import is stored without the \
                        columns and rows that no rule reads, and an import that the rules \
                        query at its endpoint is not stored at all. Derived predicates, and \
                        predicates the program declares `@output` or exports, are always \
                        stored whole.";
    }
}
