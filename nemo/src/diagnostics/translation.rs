//! The kinds of problems translation reports: a program that parses but
//! cannot be compiled, loaded, or written out as written.

use nemo_physical::{diagnostic_kinds, diagnostics::Count};

diagnostic_kinds! {
    /// A problem translation found in a parsed program:
    /// names, rules, values, declarations, and directives that do not fit together.
    pub enum TranslationError {
        Unsupported { what: String }
            => UNSUPPORTED = 203, error, "{what} is not supported",
               explain "The program uses a feature that Nemo does not support yet. \
                        The message names the feature.";
        UnsafeVariable { role: String, variable: String }
            => UNSAFE_VARIABLE = 204, error,
               "{role} `?{variable}` is not bound in the rule body",
               explain "A variable appears where only variables bound by the rule body \
                        may appear: in the head, in an aggregate or its scope, or in an \
                        operation. A variable is bound if it occurs in a positive body \
                        atom, or if it is assigned from variables that are. A rule that \
                        breaks this is called unsafe.";
        IgnoredDirective { directive: String }
            => IGNORED_DIRECTIVE = 250, warning,
               "unsupported directive `{directive}` is ignored",
               explain "Nemo parses this directive but does not act on it yet, so it has \
                        no effect: a call of the builtin it declares builds a labeled tuple \
                        instead.";
        UndeclaredPrefix { prefix: String }
            => UNDECLARED_PREFIX = 251, error, "use of undeclared prefix `{prefix}:`",
               explain "A prefixed name uses a prefix that no `@prefix` directive \
                        declares.";
        QualifiedWithoutModules { module: String, name: String }
            => QUALIFIED_WITHOUT_MODULES = 252, error,
               "`{module}::{name}` refers to a module, but the program uses no modules",
               explain "A qualified name (`module::name`) appears in a program that does \
                        not instantiate any module with `@use`.",
               hint "instantiate a module with `@use lib::tc { … } as alias .`";
        ArityMismatch { name: String, arity: Count, registered: Count }
            => ARITY_MISMATCH = 254, error,
               "`{name}` is used with {arity} column{arity:#} here, but with {registered} \
                elsewhere",
               explain "A predicate is used with two different numbers of columns. Each \
                        predicate name has a single arity.",
               hint "if these are different predicates, rename one of them";
        NoPositiveAtom
            => NO_POSITIVE_ATOM = 255, error, "rule body has no positive atom",
               explain "The body of a rule has no positive atom. Every rule needs at \
                        least one, so that its variables range over stored facts.";
        UnboundAcrossNegations { variable: String }
            => UNBOUND_ACROSS_NEGATIONS = 256, error,
               "unbound variable `?{variable}` occurs in more than one negated atom",
               explain "A variable that the positive body does not bind may occur in only \
                        one negated atom, where it is local to that negation. This one \
                        occurs in several.";
        AnonymousAsValue
            => ANONYMOUS_AS_VALUE = 257, error,
               "`_` matches a value but cannot provide one",
               explain "The anonymous variable `_` matches any value in a body atom, also \
                        inside a pattern such as `(_, ?x)`. It provides no value, so it \
                        cannot be used in a head, an expression, an aggregate, or a \
                        directive option.";
        SeveralAggregates
            => SEVERAL_AGGREGATES = 258, error,
               "aggregates appear in more than one head atom",
               explain "Aggregates appear in more than one head atom of a rule. All \
                        aggregates of a rule must be in the same head atom.";
        CombinedAggregates
            => COMBINED_AGGREGATES = 259, error,
               "head expression contains more than one aggregate",
               explain "A head expression contains more than one aggregate. Each head \
                        expression may contain at most one.";
        AggregatedGroupColumn { variable: String }
            => AGGREGATED_GROUP_COLUMN = 260, error,
               "aggregated variable `?{variable}` is also a group-by column",
               explain "A variable that is aggregated also appears outside the aggregate in \
                        the same head, as a plain term or in an expression around the \
                        aggregate, and so is a group-by column.";
        ScopeVariableKind
            => SCOPE_VARIABLE_KIND = 261, error,
               "aggregate scope can only list universal variables like `?x`",
               explain "The `per` scope of an aggregate lists something other than a \
                        universal variable, such as a parameter `$k` or an existential \
                        variable `!z`.";
        LegacyScopeList
            => LEGACY_SCOPE_LIST = 262, warning,
               "aggregate scope is written without `per`",
               explain "An aggregate lists its scope variables after a comma, the old \
                        spelling that the `per` keyword replaced. Both mean the same: one \
                        contribution per distinct combination of the aggregated value and \
                        the listed variables.",
               hint "write `#sum(?v per ?w)` instead of `#sum(?v, ?w)`";
        Unstratified
            => UNSTRATIFIED = 264, error, "the program is not stratified",
               explain "A predicate depends on itself through negation or aggregation. \
                        Such a program cannot be split into strata that are evaluated one \
                        after another, so Nemo cannot evaluate it.",
               note "a predicate is negated or aggregated within its own recursion";
        AggregateOutsideHead
            => AGGREGATE_OUTSIDE_HEAD = 265, error,
               "aggregates are only allowed in rule heads",
               explain "An aggregate appears outside a rule head, for example in a rule \
                        body or in a fact.";
        ArgumentCount { operator: String, expected: String, found: usize }
            => ARGUMENT_COUNT = 266, error,
               "`{operator}` takes {expected}, found {found}",
               explain "A builtin is called with the wrong number of arguments.";
        TooManyArguments { operator: String }
            => TOO_MANY_ARGUMENTS = 267, error,
               "`{operator}` is called with too many arguments",
               explain "A builtin is called with more than 65535 arguments, more than an \
                        expression can pass.";
        StructureTooLarge
            => STRUCTURE_TOO_LARGE = 268, error, "tuple or map has too many elements",
               explain "A tuple or map is built from more than 65535 elements, more than an \
                        expression can construct.";
        NotGround
            => NOT_GROUND = 269, error, "expected a ground term",
               explain "A variable appears where only a ground term is allowed: in a fact, \
                        a parameter value, or a guard of a directive.";
        InvalidLanguageTag { language: String }
            => INVALID_LANGUAGE_TAG = 270, error, "`{language}` is not a valid language tag",
               explain "A language-tagged string has a tag that is not a valid language \
                        tag.";
        InvalidLiteral
            => INVALID_LITERAL = 272, error, "invalid literal for its datatype",
               explain "The lexical form of a typed literal is not valid for its datatype, \
                        as in `\"abc\"^^<http://www.w3.org/2001/XMLSchema#integer>`.";
        Unrepresentable
            => UNREPRESENTABLE = 273, error, "value cannot be stored",
               explain "A value cannot be stored in a table, for example because it is a \
                        string beyond the length limit of the dictionary.";
        DuplicateMapKey { key: String }
            => DUPLICATE_MAP_KEY = 274, error, "duplicate key `{key}` in map",
               explain "A map, or a map type, contains the same key twice.";
        ConstantExpressionErrors
            => CONSTANT_EXPRESSION_ERRORS = 275, error,
               "constant expression fails to evaluate",
               explain "An expression without variables cannot be evaluated, for example \
                        because it divides by zero.";
        LabeledTupleNamedFields
            => LABELED_TUPLE_NAMED_FIELDS = 276, error,
               "labeled tuples cannot have named fields",
               explain "A labeled tuple inside a term was written with named fields. The \
                        fields of a tuple are given by position. Only atoms of declared \
                        predicates take named arguments.";
        OptionUnevaluable
            => OPTION_UNEVALUABLE = 277, error, "option value fails to evaluate",
               explain "The value of a directive option is an expression that cannot be \
                        evaluated.";
        DuplicateParameter { name: String }
            => DUPLICATE_PARAMETER = 278, error, "parameter `${name}` is declared twice",
               explain "Two `@parameter` directives declare the same parameter.";
        ParameterWithoutValue { name: String }
            => PARAMETER_WITHOUT_VALUE = 279, error, "parameter `${name}` has no value",
               explain "A parameter is declared without a value, and no value was supplied \
                        from outside.",
               hint "supply a value, for example with `--param name=value` on the command \
                     line";
        ParameterCycle { name: String }
            => PARAMETER_CYCLE = 280, error,
               "parameter `${name}` depends on itself",
               explain "The value of a parameter refers back to the parameter, directly or \
                        through other parameters.";
        UndefinedParameter { name: String }
            => UNDEFINED_PARAMETER = 281, error, "undefined parameter `${name}`",
               explain "The program refers to a parameter that no `@parameter` declares.";
        GuardBoundTwice { name: String }
            => GUARD_BOUND_TWICE = 282, error, "guard variable `?{name}` is bound twice",
               explain "The guard of a directive binds the same variable twice.";
        GuardUnbound { name: String }
            => GUARD_UNBOUND = 283, error,
               "`?{name}` is not bound by a guard of the directive",
               explain "A directive option uses a variable that no guard of the directive \
                        binds. Guards follow the options, as in \
                        `@import p from csv { resource = ?f }, ?f = \"p.csv\" .`";
        UnknownType { name: String }
            => UNKNOWN_TYPE = 284, error, "cannot find type `{name}`",
               explain "A type expression names a type that is neither a base type nor \
                        declared by a `@type` directive.";
        TypeNotConstructor
            => TYPE_NOT_CONSTRUCTOR = 285, error,
               "`@type` must declare a labeled tuple or a labeled map",
               explain "A `@type` directive declares something other than a labeled tuple \
                        or map. Type declarations describe the values built with a label, \
                        so they cannot rename other types.";
        TypeUnnamed
            => TYPE_UNNAMED = 286, error, "`@type` must name the label it declares",
               explain "A `@type` directive declares a tuple or map without a label.";
        TypeLabelIsBaseType { name: String }
            => TYPE_LABEL_IS_BASE_TYPE = 287, error,
               "`{name}` is a base type and cannot be declared with `@type`",
               explain "A `@type` directive uses the name of a base type as its label.";
        TypeLabelIsBuiltin { name: String }
            => TYPE_LABEL_IS_BUILTIN = 288, error,
               "`{name}` is a builtin and cannot be used as a label",
               explain "A `@type` directive uses the name of a builtin as its label. A \
                        builtin name always calls the builtin, so no value can carry it as \
                        a label.";
        DuplicateType { name: String }
            => DUPLICATE_TYPE = 289, error, "type `{name}` is declared twice",
               explain "Two `@type` directives declare the same name.";
        DuplicateDeclaration { predicate: String }
            => DUPLICATE_DECLARATION = 290, error, "`{predicate}` is declared twice",
               explain "A predicate is declared twice, by `@declare` or `@input`.";
        NamedArgumentsUndeclared { predicate: String }
            => NAMED_ARGUMENTS_UNDECLARED = 291, error,
               "`{predicate}` has no declared column names",
               explain "An atom names its arguments, but its predicate has no `@declare` \
                        that names its columns.",
               hint "name the columns with `@declare`, as `@declare person(name: string, \
                     age: int) .`";
        UnknownColumn { predicate: String, column: String }
            => UNKNOWN_COLUMN = 292, error, "`{predicate}` has no column named `{column}`",
               explain "A named argument names a column that the declaration of its \
                        predicate does not have.";
        ColumnGivenTwice { column: String }
            => COLUMN_GIVEN_TWICE = 293, error, "column `{column}` is given twice",
               explain "An atom gives the same named column twice.";
        ColumnNotGiven { column: String, predicate: String }
            => COLUMN_NOT_GIVEN = 294, error, "missing {column} of `{predicate}`",
               explain "A fact or rule head with named arguments leaves out a column. Only \
                        body atoms may leave columns out.";
        HeaderName { name: String }
            => HEADER_NAME = 295, error, "`{name}` is not a valid header name",
               explain "A key of the `http_headers` option is not a valid HTTP header name: a \
                        bare name or a string of letters, digits, and symbols such as `-` \
                        and `_`, without spaces.",
               hint "write a header name of letters, digits, and `-`, such as `\"X-Api-Key\"`";
        HeaderValue { name: String }
            => HEADER_VALUE = 296, error,
               "expected a string of printable ASCII characters as the value of header \
                `{name}`",
               explain "A value of the `http_headers` option is not a string, or it holds a \
                        character a header cannot carry, such as a line break or a character \
                        outside ASCII.";
        HeadersNotRemote
            => HEADERS_NOT_REMOTE = 297, error,
               "`http_headers` is only allowed for remote resources",
               explain "The `http_headers` option was given for a resource that is not \
                        fetched over HTTP or HTTPS.";
        ImportIntoInput { predicate: String }
            => IMPORT_INTO_INPUT = 298, error,
               "cannot import into `{predicate}`, which is declared with `@input`",
               explain "An import targets a predicate that the same file declares with \
                        `@input`. The rows of an input come from the predicate a `@use` \
                        binds to it.",
               hint "import into another predicate of this file";
        RdfExtensionUnknown
            => RDF_EXTENSION_UNKNOWN = 299, error,
               "cannot tell the RDF serialization from the resource name",
               explain "The `rdf` format picks its serialization by the extension of the \
                        resource, and the extension is not one it knows.",
               hint "use one of the extensions nt, nq, ttl, trig, or rdf, or name a specific \
                     format such as `turtle`";
        SparqlNoProjection
            => SPARQL_NO_PROJECTION = 300, error, "SPARQL query selects no variables",
               explain "The query of a SPARQL import selects no variables, so its result \
                        has no columns.";
        EndpointNotHttp { endpoint: String }
            => ENDPOINT_NOT_HTTP = 301, error,
               "SPARQL endpoint `{endpoint}` is not an HTTP or HTTPS URL",
               explain "The endpoint of a SPARQL import is not an HTTP or HTTPS URL.";
        NotASparqlQuery { reason: String }
            => NOT_A_SPARQL_QUERY = 302, error, "invalid SPARQL query: {reason}",
               explain "The query of a SPARQL import is not a valid SPARQL SELECT query.";
        ParquetArity { resource: String, columns: Count, predicate: String, arity: Count }
            => PARQUET_ARITY = 303, error,
               "`{resource}` has {columns} column{columns:#}, but `{predicate}` has {arity}",
               explain "A Parquet file has a different number of columns than the \
                        predicate it is imported into.";
        OptionValueType { option: String, expected: String }
            => OPTION_VALUE_TYPE = 304, error, "option `{option}` must be {expected}",
               explain "A directive option has a value of the wrong type.";
        OptionMissing { format: String, option: String }
            => OPTION_MISSING = 305, error,
               "the `{format}` format requires the `{option}` option",
               explain "A directive lacks an option that its format requires.";
        UnknownOptionValue { option: String, value: String, accepted: String }
            => UNKNOWN_OPTION_VALUE = 306, error,
               "invalid {option} `{value}`, expected one of: {accepted}",
               explain "A directive option has a value outside the set it accepts.";
        ColumnFormatNotName
            => COLUMN_FORMAT_NOT_NAME = 307, error,
               "expected a column format name, such as int, string, iri, term, or skip",
               explain "The `format` option contains something other than the names of \
                        column formats: a type named like a declarable scalar type, \
                        `term`, or `skip`.";
        ReadOptionOnExport { option: String }
            => READ_OPTION_ON_EXPORT = 308, error, "option `{option}` only applies to imports",
               explain "An export was given an option that only affects reading, such as \
                        `ignore_headers`, `quoting`, `http_headers`, or `on_error`.";
        UnknownOption { option: String, format: String }
            => UNKNOWN_OPTION = 309, error,
               "unknown option `{option}` for the `{format}` format",
               explain "A directive was given an option that its format does not take.";
        UnknownFormat { name: String }
            => UNKNOWN_FORMAT = 310, error, "unknown format `{name}`",
               explain "A directive names a data format that Nemo does not know.",
               hint "name one of the formats csv, tsv, dsv, rdf, ntriples, nquads, turtle, \
                     trig, rdfxml, json, parquet, or sparql";
        RdfExportArity { columns: Count, layout: String, predicate: String, arity: Count }
            => RDF_EXPORT_ARITY = 311, error,
               "RDF export writes {columns} columns{layout}, but `{predicate}` has {arity} \
                column{arity:#}",
               explain "An RDF export writes rows of the serialization's layout, three \
                        columns for triples and four for quads, and the predicate has a \
                        different arity.";
        ExportFormatArity { predicate: String, declared: Count, arity: Count }
            => EXPORT_FORMAT_ARITY = 312, error,
               "export of `{predicate}` lists {declared} column format{declared:#}, but the \
                predicate has {arity} column{arity:#}",
               explain "The `format` option of an export lists a different number of column \
                        formats than its predicate has columns.";
        ExportDropped { dropped: Count, predicate: String }
            => EXPORT_DROPPED = 313, warning,
               "skipped {dropped} row{dropped:#} of `{predicate}` that the export format \
                cannot write",
               explain "An export left out rows that its format cannot write: a value \
                        outside the column format a CSV export declares for it, or a term \
                        that RDF does not allow in its position, such as a literal as a \
                        subject.";
        MalformedRows { count: Count, resource: String }
            => MALFORMED_ROWS = 314, warning,
               "skipped {count} malformed row{count:#} in `{resource}`",
               explain "An import skipped rows that it could not read, for example rows \
                        with the wrong number of fields or with a value its column format \
                        does not accept.";
        InputUnsupplied { predicate: String }
            => INPUT_UNSUPPLIED = 315, warning,
               "input `{predicate}` is not bound by a `@use` in this run",
               explain "A predicate declared with `@input` is not bound by a `@use`, since \
                        the file runs on its own. It holds only the facts the file gives it \
                        and what its rules derive.";
        PruningInstability { name: String, change: String }
            => PRUNING_INSTABILITY = 316, error,
               "`{name}` was stored without the columns and rows no rule read, but {change}",
               explain "By default, an import is stored without the columns and rows that \
                        no rule reads. A later program reads more of this import, or selects \
                        its rows by another condition, and the stored table cannot supply \
                        what it needs.",
               hint "call `retain_imports()` before loading to store imports whole, or \
                     reload the program";
        FeatureUnavailable { what: String, feature: String }
            => FEATURE_UNAVAILABLE = 317, error,
               "{what} requires the `{feature}` feature, which this build does not have",
               explain "The program uses a feature that this build of Nemo was compiled \
                        without.",
               hint "rebuild the `nemo` crate with the feature enabled";
        ColumnFormatCount { format: String, columns: Count, found: Count }
            => COLUMN_FORMAT_COUNT = 319, error,
               "`format` lists {found} column format{found:#}, but a `{format}` source has \
                {columns} column{columns:#}",
               explain "The `format` option lists a different number of column formats \
                        than the source has columns.";
        TermColumnFormat { format: String }
            => TERM_COLUMN_FORMAT = 320, error,
               "column format `{format}` does not apply to a column of terms",
               explain "The `format` option of an RDF or SPARQL import names a column \
                        format other than `term` or `skip`. The columns of these sources \
                        hold RDF terms as the source writes them.",
               hint "use `term` to keep the column, or `skip` to drop it";
        LimitNotACount { value: String }
            => LIMIT_NOT_A_COUNT = 321, error, "`{value}` is not a valid row limit",
               explain "The `limit` option is not a non-negative integer.",
               hint "give `limit` a non-negative integer, such as `limit = 100`";
        BindingsOnFormat { format: String }
            => BINDINGS_ON_FORMAT = 322, error,
               "a `{format}` import cannot have binding atoms in its `where` clause",
               explain "An import of a format other than `sparql` has atoms in its `where` \
                        clause. Only a SPARQL endpoint can be asked for the rows that match \
                        given bindings. Conditions over the import's columns are allowed \
                        for every format.";
        ImportColumnCount { declared: Count, arity: Count }
            => IMPORT_COLUMN_COUNT = 323, error,
               "import names {declared} column{declared:#}, but its source has {arity}",
               explain "An import names a different number of column variables than its \
                        source has columns: the query's projection, or the columns of the \
                        file, skipped ones included. An import with binding atoms must name \
                        its columns.";
        DuplicateColumnVariable { variable: String }
            => DUPLICATE_COLUMN_VARIABLE = 324, error,
               "column variable `?{variable}` is named twice",
               explain "An import names the same column variable twice.";
        BindingVariableUnknown { variable: String }
            => BINDING_VARIABLE_UNKNOWN = 325, error,
               "`?{variable}` is not a column of the import",
               explain "The `where` clause of an import uses a variable that is not one of \
                        the import's column variables.",
               hint "list the column variables after the predicate, as in \
                     `@import q(?x, ?y) from csv { … } where ?y > 0 .`";
        BindingTermKind
            => BINDING_TERM_KIND = 326, error,
               "expected a column variable, `_`, or a constant in the binding atom",
               explain "A binding atom has a term that is neither a column variable, the \
                        anonymous variable `_`, nor a constant.";
        BindingSelf { predicate: String }
            => BINDING_SELF = 327, error,
               "the import of `{predicate}` cannot be bound by `{predicate}` itself",
               explain "A binding atom names the predicate of its own import.";
        OpaqueQueryBound { reason: String }
            => OPAQUE_QUERY_BOUND = 328, error,
               "a query using `{reason}` cannot be restricted by bindings",
               explain "The query of a bound import uses `LIMIT`, `OFFSET`, `SAMPLE`, or a \
                        nondeterministic function such as `RAND()`. These apply to the \
                        whole result, so a request restricted to some bindings would not \
                        return the matching part of it.",
               hint "remove the binding atoms from the `where` clause to fetch the whole \
                     result once, or remove the construct from the query";
        PageNotACount { value: String }
            => PAGE_NOT_A_COUNT = 329, error, "`{value}` is not a valid page size",
               explain "The `page` option is not a positive integer.",
               hint "give `page` a positive integer, the number of binding rows sent per \
                     request";
        ConditionOnSkippedColumn { variable: String }
            => CONDITION_ON_SKIPPED_COLUMN = 337, error,
               "`?{variable}` names a skipped column, which no condition can read",
               explain "A condition in the `where` clause of an import reads a column that \
                        the import's format skips. A skipped column is never read, so it has \
                        no value to test.",
               hint "read the column with a format other than `skip`";
        PrunedPredicateRows { name: String, written: Count, stored: Count }
            => PRUNED_PREDICATE_ROWS = 331, error,
               "`{name}` has {written} column{written:#}, but this row has {stored}",
               explain "Rows were given for an import that is stored without the columns no \
                        rule reads, and the rows had only the stored columns. Rows always \
                        need all columns of the predicate, as its import reads them.",
               hint "give the row with all columns, or call `retain_imports()` before \
                     loading";
        ExpressionTooLarge { limit: usize }
            => EXPRESSION_TOO_LARGE = 332, error,
               "expression uses more than {limit} constants or variables",
               explain "An expression uses more constants, or more distinct variables, than \
                        a compiled expression can address.",
               hint "split the expression into several conditions or assignments";
        ImportFailed { resource: String, reason: String }
            => IMPORT_FAILED = 333, warning,
               "could not fetch `{resource}`, so its import is empty: {reason}",
               explain "An import marked `on_error = warn` could not fetch its remote \
                        resource. The run continues without its rows.";
        OnErrorNotRemote
            => ON_ERROR_NOT_REMOTE = 334, error,
               "`on_error` is only allowed for remote resources",
               explain "The `on_error` option sets what happens when a fetch fails. It was \
                        given for a resource that is not fetched over HTTP or HTTPS.";
        RequestsFailed { failed: Count, sent: Count, resource: String, unasked: Count }
            => REQUESTS_FAILED = 335, warning,
               "{failed} of {sent} request{sent:#} to `{resource}` failed, leaving {unasked} \
                binding row{unasked:#} unanswered",
               explain "Requests of a bound import marked `on_error = warn` failed. The \
                        responses to the other requests are stored, but the bindings sent \
                        with the failed requests are not asked again, so the import is \
                        incomplete.";
        ConflictingBase
            => CONFLICTING_BASE = 336, error, "`@base` is declared again with a different IRI",
               explain "Two `@base` directives of one program have different IRIs. Relative \
                        names resolve against one base IRI.";
    }
}

impl TranslationError {
    /// Creates the kind of a directive option holding a value of the wrong type.
    pub(crate) fn option_type(option: &str, expected: &str) -> Self {
        Self::OptionValueType {
            option: option.to_owned(),
            expected: expected.to_owned(),
        }
    }

    /// Creates the kind of a directive option the named format does not take.
    pub(crate) fn unknown_option(option: &str, format: &str) -> Self {
        Self::UnknownOption {
            option: option.to_owned(),
            format: format.to_owned(),
        }
    }
}
