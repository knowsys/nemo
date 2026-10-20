//! The kinds of problems the type analysis reports.

use nemo_physical::{diagnostic_kinds, diagnostics::Count};

diagnostic_kinds! {
    /// A problem the type analysis found: a rule that cannot fire,
    /// a value outside its declared type (by `@declare`, `@parameter`, or `@type`),
    /// or a value not shown to fit it.
    pub enum TypeError {
        NeverFires
            => NEVER_FIRES = 205, warning, "this rule can never fire",
               explain "No row can satisfy the body of the rule, or no row yields a value \
                        its head aggregates. This follows from the types of values its \
                        predicates can hold, or from a condition without variables that \
                        fails to evaluate, for example because it divides by zero. The \
                        labels and notes show what rules out every row.";
        DeclaredArity { name: String, declared: Count, arity: Count }
            => DECLARED_ARITY = 450, error,
               "`{name}` is declared with {declared} column{declared:#}, but used with \
                {arity}",
               explain "A `@declare` gives a predicate a different number of columns than \
                        the program uses it with.";
        RuleWritesOutside { written: String, name: String, declared: String }
            => RULE_WRITES_OUTSIDE = 451, error,
               "this rule writes values of type {written} into a column of `{name}` declared \
                as {declared}",
               explain "A rule writes values that the declaration of a column excludes.";
        StoredValuesExcluded { name: String, column: usize, stored: String }
            => STORED_VALUES_EXCLUDED = 452, error,
               "column {column} of `{name}` already stores values of type {stored}, not all \
                of which this declaration admits",
               explain "A declaration arrived after data that its predicate already \
                        stores, and it excludes some of that data.",
               hint "declare the predicate before loading its data, or reload the program";
        ParameterOutsideType { name: String, declared: String, supplied: String }
            => PARAMETER_OUTSIDE_TYPE = 453, error,
               "parameter `${name}` is declared as {declared}, but its value has type \
                {supplied}",
               explain "The value of a typed parameter lies outside its declared type.";
        ValueOutsideType { label: String, declared: String, value: String }
            => VALUE_OUTSIDE_TYPE = 454, error,
               "`{label}` is declared as {declared}, but this value has type {value}",
               explain "A value built with a label does not match the `@type` declared for \
                        that label.";
        ConstructionKind { label: String, declared: String, builds: String }
            => CONSTRUCTION_KIND = 455, error,
               "`{label}` is declared as {declared}, but this builds a {builds}",
               explain "A value built with a label is a tuple where its `@type` declares a \
                        map, or the reverse.";
        ConstructionSize { label: String, declared: Count, built: Count }
            => CONSTRUCTION_SIZE = 456, error,
               "`{label}` is declared with {declared} field{declared:#}, but this builds \
                {built}",
               explain "A value built with a label has a different number of fields than \
                        its `@type` declares.";
        TupleFieldOutsideType { label: String, declared: String, value: String }
            => TUPLE_FIELD_OUTSIDE_TYPE = 457, error,
               "this field of `{label}` is declared as {declared}, but the value has type \
                {value}",
               explain "A field of a labeled tuple holds a value that its `@type` \
                        excludes.";
        MapFieldOutsideType { key: String, label: String, declared: String, value: String }
            => MAP_FIELD_OUTSIDE_TYPE = 458, error,
               "field `{key}` of `{label}` is declared as {declared}, but the value has type \
                {value}",
               explain "A field of a labeled map holds a value that its `@type` excludes.";
        UnknownField { label: String, declared: String, key: String }
            => UNKNOWN_FIELD = 459, error,
               "`{label}` has no field `{key}`, since it is declared as {declared}",
               explain "A labeled map has a key that its `@type` does not declare.";
        FactOutsideDeclaration { name: String, column: usize, declared: String, value: String }
            => FACT_OUTSIDE_DECLARATION = 460, error,
               "column {column} of `{name}` is declared as {declared}, but the value has type \
                {value}",
               explain "A fact writes a value that the declaration of its predicate \
                        excludes.";
        ImportOutsideDeclaration {
            kind: String,
            predicate: String,
            column: usize,
            declared: String,
        } => IMPORT_OUTSIDE_DECLARATION = 461, error,
               "this import reads values of type {kind} into column {column} of `{predicate}`, \
                declared as {declared}",
               explain "An import's format reads values that the declaration of a column \
                        does not contain. The format is the type of what an import reads, \
                        and no row is checked as it loads, so a declared column needs a \
                        format that reads only values of its declared type. Formats that \
                        read terms, such as RDF, JSON or SPARQL results, read values of \
                        several types into each column.";
        RuleBuildsOutsideType { label: String, declared: String, built: String }
            => RULE_BUILDS_OUTSIDE_TYPE = 462, error,
               "`{label}` is declared as {declared}, but this rule builds values of type \
                {built}",
               explain "A rule builds a labeled value from variables whose values the \
                        `@type` of the label excludes.";
        RuleMayWriteOutside { written: String, name: String, declared: String }
            => RULE_MAY_WRITE_OUTSIDE = 463, error,
               "this rule may write values of type {written} into a column of `{name}` \
                declared as {declared}",
               explain "A rule writes values that cannot be shown to fit the declaration of \
                        a column. Every value a declared column stores must provably fit, so \
                        a value read from a column of a wider type needs a condition that \
                        keeps only values of the declared type, or a conversion into it.";
        RuleMayBuildOutsideType { label: String, declared: String, built: String }
            => RULE_MAY_BUILD_OUTSIDE_TYPE = 464, error,
               "`{label}` is declared as {declared}, but this rule may build values of type \
                {built}",
               explain "A rule builds a labeled value whose fields cannot be shown to fit \
                        the `@type` of the label. Every value carrying the label must \
                        provably fit, so a field read from a column of a wider type needs a \
                        condition that keeps only values of the declared type, or a \
                        conversion into it.";
        DeclaredLabelWrittenOut { name: String }
            => DECLARED_LABEL_WRITTEN_OUT = 465, error,
               "`{name}` has a `@type`, so a type expression names it rather than writing \
                its shape",
               explain "A type expression writes out the shape of a label that a `@type` \
                        declares. The values carrying the label are exactly the values of \
                        its `@type`, so the label's name is the type.";
    }
}
