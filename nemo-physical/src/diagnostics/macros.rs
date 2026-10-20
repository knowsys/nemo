//! The declaration of diagnostic kinds.

/// Declares an enum of diagnostic kinds.
///
/// Each variant is one kind of problem:
/// the fields its message needs, each with optional attributes such as a doc comment,
/// the name and the number of its code,
/// its severity, `error` or `warning`,
/// its message, a `format!` template over the fields,
/// in which a [Count](crate::diagnostics::Count) field gives the plural of its noun
/// (`{rows} row{rows:#}`),
/// its explanation, shown when the code is looked up
/// and the doc comment of the variant,
/// and optionally a fixed note and a fixed hint.
/// Only the message is formatted.
///
/// # Example
///
/// ```
/// use nemo_physical::{diagnostic_kinds, diagnostics::{DiagnosticKind, Severity}};
///
/// diagnostic_kinds! {
///     /// Problems with a greeting.
///     pub enum GreetingError {
///         Nobody => NOBODY = 42, error, "the greeting names nobody",
///             explain "The greeting named nobody.",
///             hint "greet someone";
///         TooLoud {
///             /// The volume of the greeting.
///             volume: u32,
///         } => TOO_LOUD = 43, warning,
///             "a greeting at volume {volume} is too loud",
///             explain "The greeting was too loud.";
///     }
/// }
///
/// let report = GreetingError::TooLoud { volume: 11 }.diagnostic();
/// assert_eq!(report.code(), codes::TOO_LOUD);
/// assert_eq!(report.severity(), Severity::Warning);
/// assert_eq!(report.message(), "a greeting at volume 11 is too loud");
/// assert_eq!(report.field("volume"), Some("11"));
/// assert_eq!(GreetingError::ENTRIES[0].name, "Nobody");
/// assert_eq!(GreetingError::ENTRIES[0].hint, Some("greet someone"));
/// ```
#[macro_export]
macro_rules! diagnostic_kinds {
    (
        $(#[$meta:meta])*
        $vis:vis enum $Name:ident {
            $(
                $Variant:ident $({ $($(#[$field_meta:meta])* $field:ident : $Type:ty),* $(,)? })?
                    => $CONST:ident = $number:literal, $severity:ident, $message:literal,
                    explain $explanation:literal
                    $(, note $note:literal)? $(, hint $hint:literal)? ;
            )*
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq)]
        $vis enum $Name {
            $(
                #[doc = $explanation]
                $Variant $({ $($(#[$field_meta])* $field: $Type),* })?,
            )*
        }

        #[doc = concat!("The codes of the kinds of [`", stringify!($Name), "`].")]
        pub mod codes {
            $(
                #[doc = $explanation]
                pub const $CONST: $crate::diagnostics::Code =
                    $crate::diagnostics::Code::new($number);
            )*
        }

        impl $crate::diagnostics::DiagnosticKind for $Name {
            const ENTRIES: &'static [$crate::diagnostics::CodeEntry] = &[
                $(
                    $crate::diagnostics::CodeEntry::new(
                        codes::$CONST,
                        stringify!($Variant),
                        $message,
                        $explanation,
                        $crate::diagnostic_kinds!(@optional $($note)?),
                        $crate::diagnostic_kinds!(@optional $($hint)?),
                    ),
                )*
            ];

            fn code(&self) -> $crate::diagnostics::Code {
                match self {
                    $(Self::$Variant { .. } => codes::$CONST,)*
                }
            }

            fn severity(&self) -> $crate::diagnostics::Severity {
                match self {
                    $(Self::$Variant { .. } => $crate::diagnostic_kinds!(@severity $severity),)*
                }
            }

            fn message(&self) -> String {
                match self {
                    $(
                        Self::$Variant $({ $($field),* })? => {
                            $($(let _ = &$field;)*)?
                            format!($message)
                        }
                    )*
                }
            }

            fn fields(&self) -> Vec<$crate::diagnostics::Field> {
                match self {
                    $(
                        Self::$Variant $({ $($field),* })? => vec![
                            $($(
                                $crate::diagnostics::Field {
                                    name: stringify!($field),
                                    value: $field.to_string(),
                                },
                            )*)?
                        ],
                    )*
                }
            }

            fn note(&self) -> Option<&'static str> {
                match self {
                    $(Self::$Variant { .. } => $crate::diagnostic_kinds!(@optional $($note)?),)*
                }
            }

            fn hint(&self) -> Option<&'static str> {
                match self {
                    $(Self::$Variant { .. } => $crate::diagnostic_kinds!(@optional $($hint)?),)*
                }
            }
        }

        impl $Name {
            /// Returns the diagnostic this kind reports,
            /// without a source location.
            pub fn diagnostic(&self) -> $crate::diagnostics::Diagnostic {
                $crate::diagnostics::Diagnostic::from_kind(self)
            }

            /// Returns the diagnostic this kind reports,
            /// pointing at the given span if one is known.
            pub fn at(
                &self,
                span: Option<$crate::diagnostics::Span>,
            ) -> $crate::diagnostics::Diagnostic {
                match span {
                    Some(span) => self.diagnostic().with_label(span, "here"),
                    None => self.diagnostic(),
                }
            }
        }

        impl From<$Name> for $crate::diagnostics::Diagnostic {
            fn from(kind: $Name) -> Self {
                kind.diagnostic()
            }
        }
    };
    (@severity error) => {
        $crate::diagnostics::Severity::Error
    };
    (@severity warning) => {
        $crate::diagnostics::Severity::Warning
    };
    (@optional) => {
        None
    };
    (@optional $text:literal) => {
        Some($text)
    };
}
