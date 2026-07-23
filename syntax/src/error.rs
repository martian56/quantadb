use crate::Span;
use std::fmt;

/// A lexical or grammatical error with a byte span into the original SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxError {
    message: String,
    span: Span,
}

impl SyntaxError {
    #[must_use]
    pub fn new(message: impl Into<String>, span: Span) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl fmt::Display for SyntaxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at bytes {}..{}",
            self.message, self.span.start, self.span.end
        )
    }
}

impl std::error::Error for SyntaxError {}
