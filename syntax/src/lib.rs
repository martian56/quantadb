//! QuantaDB SQL syntax.
//!
//! This crate is deliberately independent from storage and execution. It turns
//! UTF-8 SQL text into a serializable, span-aware AST without performing name
//! resolution or type checking.

mod ast;
mod error;
mod lexer;
mod parser;
mod token;

pub use ast::*;
pub use error::SyntaxError;
pub use lexer::tokenize;
pub use parser::{parse_sql, parse_statement};
pub use token::{Keyword, Token, TokenKind};
