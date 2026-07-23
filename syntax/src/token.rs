use crate::Span;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Keyword {
    And,
    As,
    Bigint,
    Bool,
    Boolean,
    Begin,
    Commit,
    Create,
    Delete,
    Double,
    Drop,
    Exists,
    False,
    Float,
    From,
    If,
    Insert,
    Int,
    Integer,
    Into,
    Index,
    Is,
    Key,
    Limit,
    Not,
    Null,
    On,
    Or,
    Primary,
    Rollback,
    Select,
    Set,
    Start,
    Table,
    Text,
    True,
    Transaction,
    Unique,
    Update,
    Values,
    Varchar,
    Where,
    Work,
}

impl Keyword {
    #[must_use]
    pub fn from_identifier(identifier: &str) -> Option<Self> {
        Some(match identifier.to_ascii_uppercase().as_str() {
            "AND" => Self::And,
            "AS" => Self::As,
            "BIGINT" => Self::Bigint,
            "BOOL" => Self::Bool,
            "BOOLEAN" => Self::Boolean,
            "BEGIN" => Self::Begin,
            "COMMIT" => Self::Commit,
            "CREATE" => Self::Create,
            "DELETE" => Self::Delete,
            "DOUBLE" => Self::Double,
            "DROP" => Self::Drop,
            "EXISTS" => Self::Exists,
            "FALSE" => Self::False,
            "FLOAT" => Self::Float,
            "FROM" => Self::From,
            "IF" => Self::If,
            "INSERT" => Self::Insert,
            "INT" => Self::Int,
            "INTEGER" => Self::Integer,
            "INTO" => Self::Into,
            "INDEX" => Self::Index,
            "IS" => Self::Is,
            "KEY" => Self::Key,
            "LIMIT" => Self::Limit,
            "NOT" => Self::Not,
            "NULL" => Self::Null,
            "ON" => Self::On,
            "OR" => Self::Or,
            "PRIMARY" => Self::Primary,
            "ROLLBACK" => Self::Rollback,
            "SELECT" => Self::Select,
            "SET" => Self::Set,
            "START" => Self::Start,
            "TABLE" => Self::Table,
            "TEXT" => Self::Text,
            "TRUE" => Self::True,
            "TRANSACTION" => Self::Transaction,
            "UNIQUE" => Self::Unique,
            "UPDATE" => Self::Update,
            "VALUES" => Self::Values,
            "VARCHAR" => Self::Varchar,
            "WHERE" => Self::Where,
            "WORK" => Self::Work,
            _ => return None,
        })
    }
}

impl fmt::Display for Keyword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", format!("{self:?}").to_ascii_uppercase())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum TokenKind {
    Keyword(Keyword),
    Identifier { value: String, quoted: bool },
    Number(String),
    String(String),
    Comma,
    Dot,
    Star,
    LeftParen,
    RightParen,
    Semicolon,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Plus,
    Minus,
    Slash,
    Percent,
    Eof,
}

impl TokenKind {
    #[must_use]
    pub fn description(&self) -> String {
        match self {
            Self::Keyword(keyword) => keyword.to_string(),
            Self::Identifier { .. } => "identifier".to_owned(),
            Self::Number(_) => "number".to_owned(),
            Self::String(_) => "string".to_owned(),
            Self::Comma => "','".to_owned(),
            Self::Dot => "'.'".to_owned(),
            Self::Star => "'*'".to_owned(),
            Self::LeftParen => "'('".to_owned(),
            Self::RightParen => "')'".to_owned(),
            Self::Semicolon => "';'".to_owned(),
            Self::Equal => "'='".to_owned(),
            Self::NotEqual => "'!=' or '<>'".to_owned(),
            Self::LessThan => "'<'".to_owned(),
            Self::LessThanOrEqual => "'<='".to_owned(),
            Self::GreaterThan => "'>'".to_owned(),
            Self::GreaterThanOrEqual => "'>='".to_owned(),
            Self::Plus => "'+'".to_owned(),
            Self::Minus => "'-'".to_owned(),
            Self::Slash => "'/'".to_owned(),
            Self::Percent => "'%'".to_owned(),
            Self::Eof => "end of input".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}
