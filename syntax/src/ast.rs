use serde::{Deserialize, Serialize};

/// Half-open byte range into the source SQL.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    #[must_use]
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    #[must_use]
    pub const fn join(self, other: Self) -> Self {
        Self {
            start: self.start,
            end: other.end,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identifier {
    pub value: String,
    pub quoted: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Statement {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    DropTable(DropTable),
    DropIndex(DropIndex),
    BeginTransaction(BeginTransaction),
    Commit(Commit),
    Rollback(Rollback),
    Insert(Insert),
    Select(Select),
    Update(Update),
    Delete(Delete),
}

impl Statement {
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::CreateTable(statement) => statement.span,
            Self::CreateIndex(statement) => statement.span,
            Self::DropTable(statement) => statement.span,
            Self::DropIndex(statement) => statement.span,
            Self::BeginTransaction(statement) => statement.span,
            Self::Commit(statement) => statement.span,
            Self::Rollback(statement) => statement.span,
            Self::Insert(statement) => statement.span,
            Self::Select(statement) => statement.span,
            Self::Update(statement) => statement.span,
            Self::Delete(statement) => statement.span,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DataType {
    Boolean,
    Int64,
    Float64,
    Text { max_length: Option<u32> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: Identifier,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTable {
    pub name: Identifier,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropTable {
    pub name: Identifier,
    pub if_exists: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateIndex {
    pub name: Identifier,
    pub table: Identifier,
    pub columns: Vec<Identifier>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropIndex {
    pub name: Identifier,
    pub if_exists: bool,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginTransaction {
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rollback {
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Insert {
    pub table: Identifier,
    pub columns: Vec<Identifier>,
    pub rows: Vec<Vec<Expr>>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Select {
    pub projection: Vec<SelectItem>,
    pub from: Identifier,
    pub selection: Option<Expr>,
    pub limit: Option<u64>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SelectItem {
    Wildcard {
        span: Span,
    },
    Expression {
        expression: Expr,
        alias: Option<Identifier>,
        span: Span,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: Identifier,
    pub value: Expr,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub table: Identifier,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub table: Identifier,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnaryOperator {
    Not,
    Plus,
    Minus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOperator {
    Or,
    And,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expr {
    Identifier(Identifier),
    Literal {
        value: Literal,
        span: Span,
    },
    Unary {
        operator: UnaryOperator,
        expression: Box<Self>,
        span: Span,
    },
    Binary {
        left: Box<Self>,
        operator: BinaryOperator,
        right: Box<Self>,
        span: Span,
    },
    IsNull {
        expression: Box<Self>,
        negated: bool,
        span: Span,
    },
    Parenthesized {
        expression: Box<Self>,
        span: Span,
    },
}

impl Expr {
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Identifier(identifier) => identifier.span,
            Self::Literal { span, .. }
            | Self::Unary { span, .. }
            | Self::Binary { span, .. }
            | Self::IsNull { span, .. }
            | Self::Parenthesized { span, .. } => *span,
        }
    }
}
