use crate::{
    tokenize, Assignment, BeginTransaction, BinaryOperator, ColumnDef, Commit, CreateIndex,
    CreateTable, DataType, Delete, DropIndex, DropTable, Expr, Identifier, Insert, Keyword,
    Literal, Rollback, Select, SelectItem, Span, Statement, SyntaxError, Token, TokenKind,
    UnaryOperator, Update,
};

const MAX_EXPRESSION_DEPTH: usize = 256;

/// Parse one or more semicolon-delimited SQL statements.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, SyntaxError> {
    Parser::new(tokenize(sql)?).parse_statements()
}

/// Parse exactly one SQL statement.
pub fn parse_statement(sql: &str) -> Result<Statement, SyntaxError> {
    let mut statements = parse_sql(sql)?;
    match statements.len() {
        0 => Err(SyntaxError::new(
            "expected a SQL statement",
            Span::new(0, 0),
        )),
        1 => Ok(statements.remove(0)),
        _ => Err(SyntaxError::new(
            "expected exactly one SQL statement",
            statements[1].span(),
        )),
    }
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
    expression_depth: usize,
}

impl Parser {
    const fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            cursor: 0,
            expression_depth: 0,
        }
    }

    fn parse_statements(mut self) -> Result<Vec<Statement>, SyntaxError> {
        let mut statements = Vec::new();
        while !self.at_eof() {
            while self
                .consume_punctuation(|kind| matches!(kind, TokenKind::Semicolon))
                .is_some()
            {}
            if self.at_eof() {
                break;
            }

            statements.push(self.parse_next_statement()?);

            if self
                .consume_punctuation(|kind| matches!(kind, TokenKind::Semicolon))
                .is_none()
                && !self.at_eof()
            {
                return Err(self.error_current("expected ';' between SQL statements"));
            }
        }
        Ok(statements)
    }

    fn parse_next_statement(&mut self) -> Result<Statement, SyntaxError> {
        match self.current().kind {
            TokenKind::Keyword(Keyword::Create) => self.parse_create_statement(),
            TokenKind::Keyword(Keyword::Drop) => self.parse_drop_statement(),
            TokenKind::Keyword(Keyword::Begin | Keyword::Start) => self
                .parse_begin_transaction()
                .map(Statement::BeginTransaction),
            TokenKind::Keyword(Keyword::Commit) => self.parse_commit().map(Statement::Commit),
            TokenKind::Keyword(Keyword::Rollback) => self.parse_rollback().map(Statement::Rollback),
            TokenKind::Keyword(Keyword::Insert) => self.parse_insert().map(Statement::Insert),
            TokenKind::Keyword(Keyword::Select) => self.parse_select().map(Statement::Select),
            TokenKind::Keyword(Keyword::Update) => self.parse_update().map(Statement::Update),
            TokenKind::Keyword(Keyword::Delete) => self.parse_delete().map(Statement::Delete),
            _ => Err(self.error_current(
                "expected CREATE, DROP, BEGIN, START TRANSACTION, COMMIT, ROLLBACK, INSERT, \
                     SELECT, UPDATE, or DELETE",
            )),
        }
    }

    fn parse_create_statement(&mut self) -> Result<Statement, SyntaxError> {
        let start = self.expect_keyword(Keyword::Create)?.span;
        let unique = self.consume_keyword(Keyword::Unique).is_some();
        match self.current().kind {
            TokenKind::Keyword(Keyword::Table) if !unique => self
                .parse_create_table_after_create(start)
                .map(Statement::CreateTable),
            TokenKind::Keyword(Keyword::Index) => self
                .parse_create_index_after_create(start, unique)
                .map(Statement::CreateIndex),
            _ if unique => Err(self.error_current("expected INDEX after CREATE UNIQUE")),
            _ => Err(self.error_current("expected TABLE or INDEX after CREATE")),
        }
    }

    fn parse_create_table_after_create(&mut self, start: Span) -> Result<CreateTable, SyntaxError> {
        self.expect_keyword(Keyword::Table)?;
        let if_not_exists = if self.consume_keyword(Keyword::If).is_some() {
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };
        let name = self.parse_identifier()?;
        self.expect_punctuation(
            |kind| matches!(kind, TokenKind::LeftParen),
            "expected '(' after table name",
        )?;

        let mut columns = Vec::new();
        if self.is_punctuation(|kind| matches!(kind, TokenKind::RightParen)) {
            return Err(self.error_current("a table must contain at least one column"));
        }
        loop {
            columns.push(self.parse_column_def()?);
            if self
                .consume_punctuation(|kind| matches!(kind, TokenKind::Comma))
                .is_none()
            {
                break;
            }
        }

        let end = self
            .expect_punctuation(
                |kind| matches!(kind, TokenKind::RightParen),
                "expected ')' after column definitions",
            )?
            .span;
        Ok(CreateTable {
            name,
            if_not_exists,
            columns,
            span: start.join(end),
        })
    }

    fn parse_create_index_after_create(
        &mut self,
        start: Span,
        unique: bool,
    ) -> Result<CreateIndex, SyntaxError> {
        self.expect_keyword(Keyword::Index)?;
        let if_not_exists = if self.consume_keyword(Keyword::If).is_some() {
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };
        let name = self.parse_identifier()?;
        self.expect_keyword(Keyword::On)?;
        let table = self.parse_identifier()?;
        self.expect_punctuation(
            |kind| matches!(kind, TokenKind::LeftParen),
            "expected '(' before index columns",
        )?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_identifier()?);
            if self
                .consume_punctuation(|kind| matches!(kind, TokenKind::Comma))
                .is_none()
            {
                break;
            }
        }
        let end = self
            .expect_punctuation(
                |kind| matches!(kind, TokenKind::RightParen),
                "expected ')' after index columns",
            )?
            .span;
        Ok(CreateIndex {
            name,
            table,
            columns,
            unique,
            if_not_exists,
            span: start.join(end),
        })
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, SyntaxError> {
        let name = self.parse_identifier()?;
        let start = name.span;
        let (data_type, mut end) = self.parse_data_type()?;
        let mut nullable = true;
        let mut primary_key = false;
        let mut unique = false;
        let mut saw_nullability = false;

        loop {
            if let Some(token) = self.consume_keyword(Keyword::Primary) {
                if primary_key {
                    return Err(SyntaxError::new("duplicate PRIMARY KEY", token.span));
                }
                end = self.expect_keyword(Keyword::Key)?.span;
                primary_key = true;
                nullable = false;
            } else if let Some(token) = self.consume_keyword(Keyword::Not) {
                if saw_nullability {
                    return Err(SyntaxError::new("duplicate NULL constraint", token.span));
                }
                end = self.expect_keyword(Keyword::Null)?.span;
                nullable = false;
                saw_nullability = true;
            } else if let Some(token) = self.consume_keyword(Keyword::Null) {
                if saw_nullability || primary_key {
                    return Err(SyntaxError::new("conflicting NULL constraint", token.span));
                }
                end = token.span;
                nullable = true;
                saw_nullability = true;
            } else if let Some(token) = self.consume_keyword(Keyword::Unique) {
                if unique {
                    return Err(SyntaxError::new("duplicate UNIQUE constraint", token.span));
                }
                end = token.span;
                unique = true;
            } else {
                break;
            }
        }

        Ok(ColumnDef {
            name,
            data_type,
            nullable,
            primary_key,
            unique,
            span: start.join(end),
        })
    }

    fn parse_data_type(&mut self) -> Result<(DataType, Span), SyntaxError> {
        let token = self.advance();
        match token.kind {
            TokenKind::Keyword(Keyword::Bool | Keyword::Boolean) => {
                Ok((DataType::Boolean, token.span))
            }
            TokenKind::Keyword(Keyword::Int | Keyword::Integer | Keyword::Bigint) => {
                Ok((DataType::Int64, token.span))
            }
            TokenKind::Keyword(Keyword::Float | Keyword::Double) => {
                Ok((DataType::Float64, token.span))
            }
            TokenKind::Keyword(Keyword::Text) => {
                Ok((DataType::Text { max_length: None }, token.span))
            }
            TokenKind::Keyword(Keyword::Varchar) => {
                let mut end = token.span;
                let max_length = if self
                    .consume_punctuation(|kind| matches!(kind, TokenKind::LeftParen))
                    .is_some()
                {
                    let length_token = self.advance();
                    let TokenKind::Number(raw_length) = length_token.kind else {
                        return Err(SyntaxError::new(
                            "expected VARCHAR length",
                            length_token.span,
                        ));
                    };
                    let length = raw_length.parse::<u32>().map_err(|_| {
                        SyntaxError::new("VARCHAR length is out of range", length_token.span)
                    })?;
                    if length == 0 {
                        return Err(SyntaxError::new(
                            "VARCHAR length must be greater than zero",
                            length_token.span,
                        ));
                    }
                    end = self
                        .expect_punctuation(
                            |kind| matches!(kind, TokenKind::RightParen),
                            "expected ')' after VARCHAR length",
                        )?
                        .span;
                    Some(length)
                } else {
                    None
                };
                Ok((DataType::Text { max_length }, token.span.join(end)))
            }
            _ => Err(SyntaxError::new(
                "expected BOOL, INT, FLOAT, TEXT, or VARCHAR data type",
                token.span,
            )),
        }
    }

    fn parse_drop_statement(&mut self) -> Result<Statement, SyntaxError> {
        let start = self.expect_keyword(Keyword::Drop)?.span;
        match self.current().kind {
            TokenKind::Keyword(Keyword::Table) => self
                .parse_drop_table_after_drop(start)
                .map(Statement::DropTable),
            TokenKind::Keyword(Keyword::Index) => self
                .parse_drop_index_after_drop(start)
                .map(Statement::DropIndex),
            _ => Err(self.error_current("expected TABLE or INDEX after DROP")),
        }
    }

    fn parse_drop_table_after_drop(&mut self, start: Span) -> Result<DropTable, SyntaxError> {
        self.expect_keyword(Keyword::Table)?;
        let if_exists = if self.consume_keyword(Keyword::If).is_some() {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };
        let name = self.parse_identifier()?;
        let span = start.join(name.span);
        Ok(DropTable {
            name,
            if_exists,
            span,
        })
    }

    fn parse_drop_index_after_drop(&mut self, start: Span) -> Result<DropIndex, SyntaxError> {
        self.expect_keyword(Keyword::Index)?;
        let if_exists = if self.consume_keyword(Keyword::If).is_some() {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };
        let name = self.parse_identifier()?;
        let span = start.join(name.span);
        Ok(DropIndex {
            name,
            if_exists,
            span,
        })
    }

    fn parse_begin_transaction(&mut self) -> Result<BeginTransaction, SyntaxError> {
        let start = self.advance();
        let end = match start.kind {
            TokenKind::Keyword(Keyword::Begin) => self
                .consume_keyword(Keyword::Transaction)
                .or_else(|| self.consume_keyword(Keyword::Work))
                .map_or(start.span, |token| token.span),
            TokenKind::Keyword(Keyword::Start) => self.expect_keyword(Keyword::Transaction)?.span,
            _ => return Err(SyntaxError::new("expected BEGIN or START", start.span)),
        };
        Ok(BeginTransaction {
            span: start.span.join(end),
        })
    }

    fn parse_commit(&mut self) -> Result<Commit, SyntaxError> {
        let start = self.expect_keyword(Keyword::Commit)?.span;
        let end = self
            .consume_keyword(Keyword::Transaction)
            .or_else(|| self.consume_keyword(Keyword::Work))
            .map_or(start, |token| token.span);
        Ok(Commit {
            span: start.join(end),
        })
    }

    fn parse_rollback(&mut self) -> Result<Rollback, SyntaxError> {
        let start = self.expect_keyword(Keyword::Rollback)?.span;
        let end = self
            .consume_keyword(Keyword::Transaction)
            .or_else(|| self.consume_keyword(Keyword::Work))
            .map_or(start, |token| token.span);
        Ok(Rollback {
            span: start.join(end),
        })
    }

    fn parse_insert(&mut self) -> Result<Insert, SyntaxError> {
        let start = self.expect_keyword(Keyword::Insert)?.span;
        self.expect_keyword(Keyword::Into)?;
        let table = self.parse_identifier()?;

        let mut columns = Vec::new();
        if self
            .consume_punctuation(|kind| matches!(kind, TokenKind::LeftParen))
            .is_some()
        {
            loop {
                columns.push(self.parse_identifier()?);
                if self
                    .consume_punctuation(|kind| matches!(kind, TokenKind::Comma))
                    .is_none()
                {
                    break;
                }
            }
            self.expect_punctuation(
                |kind| matches!(kind, TokenKind::RightParen),
                "expected ')' after INSERT columns",
            )?;
        }

        self.expect_keyword(Keyword::Values)?;
        let mut rows = Vec::new();
        let mut end;
        loop {
            self.expect_punctuation(
                |kind| matches!(kind, TokenKind::LeftParen),
                "expected '(' before VALUES row",
            )?;
            let mut values = Vec::new();
            if self.is_punctuation(|kind| matches!(kind, TokenKind::RightParen)) {
                return Err(self.error_current("VALUES rows cannot be empty"));
            }
            loop {
                values.push(self.parse_expression(0)?);
                if self
                    .consume_punctuation(|kind| matches!(kind, TokenKind::Comma))
                    .is_none()
                {
                    break;
                }
            }
            end = self
                .expect_punctuation(
                    |kind| matches!(kind, TokenKind::RightParen),
                    "expected ')' after VALUES row",
                )?
                .span;
            rows.push(values);
            if self
                .consume_punctuation(|kind| matches!(kind, TokenKind::Comma))
                .is_none()
            {
                break;
            }
        }

        Ok(Insert {
            table,
            columns,
            rows,
            span: start.join(end),
        })
    }

    fn parse_select(&mut self) -> Result<Select, SyntaxError> {
        let start = self.expect_keyword(Keyword::Select)?.span;
        let mut projection = Vec::new();
        loop {
            if let Some(star) = self.consume_punctuation(|kind| matches!(kind, TokenKind::Star)) {
                projection.push(SelectItem::Wildcard { span: star.span });
            } else {
                let expression = self.parse_expression(0)?;
                let alias = if self.consume_keyword(Keyword::As).is_some() {
                    Some(self.parse_identifier()?)
                } else {
                    None
                };
                let span = alias.as_ref().map_or_else(
                    || expression.span(),
                    |identifier| expression.span().join(identifier.span),
                );
                projection.push(SelectItem::Expression {
                    expression,
                    alias,
                    span,
                });
            }
            if self
                .consume_punctuation(|kind| matches!(kind, TokenKind::Comma))
                .is_none()
            {
                break;
            }
        }

        self.expect_keyword(Keyword::From)?;
        let from = self.parse_identifier()?;
        let selection = if self.consume_keyword(Keyword::Where).is_some() {
            Some(self.parse_expression(0)?)
        } else {
            None
        };
        let (limit, end) = if self.consume_keyword(Keyword::Limit).is_some() {
            let token = self.advance();
            let TokenKind::Number(raw_limit) = token.kind else {
                return Err(SyntaxError::new(
                    "expected a non-negative integer after LIMIT",
                    token.span,
                ));
            };
            if raw_limit.contains(['.', 'e', 'E']) {
                return Err(SyntaxError::new(
                    "LIMIT must be a non-negative integer",
                    token.span,
                ));
            }
            let limit = raw_limit
                .parse::<u64>()
                .map_err(|_| SyntaxError::new("LIMIT is out of range", token.span))?;
            (Some(limit), token.span)
        } else {
            let end = selection.as_ref().map_or(from.span, Expr::span);
            (None, end)
        };

        Ok(Select {
            projection,
            from,
            selection,
            limit,
            span: start.join(end),
        })
    }

    fn parse_update(&mut self) -> Result<Update, SyntaxError> {
        let start = self.expect_keyword(Keyword::Update)?.span;
        let table = self.parse_identifier()?;
        self.expect_keyword(Keyword::Set)?;
        let mut assignments = Vec::new();
        loop {
            let column = self.parse_identifier()?;
            self.expect_punctuation(
                |kind| matches!(kind, TokenKind::Equal),
                "expected '=' in assignment",
            )?;
            let value = self.parse_expression(0)?;
            let span = column.span.join(value.span());
            assignments.push(Assignment {
                column,
                value,
                span,
            });
            if self
                .consume_punctuation(|kind| matches!(kind, TokenKind::Comma))
                .is_none()
            {
                break;
            }
        }
        let selection = if self.consume_keyword(Keyword::Where).is_some() {
            Some(self.parse_expression(0)?)
        } else {
            None
        };
        let end = selection.as_ref().map_or_else(
            || assignments.last().map_or(table.span, |item| item.span),
            Expr::span,
        );
        Ok(Update {
            table,
            assignments,
            selection,
            span: start.join(end),
        })
    }

    fn parse_delete(&mut self) -> Result<Delete, SyntaxError> {
        let start = self.expect_keyword(Keyword::Delete)?.span;
        self.expect_keyword(Keyword::From)?;
        let table = self.parse_identifier()?;
        let selection = if self.consume_keyword(Keyword::Where).is_some() {
            Some(self.parse_expression(0)?)
        } else {
            None
        };
        let end = selection.as_ref().map_or(table.span, Expr::span);
        Ok(Delete {
            table,
            selection,
            span: start.join(end),
        })
    }

    fn parse_expression(&mut self, minimum_binding_power: u8) -> Result<Expr, SyntaxError> {
        if self.expression_depth >= MAX_EXPRESSION_DEPTH {
            return Err(self.error_current("expression nesting limit exceeded"));
        }
        self.expression_depth += 1;
        let result = self.parse_expression_inner(minimum_binding_power);
        self.expression_depth -= 1;
        result
    }

    fn parse_expression_inner(&mut self, minimum_binding_power: u8) -> Result<Expr, SyntaxError> {
        let mut left = self.parse_prefix_expression()?;

        loop {
            if self.is_keyword(Keyword::Is) {
                const IS_BINDING_POWER: u8 = 5;
                if IS_BINDING_POWER < minimum_binding_power {
                    break;
                }
                self.advance();
                let negated = self.consume_keyword(Keyword::Not).is_some();
                let null = self.expect_keyword(Keyword::Null)?;
                let span = left.span().join(null.span);
                left = Expr::IsNull {
                    expression: Box::new(left),
                    negated,
                    span,
                };
                continue;
            }

            let Some((operator, left_power, right_power)) = self.current_binary_operator() else {
                break;
            };
            if left_power < minimum_binding_power {
                break;
            }
            self.advance();
            let right = self.parse_expression(right_power)?;
            let span = left.span().join(right.span());
            left = Expr::Binary {
                left: Box::new(left),
                operator,
                right: Box::new(right),
                span,
            };
        }

        Ok(left)
    }

    fn parse_prefix_expression(&mut self) -> Result<Expr, SyntaxError> {
        let operator = match self.current().kind {
            TokenKind::Keyword(Keyword::Not) => Some((UnaryOperator::Not, 5)),
            TokenKind::Plus => Some((UnaryOperator::Plus, 11)),
            TokenKind::Minus => Some((UnaryOperator::Minus, 11)),
            _ => None,
        };

        if let Some((operator, binding_power)) = operator {
            let start = self.advance().span;
            let expression = self.parse_expression(binding_power)?;
            let span = start.join(expression.span());
            return Ok(Expr::Unary {
                operator,
                expression: Box::new(expression),
                span,
            });
        }

        self.parse_primary_expression()
    }

    fn parse_primary_expression(&mut self) -> Result<Expr, SyntaxError> {
        let token = self.advance();
        match token.kind {
            TokenKind::Identifier { value, quoted } => Ok(Expr::Identifier(Identifier {
                value,
                quoted,
                span: token.span,
            })),
            TokenKind::String(value) => Ok(Expr::Literal {
                value: Literal::String(value),
                span: token.span,
            }),
            TokenKind::Number(raw_number) => {
                let value = if raw_number.contains(['.', 'e', 'E']) {
                    Literal::Float(raw_number.parse::<f64>().map_err(|_| {
                        SyntaxError::new("floating-point literal is out of range", token.span)
                    })?)
                } else {
                    Literal::Integer(raw_number.parse::<i64>().map_err(|_| {
                        SyntaxError::new("integer literal is out of range", token.span)
                    })?)
                };
                Ok(Expr::Literal {
                    value,
                    span: token.span,
                })
            }
            TokenKind::Keyword(Keyword::Null) => Ok(Expr::Literal {
                value: Literal::Null,
                span: token.span,
            }),
            TokenKind::Keyword(Keyword::True | Keyword::False) => Ok(Expr::Literal {
                value: Literal::Boolean(matches!(token.kind, TokenKind::Keyword(Keyword::True))),
                span: token.span,
            }),
            TokenKind::LeftParen => {
                let expression = self.parse_expression(0)?;
                let end = self
                    .expect_punctuation(
                        |kind| matches!(kind, TokenKind::RightParen),
                        "expected ')' after expression",
                    )?
                    .span;
                Ok(Expr::Parenthesized {
                    expression: Box::new(expression),
                    span: token.span.join(end),
                })
            }
            _ => Err(SyntaxError::new("expected an expression", token.span)),
        }
    }

    fn current_binary_operator(&self) -> Option<(BinaryOperator, u8, u8)> {
        Some(match self.current().kind {
            TokenKind::Keyword(Keyword::Or) => (BinaryOperator::Or, 1, 2),
            TokenKind::Keyword(Keyword::And) => (BinaryOperator::And, 3, 4),
            TokenKind::Equal => (BinaryOperator::Equal, 5, 6),
            TokenKind::NotEqual => (BinaryOperator::NotEqual, 5, 6),
            TokenKind::LessThan => (BinaryOperator::LessThan, 5, 6),
            TokenKind::LessThanOrEqual => (BinaryOperator::LessThanOrEqual, 5, 6),
            TokenKind::GreaterThan => (BinaryOperator::GreaterThan, 5, 6),
            TokenKind::GreaterThanOrEqual => (BinaryOperator::GreaterThanOrEqual, 5, 6),
            TokenKind::Plus => (BinaryOperator::Add, 7, 8),
            TokenKind::Minus => (BinaryOperator::Subtract, 7, 8),
            TokenKind::Star => (BinaryOperator::Multiply, 9, 10),
            TokenKind::Slash => (BinaryOperator::Divide, 9, 10),
            TokenKind::Percent => (BinaryOperator::Modulo, 9, 10),
            _ => return None,
        })
    }

    fn parse_identifier(&mut self) -> Result<Identifier, SyntaxError> {
        let token = self.advance();
        let TokenKind::Identifier { value, quoted } = token.kind else {
            return Err(SyntaxError::new("expected an identifier", token.span));
        };
        Ok(Identifier {
            value,
            quoted,
            span: token.span,
        })
    }

    fn expect_keyword(&mut self, expected: Keyword) -> Result<Token, SyntaxError> {
        if self.is_keyword(expected) {
            Ok(self.advance())
        } else {
            Err(self.error_current(format!("expected {expected}")))
        }
    }

    fn consume_keyword(&mut self, expected: Keyword) -> Option<Token> {
        self.is_keyword(expected).then(|| self.advance())
    }

    fn is_keyword(&self, expected: Keyword) -> bool {
        self.current().kind == TokenKind::Keyword(expected)
    }

    fn expect_punctuation(
        &mut self,
        predicate: impl FnOnce(&TokenKind) -> bool,
        message: &str,
    ) -> Result<Token, SyntaxError> {
        if predicate(&self.current().kind) {
            Ok(self.advance())
        } else {
            Err(self.error_current(message))
        }
    }

    fn consume_punctuation(&mut self, predicate: impl FnOnce(&TokenKind) -> bool) -> Option<Token> {
        predicate(&self.current().kind).then(|| self.advance())
    }

    fn is_punctuation(&self, predicate: impl FnOnce(&TokenKind) -> bool) -> bool {
        predicate(&self.current().kind)
    }

    fn at_eof(&self) -> bool {
        matches!(self.current().kind, TokenKind::Eof)
    }

    fn current(&self) -> &Token {
        let index = self.cursor.min(self.tokens.len().saturating_sub(1));
        &self.tokens[index]
    }

    fn advance(&mut self) -> Token {
        let token = self.current().clone();
        if !matches!(token.kind, TokenKind::Eof) {
            self.cursor += 1;
        }
        token
    }

    fn error_current(&self, message: impl Into<String>) -> SyntaxError {
        SyntaxError::new(message, self.current().span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_table_constraints() {
        let statement = parse_statement(
            "CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                email VARCHAR(320) NOT NULL UNIQUE,
                active BOOL NULL
            );",
        )
        .expect("CREATE TABLE should parse");

        let Statement::CreateTable(create) = statement else {
            panic!("expected CREATE TABLE");
        };
        assert!(create.if_not_exists);
        assert_eq!(create.name.value, "users");
        assert_eq!(create.columns.len(), 3);
        assert!(create.columns[0].primary_key);
        assert!(!create.columns[1].nullable);
        assert_eq!(
            create.columns[1].data_type,
            DataType::Text {
                max_length: Some(320)
            }
        );
    }

    #[test]
    fn parses_index_ddl() {
        let create = parse_statement(
            "CREATE UNIQUE INDEX IF NOT EXISTS users_email ON users (email, tenant_id)",
        )
        .expect("CREATE INDEX should parse");
        let Statement::CreateIndex(create) = create else {
            panic!("expected CREATE INDEX");
        };
        assert!(create.unique);
        assert!(create.if_not_exists);
        assert_eq!(create.name.value, "users_email");
        assert_eq!(create.table.value, "users");
        assert_eq!(
            create
                .columns
                .iter()
                .map(|column| column.value.as_str())
                .collect::<Vec<_>>(),
            vec!["email", "tenant_id"]
        );

        let drop =
            parse_statement("DROP INDEX IF EXISTS users_email").expect("DROP INDEX should parse");
        let Statement::DropIndex(drop) = drop else {
            panic!("expected DROP INDEX");
        };
        assert!(drop.if_exists);
        assert_eq!(drop.name.value, "users_email");
    }

    #[test]
    fn parses_transaction_control_variants() {
        let statements = parse_sql(
            "BEGIN; BEGIN TRANSACTION; BEGIN WORK; START TRANSACTION;
             COMMIT; COMMIT WORK; ROLLBACK TRANSACTION;",
        )
        .expect("transaction statements should parse");
        assert_eq!(statements.len(), 7);
        assert!(statements[..4]
            .iter()
            .all(|statement| matches!(statement, Statement::BeginTransaction(_))));
        assert!(matches!(statements[4], Statement::Commit(_)));
        assert!(matches!(statements[5], Statement::Commit(_)));
        assert!(matches!(statements[6], Statement::Rollback(_)));
    }

    #[test]
    fn rejects_incomplete_index_and_transaction_syntax() {
        assert_eq!(
            parse_statement("CREATE UNIQUE TABLE broken")
                .expect_err("CREATE UNIQUE TABLE must fail")
                .message(),
            "expected INDEX after CREATE UNIQUE"
        );
        assert_eq!(
            parse_statement("CREATE INDEX idx ON users ()")
                .expect_err("empty index key must fail")
                .message(),
            "expected an identifier"
        );
        assert_eq!(
            parse_statement("START")
                .expect_err("START requires TRANSACTION")
                .message(),
            "expected TRANSACTION"
        );
    }

    #[test]
    fn parses_multi_row_insert_with_columns() {
        let statement = parse_statement(
            "INSERT INTO users (id, email) VALUES (1, 'a@example.com'), (2, 'b@example.com')",
        )
        .expect("INSERT should parse");
        let Statement::Insert(insert) = statement else {
            panic!("expected INSERT");
        };
        assert_eq!(insert.columns.len(), 2);
        assert_eq!(insert.rows.len(), 2);
    }

    #[test]
    fn honors_boolean_and_arithmetic_precedence() {
        let statement = parse_statement(
            "SELECT id, price * 2 AS doubled FROM products
             WHERE active = true OR stock > 0 AND price + 1 < 100 LIMIT 25",
        )
        .expect("SELECT should parse");
        let Statement::Select(select) = statement else {
            panic!("expected SELECT");
        };
        assert_eq!(select.limit, Some(25));
        let Expr::Binary {
            operator: BinaryOperator::Or,
            right,
            ..
        } = select.selection.expect("selection")
        else {
            panic!("OR should be the root expression");
        };
        assert!(matches!(
            *right,
            Expr::Binary {
                operator: BinaryOperator::And,
                ..
            }
        ));
    }

    #[test]
    fn parses_update_delete_and_multiple_statements() {
        let statements = parse_sql(
            "UPDATE users SET active = false, score = score + 1 WHERE id = 7;
             DELETE FROM users WHERE active IS NOT NULL;",
        )
        .expect("statements should parse");
        assert!(matches!(statements[0], Statement::Update(_)));
        assert!(matches!(statements[1], Statement::Delete(_)));
    }

    #[test]
    fn returns_precise_errors() {
        let error = parse_statement("SELECT * users").expect_err("missing FROM must fail");
        assert_eq!(error.message(), "expected FROM");
        assert_eq!(
            &"SELECT * users"[error.span().start..error.span().end],
            "users"
        );
    }

    #[test]
    fn rejects_multiple_statements_when_one_is_required() {
        let error = parse_statement("SELECT * FROM a; SELECT * FROM b")
            .expect_err("multiple statements must fail");
        assert_eq!(error.message(), "expected exactly one SQL statement");
    }

    #[test]
    fn rejects_pathologically_nested_expressions() {
        let sql = format!(
            "SELECT {}1{} FROM values_table",
            "(".repeat(MAX_EXPRESSION_DEPTH + 1),
            ")".repeat(MAX_EXPRESSION_DEPTH + 1)
        );
        let error = parse_statement(&sql).expect_err("nesting limit must be enforced");
        assert_eq!(error.message(), "expression nesting limit exceeded");
    }
}
