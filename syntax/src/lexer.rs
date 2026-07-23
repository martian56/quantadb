use crate::{Keyword, Span, SyntaxError, Token, TokenKind};

/// Tokenize SQL while preserving byte offsets into the original UTF-8 input.
pub fn tokenize(sql: &str) -> Result<Vec<Token>, SyntaxError> {
    Lexer::new(sql).tokenize()
}

struct Lexer<'a> {
    sql: &'a str,
    cursor: usize,
}

impl<'a> Lexer<'a> {
    const fn new(sql: &'a str) -> Self {
        Self { sql, cursor: 0 }
    }

    fn tokenize(mut self) -> Result<Vec<Token>, SyntaxError> {
        let mut tokens = Vec::new();
        loop {
            self.skip_trivia()?;
            if self.cursor == self.sql.len() {
                tokens.push(Token {
                    kind: TokenKind::Eof,
                    span: Span::new(self.cursor, self.cursor),
                });
                return Ok(tokens);
            }
            tokens.push(self.next_token()?);
        }
    }

    fn next_token(&mut self) -> Result<Token, SyntaxError> {
        let start = self.cursor;
        let Some(character) = self.peek_char() else {
            return Ok(Token {
                kind: TokenKind::Eof,
                span: Span::new(start, start),
            });
        };

        let kind = match character {
            ',' => {
                self.advance();
                TokenKind::Comma
            }
            '.' => {
                self.advance();
                TokenKind::Dot
            }
            '*' => {
                self.advance();
                TokenKind::Star
            }
            '(' => {
                self.advance();
                TokenKind::LeftParen
            }
            ')' => {
                self.advance();
                TokenKind::RightParen
            }
            ';' => {
                self.advance();
                TokenKind::Semicolon
            }
            '=' => {
                self.advance();
                TokenKind::Equal
            }
            '+' => {
                self.advance();
                TokenKind::Plus
            }
            '-' => {
                self.advance();
                TokenKind::Minus
            }
            '/' => {
                self.advance();
                TokenKind::Slash
            }
            '%' => {
                self.advance();
                TokenKind::Percent
            }
            '<' => {
                self.advance();
                if self.consume_char('=') {
                    TokenKind::LessThanOrEqual
                } else if self.consume_char('>') {
                    TokenKind::NotEqual
                } else {
                    TokenKind::LessThan
                }
            }
            '>' => {
                self.advance();
                if self.consume_char('=') {
                    TokenKind::GreaterThanOrEqual
                } else {
                    TokenKind::GreaterThan
                }
            }
            '!' => {
                self.advance();
                if self.consume_char('=') {
                    TokenKind::NotEqual
                } else {
                    return Err(SyntaxError::new(
                        "expected '=' after '!'",
                        Span::new(start, self.cursor),
                    ));
                }
            }
            '\'' => return self.lex_string(),
            '"' => return self.lex_quoted_identifier(),
            character if character.is_ascii_digit() => return self.lex_number(),
            character if is_identifier_start(character) => return Ok(self.lex_identifier()),
            _ => {
                self.advance();
                return Err(SyntaxError::new(
                    format!("unexpected character '{character}'"),
                    Span::new(start, self.cursor),
                ));
            }
        };

        Ok(Token {
            kind,
            span: Span::new(start, self.cursor),
        })
    }

    fn skip_trivia(&mut self) -> Result<(), SyntaxError> {
        loop {
            while self.peek_char().is_some_and(char::is_whitespace) {
                self.advance();
            }

            if self.remaining().starts_with("--") {
                self.cursor += 2;
                while self.peek_char().is_some_and(|character| character != '\n') {
                    self.advance();
                }
                continue;
            }

            if self.remaining().starts_with("/*") {
                self.skip_block_comment()?;
                continue;
            }

            return Ok(());
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), SyntaxError> {
        let start = self.cursor;
        self.cursor += 2;
        let mut depth = 1_u32;

        while self.cursor < self.sql.len() {
            if self.remaining().starts_with("/*") {
                self.cursor += 2;
                depth = depth.saturating_add(1);
            } else if self.remaining().starts_with("*/") {
                self.cursor += 2;
                depth -= 1;
                if depth == 0 {
                    return Ok(());
                }
            } else {
                self.advance();
            }
        }

        Err(SyntaxError::new(
            "unterminated block comment",
            Span::new(start, self.sql.len()),
        ))
    }

    fn lex_identifier(&mut self) -> Token {
        let start = self.cursor;
        self.advance();
        while self.peek_char().is_some_and(is_identifier_continue) {
            self.advance();
        }

        let value = &self.sql[start..self.cursor];
        let kind = Keyword::from_identifier(value).map_or_else(
            || TokenKind::Identifier {
                value: value.to_lowercase(),
                quoted: false,
            },
            TokenKind::Keyword,
        );

        Token {
            kind,
            span: Span::new(start, self.cursor),
        }
    }

    fn lex_quoted_identifier(&mut self) -> Result<Token, SyntaxError> {
        let start = self.cursor;
        self.advance();
        let mut value = String::new();

        loop {
            let Some(character) = self.peek_char() else {
                return Err(SyntaxError::new(
                    "unterminated quoted identifier",
                    Span::new(start, self.sql.len()),
                ));
            };
            self.advance();
            if character == '"' {
                if self.consume_char('"') {
                    value.push('"');
                } else {
                    break;
                }
            } else {
                value.push(character);
            }
        }

        Ok(Token {
            kind: TokenKind::Identifier {
                value,
                quoted: true,
            },
            span: Span::new(start, self.cursor),
        })
    }

    fn lex_string(&mut self) -> Result<Token, SyntaxError> {
        let start = self.cursor;
        self.advance();
        let mut value = String::new();

        loop {
            let Some(character) = self.peek_char() else {
                return Err(SyntaxError::new(
                    "unterminated string literal",
                    Span::new(start, self.sql.len()),
                ));
            };
            self.advance();
            if character == '\'' {
                if self.consume_char('\'') {
                    value.push('\'');
                } else {
                    break;
                }
            } else {
                value.push(character);
            }
        }

        Ok(Token {
            kind: TokenKind::String(value),
            span: Span::new(start, self.cursor),
        })
    }

    fn lex_number(&mut self) -> Result<Token, SyntaxError> {
        let start = self.cursor;
        self.consume_ascii_digits();

        if self.peek_char() == Some('.')
            && self.peek_second_char().is_some_and(|c| c.is_ascii_digit())
        {
            self.advance();
            self.consume_ascii_digits();
        }

        if self.peek_char().is_some_and(|c| matches!(c, 'e' | 'E')) {
            self.advance();
            if self.peek_char().is_some_and(|c| matches!(c, '+' | '-')) {
                self.advance();
            }
            let exponent_start = self.cursor;
            self.consume_ascii_digits();
            if self.cursor == exponent_start {
                return Err(SyntaxError::new(
                    "expected digits in numeric exponent",
                    Span::new(start, self.cursor),
                ));
            }
        }

        Ok(Token {
            kind: TokenKind::Number(self.sql[start..self.cursor].to_owned()),
            span: Span::new(start, self.cursor),
        })
    }

    fn consume_ascii_digits(&mut self) {
        while self.peek_char().is_some_and(|c| c.is_ascii_digit()) {
            self.advance();
        }
    }

    fn consume_char(&mut self, expected: char) -> bool {
        if self.peek_char() == Some(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn remaining(&self) -> &'a str {
        &self.sql[self.cursor..]
    }

    fn peek_char(&self) -> Option<char> {
        self.remaining().chars().next()
    }

    fn peek_second_char(&self) -> Option<char> {
        self.remaining().chars().nth(1)
    }

    fn advance(&mut self) {
        if let Some(character) = self.peek_char() {
            self.cursor += character.len_utf8();
        }
    }
}

fn is_identifier_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

fn is_identifier_continue(character: char) -> bool {
    character == '_' || character == '$' || character.is_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_comments_unicode_and_escaped_values() {
        let tokens = tokenize(
            "/* outer /* nested */ done */ SELECT \"Odd\"\"Name\", 'it''s', café -- x\nFROM t;",
        )
        .expect("SQL should tokenize");

        assert_eq!(
            tokens[1].kind,
            TokenKind::Identifier {
                value: "Odd\"Name".to_owned(),
                quoted: true
            }
        );
        assert_eq!(tokens[3].kind, TokenKind::String("it's".to_owned()));
        assert_eq!(
            tokens[5].kind,
            TokenKind::Identifier {
                value: "café".to_owned(),
                quoted: false
            }
        );
    }

    #[test]
    fn rejects_unterminated_constructs() {
        let string_error = tokenize("SELECT 'broken").expect_err("string must fail");
        assert_eq!(string_error.message(), "unterminated string literal");

        let comment_error = tokenize("SELECT /* broken").expect_err("comment must fail");
        assert_eq!(comment_error.message(), "unterminated block comment");
    }

    #[test]
    fn rejects_malformed_exponents() {
        let error = tokenize("SELECT 1e+").expect_err("exponent must fail");
        assert_eq!(error.message(), "expected digits in numeric exponent");
    }
}
