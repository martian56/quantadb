use crate::sql::ast::{Statement as QuantaStatement, *};
use crate::storage::{Column, Value, DataType};
use crate::error::{QuantaError, Result};
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct SqlParser {
    dialect: GenericDialect,
}

impl SqlParser {
    pub fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    pub fn parse(&self, sql: &str) -> Result<QuantaStatement> {
        let mut parser = Parser::new(&self.dialect).try_with_sql(sql)
            .map_err(|e| QuantaError::SqlParseError(e.to_string()))?;

        let ast = parser.parse_statement()
            .map_err(|e| QuantaError::SqlParseError(e.to_string()))?;

        self.convert_statement(ast)
    }

    fn convert_statement(&self, stmt: sqlparser::ast::Statement) -> Result<QuantaStatement> {
        match stmt {
            sqlparser::ast::Statement::CreateTable { name, columns, .. } => {
                let table_name = self.object_name_to_string(name)?;
                let converted_columns = self.convert_columns(columns)?;
                Ok(QuantaStatement::CreateTable(CreateTableStmt {
                    table_name,
                    columns: converted_columns,
                }))
            }
            sqlparser::ast::Statement::Drop { object_type, if_exists: _, names, cascade: _, restrict: _, purge: _, temporary: _ } => {
                if object_type != ObjectType::Table {
                    return Err(QuantaError::SqlParseError("Only DROP TABLE is supported".to_string()));
                }
                if names.len() != 1 {
                    return Err(QuantaError::SqlParseError("DROP TABLE expects exactly one table name".to_string()));
                }
                let table_name = self.object_name_to_string(names[0].clone())?;
                Ok(QuantaStatement::DropTable(DropTableStmt { table_name }))
            }
            sqlparser::ast::Statement::Insert { table_name, source, .. } => {
                let table_name = self.object_name_to_string(table_name)?;
                let values = self.convert_insert_values(source.expect("INSERT source is required"))?;
                Ok(QuantaStatement::Insert(InsertStmt {
                    table_name,
                    values,
                }))
            }
            sqlparser::ast::Statement::Query(query) => {
                self.convert_query(*query)
            }
            sqlparser::ast::Statement::Delete { from, selection, .. } => {
                if from.len() != 1 {
                    return Err(QuantaError::SqlParseError("DELETE expects exactly one table".to_string()));
                }
                let table_name = match &from[0].relation {
                    TableFactor::Table { name, .. } => self.object_name_to_string(name.clone())?,
                    _ => return Err(QuantaError::SqlParseError("Only table references are supported in DELETE".to_string())),
                };
                let where_clause = if let Some(expr) = selection {
                    Some(WhereClause {
                        condition: self.convert_expr_to_condition(expr)?,
                    })
                } else {
                    None
                };
                Ok(QuantaStatement::Delete(DeleteStmt {
                    table_name,
                    where_clause,
                }))
            }
            _ => Err(QuantaError::SqlParseError(format!("Unsupported statement: {:?}", stmt))),
        }
    }

    fn convert_query(&self, query: Query) -> Result<QuantaStatement> {
        if let SetExpr::Select(select) = *query.body {
            if select.from.len() != 1 {
                return Err(QuantaError::SqlParseError("SELECT expects exactly one table".to_string()));
            }

            let table_name = match &select.from[0].relation {
                TableFactor::Table { name, .. } => self.object_name_to_string(name.clone())?,
                _ => return Err(QuantaError::SqlParseError("Only table references are supported in SELECT".to_string())),
            };
            
            let columns = if select.projection.len() == 1 {
                match &select.projection[0] {
                    SelectItem::Wildcard(_) => SelectColumns::All,
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                        SelectColumns::Specific(vec![ident.value.clone()])
                    }
                    _ => return Err(QuantaError::SqlParseError("Unsupported SELECT projection".to_string())),
                }
            } else {
                let mut column_names = Vec::new();
                for item in select.projection {
                    match item {
                        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                            column_names.push(ident.value.clone());
                        }
                        _ => return Err(QuantaError::SqlParseError("Unsupported SELECT projection".to_string())),
                    }
                }
                SelectColumns::Specific(column_names)
            };

            let where_clause = if let Some(expr) = select.selection {
                Some(WhereClause {
                    condition: self.convert_expr_to_condition(expr)?,
                })
            } else {
                None
            };

            Ok(QuantaStatement::Select(SelectStmt {
                table_name,
                columns,
                where_clause,
            }))
        } else {
            Err(QuantaError::SqlParseError("Only SELECT queries are supported".to_string()))
        }
    }

    fn convert_columns(&self, columns: Vec<ColumnDef>) -> Result<Vec<Column>> {
        let mut result = Vec::new();
        for col_def in columns {
            let name = col_def.name.value.clone();
            let data_type = self.convert_data_type(&col_def.data_type)?;
            result.push(Column { name, data_type });
        }
        Ok(result)
    }

    fn convert_data_type(&self, data_type: &sqlparser::ast::DataType) -> Result<DataType> {
        match data_type {
            sqlparser::ast::DataType::Int(_) => Ok(DataType::Int),
            sqlparser::ast::DataType::Varchar(_) | sqlparser::ast::DataType::Text => Ok(DataType::Text),
            sqlparser::ast::DataType::Boolean => Ok(DataType::Bool),
            sqlparser::ast::DataType::Float(_) | sqlparser::ast::DataType::Double => Ok(DataType::Float),
            _ => Err(QuantaError::SqlParseError(format!("Unsupported data type: {:?}", data_type))),
        }
    }

    fn convert_insert_values(&self, source: Box<Query>) -> Result<Vec<Value>> {
        if let SetExpr::Values(values) = *source.body {
            if values.rows.len() != 1 {
                return Err(QuantaError::SqlParseError("INSERT expects exactly one row".to_string()));
            }
            let mut result = Vec::new();
            for expr in &values.rows[0] {
                result.push(self.convert_expr_to_value(expr)?);
            }
            Ok(result)
        } else {
            Err(QuantaError::SqlParseError("INSERT VALUES expected".to_string()))
        }
    }

    fn convert_expr_to_value(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Value(value) => match value {
                sqlparser::ast::Value::Number(n, _) => {
                    if n.contains('.') {
                        Ok(Value::Float(n.parse().map_err(|_| QuantaError::SqlParseError("Invalid float".to_string()))?))
                    } else {
                        Ok(Value::Int(n.parse().map_err(|_| QuantaError::SqlParseError("Invalid integer".to_string()))?))
                    }
                }
                sqlparser::ast::Value::SingleQuotedString(s) | sqlparser::ast::Value::DoubleQuotedString(s) => {
                    Ok(Value::Text(s.clone()))
                }
                sqlparser::ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
                _ => Err(QuantaError::SqlParseError(format!("Unsupported value: {:?}", value))),
            },
            _ => Err(QuantaError::SqlParseError(format!("Unsupported expression in VALUES: {:?}", expr))),
        }
    }

    fn convert_expr_to_condition(&self, expr: Expr) -> Result<Condition> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_expr = self.convert_expr_to_expression(*left)?;
                let right_expr = self.convert_expr_to_expression(*right)?;
                let operator = self.convert_binary_op(op)?;
                Ok(Condition::Comparison {
                    left: left_expr,
                    operator,
                    right: right_expr,
                })
            }
            Expr::Nested(expr) => {
                Ok(Condition::Parenthesized {
                    condition: Box::new(self.convert_expr_to_condition(*expr)?),
                })
            }
            _ => Err(QuantaError::SqlParseError(format!("Unsupported expression in WHERE clause: {:?}", expr))),
        }
    }

    fn convert_expr_to_expression(&self, expr: Expr) -> Result<Expression> {
        match expr {
            Expr::Identifier(ident) => Ok(Expression::Column(ident.value)),
            Expr::Value(value) => {
                let val = self.convert_sqlparser_value_to_value(value)?;
                Ok(Expression::Literal(val))
            }
            _ => Err(QuantaError::SqlParseError(format!("Unsupported expression: {:?}", expr))),
        }
    }

    fn convert_sqlparser_value_to_value(&self, value: sqlparser::ast::Value) -> Result<Value> {
        match value {
            sqlparser::ast::Value::Number(n, _) => {
                if n.contains('.') {
                    Ok(Value::Float(n.parse().map_err(|_| QuantaError::SqlParseError("Invalid float".to_string()))?))
                } else {
                    Ok(Value::Int(n.parse().map_err(|_| QuantaError::SqlParseError("Invalid integer".to_string()))?))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) | sqlparser::ast::Value::DoubleQuotedString(s) => {
                Ok(Value::Text(s))
            }
            sqlparser::ast::Value::Boolean(b) => Ok(Value::Bool(b)),
            _ => Err(QuantaError::SqlParseError(format!("Unsupported value: {:?}", value))),
        }
    }

    fn convert_binary_op(&self, op: BinaryOperator) -> Result<ComparisonOp> {
        match op {
            BinaryOperator::Eq => Ok(ComparisonOp::Equal),
            BinaryOperator::NotEq => Ok(ComparisonOp::NotEqual),
            BinaryOperator::Lt => Ok(ComparisonOp::LessThan),
            BinaryOperator::Gt => Ok(ComparisonOp::GreaterThan),
            BinaryOperator::LtEq => Ok(ComparisonOp::LessThanOrEqual),
            BinaryOperator::GtEq => Ok(ComparisonOp::GreaterThanOrEqual),
            _ => Err(QuantaError::SqlParseError(format!("Unsupported comparison operator: {:?}", op))),
        }
    }

    fn object_name_to_string(&self, name: ObjectName) -> Result<String> {
        if name.0.len() == 1 {
            Ok(name.0[0].value.clone())
        } else {
            Err(QuantaError::SqlParseError("Multi-part table names not supported".to_string()))
        }
    }
}
