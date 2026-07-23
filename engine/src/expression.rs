use crate::{EngineError, LogicalType, Result, TableSchema, Value};
use quantadb_syntax::{BinaryOperator, Expr, Literal, UnaryOperator};

pub(crate) fn evaluate(expr: &Expr, schema: &TableSchema, row: &[Value]) -> Result<Value> {
    match expr {
        Expr::Identifier(identifier) => schema
            .columns
            .iter()
            .position(|column| column.name == identifier.value)
            .and_then(|position| row.get(position).cloned())
            .ok_or_else(|| EngineError::ColumnNotFound(identifier.value.clone())),
        Expr::Literal { value, .. } => literal(value),
        Expr::Parenthesized { expression, .. } => evaluate(expression, schema, row),
        Expr::IsNull {
            expression,
            negated,
            ..
        } => Ok(Value::Boolean(
            evaluate(expression, schema, row)?.is_null() != *negated,
        )),
        Expr::Unary {
            operator,
            expression,
            ..
        } => unary(*operator, evaluate(expression, schema, row)?),
        Expr::Binary {
            left,
            operator,
            right,
            ..
        } => binary(
            *operator,
            evaluate(left, schema, row)?,
            evaluate(right, schema, row)?,
        ),
    }
}

pub(crate) fn infer_type(expr: &Expr, schema: &TableSchema) -> Result<(LogicalType, bool)> {
    Ok(match expr {
        Expr::Identifier(identifier) => {
            let column = schema
                .columns
                .iter()
                .find(|column| column.name == identifier.value)
                .ok_or_else(|| EngineError::ColumnNotFound(identifier.value.clone()))?;
            (column.data_type.clone(), column.nullable)
        }
        Expr::Literal { value, .. } => match value {
            Literal::Null => (LogicalType::Unknown, true),
            Literal::Boolean(_) => (LogicalType::Boolean, false),
            Literal::Integer(_) => (LogicalType::Int64, false),
            Literal::Float(_) => (LogicalType::Float64, false),
            Literal::String(_) => (LogicalType::Text { max_length: None }, false),
        },
        Expr::IsNull { .. } => (LogicalType::Boolean, false),
        Expr::Parenthesized { expression, .. } | Expr::Unary { expression, .. } => {
            infer_type(expression, schema)?
        }
        Expr::Binary {
            operator,
            left,
            right,
            ..
        } => {
            let (left_type, left_null) = infer_type(left, schema)?;
            let (right_type, right_null) = infer_type(right, schema)?;
            let data_type = match operator {
                BinaryOperator::Or
                | BinaryOperator::And
                | BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual => LogicalType::Boolean,
                _ if matches!(left_type, LogicalType::Float64)
                    || matches!(right_type, LogicalType::Float64) =>
                {
                    LogicalType::Float64
                }
                _ => LogicalType::Int64,
            };
            (data_type, left_null || right_null)
        }
    })
}

pub(crate) fn predicate(expr: &Expr, schema: &TableSchema, row: &[Value]) -> Result<bool> {
    match evaluate(expr, schema, row)? {
        Value::Boolean(value) => Ok(value),
        Value::Null => Ok(false),
        _ => Err(EngineError::Expression(
            "WHERE expression must be boolean".to_owned(),
        )),
    }
}

fn literal(literal: &Literal) -> Result<Value> {
    Ok(match literal {
        Literal::Null => Value::Null,
        Literal::Boolean(value) => Value::Boolean(*value),
        Literal::Integer(value) => Value::Integer(*value),
        Literal::Float(value) if value.is_finite() => Value::Float(*value),
        Literal::Float(_) => {
            return Err(EngineError::Expression(
                "non-finite float literal is not supported".to_owned(),
            ));
        }
        Literal::String(value) => Value::Text(value.clone()),
    })
}

fn unary(operator: UnaryOperator, value: Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match (operator, value) {
        (UnaryOperator::Not, Value::Boolean(value)) => Ok(Value::Boolean(!value)),
        (UnaryOperator::Plus, Value::Integer(value)) => Ok(Value::Integer(value)),
        (UnaryOperator::Plus, Value::Float(value)) => Ok(Value::Float(value)),
        (UnaryOperator::Minus, Value::Integer(value)) => value
            .checked_neg()
            .map(Value::Integer)
            .ok_or_else(|| EngineError::Expression("integer negation overflow".to_owned())),
        (UnaryOperator::Minus, Value::Float(value)) => Ok(Value::Float(-value)),
        _ => Err(EngineError::Expression(
            "invalid operand for unary operator".to_owned(),
        )),
    }
}

fn binary(operator: BinaryOperator, left: Value, right: Value) -> Result<Value> {
    use BinaryOperator as Op;
    if matches!(operator, Op::And | Op::Or) {
        return boolean_logic(operator, left, right);
    }
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    match operator {
        Op::Equal
        | Op::NotEqual
        | Op::LessThan
        | Op::LessThanOrEqual
        | Op::GreaterThan
        | Op::GreaterThanOrEqual => compare(operator, left, right),
        Op::Add | Op::Subtract | Op::Multiply | Op::Divide | Op::Modulo => {
            arithmetic(operator, left, right)
        }
        Op::And | Op::Or => unreachable!("handled above"),
    }
}

fn boolean_logic(operator: BinaryOperator, left: Value, right: Value) -> Result<Value> {
    let boolean = |value| match value {
        Value::Boolean(value) => Ok(Some(value)),
        Value::Null => Ok(None),
        _ => Err(EngineError::Expression(
            "AND/OR operands must be boolean".to_owned(),
        )),
    };
    let left = boolean(left)?;
    let right = boolean(right)?;
    Ok(match operator {
        BinaryOperator::And => match (left, right) {
            (Some(false), _) | (_, Some(false)) => Value::Boolean(false),
            (Some(true), Some(true)) => Value::Boolean(true),
            _ => Value::Null,
        },
        BinaryOperator::Or => match (left, right) {
            (Some(true), _) | (_, Some(true)) => Value::Boolean(true),
            (Some(false), Some(false)) => Value::Boolean(false),
            _ => Value::Null,
        },
        _ => unreachable!("caller passes only boolean operators"),
    })
}

fn compare(operator: BinaryOperator, left: Value, right: Value) -> Result<Value> {
    let ordering = match (&left, &right) {
        (Value::Boolean(left), Value::Boolean(right)) => left.partial_cmp(right),
        (Value::Integer(left), Value::Integer(right)) => left.partial_cmp(right),
        (Value::Float(left), Value::Float(right)) => left.partial_cmp(right),
        (Value::Integer(left), Value::Float(right)) => (*left as f64).partial_cmp(right),
        (Value::Float(left), Value::Integer(right)) => left.partial_cmp(&(*right as f64)),
        (Value::Text(left), Value::Text(right)) => left.partial_cmp(right),
        _ => {
            return Err(EngineError::Expression(
                "values of these types cannot be compared".to_owned(),
            ));
        }
    }
    .ok_or_else(|| EngineError::Expression("comparison is unordered".to_owned()))?;
    Ok(Value::Boolean(match operator {
        BinaryOperator::Equal => ordering.is_eq(),
        BinaryOperator::NotEqual => !ordering.is_eq(),
        BinaryOperator::LessThan => ordering.is_lt(),
        BinaryOperator::LessThanOrEqual => !ordering.is_gt(),
        BinaryOperator::GreaterThan => ordering.is_gt(),
        BinaryOperator::GreaterThanOrEqual => !ordering.is_lt(),
        _ => unreachable!("caller passes only comparison operators"),
    }))
}

fn arithmetic(operator: BinaryOperator, left: Value, right: Value) -> Result<Value> {
    if let (Value::Integer(left), Value::Integer(right)) = (&left, &right) {
        let value = match operator {
            BinaryOperator::Add => left.checked_add(*right),
            BinaryOperator::Subtract => left.checked_sub(*right),
            BinaryOperator::Multiply => left.checked_mul(*right),
            BinaryOperator::Divide => left.checked_div(*right),
            BinaryOperator::Modulo => left.checked_rem(*right),
            _ => unreachable!("caller passes only arithmetic operators"),
        };
        return value.map(Value::Integer).ok_or_else(|| {
            EngineError::Expression("integer overflow or division by zero".to_owned())
        });
    }
    let number = |value| match value {
        Value::Integer(value) => Ok(value as f64),
        Value::Float(value) => Ok(value),
        _ => Err(EngineError::Expression(
            "arithmetic operands must be numeric".to_owned(),
        )),
    };
    let left = number(left)?;
    let right = number(right)?;
    if right == 0.0 && matches!(operator, BinaryOperator::Divide | BinaryOperator::Modulo) {
        return Err(EngineError::Expression("division by zero".to_owned()));
    }
    let result = match operator {
        BinaryOperator::Add => left + right,
        BinaryOperator::Subtract => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => left / right,
        BinaryOperator::Modulo => left % right,
        _ => unreachable!("caller passes only arithmetic operators"),
    };
    if !result.is_finite() {
        return Err(EngineError::Expression(
            "floating-point result is not finite".to_owned(),
        ));
    }
    Ok(Value::Float(result))
}
