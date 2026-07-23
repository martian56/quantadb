use crate::{
    codec::{encode_identity, row_key, unique_key},
    expression::evaluate,
    EngineError, Result, TableSchema, Value,
};
use quantadb_mvcc::Transaction;
use quantadb_syntax::{BinaryOperator, Expr};

pub(crate) enum AccessPath {
    Scan,
    Point(Option<Vec<u8>>),
}

pub(crate) fn plan(
    transaction: &Transaction,
    schema: &TableSchema,
    selection: Option<&Expr>,
) -> Result<AccessPath> {
    let Some((column_name, expression)) = selection.and_then(equality_column_and_constant) else {
        return Ok(AccessPath::Scan);
    };
    let Some(position) = schema
        .columns
        .iter()
        .position(|column| column.name == column_name)
    else {
        return Err(EngineError::ColumnNotFound(column_name.to_owned()));
    };
    let column = &schema.columns[position];
    if !column.primary_key && !column.unique {
        return Ok(AccessPath::Scan);
    }
    let mut value = evaluate(expression, schema, &vec![Value::Null; schema.columns.len()])?;
    if value.is_null() {
        return Ok(AccessPath::Point(None));
    }
    if matches!(column.data_type, crate::LogicalType::Float64) {
        value = match value {
            Value::Integer(integer) => Value::Float(integer as f64),
            value => value,
        };
    }
    column.validate(&value)?;

    if column.primary_key {
        return Ok(AccessPath::Point(Some(row_key(
            schema.id,
            &encode_identity(&value)?,
        )?)));
    }
    let owner = transaction.get(&unique_key(schema.id, position, &value)?)?;
    Ok(AccessPath::Point(owner))
}

fn equality_column_and_constant(expression: &Expr) -> Option<(&str, &Expr)> {
    let Expr::Binary {
        left,
        operator: BinaryOperator::Equal,
        right,
        ..
    } = expression
    else {
        return None;
    };
    match (&**left, &**right) {
        (Expr::Identifier(identifier), constant) if is_constant(constant) => {
            Some((&identifier.value, constant))
        }
        (constant, Expr::Identifier(identifier)) if is_constant(constant) => {
            Some((&identifier.value, constant))
        }
        _ => None,
    }
}

fn is_constant(expression: &Expr) -> bool {
    match expression {
        Expr::Identifier(_) => false,
        Expr::Literal { .. } => true,
        Expr::Unary { expression, .. }
        | Expr::IsNull { expression, .. }
        | Expr::Parenthesized { expression, .. } => is_constant(expression),
        Expr::Binary { left, right, .. } => is_constant(left) && is_constant(right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quantadb_syntax::{parse_statement, Statement};

    #[test]
    fn recognizes_only_column_constant_equalities() {
        let Statement::Select(select) =
            parse_statement("SELECT * FROM t WHERE 7 = id").expect("parse")
        else {
            panic!("select");
        };
        assert!(matches!(
            equality_column_and_constant(select.selection.as_ref().expect("where")),
            Some(("id", _))
        ));

        let Statement::Select(select) =
            parse_statement("SELECT * FROM t WHERE id > 7").expect("parse")
        else {
            panic!("select");
        };
        assert!(equality_column_and_constant(select.selection.as_ref().expect("where")).is_none());
    }
}
