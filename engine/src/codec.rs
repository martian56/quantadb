use crate::{schema::IndexSchema, EngineError, Result, TableSchema, Value};
use serde::{Deserialize, Serialize};

const CATALOG_PREFIX: &[u8] = b"\0quanta/catalog/table/";
const INDEX_CATALOG_PREFIX: &[u8] = b"\0quanta/catalog/index/";
const ROW_PREFIX: &[u8] = b"\0quanta/row/";
const UNIQUE_PREFIX: &[u8] = b"\0quanta/unique/";
const INDEX_ENTRY_PREFIX: &[u8] = b"\0quanta/index/";
const TABLE_ID_COUNTER_KEY: &[u8] = b"\0quanta/catalog/next_table_id";
const INDEX_ID_COUNTER_KEY: &[u8] = b"\0quanta/catalog/next_index_id";
const ROW_FORMAT_VERSION: u16 = 1;

#[derive(Serialize, Deserialize)]
struct RowRecord {
    format_version: u16,
    values: Vec<Value>,
}

pub(crate) fn table_id_counter_key() -> &'static [u8] {
    TABLE_ID_COUNTER_KEY
}

pub(crate) fn catalog_key(name: &str) -> Result<Vec<u8>> {
    component_key(CATALOG_PREFIX, name.as_bytes())
}

pub(crate) fn index_id_counter_key() -> &'static [u8] {
    INDEX_ID_COUNTER_KEY
}

pub(crate) fn index_catalog_key(name: &str) -> Result<Vec<u8>> {
    component_key(INDEX_CATALOG_PREFIX, name.as_bytes())
}

pub(crate) fn index_catalog_prefix() -> &'static [u8] {
    INDEX_CATALOG_PREFIX
}

pub(crate) fn index_entry_prefix(index_id: u64) -> Vec<u8> {
    let mut key = INDEX_ENTRY_PREFIX.to_vec();
    key.extend_from_slice(&index_id.to_be_bytes());
    key
}

/// The entry key prefix shared by every row with these indexed values.
///
/// Each value is length delimited so multi-column boundaries are
/// unambiguous, and a shorter column list is a strict byte prefix of a
/// longer one, which is what makes leading-column lookups work.
pub(crate) fn index_value_prefix(index_id: u64, values: &[&Value]) -> Result<Vec<u8>> {
    let mut key = index_entry_prefix(index_id);
    for value in values {
        let identity = encode_identity(value)?;
        let length = u32::try_from(identity.len())
            .map_err(|_| EngineError::InvalidRow("indexed value is too large".to_owned()))?;
        key.extend_from_slice(&length.to_be_bytes());
        key.extend_from_slice(&identity);
    }
    Ok(key)
}

/// The full entry key for one row in one index.
///
/// Unique indexes key on the values alone, so two rows with equal values
/// collide inside a transaction and conflict across transactions. Regular
/// indexes append the row key, so equal values coexist and an entry is
/// removable without touching its neighbors.
pub(crate) fn index_entry_key(
    index: &IndexSchema,
    values: &[&Value],
    row_key: &[u8],
) -> Result<Vec<u8>> {
    let mut key = index_value_prefix(index.id, values)?;
    if !index.unique {
        let length = u32::try_from(row_key.len())
            .map_err(|_| EngineError::InvalidRow("row key is too large".to_owned()))?;
        key.extend_from_slice(&length.to_be_bytes());
        key.extend_from_slice(row_key);
    }
    Ok(key)
}

pub(crate) fn encode_index(index: &IndexSchema) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(index)?)
}

pub(crate) fn decode_index(bytes: &[u8]) -> Result<IndexSchema> {
    let index: IndexSchema = serde_json::from_slice(bytes)?;
    if index.format_version != 1 {
        return Err(EngineError::CorruptRecord(format!(
            "unsupported index format version {}",
            index.format_version
        )));
    }
    Ok(index)
}

pub(crate) fn row_prefix(table_id: u64) -> Vec<u8> {
    let mut key = ROW_PREFIX.to_vec();
    key.extend_from_slice(&table_id.to_be_bytes());
    key
}

pub(crate) fn unique_prefix(table_id: u64) -> Vec<u8> {
    let mut key = UNIQUE_PREFIX.to_vec();
    key.extend_from_slice(&table_id.to_be_bytes());
    key
}

pub(crate) fn unique_key(table_id: u64, column: usize, value: &Value) -> Result<Vec<u8>> {
    let column = u32::try_from(column)
        .map_err(|_| EngineError::InvalidSchema("column position exceeds u32".to_owned()))?;
    let identity = encode_identity(value)?;
    let mut key = unique_prefix(table_id);
    key.extend_from_slice(&column.to_be_bytes());
    key.extend_from_slice(&(identity.len() as u32).to_be_bytes());
    key.extend_from_slice(&identity);
    Ok(key)
}

pub(crate) fn row_key(table_id: u64, identity: &[u8]) -> Result<Vec<u8>> {
    let mut key = row_prefix(table_id);
    let length = u32::try_from(identity.len())
        .map_err(|_| EngineError::InvalidRow("row identity is too large".to_owned()))?;
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(identity);
    Ok(key)
}

pub(crate) fn encode_identity(value: &Value) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    match value {
        Value::Null => {
            return Err(EngineError::ConstraintViolation(
                "primary key cannot be NULL".to_owned(),
            ));
        }
        Value::Boolean(value) => {
            bytes.push(1);
            bytes.push(u8::from(*value));
        }
        Value::Integer(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        Value::Float(value) => {
            if !value.is_finite() {
                return Err(EngineError::InvalidRow(
                    "primary key cannot be a non-finite float".to_owned(),
                ));
            }
            bytes.push(3);
            let bits = value.to_bits();
            let ordered = if bits >> 63 == 0 {
                bits ^ (1_u64 << 63)
            } else {
                !bits
            };
            bytes.extend_from_slice(&ordered.to_be_bytes());
        }
        Value::Text(value) => {
            bytes.push(4);
            bytes.extend_from_slice(value.as_bytes());
        }
    }
    Ok(bytes)
}

pub(crate) fn encode_schema(schema: &TableSchema) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(schema)?)
}

pub(crate) fn decode_schema(bytes: &[u8]) -> Result<TableSchema> {
    let schema: TableSchema = serde_json::from_slice(bytes)?;
    if schema.format_version != 1 {
        return Err(EngineError::CorruptRecord(format!(
            "unsupported schema format version {}",
            schema.format_version
        )));
    }
    Ok(schema)
}

pub(crate) fn encode_row(values: &[Value]) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&RowRecord {
        format_version: ROW_FORMAT_VERSION,
        values: values.to_vec(),
    })?)
}

pub(crate) fn decode_row(bytes: &[u8]) -> Result<Vec<Value>> {
    let row: RowRecord = serde_json::from_slice(bytes)?;
    if row.format_version != ROW_FORMAT_VERSION {
        return Err(EngineError::CorruptRecord(format!(
            "unsupported row format version {}",
            row.format_version
        )));
    }
    Ok(row.values)
}

pub(crate) fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

pub(crate) fn decode_u64(bytes: &[u8]) -> Result<u64> {
    bytes.try_into().map(u64::from_be_bytes).map_err(|_| {
        EngineError::CorruptRecord(format!("expected an 8-byte integer, got {}", bytes.len()))
    })
}

fn component_key(prefix: &[u8], component: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(component.len())
        .map_err(|_| EngineError::InvalidSchema("identifier is too large".to_owned()))?;
    let mut key = Vec::with_capacity(prefix.len() + 4 + component.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(component);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_keys_are_unambiguous_and_rows_round_trip() {
        assert_ne!(
            catalog_key("ab").expect("key"),
            catalog_key("a").expect("key")
        );
        assert_ne!(
            row_key(1, b"identity").expect("row key"),
            row_key(2, b"identity").expect("row key")
        );
        let values = vec![
            Value::Null,
            Value::Integer(-7),
            Value::Text("hello".to_owned()),
        ];
        assert_eq!(
            decode_row(&encode_row(&values).expect("encode")).expect("decode"),
            values
        );
    }
}
