use std::collections::HashMap;

use crate::server::connection::Connection;
use crate::store::registry;
use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;

/// Execute an SQL INSERT statement.
/// Returns (insert_id, optional_select_result) on success.
pub fn sql_insert(
    query: &str,
    _metadata: &OwnedRow,
    _conn: Option<&Connection>,
) -> Result<(String, Option<Vec<OwnedRow>>), String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let ast = match Parser::parse_sql(&dialect, query) {
        Ok(stmts) => stmts,
        Err(e) => return Err(format!("SQL parse error: {}", e)),
    };

    if ast.is_empty() {
        return Err("Empty SQL statement".into());
    }

    match &ast[0] {
        sqlparser::ast::Statement::Insert(insert) => {
            let table_name = insert.table_name.to_string().replace('`', "");

            // Extract column names
            let column_names: Vec<String> = insert
                .columns
                .iter()
                .map(|c| c.value.clone())
                .collect();

            // Extract values from the source query (INSERT INTO ... VALUES ...)
            let source = insert.source.as_ref()
                .ok_or("INSERT missing VALUES clause")?;

            let rows_values = extract_insert_values(source.body.as_ref())?;

            if rows_values.is_empty() {
                return Err("No values to insert".into());
            }

            let table = registry::get_table(&table_name)
                .ok_or_else(|| format!("Table doesn't exist: {}", table_name))?;

            let mut last_insert_id = String::new();

            for values in &rows_values {
                let mut columns = HashMap::new();

                if column_names.is_empty() {
                    // No column names specified — values are positional
                    // This is uncommon in Uniform; treat as error
                    return Err("Column names are required for INSERT".into());
                }

                for (i, col_name) in column_names.iter().enumerate() {
                    if i < values.len() {
                        columns.insert(col_name.clone(), values[i].clone());
                    }
                }

                match table.insert(columns) {
                    Ok(seq_id) => {
                        // Get the Fb_id from the inserted row
                        if let Some(entry) = table.rows.get(&seq_id) {
                            if let Some(Value::String(id)) = entry.value().columns.get("Fb_id") {
                                last_insert_id = id.clone();
                            }
                        }

                        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                            logging::log(&format!(
                                "INSERT: inserted row seq_id={} into table {}",
                                seq_id, table_name
                            ));
                        }
                    }
                    Err(e) => return Err(e),
                }
            }

            // TODO: Phase 3 — run insert pipeline procs
            Ok((last_insert_id, None))
        }
        _ => Err("Expected INSERT statement".into()),
    }
}

/// Insert a JSON string into a table (RPUSH command).
pub fn insert_json_string(
    table_name: &str,
    json_data: &str,
    _metadata: &OwnedRow,
    _conn: Option<&Connection>,
) -> Result<(String, Option<Vec<OwnedRow>>), String> {
    let table = registry::get_table(table_name)
        .ok_or_else(|| format!("Table doesn't exist: {}", table_name))?;

    // Parse JSON
    let parsed: serde_json::Value =
        serde_json::from_str(json_data).map_err(|e| format!("Invalid JSON: {}", e))?;

    let columns = match parsed {
        serde_json::Value::Object(map) => {
            let mut cols = HashMap::new();
            for (k, v) in map {
                cols.insert(k, Value::from(v));
            }
            cols
        }
        _ => return Err("JSON data must be an object".into()),
    };

    match table.insert(columns) {
        Ok(seq_id) => {
            let mut insert_id = String::new();
            if let Some(entry) = table.rows.get(&seq_id) {
                if let Some(Value::String(id)) = entry.value().columns.get("Fb_id") {
                    insert_id = id.clone();
                }
            }

            if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                logging::log(&format!(
                    "RPUSH: inserted row seq_id={} into table {}",
                    seq_id, table_name
                ));
            }

            // TODO: Phase 3 — run insert pipeline procs
            Ok((insert_id, None))
        }
        Err(e) => Err(e),
    }
}

/// Extract values from INSERT statement body.
fn extract_insert_values(
    body: &sqlparser::ast::SetExpr,
) -> Result<Vec<Vec<Value>>, String> {
    match body {
        sqlparser::ast::SetExpr::Values(values) => {
            let mut result = Vec::new();
            for row_values in &values.rows {
                let mut row = Vec::new();
                for expr in row_values {
                    row.push(expr_to_value(expr));
                }
                result.push(row);
            }
            Ok(result)
        }
        _ => Err("Unsupported INSERT body".into()),
    }
}

/// Convert a SQL expression to a Value.
fn expr_to_value(expr: &sqlparser::ast::Expr) -> Value {
    match expr {
        sqlparser::ast::Expr::Value(v) => match v {
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(f) = n.parse::<f64>() {
                    Value::Number(f)
                } else {
                    Value::String(n.clone())
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => Value::String(s.clone()),
            sqlparser::ast::Value::DoubleQuotedString(s) => Value::String(s.clone()),
            sqlparser::ast::Value::Boolean(b) => Value::Bool(*b),
            sqlparser::ast::Value::Null => Value::Null,
            other => Value::String(format!("{}", other)),
        },
        sqlparser::ast::Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr: inner,
        } => {
            if let sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) = inner.as_ref()
            {
                if let Ok(f) = n.parse::<f64>() {
                    Value::Number(-f)
                } else {
                    Value::String(format!("-{}", n))
                }
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}
