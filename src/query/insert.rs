use std::collections::HashMap;

use crate::query::insert_pipeline;
use crate::server::connection::Connection;
use crate::store::registry::{self, PROCS};
use crate::types::row::OwnedRow;
use crate::types::value::Value;

/// Execute an SQL INSERT statement.
/// Returns (insert_id, optional_select_result) on success.
pub fn sql_insert(
    query: &str,
    metadata: &OwnedRow,
    conn: Option<&Connection>,
) -> Result<(String, Option<Vec<OwnedRow>>), String> {
    // Try the fast hand-rolled parser first; fall back to sqlparser on failure.
    let (table_name, column_names, rows_values) = match fast_parse_insert(query) {
        Some(parsed) => parsed,
        None => slow_parse_insert(query)?,
    };

    if column_names.is_empty() {
        return Err("Column names are required for INSERT".into());
    }
    if rows_values.is_empty() {
        return Err("No values to insert".into());
    }

    // Fast batch path: if no procs are configured for this table, skip the
    // full 13-stage pipeline and insert directly into storage.
    let table = registry::get_table(&table_name)
        .ok_or_else(|| format!("Table <{}> doesn't exist.", table_name))?;
    let has_procs = {
        let procs = PROCS.read();
        procs
            .iter()
            .any(|p| p.enabled && (p.src == table_name || p.table == table_name))
    };

    if has_procs {
        return sql_insert_slow(&table_name, &column_names, &rows_values, metadata, conn);
    }

    // --- Fast path: no procs, batch all rows with amortized overhead ---
    let now = chrono::Utc::now();
    let now_str = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let role = crate::util::cluster::get_cluster_role();
    let version_key = format!("x_uniform_{}", role);
    let version_val = Value::String(crate::config::vars::version().to_string());

    let mut last_insert_id = String::new();

    for values in rows_values {
        let mut columns = HashMap::with_capacity(column_names.len() + 3);

        // Move values instead of cloning (values is consumed)
        for (col_name, val) in column_names.iter().zip(values) {
            columns.insert(col_name.clone(), val);
        }

        // Standard fields (normally set by insert_pipeline)
        let u_id = uuid::Uuid::new_v4().to_string();
        columns
            .entry("u_created_at".into())
            .or_insert_with(|| Value::String(now_str.clone()));
        columns
            .entry("u_id".into())
            .or_insert_with(|| Value::String(u_id.clone()));
        columns.insert(version_key.clone(), version_val.clone());

        // Use insert_raw to skip Row::new's redundant UUID/timestamp generation.
        table.insert_raw(columns, now).map_err(|e| e.to_string())?;
        last_insert_id = u_id;
    }

    Ok((last_insert_id, None))
}

/// Slow per-row INSERT path through the full pipeline (used when procs exist).
fn sql_insert_slow(
    table_name: &str,
    column_names: &[String],
    rows_values: &[Vec<Value>],
    metadata: &OwnedRow,
    conn: Option<&Connection>,
) -> Result<(String, Option<Vec<OwnedRow>>), String> {
    let now = chrono::Utc::now();
    let mut last_insert_id = String::new();

    for values in rows_values {
        let mut columns = HashMap::with_capacity(column_names.len());

        for (i, col_name) in column_names.iter().enumerate() {
            if i < values.len() {
                columns.insert(col_name.clone(), values[i].clone());
            }
        }

        let mut owned_row = OwnedRow {
            seq_id: 0,
            columns,
            created: now,
        };
        let (ok, err, _result, id) =
            insert_pipeline::insert(table_name, &mut owned_row, metadata, true, conn);
        if !ok {
            return Err(err);
        }
        last_insert_id = id;
    }

    Ok((last_insert_id, None))
}

// ---------------------------------------------------------------------------
// Fast INSERT parser — zero-alloc scan, no AST.
// Handles: INSERT INTO [<db>.]<table> (<cols>) VALUES (<vals>), ...
// Returns None on anything it can't handle (subqueries, DEFAULT, expressions).
// ---------------------------------------------------------------------------

/// Try to parse an INSERT statement without building an AST.
fn fast_parse_insert(query: &str) -> Option<(String, Vec<String>, Vec<Vec<Value>>)> {
    let b = query.as_bytes();
    let len = b.len();
    let mut pos = skip_ws(b, 0);

    // INSERT
    pos = expect_keyword(b, pos, b"INSERT")?;
    pos = skip_ws(b, pos);

    // Optional: IGNORE / INTO
    if let Some(p) = expect_keyword(b, pos, b"IGNORE") {
        pos = skip_ws(b, p);
    }
    pos = expect_keyword(b, pos, b"INTO")?;
    pos = skip_ws(b, pos);

    // Table name (may be `db`.`table` or just table)
    let (table_name, p) = parse_ident(b, pos)?;
    pos = skip_ws(b, p);

    // Skip optional db.table prefix
    let (table_name, mut pos) = if pos < len && b[pos] == b'.' {
        let p = pos + 1;
        let p = skip_ws(b, p);
        let (name, p) = parse_ident(b, p)?;
        (name, skip_ws(b, p))
    } else {
        (table_name, pos)
    };

    // ( column_list )
    if pos >= len || b[pos] != b'(' {
        return None;
    }
    pos += 1;
    let mut column_names = Vec::new();
    loop {
        pos = skip_ws(b, pos);
        let (name, p) = parse_ident(b, pos)?;
        column_names.push(name);
        pos = skip_ws(b, p);
        if pos >= len {
            return None;
        }
        if b[pos] == b')' {
            pos += 1;
            break;
        }
        if b[pos] == b',' {
            pos += 1;
        } else {
            return None;
        }
    }
    pos = skip_ws(b, pos);

    // VALUES
    pos = expect_keyword(b, pos, b"VALUES")?;
    pos = skip_ws(b, pos);

    // Value tuples
    let ncols = column_names.len();
    let mut rows = Vec::new();
    loop {
        if pos >= len || b[pos] != b'(' {
            return None;
        }
        pos += 1;

        let mut row = Vec::with_capacity(ncols);
        loop {
            pos = skip_ws(b, pos);
            let (val, p) = parse_value(b, pos)?;
            row.push(val);
            pos = skip_ws(b, p);
            if pos >= len {
                return None;
            }
            if b[pos] == b')' {
                pos += 1;
                break;
            }
            if b[pos] == b',' {
                pos += 1;
            } else {
                return None;
            }
        }
        rows.push(row);

        pos = skip_ws(b, pos);
        if pos >= len {
            break;
        }
        if b[pos] == b',' {
            pos += 1;
            pos = skip_ws(b, pos);
            continue;
        }
        // Trailing semicolon or end
        break;
    }

    Some((table_name, column_names, rows))
}

/// Parse a SQL value literal. Returns (Value, new_pos).
fn parse_value(b: &[u8], pos: usize) -> Option<(Value, usize)> {
    let len = b.len();
    if pos >= len {
        return None;
    }

    match b[pos] {
        // Single-quoted string
        b'\'' => {
            let mut s = String::new();
            let mut i = pos + 1;
            while i < len {
                if b[i] == b'\'' {
                    if i + 1 < len && b[i + 1] == b'\'' {
                        // Escaped quote
                        s.push('\'');
                        i += 2;
                        continue;
                    }
                    return Some((Value::String(s), i + 1));
                }
                if b[i] == b'\\' && i + 1 < len {
                    // Backslash escape
                    i += 1;
                    match b[i] {
                        b'\'' => s.push('\''),
                        b'\\' => s.push('\\'),
                        b'n' => s.push('\n'),
                        b'r' => s.push('\r'),
                        b't' => s.push('\t'),
                        b'0' => s.push('\0'),
                        other => {
                            s.push('\\');
                            s.push(other as char);
                        }
                    }
                    i += 1;
                    continue;
                }
                s.push(b[i] as char);
                i += 1;
            }
            None // unterminated string
        }
        // Double-quoted string
        b'"' => {
            let mut s = String::new();
            let mut i = pos + 1;
            while i < len {
                if b[i] == b'"' {
                    if i + 1 < len && b[i + 1] == b'"' {
                        s.push('"');
                        i += 2;
                        continue;
                    }
                    return Some((Value::String(s), i + 1));
                }
                if b[i] == b'\\' && i + 1 < len {
                    i += 1;
                    match b[i] {
                        b'"' => s.push('"'),
                        b'\\' => s.push('\\'),
                        b'n' => s.push('\n'),
                        b'r' => s.push('\r'),
                        b't' => s.push('\t'),
                        b'0' => s.push('\0'),
                        other => {
                            s.push('\\');
                            s.push(other as char);
                        }
                    }
                    i += 1;
                    continue;
                }
                s.push(b[i] as char);
                i += 1;
            }
            None
        }
        // Number or negative number
        b'-' | b'0'..=b'9' => {
            let start = pos;
            let mut i = pos;
            if b[i] == b'-' {
                i += 1;
            }
            while i < len && (b[i].is_ascii_digit() || b[i] == b'.') {
                i += 1;
            }
            // Scientific notation
            if i < len && (b[i] == b'e' || b[i] == b'E') {
                i += 1;
                if i < len && (b[i] == b'+' || b[i] == b'-') {
                    i += 1;
                }
                while i < len && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            let num_str = std::str::from_utf8(&b[start..i]).ok()?;
            if let Ok(f) = num_str.parse::<f64>() {
                Some((Value::Number(f), i))
            } else {
                Some((Value::String(num_str.to_string()), i))
            }
        }
        // NULL / TRUE / FALSE keyword
        b'N' | b'n' => expect_keyword(b, pos, b"NULL").map(|p| (Value::Null, p)),
        b'T' | b't' => expect_keyword(b, pos, b"TRUE").map(|p| (Value::Bool(true), p)),
        b'F' | b'f' => expect_keyword(b, pos, b"FALSE").map(|p| (Value::Bool(false), p)),
        // Anything else (DEFAULT, expressions, subqueries) — bail out
        _ => None,
    }
}

/// Parse an identifier (backtick-quoted or bare).
fn parse_ident(b: &[u8], pos: usize) -> Option<(String, usize)> {
    let len = b.len();
    if pos >= len {
        return None;
    }
    if b[pos] == b'`' {
        // Backtick-quoted
        let mut i = pos + 1;
        while i < len && b[i] != b'`' {
            i += 1;
        }
        if i >= len {
            return None;
        }
        let name = std::str::from_utf8(&b[pos + 1..i]).ok()?.to_string();
        Some((name, i + 1))
    } else if b[pos].is_ascii_alphabetic() || b[pos] == b'_' {
        let start = pos;
        let mut i = pos;
        while i < len && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
            i += 1;
        }
        let name = std::str::from_utf8(&b[start..i]).ok()?.to_string();
        Some((name, i))
    } else {
        None
    }
}

/// Skip whitespace, return new position.
fn skip_ws(b: &[u8], mut pos: usize) -> usize {
    let len = b.len();
    while pos < len && b[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

/// Case-insensitive keyword match. Returns position after keyword if matched,
/// ensuring the keyword is not followed by an alphanumeric char.
fn expect_keyword(b: &[u8], pos: usize, keyword: &[u8]) -> Option<usize> {
    let end = pos + keyword.len();
    if end > b.len() {
        return None;
    }
    for (i, &kc) in keyword.iter().enumerate() {
        if !b[pos + i].eq_ignore_ascii_case(&kc) {
            return None;
        }
    }
    // Must not be followed by alphanumeric/underscore (word boundary)
    if end < b.len() && (b[end].is_ascii_alphanumeric() || b[end] == b'_') {
        return None;
    }
    Some(end)
}

// ---------------------------------------------------------------------------
// Fallback: full sqlparser path
// ---------------------------------------------------------------------------

/// Parse INSERT using sqlparser (handles all edge cases).
type ParsedInsert = (String, Vec<String>, Vec<Vec<Value>>);

fn slow_parse_insert(query: &str) -> Result<ParsedInsert, String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, query).map_err(|e| format!("SQL parse error: {}", e))?;

    if ast.is_empty() {
        return Err("Empty SQL statement".into());
    }

    match &ast[0] {
        sqlparser::ast::Statement::Insert(insert) => {
            let table_name = insert.table_name.to_string().replace('`', "");
            let column_names: Vec<String> =
                insert.columns.iter().map(|c| c.value.clone()).collect();

            let source = insert
                .source
                .as_ref()
                .ok_or("INSERT missing VALUES clause")?;

            let rows_values = extract_insert_values(source.body.as_ref())?;
            Ok((table_name, column_names, rows_values))
        }
        _ => Err("Expected INSERT statement".into()),
    }
}

/// Extract values from INSERT statement body (sqlparser AST).
fn extract_insert_values(body: &sqlparser::ast::SetExpr) -> Result<Vec<Vec<Value>>, String> {
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

/// Convert a SQL expression to a Value (sqlparser AST).
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

/// Insert a JSON string into a table (RPUSH command).
pub fn insert_json_string(
    table_name: &str,
    json_data: &str,
    metadata: &OwnedRow,
    conn: Option<&Connection>,
) -> Result<(String, Option<Vec<OwnedRow>>), String> {
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

    let mut row = OwnedRow {
        seq_id: 0,
        columns,
        created: chrono::Utc::now(),
    };
    let (ok, err, result, insert_id) =
        insert_pipeline::insert(table_name, &mut row, metadata, false, conn);
    if !ok {
        return Err(err);
    }
    Ok((insert_id, result))
}
