use std::sync::atomic::Ordering;

use crate::query::condition;
use crate::query::insert_pipeline;
use crate::query::planner::{self, QueryPlan};
use crate::server::connection::Connection;
use crate::store::registry;
use crate::store::table::{QueueOrder, Table};
use crate::types::row::OwnedRow;
use crate::util::logging;

/// Execute a SELECT (pop) query.
/// SELECT in Uniform is destructive — it atomically removes matched rows.
pub fn sql_select(
    query: &str,
    force_limit: i32,
    force_offset: i32,
    _max_scan_time_ms: i32,
    metadata: &OwnedRow,
    _conn: Option<&Connection>,
) -> Result<Vec<OwnedRow>, String> {
    // Parse the SQL using sqlparser
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

    let stmt = &ast[0];

    // Extract table name, WHERE clause, LIMIT, OFFSET, and selected columns
    let (mut table_name, where_clause, mut limit, mut offset, select_columns) =
        extract_select_parts(stmt)?;

    // ========== DEFLECT_READ chain ==========
    let (resolved_name, deflect_patches) =
        insert_pipeline::resolve_deflect_read(&table_name, metadata);
    let original_table_name = table_name.clone();
    if !resolved_name.is_empty() && resolved_name != table_name {
        table_name = resolved_name;
    }

    // Check READ_OFFLINE
    if insert_pipeline::check_read_offline(&table_name, metadata) {
        return Err("Table is in READ_OFFLINE mode.".into());
    }

    // Check READ_ONLY mode (non-destructive select)
    let read_only_mode = insert_pipeline::check_read_only(&table_name, metadata);

    // Apply force limits
    if force_limit >= 0 && (limit < 0 || limit > force_limit as i64) {
        limit = force_limit as i64;
    }
    if force_offset >= 0 && offset < force_offset as i64 {
        offset = force_offset as i64;
    }

    // Default limit to -1 (unlimited) if not specified
    if limit < 0 {
        limit = -1;
    }
    if offset < 0 {
        offset = 0;
    }

    // Get the table
    let table = registry::get_table(&table_name)
        .ok_or_else(|| format!("Table doesn't exist: {}", table_name))?;

    // Apply table force limits
    if table.force_limit >= 0 && (limit < 0 || limit > table.force_limit as i64) {
        limit = table.force_limit as i64;
    }
    if table.force_offset >= 0 && offset < table.force_offset as i64 {
        offset = table.force_offset as i64;
    }

    // Build query plan (index scan vs full scan)
    let plan = planner::plan_query(&where_clause, &table);

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!(
            "SELECT: table={}, plan={:?}, limit={}, offset={}, read_only={}",
            table_name, plan, limit, offset, read_only_mode
        ));
    }

    // Execute the select (pop) with plan
    let actual_limit = if limit < 0 {
        usize::MAX
    } else {
        (limit + offset) as usize
    };

    let order = table.order.clone();
    let result = if read_only_mode {
        // READ_ONLY: peek without claiming/removing rows
        select_peek(&table, &plan, &where_clause, actual_limit, &order)
    } else {
        select_pop(&table, &plan, &where_clause, actual_limit, &order)
    };

    // Apply offset
    let offset = offset as usize;
    let mut rows: Vec<OwnedRow> = if offset > 0 && offset < result.len() {
        result.into_iter().skip(offset).collect()
    } else if offset >= result.len() {
        Vec::new()
    } else {
        result
    };

    // ========== Run post-select proc pipeline ==========
    insert_pipeline::run_select_pipeline(
        &original_table_name,
        &mut rows,
        metadata,
        _conn,
        query,
        &deflect_patches,
    );

    // Apply column projection
    if !select_columns.is_empty() && select_columns[0] != "*" {
        for row in &mut rows {
            row.filter_keys(&select_columns);
        }
    }

    Ok(rows)
}

/// SELECT (peek) — non-destructive read for READ_ONLY mode.
/// Reads rows without claiming or removing them.
fn select_peek(
    table: &Table,
    plan: &QueryPlan,
    where_clause: &Option<String>,
    limit: usize,
    order: &QueueOrder,
) -> Vec<OwnedRow> {
    let candidates: Vec<u64> = match plan {
        QueryPlan::IndexScan {
            index_name,
            value,
            ..
        } => {
            if let Some(idx) = table.indexes.get(index_name) {
                let mut seq_ids = idx.find_eq(value);
                if matches!(order, QueueOrder::Lifo) {
                    seq_ids.reverse();
                }
                seq_ids
            } else {
                table.all_seq_ids_ordered()
            }
        }
        QueryPlan::FullScan => table.all_seq_ids_ordered(),
    };

    let mut result = Vec::new();
    for seq_id in candidates {
        if result.len() >= limit {
            break;
        }

        if let Some(row_ref) = table.rows.get(&seq_id) {
            let row = row_ref.value();
            if row.is_claimed() {
                continue;
            }

            if let Some(where_str) = where_clause {
                if !matches!(plan, QueryPlan::IndexScan { .. }) || plan.has_residual() {
                    if !condition::row_meets_conditions_simple(where_str, &row.columns) {
                        continue;
                    }
                }
            }

            // READ_ONLY: just clone, do NOT claim
            result.push(row.to_owned_row());
        }
    }

    result
}

/// SELECT (pop) — no table-level lock at any point.
fn select_pop(
    table: &Table,
    plan: &QueryPlan,
    where_clause: &Option<String>,
    limit: usize,
    order: &QueueOrder,
) -> Vec<OwnedRow> {
    // Step 1: Get candidate seq_ids — respects FIFO/LIFO queue ordering
    let candidates: Vec<u64> = match plan {
        QueryPlan::IndexScan {
            index_name,
            value,
            ..
        } => {
            if let Some(idx) = table.indexes.get(index_name) {
                let mut seq_ids = idx.find_eq(value);
                if matches!(order, QueueOrder::Lifo) {
                    seq_ids.reverse();
                }
                seq_ids
            } else {
                table.all_seq_ids_ordered()
            }
        }
        QueryPlan::FullScan => table.all_seq_ids_ordered(),
    };

    // Step 2: For each candidate, try to claim via per-row CAS
    let mut result = Vec::new();
    for seq_id in candidates {
        if result.len() >= limit {
            break;
        }

        // SkipMap.get() is lock-free
        if let Some(row_ref) = table.rows.get(&seq_id) {
            let row = row_ref.value();

            // Skip already-claimed rows
            if row.is_claimed() {
                continue;
            }

            // Evaluate WHERE residual (if not covered by index)
            if let Some(where_str) = where_clause {
                if !matches!(plan, QueryPlan::IndexScan { .. })
                    || plan.has_residual()
                {
                    if !condition::row_meets_conditions_simple(where_str, &row.columns) {
                        continue;
                    }
                }
            }

            // PER-ROW CAS: try to claim this specific row
            if row.try_claim() {
                // WON — clone the row data
                result.push(row.to_owned_row());
                table.row_count.fetch_sub(1, Ordering::Relaxed);
            }
            // LOST CAS → another query took it, skip to next
        }
    }

    // Step 3: Cleanup — remove claimed rows from SkipMap + indexes.
    // remove_claimed_rows takes &self (all operations are on lock-free SkipMaps).
    if !result.is_empty() {
        let claimed_ids: Vec<u64> = result.iter().map(|r| r.seq_id).collect();
        table.remove_claimed_rows(&claimed_ids);
    }

    result
}

/// Extract SELECT components from a parsed SQL statement.
fn extract_select_parts(
    stmt: &sqlparser::ast::Statement,
) -> Result<(String, Option<String>, i64, i64, Vec<String>), String> {
    use sqlparser::ast::Statement;

    match stmt {
        Statement::Query(query) => {
            let body = query.body.as_ref();
            match body {
                sqlparser::ast::SetExpr::Select(select) => {
                    // Table name
                    let table_name = if let Some(from) = select.from.first() {
                        extract_table_name(&from.relation)
                    } else {
                        return Err("No FROM clause".into());
                    };

                    // WHERE clause (as string for now — we'll use the original)
                    let where_clause = select
                        .selection
                        .as_ref()
                        .map(|expr| format!("{}", expr));

                    // Selected columns
                    let select_columns: Vec<String> = select
                        .projection
                        .iter()
                        .filter_map(|item| match item {
                            sqlparser::ast::SelectItem::UnnamedExpr(
                                sqlparser::ast::Expr::Identifier(ident),
                            ) => Some(ident.value.clone()),
                            sqlparser::ast::SelectItem::Wildcard(_) => Some("*".to_string()),
                            _ => None,
                        })
                        .collect();

                    // LIMIT and OFFSET
                    let mut limit: i64 = -1;
                    let mut offset: i64 = 0;

                    if let Some(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _))) =
                        &query.limit
                    {
                        limit = n.parse::<i64>().unwrap_or(-1);
                    }

                    if let Some(sqlparser::ast::Offset {
                        value: sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)),
                        ..
                    }) = &query.offset
                    {
                        offset = n.parse::<i64>().unwrap_or(0);
                    }

                    Ok((table_name, where_clause, limit, offset, select_columns))
                }
                _ => Err("Unsupported query type".into()),
            }
        }
        _ => Err("Expected SELECT statement".into()),
    }
}

fn extract_table_name(table_factor: &sqlparser::ast::TableFactor) -> String {
    match table_factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
        }
        _ => String::new(),
    }
}
