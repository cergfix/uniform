use crate::store::table::Table;
use crate::types::value::Value;

/// Query plan — determines how to find rows for a SELECT.
#[derive(Debug)]
pub enum QueryPlan {
    /// Use a secondary index for lookup.
    IndexScan {
        index_name: String,
        value: Value,
        residual: bool, // true if WHERE has conditions beyond the indexed column
    },
    /// Full table scan (iterate SkipMap in seq_id order).
    FullScan,
}

impl QueryPlan {
    pub fn has_residual(&self) -> bool {
        match self {
            QueryPlan::IndexScan { residual, .. } => *residual,
            QueryPlan::FullScan => true,
        }
    }
}

/// Plan a query by inspecting WHERE conditions and available indexes.
/// Returns the best plan.
pub fn plan_query(where_clause: &Option<String>, table: &Table) -> QueryPlan {
    // If no WHERE clause, full scan
    let where_str = match where_clause {
        Some(s) if !s.is_empty() => s,
        _ => return QueryPlan::FullScan,
    };

    // If no indexes, full scan
    if table.indexes.is_empty() {
        return QueryPlan::FullScan;
    }

    // Parse the WHERE clause to extract equality predicates
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let sql = format!("SELECT * FROM t WHERE {}", where_str);
    let dialect = GenericDialect {};
    let ast = match Parser::parse_sql(&dialect, &sql) {
        Ok(stmts) => stmts,
        Err(_) => return QueryPlan::FullScan,
    };

    let expr = match ast.first() {
        Some(sqlparser::ast::Statement::Query(q)) => {
            match q.body.as_ref() {
                sqlparser::ast::SetExpr::Select(s) => s.selection.clone(),
                _ => None,
            }
        }
        _ => None,
    };

    let expr = match expr {
        Some(e) => e,
        None => return QueryPlan::FullScan,
    };

    // Extract top-level equality predicates (column = value)
    let predicates = extract_equality_predicates(&expr);

    // Find the best matching index
    // Priority: unique > non-unique
    let mut best_plan: Option<QueryPlan> = None;

    for (col, val) in &predicates {
        for (idx_name, idx) in &table.indexes {
            if idx.column == *col {
                let is_better = match &best_plan {
                    None => true,
                    Some(QueryPlan::IndexScan { index_name, .. }) => {
                        // Prefer unique indexes
                        let current_idx = table.indexes.get(index_name);
                        let current_unique =
                            current_idx.map(|i| i.unique).unwrap_or(false);
                        idx.unique && !current_unique
                    }
                    _ => true,
                };

                if is_better {
                    let has_residual = predicates.len() > 1
                        || has_non_equality_conditions(&expr);
                    best_plan = Some(QueryPlan::IndexScan {
                        index_name: idx_name.clone(),
                        value: val.clone(),
                        residual: has_residual,
                    });
                }
            }
        }
    }

    best_plan.unwrap_or(QueryPlan::FullScan)
}

/// Extract top-level equality predicates (column = literal) from an expression.
fn extract_equality_predicates(expr: &sqlparser::ast::Expr) -> Vec<(String, Value)> {
    use sqlparser::ast::{BinaryOperator, Expr};

    let mut result = Vec::new();

    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            if let Some((col, val)) = extract_column_value_pair(left, right) {
                result.push((col, val));
            }
        }
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            result.extend(extract_equality_predicates(left));
            result.extend(extract_equality_predicates(right));
        }
        Expr::Nested(inner) => {
            result.extend(extract_equality_predicates(inner));
        }
        _ => {}
    }

    result
}

/// Try to extract a (column_name, value) pair from left = right.
fn extract_column_value_pair(
    left: &sqlparser::ast::Expr,
    right: &sqlparser::ast::Expr,
) -> Option<(String, Value)> {
    use sqlparser::ast::Expr;

    // column = value
    if let Expr::Identifier(ident) = left {
        if let Some(val) = expr_to_value(right) {
            return Some((ident.value.clone(), val));
        }
    }
    // value = column
    if let Expr::Identifier(ident) = right {
        if let Some(val) = expr_to_value(left) {
            return Some((ident.value.clone(), val));
        }
    }

    None
}

fn expr_to_value(expr: &sqlparser::ast::Expr) -> Option<Value> {
    match expr {
        sqlparser::ast::Expr::Value(v) => match v {
            sqlparser::ast::Value::Number(n, _) => {
                n.parse::<f64>().ok().map(Value::Number)
            }
            sqlparser::ast::Value::SingleQuotedString(s) => {
                Some(Value::String(s.clone()))
            }
            sqlparser::ast::Value::DoubleQuotedString(s) => {
                Some(Value::String(s.clone()))
            }
            sqlparser::ast::Value::Boolean(b) => Some(Value::Bool(*b)),
            sqlparser::ast::Value::Null => Some(Value::Null),
            _ => None,
        },
        _ => None,
    }
}

/// Check if an expression has non-equality conditions (>, <, LIKE, etc.)
fn has_non_equality_conditions(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::{BinaryOperator, Expr};

    match expr {
        Expr::BinaryOp { op, left, right, .. } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                has_non_equality_conditions(left) || has_non_equality_conditions(right)
            }
            BinaryOperator::Eq => false,
            _ => true,
        },
        Expr::Like { .. } | Expr::InList { .. } => true,
        Expr::Nested(inner) => has_non_equality_conditions(inner),
        _ => false,
    }
}
