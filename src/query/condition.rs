use std::collections::HashMap;

use crate::types::value::Value;

/// Simplified condition evaluator — checks if a row matches a WHERE clause string.
/// This is a basic implementation for Phase 1; full expression tree evaluation comes in Phase 2.
///
/// Supports:
/// - column = 'value'
/// - column != 'value'
/// - column > value
/// - column < value
/// - column >= value
/// - column <= value
/// - column LIKE 'pattern'
/// - AND / OR combinations
pub fn row_meets_conditions_simple(where_str: &str, columns: &HashMap<String, Value>) -> bool {
    // Try to parse with sqlparser for proper expression evaluation
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let sql = format!("SELECT * FROM t WHERE {}", where_str);
    let dialect = GenericDialect {};
    if let Ok(ast) = Parser::parse_sql(&dialect, &sql) {
        if let Some(sqlparser::ast::Statement::Query(query)) = ast.first() {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                if let Some(expr) = &select.selection {
                    return evaluate_expr(expr, columns);
                }
            }
        }
    }

    // If parsing fails, default to true (pass through)
    true
}

/// Recursively evaluate a SQL expression against row columns.
fn evaluate_expr(expr: &sqlparser::ast::Expr, columns: &HashMap<String, Value>) -> bool {
    use sqlparser::ast::{BinaryOperator, Expr};

    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => evaluate_expr(left, columns) && evaluate_expr(right, columns),
            BinaryOperator::Or => evaluate_expr(left, columns) || evaluate_expr(right, columns),
            BinaryOperator::Eq => {
                let left_val = resolve_expr_value(left, columns);
                let right_val = resolve_expr_value(right, columns);
                values_equal(&left_val, &right_val)
            }
            BinaryOperator::NotEq => {
                let left_val = resolve_expr_value(left, columns);
                let right_val = resolve_expr_value(right, columns);
                !values_equal(&left_val, &right_val)
            }
            BinaryOperator::Lt => {
                let left_val = resolve_expr_value(left, columns);
                let right_val = resolve_expr_value(right, columns);
                values_lt(&left_val, &right_val)
            }
            BinaryOperator::LtEq => {
                let left_val = resolve_expr_value(left, columns);
                let right_val = resolve_expr_value(right, columns);
                values_equal(&left_val, &right_val) || values_lt(&left_val, &right_val)
            }
            BinaryOperator::Gt => {
                let left_val = resolve_expr_value(left, columns);
                let right_val = resolve_expr_value(right, columns);
                values_lt(&right_val, &left_val)
            }
            BinaryOperator::GtEq => {
                let left_val = resolve_expr_value(left, columns);
                let right_val = resolve_expr_value(right, columns);
                values_equal(&left_val, &right_val) || values_lt(&right_val, &left_val)
            }
            _ => true, // Unsupported operator — pass through
        },
        Expr::Like {
            expr: left,
            pattern,
            negated,
            ..
        } => {
            let left_val = resolve_expr_value(left, columns);
            let pattern_val = resolve_expr_value(pattern, columns);
            let matches = like_match(&left_val.to_string_repr(), &pattern_val.to_string_repr());
            if *negated {
                !matches
            } else {
                matches
            }
        }
        Expr::IsNull(inner) => {
            let val = resolve_expr_value(inner, columns);
            val.is_null()
        }
        Expr::IsNotNull(inner) => {
            let val = resolve_expr_value(inner, columns);
            !val.is_null()
        }
        Expr::Nested(inner) => evaluate_expr(inner, columns),
        Expr::InList {
            expr: left,
            list,
            negated,
        } => {
            let left_val = resolve_expr_value(left, columns);
            let found = list.iter().any(|item| {
                let item_val = resolve_expr_value(item, columns);
                values_equal(&left_val, &item_val)
            });
            if *negated {
                !found
            } else {
                found
            }
        }
        _ => true, // Unsupported expression — pass through
    }
}

/// Resolve a SQL expression to a Value (column reference or literal).
fn resolve_expr_value(expr: &sqlparser::ast::Expr, columns: &HashMap<String, Value>) -> Value {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Identifier(ident) => columns.get(&ident.value).cloned().unwrap_or(Value::Null),
        Expr::Value(v) => match v {
            sqlparser::ast::Value::Number(n, _) => {
                n.parse::<f64>().map(Value::Number).unwrap_or(Value::Null)
            }
            sqlparser::ast::Value::SingleQuotedString(s) => Value::String(s.clone()),
            sqlparser::ast::Value::DoubleQuotedString(s) => Value::String(s.clone()),
            sqlparser::ast::Value::Boolean(b) => Value::Bool(*b),
            sqlparser::ast::Value::Null => Value::Null,
            other => Value::String(format!("{}", other)),
        },
        Expr::CompoundIdentifier(parts) => {
            let col_name = parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            columns.get(&col_name).cloned().unwrap_or(Value::Null)
        }
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr: inner,
        } => {
            if let Expr::Value(sqlparser::ast::Value::Number(n, _)) = inner.as_ref() {
                n.parse::<f64>()
                    .map(|f| Value::Number(-f))
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Number(a), Value::Number(b)) => (a - b).abs() < f64::EPSILON,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Null, Value::Null) => true,
        // Cross-type: try numeric comparison
        (Value::String(s), Value::Number(n)) | (Value::Number(n), Value::String(s)) => s
            .parse::<f64>()
            .map(|f| (f - n).abs() < f64::EPSILON)
            .unwrap_or(false),
        _ => false,
    }
}

fn values_lt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => a < b,
        (Value::String(a), Value::String(b)) => a < b,
        (Value::String(s), Value::Number(n)) => s.parse::<f64>().map(|f| f < *n).unwrap_or(false),
        (Value::Number(n), Value::String(s)) => s.parse::<f64>().map(|f| *n < f).unwrap_or(false),
        _ => false,
    }
}

/// SQL LIKE pattern matching (% = any chars, _ = single char).
fn like_match(value: &str, pattern: &str) -> bool {
    let re_pattern = pattern.replace('%', ".*").replace('_', ".");
    regex::Regex::new(&format!("^{}$", re_pattern))
        .map(|re| re.is_match(value))
        .unwrap_or(false)
}
