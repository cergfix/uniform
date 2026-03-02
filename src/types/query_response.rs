use crate::types::row::OwnedRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    String = 0,
    Number = 1,
    Bool = 2,
    Result = 3,
}

#[derive(Debug, Clone)]
pub struct QueryResponse {
    pub result: Vec<OwnedRow>,
    pub value_str: String,
    pub value_int: i32,
    pub affected_rows: i32,
    pub query_type: QueryType,
    pub err: String,
    pub success: bool,
}

impl QueryResponse {
    pub fn new() -> Self {
        QueryResponse {
            result: Vec::new(),
            value_str: String::new(),
            value_int: 0,
            affected_rows: 0,
            query_type: QueryType::Bool,
            err: String::new(),
            success: false,
        }
    }

    pub fn ok_bool() -> Self {
        QueryResponse {
            success: true,
            query_type: QueryType::Bool,
            ..Self::new()
        }
    }

    pub fn ok_string(value: String) -> Self {
        QueryResponse {
            success: true,
            query_type: QueryType::String,
            value_str: value,
            ..Self::new()
        }
    }

    pub fn ok_number(value: i32) -> Self {
        QueryResponse {
            success: true,
            query_type: QueryType::Number,
            value_int: value,
            ..Self::new()
        }
    }

    pub fn ok_result(rows: Vec<OwnedRow>) -> Self {
        QueryResponse {
            success: true,
            query_type: QueryType::Result,
            result: rows,
            ..Self::new()
        }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        QueryResponse {
            success: false,
            err: msg.into(),
            ..Self::new()
        }
    }

    pub fn err_typed(query_type: QueryType, msg: impl Into<String>) -> Self {
        QueryResponse {
            success: false,
            query_type,
            err: msg.into(),
            ..Self::new()
        }
    }
}

impl Default for QueryResponse {
    fn default() -> Self {
        Self::new()
    }
}
