use crate::store::proc::ProcType;
use crate::store::registry::ServerProtocol;
use crate::store::worker::WorkerClass;

/// Parsed command — replaces Go's 43+ regex if-else cascade.
/// Uses first-word prefix dispatch for O(1) routing.
#[derive(Debug, Clone)]
pub enum Command {
    // DML
    Select(String),
    InsertInto(String),
    RPush { table: String, data: String },
    RPop { table: String },

    // DDL - Create
    CreateTable { name: String, options: String },
    CreateIndex { name: String, options: String },
    CreateServer { protocol: ServerProtocol, name: String, options: String },
    CreateProc { proc_type: ProcType, name: String, options: String },

    // DDL - Alter
    AlterTable { name: String, options: String },
    AlterServer { name: String, options: String },
    AlterProc { name: String, options: String },

    // DDL - Drop
    DropTable { name: String },
    DropIndex { name: String },
    DropServer { name: String },
    DropProc { name: String },
    DropWorker { name: String },
    DropConnection { addr: String },

    // Workers
    StartWorker { class: WorkerClass, name: String, options: String },

    // Show
    Show { what: String, filter: Option<ShowFilter> },

    // Set
    Set { key: String, value: String },

    // System
    Ping,
    Quit,
    Exit,
    Shutdown,
    Version,
    Sleep { ms: u64 },
    NSleep { ns: u64 },

    // Queue operations
    LLen { table: String },
    LLenM { table: String },
    TableState { table: String },

    // Metadata
    Metadata(String),

    // MySQL compat stubs (no-ops)
    SelectDatabase,
    SelectSchema,
    SelectVersion,
    SelectLastInsertId,
    Use,
    Lock,
    Unlock,
    SetGeneric,

    // Comment or empty
    Empty,
    Comment,

    // Unknown
    Unknown(String),
}

/// SHOW filter — either LIKE or WHERE.
#[derive(Debug, Clone)]
pub enum ShowFilter {
    Like(String),
    Where(String),
}

/// Parse a query string into a Command.
/// Uses first-word prefix dispatch (not sequential regexes).
pub fn parse_command(query: &str) -> Command {
    let trimmed = query.trim();
    if trimmed.is_empty() || trimmed.len() < 2 {
        return Command::Empty;
    }

    // Handle comments
    if trimmed.starts_with('#') || trimmed.starts_with("//") {
        return Command::Comment;
    }

    // Handle SELECT @@ (MySQL compat)
    let upper = trimmed.to_uppercase();

    if upper.starts_with("SELECT @@") {
        return Command::SelectVersion;
    }

    // Get the first word for dispatch
    let first_space = trimmed.find(|c: char| c.is_whitespace()).unwrap_or(trimmed.len());
    let first_word = &upper[..first_space];

    match first_word {
        "SELECT" => parse_select_family(trimmed, &upper),
        "INSERT" => {
            if upper.starts_with("INSERT INTO ") {
                Command::InsertInto(trimmed.to_string())
            } else {
                Command::Unknown(trimmed.to_string())
            }
        }
        "RPUSH" | "LPUSH" => parse_rpush(trimmed),
        "RPOP" | "LPOP" => parse_rpop(trimmed),
        "CREATE" => parse_create_family(trimmed, &upper),
        "ALTER" => parse_alter_family(trimmed, &upper),
        "DROP" => parse_drop_family(trimmed, &upper),
        "START" => parse_start_family(trimmed, &upper),
        "SHOW" => parse_show_family(trimmed, &upper),
        "SET" => parse_set(trimmed, &upper),
        "PING" => Command::Ping,
        "QUIT" => Command::Quit,
        "EXIT" => Command::Exit,
        "SHUTDOWN" => Command::Shutdown,
        "VERSION" => Command::Version,
        "SLEEP" => parse_sleep(trimmed),
        "NSLEEP" => parse_nsleep(trimmed),
        "LLEN" => parse_llen(trimmed),
        "LLENM" => parse_llenm(trimmed),
        "TABLESTATE" => parse_tablestate(trimmed),
        "USE" => Command::Use,
        "LOCK" => Command::Lock,
        "UNLOCK" => Command::Unlock,
        _ => Command::Unknown(trimmed.to_string()),
    }
}

fn parse_select_family(query: &str, upper: &str) -> Command {
    if upper.starts_with("SELECT LAST_INSERT_ID()") {
        Command::SelectLastInsertId
    } else if upper.starts_with("SELECT DATABASE") {
        Command::SelectDatabase
    } else if upper.starts_with("SELECT SCHEMA") {
        Command::SelectSchema
    } else if upper.contains("@@VERSION_COMMENT") {
        Command::SelectVersion
    } else {
        Command::Select(query.to_string())
    }
}

fn parse_rpush(query: &str) -> Command {
    // RPUSH|LPUSH table_name json_data
    let parts: Vec<&str> = query.splitn(3, char::is_whitespace).collect();
    if parts.len() < 3 {
        return Command::Unknown(query.to_string());
    }
    let table = parts[1].trim().trim_matches(|c| c == '\'' || c == '"');
    let data = parts[2].trim();
    Command::RPush {
        table: table.to_string(),
        data: data.to_string(),
    }
}

fn parse_rpop(query: &str) -> Command {
    // RPOP|LPOP table_name
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() < 2 {
        return Command::Unknown(query.to_string());
    }
    let table = parts[1].trim().trim_matches(|c| c == '\'' || c == '"');
    Command::RPop {
        table: table.to_string(),
    }
}

fn parse_create_family(query: &str, upper: &str) -> Command {
    // Second word: TABLE, SERVER, PROC, INDEX
    let words: Vec<&str> = upper.split_whitespace().collect();
    if words.len() < 3 {
        return Command::Unknown(query.to_string());
    }

    match words[1] {
        "TABLE" => {
            if let Some((name, options)) = extract_name_and_options(query, 2) {
                Command::CreateTable { name, options }
            } else {
                Command::Unknown(query.to_string())
            }
        }
        "INDEX" => {
            if let Some((name, options)) = extract_name_and_options(query, 2) {
                Command::CreateIndex { name, options }
            } else {
                Command::Unknown(query.to_string())
            }
        }
        "SERVER" => {
            // CREATE SERVER REDIS|MYSQL|HTTP|FASTCGI name (options)
            if words.len() < 4 {
                return Command::Unknown(query.to_string());
            }
            let protocol = match ServerProtocol::from_str(words[2]) {
                Some(p) => p,
                None => return Command::Unknown(query.to_string()),
            };
            if let Some((name, options)) = extract_name_and_options(query, 3) {
                Command::CreateServer {
                    protocol,
                    name,
                    options,
                }
            } else {
                Command::Unknown(query.to_string())
            }
        }
        "PROC" => {
            // CREATE PROC <PROC_TYPE> name (options)
            if words.len() < 4 {
                return Command::Unknown(query.to_string());
            }
            let proc_type = match ProcType::from_str(words[2]) {
                Some(pt) => pt,
                None => return Command::Unknown(query.to_string()),
            };
            if let Some((name, options)) = extract_name_and_options(query, 3) {
                Command::CreateProc {
                    proc_type,
                    name,
                    options,
                }
            } else {
                Command::Unknown(query.to_string())
            }
        }
        _ => Command::Unknown(query.to_string()),
    }
}

fn parse_alter_family(query: &str, upper: &str) -> Command {
    let words: Vec<&str> = upper.split_whitespace().collect();
    if words.len() < 3 {
        return Command::Unknown(query.to_string());
    }

    match words[1] {
        "TABLE" => {
            if let Some((name, options)) = extract_name_and_options(query, 2) {
                Command::AlterTable { name, options }
            } else {
                Command::Unknown(query.to_string())
            }
        }
        "SERVER" => {
            if let Some((name, options)) = extract_name_and_options(query, 2) {
                Command::AlterServer { name, options }
            } else {
                Command::Unknown(query.to_string())
            }
        }
        "PROC" => {
            if let Some((name, options)) = extract_name_and_options(query, 2) {
                Command::AlterProc { name, options }
            } else {
                Command::Unknown(query.to_string())
            }
        }
        _ => Command::Unknown(query.to_string()),
    }
}

fn parse_drop_family(query: &str, upper: &str) -> Command {
    let words: Vec<&str> = upper.split_whitespace().collect();
    if words.len() < 3 {
        return Command::Unknown(query.to_string());
    }

    let name = extract_name_from_words(query, 2);

    match words[1] {
        "TABLE" => Command::DropTable { name },
        "INDEX" => Command::DropIndex { name },
        "SERVER" => Command::DropServer { name },
        "PROC" => Command::DropProc { name },
        "WORKER" => Command::DropWorker { name },
        "CONNECTION" => Command::DropConnection { addr: name },
        _ => Command::Unknown(query.to_string()),
    }
}

fn parse_start_family(query: &str, upper: &str) -> Command {
    // START WORKER <CLASS> name (options)
    let words: Vec<&str> = upper.split_whitespace().collect();
    if words.len() < 4 || words[1] != "WORKER" {
        return Command::Unknown(query.to_string());
    }

    let class = match WorkerClass::from_str(words[2]) {
        Some(c) => c,
        None => return Command::Unknown(query.to_string()),
    };

    if let Some((name, options)) = extract_name_and_options(query, 3) {
        Command::StartWorker {
            class,
            name,
            options,
        }
    } else {
        Command::Unknown(query.to_string())
    }
}

fn parse_show_family(query: &str, upper: &str) -> Command {
    // SHOW <what> [LIKE pattern | WHERE condition]
    let words: Vec<&str> = upper.split_whitespace().collect();
    if words.len() < 2 {
        return Command::Unknown(query.to_string());
    }

    let what = words[1].to_lowercase();

    // Check for LIKE or WHERE
    if words.len() >= 4 {
        let rest_upper: String = words[2..].join(" ");
        if rest_upper.starts_with("LIKE ") {
            let like_val = query
                .split_whitespace()
                .skip(3)
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            return Command::Show {
                what,
                filter: Some(ShowFilter::Like(like_val)),
            };
        }
        if rest_upper.starts_with("WHERE ") {
            let where_clause = query
                .split_whitespace()
                .skip(3)
                .collect::<Vec<_>>()
                .join(" ");
            return Command::Show {
                what,
                filter: Some(ShowFilter::Where(where_clause)),
            };
        }
    }

    Command::Show { what, filter: None }
}

fn parse_set(query: &str, upper: &str) -> Command {
    let words: Vec<&str> = query.split_whitespace().collect();
    if words.len() < 3 {
        // Generic SET (MySQL compat)
        if upper.starts_with("SET ") {
            return Command::SetGeneric;
        }
        return Command::Unknown(query.to_string());
    }

    let key = words[1].to_string();
    let value = words[2]
        .trim_matches(|c| c == '\'' || c == '"')
        .to_string();
    Command::Set { key, value }
}

fn parse_sleep(query: &str) -> Command {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() >= 2 {
        if let Ok(ms) = parts[1].parse::<u64>() {
            return Command::Sleep { ms };
        }
    }
    Command::Unknown(query.to_string())
}

fn parse_nsleep(query: &str) -> Command {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() >= 2 {
        if let Ok(ns) = parts[1].parse::<u64>() {
            return Command::NSleep { ns };
        }
    }
    Command::Unknown(query.to_string())
}

fn parse_llen(query: &str) -> Command {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() >= 2 {
        let table = parts[1].trim_matches(|c| c == '\'' || c == '"');
        Command::LLen {
            table: table.to_string(),
        }
    } else {
        Command::Unknown(query.to_string())
    }
}

fn parse_llenm(query: &str) -> Command {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() >= 2 {
        let table = parts[1].trim_matches(|c| c == '\'' || c == '"');
        Command::LLenM {
            table: table.to_string(),
        }
    } else {
        Command::Unknown(query.to_string())
    }
}

fn parse_tablestate(query: &str) -> Command {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() >= 2 {
        let table = parts[1].trim_matches(|c| c == '\'' || c == '"');
        Command::TableState {
            table: table.to_string(),
        }
    } else {
        Command::Unknown(query.to_string())
    }
}

/// Extract name and parenthesized options from a query.
/// `skip` is how many space-separated words to skip before the name.
/// E.g., "CREATE TABLE myname (options)" with skip=2 → ("myname", "options")
fn extract_name_and_options(query: &str, skip: usize) -> Option<(String, String)> {
    let words: Vec<&str> = query.split_whitespace().collect();
    if words.len() <= skip {
        return None;
    }

    let name = words[skip]
        .trim_matches(|c| c == '\'' || c == '"')
        .to_string();

    // Find the parenthesized options
    let _remaining = query
        .splitn(skip + 2, char::is_whitespace)
        .last()
        .unwrap_or("");

    // Find everything after the name — look for first '(' and last ')'
    if let Some(paren_start) = query.find('(') {
        if let Some(paren_end) = query.rfind(')') {
            if paren_start < paren_end {
                let options = query[paren_start + 1..paren_end].trim().to_string();
                return Some((name, options));
            }
        }
    }

    // No options — that's valid for some commands
    Some((name, String::new()))
}

/// Extract a name from word position in the query string.
fn extract_name_from_words(query: &str, position: usize) -> String {
    let words: Vec<&str> = query.split_whitespace().collect();
    if words.len() > position {
        words[position]
            .trim_matches(|c: char| c == '\'' || c == '"')
            .to_string()
    } else {
        String::new()
    }
}

/// Strip metadata prefix `/* METADATA ({...}) */` from a query.
/// Returns the metadata JSON (if any) and the cleaned query.
pub fn strip_metadata(query: &str) -> (Option<String>, String) {
    let trimmed = query.trim();
    if !trimmed.starts_with("/*") {
        return (None, trimmed.to_string());
    }

    // Find the METADATA pattern
    if let Some(meta_start) = trimmed.find("METADATA") {
        if let Some(paren_start) = trimmed[meta_start..].find('(') {
            let abs_paren_start = meta_start + paren_start + 1;
            // Find matching closing paren
            if let Some(paren_end) = trimmed[abs_paren_start..].find(')') {
                let abs_paren_end = abs_paren_start + paren_end;
                let metadata_json = trimmed[abs_paren_start..abs_paren_end].trim().to_string();

                // Find the end of the comment
                if let Some(comment_end) = trimmed.find("*/") {
                    let rest = trimmed[comment_end + 2..].trim();
                    return (Some(metadata_json), rest.to_string());
                }
            }
        }
    }

    (None, trimmed.to_string())
}

/// Strip all comments `/* ... */` from a query.
pub fn strip_comments(query: &str) -> String {
    let mut result = query.to_string();
    while let Some(start) = result.find("/*") {
        if let Some(end) = result[start..].find("*/") {
            let before = &result[..start];
            let after = &result[start + end + 2..];
            result = format!("{}{}", before.trim_end(), after.trim_start());
        } else {
            break;
        }
    }
    result.trim().to_string()
}
