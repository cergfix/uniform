use std::collections::HashMap;

use crate::types::value::Value;

/// Proc type — 24 variants matching Go's PROC_TYPE_* constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcType {
    MirrorWrite = 0,
    MirrorOnRead = 1,
    DeflectWrite = 2,
    DeflectRead = 3,
    DeflectWriteOnFullTable = 4,
    DeflectReadOnEmptyResponse = 5,
    FrontendIoMix = 6,
    LongPoll = 7,
    ReadOnly = 8,
    ReadReduce = 9,
    BufferCatch = 10,
    EncryptWrite = 11,
    EncryptOnRead = 12,
    AutoReply = 13,
    GzipWrite = 14,
    GzipOnRead = 15,
    DecryptWrite = 16,
    DecryptOnRead = 17,
    ReadOffline = 18,
    WriteOffline = 19,
    PingOffline = 20,
    GunzipWrite = 21,
    GunzipOnRead = 22,
}

impl ProcType {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "MIRROR_WRITE" => Some(ProcType::MirrorWrite),
            "MIRROR_ON_READ" => Some(ProcType::MirrorOnRead),
            "DEFLECT_WRITE" => Some(ProcType::DeflectWrite),
            "DEFLECT_READ" => Some(ProcType::DeflectRead),
            "DEFLECT_WRITE_ON_FULL_TABLE" => Some(ProcType::DeflectWriteOnFullTable),
            "DEFLECT_READ_ON_EMPTY_RESPONSE" => Some(ProcType::DeflectReadOnEmptyResponse),
            "FRONTEND_IO_MIX" => Some(ProcType::FrontendIoMix),
            "LONG_POLL" => Some(ProcType::LongPoll),
            "READ_ONLY" => Some(ProcType::ReadOnly),
            "READ_REDUCE" => Some(ProcType::ReadReduce),
            "BUFFER_CATCH" => Some(ProcType::BufferCatch),
            "ENCRYPT_WRITE" => Some(ProcType::EncryptWrite),
            "ENCRYPT_ON_READ" => Some(ProcType::EncryptOnRead),
            "AUTO_REPLY" => Some(ProcType::AutoReply),
            "GZIP_WRITE" => Some(ProcType::GzipWrite),
            "GZIP_ON_READ" => Some(ProcType::GzipOnRead),
            "DECRYPT_WRITE" => Some(ProcType::DecryptWrite),
            "DECRYPT_ON_READ" => Some(ProcType::DecryptOnRead),
            "READ_OFFLINE" => Some(ProcType::ReadOffline),
            "WRITE_OFFLINE" => Some(ProcType::WriteOffline),
            "PING_OFFLINE" => Some(ProcType::PingOffline),
            "GUNZIP_WRITE" => Some(ProcType::GunzipWrite),
            "GUNZIP_ON_READ" => Some(ProcType::GunzipOnRead),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ProcType::MirrorWrite => "MIRROR_WRITE",
            ProcType::MirrorOnRead => "MIRROR_ON_READ",
            ProcType::DeflectWrite => "DEFLECT_WRITE",
            ProcType::DeflectRead => "DEFLECT_READ",
            ProcType::DeflectWriteOnFullTable => "DEFLECT_WRITE_ON_FULL_TABLE",
            ProcType::DeflectReadOnEmptyResponse => "DEFLECT_READ_ON_EMPTY_RESPONSE",
            ProcType::FrontendIoMix => "FRONTEND_IO_MIX",
            ProcType::LongPoll => "LONG_POLL",
            ProcType::ReadOnly => "READ_ONLY",
            ProcType::ReadReduce => "READ_REDUCE",
            ProcType::BufferCatch => "BUFFER_CATCH",
            ProcType::EncryptWrite => "ENCRYPT_WRITE",
            ProcType::EncryptOnRead => "ENCRYPT_ON_READ",
            ProcType::AutoReply => "AUTO_REPLY",
            ProcType::GzipWrite => "GZIP_WRITE",
            ProcType::GzipOnRead => "GZIP_ON_READ",
            ProcType::DecryptWrite => "DECRYPT_WRITE",
            ProcType::DecryptOnRead => "DECRYPT_ON_READ",
            ProcType::ReadOffline => "READ_OFFLINE",
            ProcType::WriteOffline => "WRITE_OFFLINE",
            ProcType::PingOffline => "PING_OFFLINE",
            ProcType::GunzipWrite => "GUNZIP_WRITE",
            ProcType::GunzipOnRead => "GUNZIP_ON_READ",
        }
    }
}

/// Extended case query method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtCaseQueryMethod {
    Count = 0,
    Success = 1,
}

/// Proc — a trigger/pipeline step attached to a table.
#[derive(Debug, Clone)]
pub struct Proc {
    pub name: String,
    pub src: String,
    pub dest: String,
    pub table: String,
    pub proc_type: ProcType,

    pub case_query_string: String,
    pub case_query_parsed: bool,

    pub patch_string: String,
    pub patch_data: Option<HashMap<String, Value>>,

    pub post_wait_ms: i32,
    pub enabled: bool,

    pub ext_case_query_method: ExtCaseQueryMethod,

    pub start_time_string: String,
    pub end_time_string: String,

    pub reduce_key: String,
    pub reduce_to_latest: bool,

    pub encrypt_key: String,
    pub encrypt_fields: String,

    pub decrypt_key: String,
    pub decrypt_fields: String,

    pub reply_status: String,
    pub reply_body: String,

    pub wait_ms: i32,

    pub gzip_fields: String,
    pub gunzip_fields: String,
}

impl Proc {
    pub fn new(name: String, proc_type: ProcType) -> Self {
        Proc {
            name,
            src: String::new(),
            dest: String::new(),
            table: String::new(),
            proc_type,
            case_query_string: String::new(),
            case_query_parsed: false,
            patch_string: String::new(),
            patch_data: None,
            post_wait_ms: 0,
            enabled: true,
            ext_case_query_method: ExtCaseQueryMethod::Count,
            start_time_string: String::new(),
            end_time_string: String::new(),
            reduce_key: String::new(),
            reduce_to_latest: false,
            encrypt_key: String::new(),
            encrypt_fields: String::new(),
            decrypt_key: String::new(),
            decrypt_fields: String::new(),
            reply_status: String::new(),
            reply_body: String::new(),
            wait_ms: 0,
            gzip_fields: String::new(),
            gunzip_fields: String::new(),
        }
    }
}
