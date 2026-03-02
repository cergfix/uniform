// MySQL wire protocol constants

pub const COM_QUIT: u8 = 0x01;
pub const COM_INIT_DB: u8 = 0x02;
pub const COM_QUERY: u8 = 0x03;
pub const COM_PING: u8 = 0x0e;

pub const OK_HEADER: u8 = 0x00;
pub const ERR_HEADER: u8 = 0xff;
pub const EOF_HEADER: u8 = 0xfe;

pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;

pub const CLIENT_PROTOCOL_41: u32 = 0x00000200;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x00008000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x00080000;

pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";

pub const SERVER_VERSION: &str = "5.7.0-Uniform";
