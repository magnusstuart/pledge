use std::fmt::{Display, Formatter};

use super::{reader::ByteReaderErrorKind, types::ColumnMetadata};

use super::reader::{ByteReader, ByteReaderError};
use bytes::{BufMut, BytesMut};
use sqlx::{Row, postgres::PgRow};

use super::SQLCommand;

/*
 * Docs to look at for this
 * https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY
 * https://www.postgresql.org/docs/current/protocol-message-formats.html
 */

pub(super) trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub(super) trait Decode {
    type Output;
    fn decode(&self) -> Result<Self::Output, ByteReaderError>;
}

pub(super) struct AuthenticationOk;
impl Encode for AuthenticationOk {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = BytesMut::with_capacity(9);
        bytes.put_u8(b'R');
        bytes.put_i32(8);
        bytes.put_i32(0);
        bytes.to_vec()
    }
}

pub(super) struct ReadyForQuery {
    pub(super) status: u8,
}
impl Encode for ReadyForQuery {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = BytesMut::with_capacity(6);
        bytes.put_u8(b'Z');
        bytes.put_i32(5);
        bytes.put_u8(self.status);
        bytes.to_vec()
    }
}

pub(super) struct RowDescription<'a> {
    pub(super) columns: &'a [ColumnMetadata],
}
impl<'a> Encode for RowDescription<'a> {
    fn encode(&self) -> Vec<u8> {
        let mut payload = BytesMut::new();
        for column in self.columns {
            payload.put(column.name.as_bytes()); // Name
            payload.put_u8(0); // null terminator
            payload.put_i32(column.table_oid); // table OID
            payload.put_i16(column.attribute_number); // attribute number
            payload.put_i32(column.type_oid); // type OID
            payload.put_i16(column.type_len); // data type size
            payload.put_i32(-1); //type modifier, -1 is a temporary workaround
            payload.put_i16(0); // format code, 0 = text, 1 = binary
        }
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'T');
        bytes.put_i32((payload.len() + 6) as i32); // Length of message
        bytes.put_i16(self.columns.len() as i16); // Number of columns
        bytes.put(payload);
        bytes.to_vec()
    }
}

pub(super) struct DataRow<'a> {
    pub(super) row: &'a PgRow,
}
impl<'a> Encode for DataRow<'a> {
    fn encode(&self) -> Vec<u8> {
        let mut payload = BytesMut::new();
        for index in 0..(self.row.columns().len()) {
            let row_value = match self.row.try_get_raw(index) {
                Ok(pg_value_ref) => match pg_value_ref.as_bytes() {
                    Ok(value) => Some(value),
                    Err(_) => None,
                },
                Err(_) => None,
            };
            match row_value {
                Some(row_value) => {
                    println!("row value: {:?}", row_value);
                    payload.put_i32(row_value.len() as i32);
                    payload.put(row_value);
                }
                None => payload.put_i32(-1),
            }
        }

        let mut bytes = BytesMut::new();
        bytes.put_u8(b'D');
        bytes.put_i32((payload.len() + 6) as i32); // Length of message
        bytes.put_i16(self.row.len() as i16);
        bytes.put(payload);
        bytes.to_vec()
    }
}

pub(super) struct CommandComplete<'a> {
    pub(super) rows: u16,
    pub(super) command_tag: &'a SQLCommand,
}
impl<'a> Encode for CommandComplete<'a> {
    fn encode(&self) -> Vec<u8> {
        let mut payload = BytesMut::new();
        let command_tag = self.create_command_tag();
        payload.put(command_tag.as_bytes());
        payload.put_u8(0); // null terminator

        let mut bytes = BytesMut::new();
        bytes.put_u8(b'C');
        bytes.put_i32((payload.len() + 4) as i32); // Length of message
        bytes.put(payload);
        bytes.to_vec()
    }
}

impl<'a> CommandComplete<'a> {
    fn create_command_tag(&self) -> String {
        match self.command_tag {
            SQLCommand::Insert => format!("INSERT 0 {}", self.rows),
            SQLCommand::Delete => format!("DELETE {}", self.rows),
            SQLCommand::Update => format!("UPDATE {}", self.rows),
            SQLCommand::Merge => format!("MERGE {}", self.rows),
            SQLCommand::Select => format!("SELECT {}", self.rows),
            SQLCommand::CreateTableAs => format!("SELECT {}", self.rows),
            SQLCommand::Move => format!("MOVE {}", self.rows),
            SQLCommand::Fetch => format!("FETCH {}", self.rows),
            SQLCommand::Copy => format!("COPY {}", self.rows),
        }
    }
}

pub(super) enum ErrorResponseSeverity {
    Error,
    Fatal,
    Panic,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl Display for ErrorResponseSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorResponseSeverity::Error => write!(f, "ERROR"),
            ErrorResponseSeverity::Fatal => write!(f, "FATAL"),
            ErrorResponseSeverity::Panic => write!(f, "PANIC"),
            ErrorResponseSeverity::Warning => write!(f, "WARNING"),
            ErrorResponseSeverity::Notice => write!(f, "NOTICE"),
            ErrorResponseSeverity::Debug => write!(f, "DEBUG"),
            ErrorResponseSeverity::Info => write!(f, "INFO"),
            ErrorResponseSeverity::Log => write!(f, "LOG"),
        }
    }
}

pub(super) struct ErrorResponse {
    pub(super) severity: ErrorResponseSeverity,
    pub(super) error_message: String,
    pub(super) sql_state_code: String,
}

impl Encode for ErrorResponse {
    fn encode(&self) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_u8(b'S');
        payload.put(self.severity.to_string().as_bytes());
        payload.put_u8(0);
        payload.put_u8(b'V');
        payload.put(self.severity.to_string().as_bytes());
        payload.put_u8(0);
        payload.put_u8(b'C');
        payload.put(self.sql_state_code.as_bytes());
        payload.put_u8(0);
        payload.put_u8(b'M');
        payload.put(self.error_message.as_bytes());
        payload.put_u8(0);
        payload.put_u8(0); // Signals no more fields

        let mut bytes = BytesMut::new();
        bytes.put_u8(b'E');
        bytes.put_i32((payload.len() + 4) as i32); // Length of message
        bytes.put(payload);
        bytes.to_vec()
    }
}

pub(super) struct ParseComplete;
impl Encode for ParseComplete {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'1');
        bytes.put_i32(4);
        bytes.to_vec()
    }
}

pub(super) struct BindComplete;
impl Encode for BindComplete {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'2');
        bytes.put_i32(4);
        bytes.to_vec()
    }
}

// Client
#[derive(Clone)]
pub(super) enum ClientMessageContent {
    ParseComplete {
        start: usize,
        end: usize,
    },
    BindComplete {
        start: usize,
        end: usize,
    },
    QueryMessage {
        data: QueryMessageContent,
        start: usize,
        end: usize,
    },
    ParseMessage {
        data: ParseMessageContent,
        start: usize,
        end: usize,
    },
    BindMessage {
        data: BindMessageContent,
        start: usize,
        end: usize,
    },
    DescribeMessage {
        data: DescribeMessageContent,
        start: usize,
        end: usize,
    },
    ExecuteMessage {
        data: ExecuteMessageContent,
        start: usize,
        end: usize,
    },
    SyncMessage {
        start: usize,
        end: usize,
    },
    CloseMessage,
    TerminateMessage,
    UnknownMessage,
}

pub(super) enum DBMessageContent {
    ParseComplete {
        start: usize,
        end: usize,
    },
    BindComplete {
        start: usize,
        end: usize,
    },
    ParameterDescription {
        data: Vec<u8>,
        start: usize,
        end: usize,
    },
    RowDescription {
        data: Vec<u8>,
        start: usize,
        end: usize,
    },
    DataRow {
        data: Vec<u8>,
        start: usize,
        end: usize,
    },
    CommandComplete {
        start: usize,
        end: usize,
    },
    ReadyForQuery {
        data: Vec<u8>,
        start: usize,
        end: usize,
    },
    AuthenticationOk {
        start: usize,
        end: usize,
    },
    UnknownMessage,
}

#[derive(Clone)]
pub(super) struct QueryMessageContent {
    pub(super) query: String,
}
pub(super) struct Query {
    pub(super) bytes: Vec<u8>,
}
impl Decode for Query {
    type Output = QueryMessageContent;
    fn decode(&self) -> Result<QueryMessageContent, ByteReaderError> {
        Ok(QueryMessageContent {
            query: ByteReader::new(self.bytes.to_vec(), 0).read_cstring()?,
        })
    }
}
#[derive(Clone)]
pub(super) struct ParseMessageContent {
    pub(super) prepared_statement_name: String,
    pub(super) query: String,
    pub(super) parameter_data_types: Vec<i32>,
}
pub(super) struct Parse {
    pub(super) bytes: Vec<u8>,
}
impl Decode for Parse {
    type Output = ParseMessageContent;
    fn decode(&self) -> Result<ParseMessageContent, ByteReaderError> {
        let mut reader = ByteReader::new(self.bytes.to_vec(), 0);
        let prepared_statement_name = reader.read_cstring()?;
        let query = reader.read_cstring()?;
        let parameter_data_types_len: usize = reader.read_i16_as_usize()?;
        let mut parameter_data_types = Vec::with_capacity(parameter_data_types_len);
        for _ in 0..parameter_data_types_len {
            parameter_data_types.push(reader.read_i32()?)
        }
        let parsed_message = ParseMessageContent {
            prepared_statement_name,
            query,
            parameter_data_types,
        };
        Ok(parsed_message)
    }
}
#[derive(Clone)]
pub(super) struct BindMessageContent {
    pub(super) portal_name: String,
    pub(super) source_prepared_statement_name: String,
    /// If this is empty it should interpreted as text format for all values
    pub(super) parameter_format_codes: Vec<i16>,
    pub(super) parameter_values: Vec<Option<Vec<u8>>>,
    pub(super) result_column_format_codes: Vec<i16>,
}
pub(super) struct Bind {
    pub(super) bytes: Vec<u8>,
}
impl Decode for Bind {
    type Output = BindMessageContent;
    fn decode(&self) -> Result<BindMessageContent, ByteReaderError> {
        let mut reader = ByteReader::new(self.bytes.to_vec(), 0);
        let portal_name = reader.read_cstring()?;
        let source_prepared_statement_name = reader.read_cstring()?;
        let parameter_format_codes_len = reader.read_i16_as_usize()?;
        let mut parameter_format_codes = Vec::with_capacity(parameter_format_codes_len);
        for _ in 0..parameter_format_codes_len {
            parameter_format_codes.push(reader.read_i16()?)
        }
        let parameter_values_len = reader.read_i16_as_usize()?;
        let mut parameter_values = Vec::with_capacity(parameter_values_len);
        for _ in 0..parameter_values_len {
            let byte_value_length = reader.read_i32()?;
            // This basically means that the parameter value is null
            if byte_value_length == -1 {
                parameter_values.push(None)
            } else {
                let byte_value_length_usize: usize =
                    byte_value_length.try_into().map_err(|_| ByteReaderError {
                        kind: ByteReaderErrorKind::NegativeLength,
                        message: "the only case where this value should be negative is when the parameter '-1' which means
                          null".to_string(),
                    })?;
                parameter_values.push(Some(reader.read_bytes(byte_value_length_usize)?));
            }
        }

        let result_column_format_codes_len = reader.read_i16_as_usize()?;
        let mut result_column_format_codes = Vec::with_capacity(result_column_format_codes_len);
        for _ in 0..result_column_format_codes_len {
            result_column_format_codes.push(reader.read_i16()?);
        }

        let parsed_message = BindMessageContent {
            portal_name,
            source_prepared_statement_name,
            parameter_format_codes,
            parameter_values,
            result_column_format_codes,
        };

        Ok(parsed_message)
    }
}
#[derive(Clone)]
pub(super) enum DescribeMessageContentTarget {
    PreparedStatement,
    Portal,
}

impl TryFrom<u8> for DescribeMessageContentTarget {
    type Error = ByteReaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'S' => Ok(Self::PreparedStatement),
            b'P' => Ok(Self::Portal),
            _ => Err(ByteReaderError {
                kind: ByteReaderErrorKind::ProtocolError,
                message: "target byte must be 'S' or 'P'".to_string(),
            }),
        }
    }
}
impl Display for DescribeMessageContentTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PreparedStatement => write!(f, "PreparedStatement"),
            Self::Portal => write!(f, "Portal"),
        }
    }
}
#[derive(Clone)]
pub(super) struct DescribeMessageContent {
    /// 'S' means prepared_statement and 'P' means portal
    pub(super) target: DescribeMessageContentTarget,
    /// The name of the target to describe, if empty it's the unnamed one.
    pub(super) name: String,
}

pub(super) struct Describe {
    pub(super) bytes: Vec<u8>,
}
impl Decode for Describe {
    type Output = DescribeMessageContent;
    fn decode(&self) -> Result<DescribeMessageContent, ByteReaderError> {
        let mut reader = ByteReader::new(self.bytes.to_vec(), 0);
        let target: DescribeMessageContentTarget = reader.read_u8()?.try_into()?;
        let name = reader.read_cstring()?;

        let parsed_message = DescribeMessageContent { target, name };

        Ok(parsed_message)
    }
}
#[derive(Clone)]
pub(super) struct ExecuteMessageContent {
    /// The name of the target (portal) to execute, if empty it's the unnamed one.
    pub(super) name: String,
    pub(super) rows_to_return_limit: i32,
}

pub(super) struct Execute {
    pub(super) bytes: Vec<u8>,
}
impl Decode for Execute {
    type Output = ExecuteMessageContent;
    fn decode(&self) -> Result<ExecuteMessageContent, ByteReaderError> {
        let mut reader = ByteReader::new(self.bytes.to_vec(), 0);
        let name = reader.read_cstring()?;
        let rows_to_return_limit = reader.read_i32()?;

        let parsed_message = ExecuteMessageContent {
            name,
            rows_to_return_limit,
        };

        Ok(parsed_message)
    }
}

#[derive(Clone)]
pub(super) enum CloseMessageContentTarget {
    PreparedStatement,
    Portal,
}
impl TryFrom<u8> for CloseMessageContentTarget {
    type Error = ByteReaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'S' => Ok(Self::PreparedStatement),
            b'P' => Ok(Self::Portal),
            _ => Err(ByteReaderError {
                kind: ByteReaderErrorKind::ProtocolError,
                message: "target byte must be 'S' or 'P'".to_string(),
            }),
        }
    }
}
impl Display for CloseMessageContentTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PreparedStatement => write!(f, "PreparedStatement"),
            Self::Portal => write!(f, "Portal"),
        }
    }
}

#[derive(Clone)]
pub(super) struct CloseMessageContent {
    pub(super) target: CloseMessageContentTarget,
    pub(super) name: String,
}

pub(super) struct Close {
    pub(super) bytes: Vec<u8>,
}
impl Decode for Close {
    type Output = CloseMessageContent;
    fn decode(&self) -> Result<CloseMessageContent, ByteReaderError> {
        let mut reader = ByteReader::new(self.bytes.to_vec(), 0);
        let target: CloseMessageContentTarget = reader.read_u8()?.try_into()?;
        let name = reader.read_cstring()?;

        let parsed_message = CloseMessageContent { target, name };

        Ok(parsed_message)
    }
}
