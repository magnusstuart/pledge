use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    ops::Range,
    sync::Arc,
    time::Duration,
};

use super::MessageFramer;
use crate::{
    AppState,
    cache::lfu::CachedResponse,
    wire::{
        messages::{
            BindMessageContent, DescribeMessageContent, ExecuteMessageContent, ParseMessageContent,
        },
        writer::ByteWriter,
    },
};
use tokio::{
    io::AsyncReadExt,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

#[derive(Debug)]
pub(super) enum WireProtocolStates {
    WaitingForSSL,
    WaitingForStartup,
    ReadyForQuery,
}

pub(super) enum SQLCommand {
    Insert,
    Delete,
    Update,
    Merge,
    Select,
    CreateTableAs,
    Move,
    Fetch,
    Copy,
}

pub(super) struct ProtocolState {
    pub app_state: AppState,
    pub client_state: WireProtocolStates,
    pub db_state: WireProtocolStates,
    pub client_buffer: Vec<u8>,
    pub db_buffer: Vec<u8>,
    pub prepared_statements: HashMap<String, PreparedStatement>,
    pub portals: HashMap<String, Portal>,
}
pub(super) struct PreparedStatementState {
    pub stmt: PreparedStatement,
    pub backend_knows_about_it: bool,
}

pub(super) struct ClientState {
    pub app_state: AppState,
    /// The bool represents whether the backend know about this statement
    pub prepared_statements: HashMap<String, PreparedStatementState>,
    pub portals: HashMap<String, Portal>,
    pub framer: MessageFramer,
    pub buffer_state: BufferState,
    pub scratch: Scratch,
}

pub(super) struct DBState {
    pub app_state: AppState,
    pub framer: MessageFramer,
    pub buffer_state: BufferState,
    pub scratch: Scratch,
}

#[derive(Clone)]
pub(super) enum ScratchKind {
    Parse,
    Bind,
    Describe,
    Execute,
    Query,
    Sync,
    Close,
    Terminate,
}

#[derive(Clone)]
pub(super) struct ScratchEntry {
    pub bytes: Vec<u8>,
    pub kind: ScratchKind,
    pub execute: Option<ExecuteMessageContent>,
}

pub(super) struct Scratch {
    pub entries: Vec<ScratchEntry>,
    pub parses_by_stmt_name: HashMap<String, (usize, ParseMessageContent)>,
    pub binds_by_portal_name: HashMap<String, (usize, BindMessageContent)>,
    pub describes_by_name: HashMap<String, (usize, DescribeMessageContent)>,
}

impl Scratch {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            parses_by_stmt_name: HashMap::new(),
            binds_by_portal_name: HashMap::new(),
            describes_by_name: HashMap::new(),
        }
    }
    pub fn reset(&mut self) {
        self.entries.clear();
        self.parses_by_stmt_name.clear();
        self.binds_by_portal_name.clear();
        self.describes_by_name.clear();
    }
}

pub(super) struct BufferState {
    pub buffer: Vec<u8>,
    pub read_cursor: usize,
    pub write_cursor: usize,
}

impl BufferState {
    pub async fn read_from_stream(&mut self, stream: &mut OwnedReadHalf) -> Result<usize, Error> {
        let free_space = self.buffer.len() - self.write_cursor;
        if free_space < self.buffer.len() / 4 {
            println!("Buffer close to being full, compacting");
            self.compact();
        }
        if self.write_cursor >= self.buffer.len() {
            println!("Buffer too small, resizing");
            self.buffer.resize(self.buffer.len() * 2, 0);
            return Box::pin(self.read_from_stream(stream)).await;
        }
        let read = stream.read(&mut self.buffer[self.write_cursor..]).await?;
        self.write_cursor += read;
        Ok(read)
    }
    pub fn compact(&mut self) {
        if self.read_cursor > 0 {
            self.buffer
                .copy_within(self.read_cursor..self.write_cursor, 0);
            self.write_cursor -= self.read_cursor;
            self.read_cursor = 0;
        }
    }

    pub fn pending_data_len(&self) -> usize {
        self.write_cursor - self.read_cursor
    }

    pub fn pending_data(&self) -> &[u8] {
        &self.buffer[self.read_cursor..self.write_cursor]
    }

    // This can and should be optimized at some point
    pub fn pending_data_excluding(&mut self, replay_trim: &[ReplayTrim]) -> Option<Vec<u8>> {
        if replay_trim.is_empty() {
            return Some(self.pending_data().to_vec());
        }
        let mut buffer = self.buffer[self.read_cursor..self.write_cursor].to_vec();
        let mut writer = ByteWriter::new(&mut buffer, 0);
        writer.trim_from_pending_commands(replay_trim);

        if writer.get_buffer().len() > 0 {
            return Some(buffer);
        }
        return None;
    }

    pub fn consume(&mut self, n: &usize) -> Result<(), Error> {
        match self.read_cursor + n <= self.write_cursor {
            true => {
                self.read_cursor += n;
                Ok(())
            }
            false => Err(Error::new(
                ErrorKind::InvalidData,
                "consume: n is larger than pending data",
            )),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.read_cursor == self.write_cursor
    }
}

pub(super) enum StateHandlingResult {
    Continue(WireProtocolStates),
    Break(String),
    Error(String),
}

pub(super) struct PreparedStatement {
    pub query: String,
    pub parameter_data_types: Vec<i32>,
}

#[derive(Clone)]
pub(super) struct ColumnMetadata {
    pub name: String,
    pub table_oid: i32,
    pub attribute_number: i16,
    pub type_oid: i32,
    pub type_len: i16,
}

pub(super) struct Portal {
    pub source_prepared_statement_name: String,
    pub parameter_format_codes: Vec<i16>,
    pub parameter_values: Vec<Option<Vec<u8>>>,
    pub result_column_format_codes: Vec<i16>,
}

#[derive(Clone)]
pub(super) enum ProtocolMode {
    Simple,
    Extended,
}

#[derive(Clone)]
pub(super) struct Cycle {
    pub slots: Vec<CommandSlot>,
}
#[derive(Clone)]
pub(super) enum CommandSlot {
    Passthrough(CommandSlotPassthrough), // not configured: clean passthrough to the db
    Skip(CommandSlotSkip),
    Replay(CommandSlotReplay), // cache hit: write these to client, skip DB
    Capture(CommandSlotCapture), // cache miss: next DB response belongs to this key
}

#[derive(Clone)]
pub(super) struct CommandSlotPassthrough {
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub(super) struct CommandSlotSkip {
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub(super) struct CommandSlotReplay {
    pub key: String,
    pub data: Arc<CachedResponse>,
    pub describe_kind: DescribeKind,
    pub protocol_mode: ProtocolMode,
    pub query: String,
}

#[derive(Clone)]
pub(super) struct CommandSlotCapture {
    pub key: String,
    pub describe_kind: DescribeKind,
    pub protocol_mode: ProtocolMode,
    pub query: String,
    pub ttl: Duration,
}

#[derive(Clone)]
pub(super) enum DescribeKind {
    None,
    Portal,
    Statement,
}

pub(super) enum ReplayTrim {
    Extended(ReplayTrimExtended),
    Simple(ReplayTrimSimple),
}

pub(super) struct ReplayTrimExtended {
    pub execute: Range<usize>,
    pub parse: Option<Range<usize>>,
    pub bind: Option<Range<usize>>,
    pub describe: Option<Range<usize>>,
    pub sync: Option<Range<usize>>,
}

impl ReplayTrimExtended {
    pub fn new() -> Self {
        Self {
            execute: 0..0,
            parse: None,
            bind: None,
            describe: None,
            sync: None,
        }
    }
}

pub(super) struct ReplayTrimSimple {
    pub query: Range<usize>,
}

pub(super) struct CachePlan {
    pub key: String,
    pub ttl: Duration,
}
