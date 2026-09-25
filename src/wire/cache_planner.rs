use std::{
    collections::{BTreeMap, HashMap},
    io::Error,
    ops::Range,
    sync::Arc,
    time::{Duration, Instant},
};

use sqlx::query;
use time::format_description::parse;

use crate::{
    AppState,
    cache::{QueryTemplate, lfu::CachedResponse, store::cache_key_wire},
    wire::{
        DBState, Decode, MessageFramer, Scratch, data_phase,
        messages::{Close, CloseMessageContentTarget},
        types::{
            CachePlan, CommandSlotCapture, CommandSlotPassthrough, CommandSlotReplay,
            CommandSlotSkip, Cycle, MessageKind, PreparedStatementState, ScratchEntry,
        },
    },
};

use super::{
    messages::{
        Bind, BindMessageContent, ClientMessageContent,
        ClientMessageContent::{
            BindMessage, DescribeMessage, ExecuteMessage, ParseMessage, QueryMessage, SyncMessage,
            UnknownMessage,
        },
        Describe, DescribeMessageContent, DescribeMessageContentTarget, Execute,
        ExecuteMessageContent, Parse, ParseMessageContent, Query,
    },
    types::{
        ClientState, CommandSlot, DescribeKind, Portal, PreparedStatement, ProtocolMode,
        ReplayTrim, ReplayTrimExtended, ReplayTrimSimple, StateHandlingResult,
    },
};

pub(super) fn get_from_cache(client_state: &ClientState, key: &str) -> Option<Arc<CachedResponse>> {
    client_state.app_state.cache.get(key)
}

pub(super) fn set_in_cache(app_state: &AppState, ttl: Duration, key: &str, data: CachedResponse) {
    println!("cache_key set: {}", key);
    app_state
        .cache
        .insert(key.to_string(), data, Instant::now() + ttl);
}

pub(super) fn find_template(
    content: &ExecuteMessageContent,
    client_state: &mut ClientState,
) -> Option<CachePlan> {
    let portal = client_state.portals.get(&content.name)?;

    let prepared_statement = client_state
        .prepared_statements
        .get(&portal.source_prepared_statement_name)?;

    if let Some(ttl) = resolve_ttl(&client_state.app_state, &prepared_statement.stmt.query) {
        return Some(CachePlan {
            key: cache_key_wire(&prepared_statement.stmt.query, &portal.parameter_values),
            ttl,
        });
    }
    None
}

pub(super) fn find_template_simple(
    query: &str,
    client_state: &mut ClientState,
) -> Option<CachePlan> {
    if let Some(ttl) = resolve_ttl(&client_state.app_state, query) {
        return Some(CachePlan {
            key: cache_key_wire(query, &Vec::new()),
            ttl,
        });
    }
    None
}

pub(super) fn resolve_ttl(app_state: &AppState, query: &str) -> Option<Duration> {
    let ttl = {
        let template = app_state.matcher.find_template(query)?;
        match template.ttl {
            Some(ttl) => Duration::from_secs(ttl),
            None => Duration::from_secs(app_state.global_ttl),
        }
    };
    Some(ttl)
}

pub(super) async fn find_command_slots(
    client_state: &mut ClientState,
) -> Result<Vec<Cycle>, Error> {
    let mut cycles: Vec<Cycle> = Vec::new();
    client_state
        .framer
        .add_buffer(client_state.buffer_state.pending_data());

    'next_message_loop: while let Ok(Some(msg)) = client_state.framer.next_message() {
        let type_byte = msg[0];
        match type_byte {
            b'Q' => {
                // From the docs: "(Note that a simple Query message also destroys the unnamed statement.)"
                client_state.prepared_statements.remove("");
                let body = msg[5..].to_vec();
                let query_content = match (Query { bytes: body }).decode() {
                    Ok(decoded) => decoded,
                    Err(e) => return Err(Error::new(std::io::ErrorKind::Other, e.message)),
                };

                let cache_plan = match find_template_simple(&query_content.query, client_state) {
                    Some(cache_plan) => cache_plan,
                    None => {
                        cycles.push(Cycle {
                            slots: vec![CommandSlot::Passthrough(CommandSlotPassthrough {
                                bytes: msg,
                                kind: MessageKind::Query,
                            })],
                            synthesize_sync: false,
                        });
                        continue;
                    }
                };
                match get_from_cache(client_state, &cache_plan.key) {
                    Some(cached_response) => cycles.push(Cycle {
                        slots: vec![CommandSlot::Replay(CommandSlotReplay {
                            data: cached_response,
                            describe_kind: DescribeKind::None,
                            protocol_mode: ProtocolMode::Simple,
                            query: query_content.query,
                            key: cache_plan.key,
                        })],
                        synthesize_sync: false,
                    }),
                    None => cycles.push(Cycle {
                        slots: vec![CommandSlot::Capture(CommandSlotCapture {
                            bytes: msg,
                            key: cache_plan.key,
                            describe_kind: DescribeKind::None,
                            protocol_mode: ProtocolMode::Simple,
                            query: query_content.query,
                            ttl: cache_plan.ttl,
                        })],
                        synthesize_sync: false,
                    }),
                };
            }
            b'P' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: MessageKind::Parse,
                    execute: None,
                });
            }
            b'B' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: MessageKind::Bind,
                    execute: None,
                });
            }
            b'D' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: MessageKind::Describe,
                    execute: None,
                });
            }
            b'E' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: MessageKind::Execute,
                    execute: None,
                });
            }
            b'S' => {
                cycles.push(Cycle {
                    slots: sync_message_handle_entries(client_state).await?,
                    // Important that we synthesize the Sync message if cycle exists because of a
                    // Sync message
                    synthesize_sync: true,
                });
                client_state.scratch.reset();
            }
            b'C' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: MessageKind::Close,
                    execute: None,
                });
            }
            b'X' => {
                // Terminate means stop immediately
                println!("terminated session");
                client_state.scratch.reset();
                cycles.push(Cycle {
                    slots: vec![CommandSlot::Passthrough(CommandSlotPassthrough {
                        bytes: msg.clone(),
                        kind: MessageKind::Terminate,
                    })],
                    synthesize_sync: false,
                });
                break 'next_message_loop;
            }
            _ => {
                return Err(Error::new(
                    std::io::ErrorKind::Other,
                    "unexpected message type, the type byte is unrecognized",
                ));
            }
        }
    }
    Ok(cycles)
}

pub(super) fn handle_command_slot_messages(
    db_state: &mut DBState,
    command_slots: &[CommandSlot],
) -> Result<Vec<u8>, String> {
    let mut complete_byte_stream = Vec::new();
    let replays_and_captures = replays_and_captures_in_command_slots(command_slots);

    if replays_and_captures.replays.is_empty() && replays_and_captures.captures.is_empty() {
        while let Some(next_message) = db_state.framer.next_message()? {
            complete_byte_stream.extend_from_slice(&next_message);
        }
        return Ok(complete_byte_stream);
    }
    // Maybe do something where i iterate the command_slots and while doing so i also look at the
    // next_message in the framer, of course something should be done smart about the DataRow and
    // alike, as they can take up A LOT of entries (as many as there was rows returned).

    'capture_loop: for (index, capture) in replays_and_captures.captures {
        let mut data_to_capture: Vec<u8> = Vec::new();
        let mut param_desc_to_capture: Vec<u8> = Vec::new();
        let mut row_desc_to_capture: Vec<u8> = Vec::new();

        if capture.protocol_mode == ProtocolMode::Simple {
            while let Some(next_message) = db_state.framer.next_message()? {
                match next_message[0] {
                    b'C' => {
                        data_to_capture.extend_from_slice(&next_message);
                        set_in_cache(
                            &db_state.app_state,
                            capture.ttl,
                            &capture.key,
                            CachedResponse {
                                param_desc: None,
                                row_desc: None,
                                data: data_to_capture.clone(),
                            },
                        );
                    }
                    b'Z' => {
                        complete_byte_stream.extend_from_slice(&data_to_capture);
                        complete_byte_stream.extend_from_slice(&next_message);
                        continue 'capture_loop;
                    }
                    _ => data_to_capture.extend_from_slice(&next_message),
                }
            }
        } else {
            while let Some(next_message) = db_state.framer.next_message()? {
                match next_message[0] {
                    b'C' => {
                        data_to_capture.extend_from_slice(&next_message);
                        set_in_cache(
                            &db_state.app_state,
                            capture.ttl,
                            &capture.key,
                            CachedResponse {
                                param_desc: {
                                    if capture.describe_kind == DescribeKind::Statement {
                                        Some(param_desc_to_capture.clone())
                                    } else {
                                        None
                                    }
                                },
                                row_desc: {
                                    if capture.describe_kind != DescribeKind::None {
                                        Some(row_desc_to_capture.clone())
                                    } else {
                                        None
                                    }
                                },
                                data: data_to_capture.clone(),
                            },
                        );
                    }
                    b't' => param_desc_to_capture = next_message.clone(),
                    b'T' => row_desc_to_capture = next_message.clone(),
                    b'D' => data_to_capture.extend_from_slice(&next_message),
                    b'Z' => {
                        complete_byte_stream.extend_from_slice(&data_to_capture);
                        complete_byte_stream.extend_from_slice(&next_message);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    for (index, command_slot) in command_slots.iter().enumerate() {
        let next_message = db_state.framer.next_message()?;

        match command_slot {
            CommandSlot::Passthrough(CommandSlotPassthrough { bytes, kind }) => {}
            CommandSlot::Skip(CommandSlotSkip { bytes, kind }) => {}
            CommandSlot::Replay(CommandSlotReplay {
                key,
                data,
                describe_kind,
                protocol_mode,
                query,
            }) => {}
            CommandSlot::Capture(CommandSlotCapture {
                bytes,
                key,
                describe_kind,
                protocol_mode,
                query,
                ttl,
            }) => {}
        }
    }

    Ok(complete_byte_stream)
}

/// Named prepared statements must be explicitly closed before they can be redefined by another Parse message,
/// but this is not required for the unnamed statement.
async fn parse_message(
    content: &ParseMessageContent,
    client_state: &mut ClientState,
) -> Result<(), StateHandlingResult> {
    if !content.prepared_statement_name.is_empty()
        && client_state
            .prepared_statements
            .contains_key(&content.prepared_statement_name)
    {
        return Err(StateHandlingResult::Error(
            "prepared statement already exists".to_string(),
        ));
    }

    let prepared_statement = PreparedStatementState {
        stmt: PreparedStatement {
            query: content.query.clone(),
            parameter_data_types: content.parameter_data_types.clone(),
        },
        backend_knows_about_it: false,
    };
    client_state
        .prepared_statements
        .insert(content.prepared_statement_name.clone(), prepared_statement);

    println!("saved prepared statement");

    Ok(())
}

async fn bind_message(
    content: &BindMessageContent,
    client_state: &mut ClientState,
) -> Result<(), StateHandlingResult> {
    if let Some(prepared_statement) = client_state
        .prepared_statements
        .get(&content.source_prepared_statement_name)
    {
        if client_state
            .app_state
            .matcher
            .template_exists(&prepared_statement.stmt.query)
        {
            let portal = Portal {
                source_prepared_statement_name: content.source_prepared_statement_name.clone(),
                parameter_format_codes: content.parameter_format_codes.clone(),
                parameter_values: content.parameter_values.clone(),
                result_column_format_codes: content.result_column_format_codes.clone(),
            };
            client_state
                .portals
                .insert(content.portal_name.clone(), portal);
        }
        println!("saved in portals");
    };
    Ok(())
}

fn describe_message(content: &DescribeMessageContent) -> DescribeKind {
    match content.target {
        DescribeMessageContentTarget::PreparedStatement => DescribeKind::Statement,
        DescribeMessageContentTarget::Portal => DescribeKind::Portal,
    }
}

fn get_prepared_statement_in_session<'a>(
    name: &str,
    client_state: &'a mut ClientState,
) -> Option<&'a PreparedStatementState> {
    client_state.prepared_statements.get(name)
}

fn get_portal_in_session<'a>(name: &str, client_state: &'a mut ClientState) -> Option<&'a Portal> {
    client_state.portals.get(name)
}

fn cache_data_can_satisfy(response: &CachedResponse, kind: &DescribeKind) -> bool {
    match kind {
        DescribeKind::None => response.has_data(),
        DescribeKind::Portal => response.has_data() && response.has_row_desc(),
        DescribeKind::Statement => {
            response.has_data() && response.has_row_desc() && response.has_param_desc()
        }
    }
}

fn resolve_execute_chain(client_state: &ClientState, portal_name: &str) -> Option<PairedMessages> {
    let mut paired_messages: PairedMessages = PairedMessages {
        parse_entry: None,
        bind_entry: None,
        query: String::new(),
    };

    let mut prepared_statement_name: Option<String> = None;

    match client_state.scratch.binds_by_portal_name.get(portal_name) {
        Some((index, portal)) => {
            paired_messages.bind_entry = Some(*index);
            prepared_statement_name = Some(portal.source_prepared_statement_name.clone());
        }
        None => {
            if let Some(portal) = client_state.portals.get(portal_name) {
                prepared_statement_name = Some(portal.source_prepared_statement_name.clone());
            }
        }
    }

    let stmt_name = prepared_statement_name?;
    match client_state.scratch.parses_by_stmt_name.get(&stmt_name) {
        Some((index, statement)) => {
            paired_messages.parse_entry = Some(*index);
            paired_messages.query = statement.query.clone();
        }
        None => {
            if let Some(statement) = client_state.prepared_statements.get(&stmt_name) {
                paired_messages.query = statement.stmt.query.clone();
            }
        }
    }
    Some(paired_messages)
}

struct PairedMessages {
    parse_entry: Option<usize>,
    bind_entry: Option<usize>,
    // descibe_entry: Option<usize>,
    query: String,
}

async fn sync_message_handle_entries(
    client_state: &mut ClientState,
) -> Result<Vec<CommandSlot>, Error> {
    let mut command_slots: Vec<Option<CommandSlot>> =
        vec![None; client_state.scratch.entries.len()];
    for (index, entry) in client_state.scratch.entries.clone().iter().enumerate() {
        match entry.kind {
            MessageKind::Parse => {
                let body = entry.bytes[5..].to_vec();
                let parse_content = match (Parse { bytes: body }).decode() {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        return Err(Error::new(std::io::ErrorKind::Other, e.message));
                    }
                };
                let _ = parse_message(&parse_content, client_state).await;
                client_state.scratch.parses_by_stmt_name.insert(
                    parse_content.prepared_statement_name.clone(),
                    (index, parse_content),
                );
            }
            MessageKind::Bind => {
                let body = entry.bytes[5..].to_vec();
                let bind_content = match (Bind { bytes: body }).decode() {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        return Err(Error::new(std::io::ErrorKind::Other, e.message));
                    }
                };
                let _ = bind_message(&bind_content, client_state).await;
                client_state
                    .scratch
                    .binds_by_portal_name
                    .insert(bind_content.portal_name.clone(), (index, bind_content));
            }
            MessageKind::Describe => {
                let body = entry.bytes[5..].to_vec();
                let describe_content = match (Describe { bytes: body }).decode() {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        return Err(Error::new(std::io::ErrorKind::Other, e.message));
                    }
                };
                client_state
                    .scratch
                    .describes_by_name
                    .insert(describe_content.name.clone(), (index, describe_content));
            }
            MessageKind::Close => {
                let body = entry.bytes[5..].to_vec();
                let close_content = match (Close { bytes: body }).decode() {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        return Err(Error::new(std::io::ErrorKind::Other, e.message));
                    }
                };
                match close_content.target {
                    CloseMessageContentTarget::PreparedStatement => {
                        client_state.prepared_statements.remove(&close_content.name);

                        client_state
                            .scratch
                            .parses_by_stmt_name
                            .remove(&close_content.name);
                    }
                    CloseMessageContentTarget::Portal => {
                        client_state.portals.remove(&close_content.name);
                        client_state
                            .scratch
                            .binds_by_portal_name
                            .remove(&close_content.name);
                    }
                };
            }
            MessageKind::Execute => {
                let body = entry.bytes[5..].to_vec();
                let execute_content = match (Execute { bytes: body }).decode() {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        return Err(Error::new(std::io::ErrorKind::Other, e.message));
                    }
                };

                let cache_plan = {
                    if execute_content.rows_to_return_limit == 0 {
                        match find_template(&execute_content, client_state) {
                            Some(cache_plan) => cache_plan,
                            None => {
                                command_slots[index] =
                                    Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                        bytes: entry.bytes.clone(),
                                        kind: entry.kind.clone(),
                                    }));
                                continue;
                            }
                        }
                    } else {
                        command_slots[index] =
                            Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                bytes: entry.bytes.clone(),
                                kind: entry.kind.clone(),
                            }));
                        continue;
                    }
                };
                let cache_response = get_from_cache(client_state, &cache_plan.key);
                let mut describe_kind: DescribeKind = DescribeKind::None;

                match resolve_execute_chain(client_state, &execute_content.name) {
                    Some(paired_messages) => {
                        // We do this check for the describe_kind now, as we need it to see whether
                        // the data we (possibly) have cached is complete/contains what is needed
                        if let Some((_, describe)) = client_state
                            .scratch
                            .describes_by_name
                            .get(&execute_content.name)
                        {
                            describe_kind = describe_message(describe);
                        }
                        if let Some(cached) = cache_response
                            && cache_data_can_satisfy(&cached, &describe_kind)
                        {
                            if let Some((describe_index, _)) = client_state
                                .scratch
                                .describes_by_name
                                .get(&execute_content.name)
                                && let Some(entry) =
                                    client_state.scratch.entries.get(*describe_index)
                            {
                                command_slots[*describe_index] =
                                    Some(CommandSlot::Skip(CommandSlotSkip {
                                        bytes: entry.bytes.clone(),
                                        kind: MessageKind::Describe,
                                    }));
                            };
                            command_slots[index] = Some(CommandSlot::Replay(CommandSlotReplay {
                                key: cache_plan.key,
                                describe_kind,
                                protocol_mode: ProtocolMode::Extended,
                                query: paired_messages.query,
                                data: cached,
                            }));
                            if let Some(bind_index) = paired_messages.bind_entry
                                && let Some(entry) = client_state.scratch.entries.get(bind_index)
                            {
                                command_slots[bind_index] =
                                    Some(CommandSlot::Skip(CommandSlotSkip {
                                        bytes: entry.bytes.clone(),
                                        kind: MessageKind::Bind,
                                    }))
                            }
                            if let Some(parse_index) = paired_messages.parse_entry
                                && let Some(entry) = client_state.scratch.entries.get(parse_index)
                            {
                                command_slots[parse_index] =
                                    Some(CommandSlot::Skip(CommandSlotSkip {
                                        bytes: entry.bytes.clone(),
                                        kind: MessageKind::Parse,
                                    }))
                            }
                        } else {
                            if let Some((describe_index, _)) = client_state
                                .scratch
                                .describes_by_name
                                .get(&execute_content.name)
                                && let Some(entry) =
                                    client_state.scratch.entries.get(*describe_index)
                            {
                                command_slots[*describe_index] =
                                    Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                        bytes: entry.bytes.clone(),
                                        kind: MessageKind::Describe,
                                    }))
                            }
                            command_slots[index] = Some(CommandSlot::Capture(CommandSlotCapture {
                                bytes: entry.bytes.clone(),
                                key: cache_plan.key,
                                describe_kind,
                                protocol_mode: ProtocolMode::Extended,
                                query: paired_messages.query,
                                ttl: cache_plan.ttl,
                            }));

                            if let Some(bind_index) = paired_messages.bind_entry
                                && let Some(entry) = client_state.scratch.entries.get(bind_index)
                            {
                                command_slots[bind_index] =
                                    Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                        bytes: entry.bytes.clone(),
                                        kind: MessageKind::Bind,
                                    }))
                            }
                            if let Some(parse_index) = paired_messages.parse_entry
                                && let Some(entry) = client_state.scratch.entries.get(parse_index)
                            {
                                command_slots[parse_index] =
                                    Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                        bytes: entry.bytes.clone(),
                                        kind: MessageKind::Parse,
                                    }))
                            }
                        }
                    }
                    None => {
                        // TODO! add proper logging when this happens, as it shouldn't really be
                        // able to happen
                        command_slots[index] =
                            Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                bytes: entry.bytes.clone(),
                                kind: entry.kind.clone(),
                            }));
                    }
                }
            }
            _ => {}
        }
    }

    for (index, entry) in command_slots.iter_mut().enumerate() {
        if entry.is_none()
            && let Some(scratch_entry) = client_state.scratch.entries.get(index)
        {
            *entry = Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                bytes: scratch_entry.bytes.clone(),
                kind: scratch_entry.kind.clone(),
            }));
        }
    }

    Ok(command_slots.into_iter().flatten().collect())
}

struct ReplaysAndCaptures {
    replays: Vec<(usize, CommandSlotReplay)>,
    captures: Vec<(usize, CommandSlotCapture)>,
}

fn replays_and_captures_in_command_slots(command_slots: &[CommandSlot]) -> ReplaysAndCaptures {
    let mut replays_and_captures = ReplaysAndCaptures {
        replays: Vec::new(),
        captures: Vec::new(),
    };

    for (index, command_slot) in command_slots.iter().enumerate() {
        if let CommandSlot::Replay(replay) = command_slot {
            replays_and_captures.replays.push((index, replay.clone()))
        }
        if let CommandSlot::Capture(capture) = command_slot {
            replays_and_captures.captures.push((index, capture.clone()))
        }
    }

    replays_and_captures
}

pub(super) fn command_slots_contains_replay_or_capture(command_slots: &[CommandSlot]) -> bool {
    for command_slot in command_slots {
        if let CommandSlot::Replay(_) | CommandSlot::Capture(_) = command_slot {
            return true;
        }
    }
    false
}

pub(super) fn cycle_contains_replay_and_capture(cycle: &Cycle) -> bool {
    let mut has_replay = false;
    let mut has_capture = false;
    for slot in &cycle.slots {
        if let CommandSlot::Replay(_) = slot {
            has_replay = true;
        }
        if let CommandSlot::Capture(_) = slot {
            has_capture = true;
        }
    }
    has_replay && has_capture
}

pub(super) fn cycle_contains_replay(cycle: &Cycle) -> bool {
    for slot in &cycle.slots {
        if let CommandSlot::Replay(_) = slot {
            return true;
        }
    }
    false
}

pub(super) fn cycle_contains_capture(cycle: &Cycle) -> bool {
    for slot in &cycle.slots {
        if let CommandSlot::Capture(_) = slot {
            return true;
        }
    }
    false
}
