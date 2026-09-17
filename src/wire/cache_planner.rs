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
        Decode, MessageFramer, Scratch, data_phase,
        messages::{Close, CloseMessageContentTarget},
        types::{
            CachePlan, CommandSlotCapture, CommandSlotPassthrough, CommandSlotReplay,
            CommandSlotSkip, Cycle, PreparedStatementState, ScratchEntry, ScratchKind,
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

pub(super) async fn find_command_slot_messages(
    client_state: &mut ClientState,
) -> Result<Vec<Cycle>, Error> {
    let mut cycles: Vec<Cycle> = Vec::new();
    // let mut command_slots: Vec<CommandSlot> = Vec::new();
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
                            })],
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
                    }),
                    None => cycles.push(Cycle {
                        slots: vec![CommandSlot::Capture(CommandSlotCapture {
                            key: cache_plan.key,
                            describe_kind: DescribeKind::None,
                            protocol_mode: ProtocolMode::Simple,
                            query: query_content.query,
                            ttl: cache_plan.ttl,
                        })],
                    }),
                };
            }
            b'P' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: ScratchKind::Parse,
                    execute: None,
                });
            }
            b'B' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: ScratchKind::Bind,
                    execute: None,
                });
            }
            b'D' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: ScratchKind::Describe,
                    execute: None,
                });
            }
            b'E' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: ScratchKind::Execute,
                    execute: None,
                });
            }
            b'S' => {
                cycles.push(Cycle {
                    slots: sync_message_handle_entries(client_state).await?,
                });
                client_state.scratch.reset();
            }
            b'C' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg.clone(),
                    kind: ScratchKind::Close,
                    execute: None,
                });
            }
            b'X' => {
                // Terminate means stop immediately
                client_state.scratch.reset();
                cycles.push(Cycle {
                    slots: vec![CommandSlot::Passthrough(CommandSlotPassthrough {
                        bytes: msg.clone(),
                    })],
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
            ScratchKind::Parse => {
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
            ScratchKind::Bind => {
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
            ScratchKind::Describe => {
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
            ScratchKind::Close => {
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
            ScratchKind::Execute => {
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
                                    }));
                                continue;
                            }
                        }
                    } else {
                        command_slots[index] =
                            Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                bytes: entry.bytes.clone(),
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
                            {
                                command_slots[*describe_index] =
                                    Some(CommandSlot::Skip(CommandSlotSkip {
                                        bytes: client_state
                                            .scratch
                                            .entries
                                            .get(*describe_index)
                                            .unwrap()
                                            .bytes
                                            .clone(),
                                    }));
                            };
                            command_slots[index] = Some(CommandSlot::Replay(CommandSlotReplay {
                                key: cache_plan.key,
                                describe_kind,
                                protocol_mode: ProtocolMode::Extended,
                                query: paired_messages.query,
                                data: cached,
                            }));
                            if let Some(bind_index) = paired_messages.bind_entry {
                                command_slots[bind_index] =
                                    Some(CommandSlot::Skip(CommandSlotSkip {
                                        bytes: client_state
                                            .scratch
                                            .entries
                                            .get(bind_index)
                                            .unwrap()
                                            .bytes
                                            .clone(),
                                    }))
                            }
                            if let Some(parse_index) = paired_messages.parse_entry {
                                command_slots[parse_index] =
                                    Some(CommandSlot::Skip(CommandSlotSkip {
                                        bytes: client_state
                                            .scratch
                                            .entries
                                            .get(parse_index)
                                            .unwrap()
                                            .bytes
                                            .clone(),
                                    }))
                            }
                        } else {
                            if let Some((describe_index, _)) = client_state
                                .scratch
                                .describes_by_name
                                .get(&execute_content.name)
                            {
                                command_slots[*describe_index] =
                                    Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                        bytes: client_state
                                            .scratch
                                            .entries
                                            .get(*describe_index)
                                            .unwrap()
                                            .bytes
                                            .clone(),
                                    }))
                            }
                            command_slots[index] = Some(CommandSlot::Capture(CommandSlotCapture {
                                key: cache_plan.key,
                                describe_kind,
                                protocol_mode: ProtocolMode::Extended,
                                query: paired_messages.query,
                                ttl: cache_plan.ttl,
                            }));

                            if let Some(bind_index) = paired_messages.bind_entry {
                                command_slots[bind_index] =
                                    Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                        bytes: client_state
                                            .scratch
                                            .entries
                                            .get(bind_index)
                                            .unwrap()
                                            .bytes
                                            .clone(),
                                    }))
                            }
                            if let Some(parse_index) = paired_messages.parse_entry {
                                command_slots[parse_index] =
                                    Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                                        bytes: client_state
                                            .scratch
                                            .entries
                                            .get(parse_index)
                                            .unwrap()
                                            .bytes
                                            .clone(),
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
                            }));
                    }
                }
            }
            _ => {}
        }
    }

    for (index, entry) in command_slots.iter_mut().enumerate() {
        if entry.is_none() {
            *entry = Some(CommandSlot::Passthrough(CommandSlotPassthrough {
                bytes: client_state
                    .scratch
                    .entries
                    .get(index)
                    .unwrap()
                    .bytes
                    .clone(),
            }));
        }
    }

    Ok(command_slots.into_iter().flatten().collect())
}
