use std::{io::Error, sync::Arc, time::Duration};

use crate::{
    AppState,
    cache::{lfu::CachedResponse, store::cache_key_wire},
    wire::{
        Decode, Scratch,
        messages::{Close, CloseMessageContentTarget},
        types::{
            CachePlan, CommandSlotCapture, CommandSlotPassthrough, CommandSlotReplay,
            CommandSlotSkip, Cycle, MessageKind, PreparedStatementState, ScratchEntry,
        },
    },
};

use super::{
    messages::{
        Bind, BindMessageContent, Describe, DescribeMessageContent, DescribeMessageContentTarget,
        Execute, ExecuteMessageContent, Parse, ParseMessageContent, Query,
    },
    types::{
        ClientState, CommandSlot, DescribeKind, Portal, PreparedStatement, ProtocolMode,
        StateHandlingResult,
    },
};

struct PairedMessages {
    parse_entry: Option<usize>,
    bind_entry: Option<usize>,
    describe_entry: Option<usize>,
    describe_kind: DescribeKind,
    query: String,
}

struct ExecutePlan {
    execute_slot: CommandSlot,
    describe_slot: Option<(usize, CommandSlot)>,
    bind_slot: Option<(usize, CommandSlot)>,
    parse_slot: Option<(usize, CommandSlot)>,
}

pub(super) fn get_from_cache(app_state: &AppState, key: &str) -> Option<Arc<CachedResponse>> {
    app_state.cache.get(key)
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

    while let Ok(Some(msg)) = client_state.framer.next_message() {
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
                if let Some(cache_plan) = find_template_simple(&query_content.query, client_state) {
                    match get_from_cache(&client_state.app_state, &cache_plan.key) {
                        Some(cached_response) => cycles.push(Cycle {
                            slots: vec![CommandSlot::Replay(CommandSlotReplay {
                                data: cached_response,
                                describe_kind: DescribeKind::None,
                                protocol_mode: ProtocolMode::Simple,
                                query: query_content.query,
                                key: cache_plan.key,
                            })],
                            protocol_mode: ProtocolMode::Simple,
                            needs_db: false,
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
                            protocol_mode: ProtocolMode::Simple,
                            needs_db: true,
                        }),
                    };
                } else {
                    cycles.push(Cycle {
                        slots: vec![CommandSlot::Passthrough(CommandSlotPassthrough {
                            bytes: msg,
                            kind: MessageKind::Query,
                        })],
                        protocol_mode: ProtocolMode::Simple,
                        needs_db: true,
                    });
                }
            }
            b'P' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg,
                    kind: MessageKind::Parse,
                });
            }
            b'B' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg,
                    kind: MessageKind::Bind,
                });
            }
            b'D' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg,
                    kind: MessageKind::Describe,
                });
            }
            b'E' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg,
                    kind: MessageKind::Execute,
                });
            }
            b'S' => {
                cycles.push(sync_message_handle_entries(client_state).await?);
                client_state.scratch.reset();
            }
            b'C' => {
                client_state.scratch.entries.push(ScratchEntry {
                    bytes: msg,
                    kind: MessageKind::Close,
                });
            }
            b'X' => {
                // Terminate means stop immediately
                println!("terminated session");
                client_state.scratch.reset();
                cycles.push(Cycle {
                    slots: vec![CommandSlot::Passthrough(CommandSlotPassthrough {
                        bytes: msg,
                        kind: MessageKind::Terminate,
                    })],
                    protocol_mode: ProtocolMode::Extended,
                    needs_db: false,
                });
                return Ok(cycles);
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
        && client_state
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
        describe_entry: None,
        describe_kind: DescribeKind::None,
        query: String::new(),
    };

    let mut prepared_statement_name: Option<String> = None;

    match client_state.scratch.describes_by_name.get(portal_name) {
        Some((index, describe)) => {
            paired_messages.describe_entry = Some(*index);
            paired_messages.describe_kind = describe_message(describe);
        }
        None => (),
    }

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

async fn sync_message_handle_entries(client_state: &mut ClientState) -> Result<Cycle, Error> {
    let mut command_slots: Vec<Option<CommandSlot>> =
        vec![None; client_state.scratch.entries.len()];
    for (index, entry) in client_state.scratch.entries.clone().iter().enumerate() {
        match entry.kind {
            MessageKind::Parse => apply_parse(entry, index, client_state).await?,
            MessageKind::Bind => apply_bind(entry, index, client_state).await?,
            MessageKind::Describe => apply_describe(entry, index, client_state).await?,
            MessageKind::Close => apply_close(entry, client_state).await?,
            MessageKind::Execute => {
                let execute_plan = plan_execute(entry, client_state).await;
                command_slots[index] = Some(execute_plan.execute_slot);
                for (index, slot) in [
                    execute_plan.describe_slot,
                    execute_plan.bind_slot,
                    execute_plan.parse_slot,
                ]
                .into_iter()
                .flatten()
                {
                    command_slots[index] = Some(slot);
                }
            }
            _ => (),
        }
    }
    for (index, entry) in command_slots.iter_mut().enumerate() {
        if entry.is_none()
            && let Some(scratch_entry) = client_state.scratch.entries.get(index)
        {
            *entry = Some(passthrough_slot(
                scratch_entry.bytes.clone(),
                scratch_entry.kind.clone(),
            ));
        }
    }

    let slots: Vec<CommandSlot> = command_slots.into_iter().flatten().collect();

    let needs_db = slots.iter().any(|slot| slot.needs_db());

    Ok(Cycle {
        slots,
        needs_db,
        protocol_mode: ProtocolMode::Extended,
    })
}

async fn apply_parse(
    entry: &ScratchEntry,
    index: usize,
    client_state: &mut ClientState,
) -> Result<(), Error> {
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

    Ok(())
}

async fn apply_bind(
    entry: &ScratchEntry,
    index: usize,
    client_state: &mut ClientState,
) -> Result<(), Error> {
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
    Ok(())
}

async fn apply_describe(
    entry: &ScratchEntry,
    index: usize,
    client_state: &mut ClientState,
) -> Result<(), Error> {
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
    Ok(())
}

async fn apply_close(entry: &ScratchEntry, client_state: &mut ClientState) -> Result<(), Error> {
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
    Ok(())
}

async fn plan_execute(entry: &ScratchEntry, client_state: &mut ClientState) -> ExecutePlan {
    let mut execute_plan: ExecutePlan = ExecutePlan {
        execute_slot: CommandSlot::Passthrough(CommandSlotPassthrough {
            bytes: entry.bytes.clone(),
            kind: entry.kind.clone(),
        }),
        describe_slot: None,
        bind_slot: None,
        parse_slot: None,
    };
    let body = entry.bytes[5..].to_vec();
    let execute_content = match (Execute { bytes: body }).decode() {
        Ok(decoded) => decoded,
        Err(_) => return execute_plan,
    };

    let cache_plan = {
        if execute_content.rows_to_return_limit == 0 {
            match find_template(&execute_content, client_state) {
                Some(cache_plan) => cache_plan,
                None => {
                    return execute_plan;
                }
            }
        } else {
            return execute_plan;
        }
    };

    let cache_response = get_from_cache(&client_state.app_state, &cache_plan.key);

    match resolve_execute_chain(client_state, &execute_content.name) {
        Some(paired_messages) => {
            match cache_response
                .filter(|c| cache_data_can_satisfy(c, &paired_messages.describe_kind))
            {
                Some(cached) => {
                    execute_plan.execute_slot = CommandSlot::Replay(CommandSlotReplay {
                        key: cache_plan.key,
                        data: cached,
                        describe_kind: paired_messages.describe_kind,
                        protocol_mode: ProtocolMode::Extended,
                        query: paired_messages.query,
                    });
                    execute_plan.describe_slot = paired_slot(
                        &client_state.scratch,
                        paired_messages.describe_entry,
                        skip_slot,
                    );
                    execute_plan.parse_slot = paired_slot(
                        &client_state.scratch,
                        paired_messages.parse_entry,
                        skip_slot,
                    );
                    execute_plan.bind_slot =
                        paired_slot(&client_state.scratch, paired_messages.bind_entry, skip_slot);
                }
                None => {
                    execute_plan.execute_slot = CommandSlot::Capture(CommandSlotCapture {
                        bytes: entry.bytes.clone(),
                        key: cache_plan.key,
                        describe_kind: paired_messages.describe_kind,
                        protocol_mode: ProtocolMode::Extended,
                        query: paired_messages.query,
                        ttl: cache_plan.ttl,
                    });
                }
            }
        }
        None => {
            eprintln!(
                "execute chain unresolved for portal '{}', falling back to passthrough",
                execute_content.name
            );
            return execute_plan;
        }
    }

    execute_plan
}

fn skip_slot(bytes: Vec<u8>, kind: MessageKind) -> CommandSlot {
    CommandSlot::Skip(CommandSlotSkip { bytes, kind })
}

fn passthrough_slot(bytes: Vec<u8>, kind: MessageKind) -> CommandSlot {
    CommandSlot::Passthrough(CommandSlotPassthrough { bytes, kind })
}

fn paired_slot(
    scratch: &Scratch,
    index: Option<usize>,
    make: fn(Vec<u8>, MessageKind) -> CommandSlot,
) -> Option<(usize, CommandSlot)> {
    let entry = scratch.entries.get(index?)?;
    Some((index?, make(entry.bytes.clone(), entry.kind.clone())))
}
