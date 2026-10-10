use std::time::{Duration, Instant};

use crate::{
    AppState,
    cache::lfu::CachedResponse,
    wire::{
        DBState,
        messages::{BindComplete, Encode, ParseComplete},
        types::{CommandSlotCapture, CommandSlotReplay, Cycle, MessageKind},
    },
};

use super::types::{CommandSlot, DescribeKind, ProtocolMode};

enum ReplayOrCapture {
    Replay {
        replay_content: CommandSlotReplay,
        synthesize_parse_complete: bool,
        synthesize_bind_complete: bool,
    },
    Capture(CommandSlotCapture),
    NonCacheConfigured,
}

pub(super) fn set_in_cache(app_state: &AppState, ttl: Duration, key: &str, data: CachedResponse) {
    println!("cache_key set: {}", key);
    app_state
        .cache
        .insert(key.to_string(), data, Instant::now() + ttl);
}

// TODO! Add something where if a CommandSlot expects more data, but it isn't in the buffer, then
// read more data from the TCPStream, it might have gotten split up into multiple streams for
// various reasons
pub(super) fn handle_command_slot_messages(
    db_state: &mut DBState,
    cycle: &Cycle,
) -> Result<Vec<u8>, String> {
    let mut complete_byte_stream = Vec::new();
    let replays_or_captures = replays_or_captures_in_command_slots(&cycle.slots);

    if replays_or_captures.is_empty() {
        println!("No replays_or_captures_in_command_slots");
        while let Some(next_message) = db_state.framer.next_message()? {
            complete_byte_stream.extend_from_slice(&next_message);
        }
        return Ok(complete_byte_stream);
    }
    for slot in replays_or_captures {
        match slot {
            ReplayOrCapture::NonCacheConfigured => {
                while let Some(next_message) = db_state.framer.next_message()? {
                    complete_byte_stream.extend_from_slice(&next_message);
                    if next_message[0] == b'C' {
                        break;
                    }
                }
            }
            ReplayOrCapture::Capture(capture) => {
                handle_capture_command_slot(db_state, capture, &mut complete_byte_stream)?
            }
            ReplayOrCapture::Replay {
                replay_content,
                synthesize_parse_complete,
                synthesize_bind_complete,
            } => handle_replay_command_slot(
                replay_content,
                synthesize_parse_complete,
                synthesize_bind_complete,
                &mut complete_byte_stream,
            )?,
        }
    }
    while let Some(next_message) = db_state.framer.next_message()? {
        complete_byte_stream.extend_from_slice(&next_message);
        if next_message[0] == b'Z' {
            break;
        }
    }
    if !cycle.needs_db {
        println!("Synthesizing the Ready For Query");
        // this has be to "hardened" in terms of actually representing the true state,
        // such as if the we are in a transaction block etc.
        complete_byte_stream.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
    }
    Ok(complete_byte_stream)
}

fn handle_capture_command_slot(
    db_state: &mut DBState,
    capture: CommandSlotCapture,
    complete_byte_stream: &mut Vec<u8>,
) -> Result<(), String> {
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
                    break;
                }
                b'E' => {
                    eprintln!("--ERROR--\n Bytes: {:?}\n--ERROR--", next_message);
                    complete_byte_stream.extend_from_slice(&next_message);
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
                    if !param_desc_to_capture.is_empty() {
                        complete_byte_stream.extend_from_slice(&param_desc_to_capture);
                    }
                    if !row_desc_to_capture.is_empty() {
                        complete_byte_stream.extend_from_slice(&row_desc_to_capture);
                    }
                    // TODO! Should maybe add a "NoData" message if data_to_capture is is_empty
                    complete_byte_stream.extend_from_slice(&data_to_capture);
                    complete_byte_stream.extend_from_slice(&next_message);
                    break;
                }
                b'E' => {
                    eprintln!("--ERROR--\n Bytes: {:?}\n--ERROR--", next_message);
                    complete_byte_stream.extend_from_slice(&next_message);
                    return Err(format!(" --ERROR--\n Bytes: {:?}\n--ERROR--", next_message));
                }
                _ => {
                    complete_byte_stream.extend_from_slice(&next_message);
                }
            }
        }
    }
    Ok(())
}

fn handle_replay_command_slot(
    replay_content: CommandSlotReplay,
    synthesize_parse_complete: bool,
    synthesize_bind_complete: bool,
    complete_byte_stream: &mut Vec<u8>,
) -> Result<(), String> {
    println!("Replaying a {:?} message", replay_content.protocol_mode);
    if replay_content.protocol_mode == ProtocolMode::Simple {
        complete_byte_stream.extend_from_slice(&replay_content.data.get_data());
    } else {
        if synthesize_parse_complete {
            complete_byte_stream.extend_from_slice(&ParseComplete.encode());
        }
        if synthesize_bind_complete {
            complete_byte_stream.extend_from_slice(&BindComplete.encode());
        }
        complete_byte_stream.extend_from_slice(&cache_bytes_from_describe_kind(
            &replay_content.data,
            &replay_content.describe_kind,
        )?);
    }
    Ok(())
}

fn cache_bytes_from_describe_kind(
    response: &CachedResponse,
    kind: &DescribeKind,
) -> Result<Vec<u8>, String> {
    if kind == &DescribeKind::None {
        return Ok(response.get_data().clone());
    }

    let mut data: Vec<u8> = Vec::new();
    if kind == &DescribeKind::Portal {
        if let Some(row_desc) = response.get_row_desc() {
            data.extend_from_slice(&row_desc);
        } else {
            return Err("RowDescription not present in cached data".to_string());
        }
        data.extend_from_slice(&response.get_data());
    } else if kind == &DescribeKind::Statement {
        if let Some(param_desc) = response.get_param_desc() {
            data.extend_from_slice(&param_desc);
        } else {
            return Err("ParameterDescription not present in cached data".to_string());
        }
        if let Some(row_desc) = response.get_row_desc() {
            data.extend_from_slice(&row_desc);
        } else {
            return Err("RowDescription not present in cached data".to_string());
        }
    }

    Ok(data)
}

fn replays_or_captures_in_command_slots(command_slots: &[CommandSlot]) -> Vec<ReplayOrCapture> {
    let mut replays_or_captures: Vec<ReplayOrCapture> = Vec::new();
    let mut skipped_parse = false;
    let mut skipped_bind = false;
    for command_slot in command_slots {
        match command_slot {
            CommandSlot::Passthrough(passthrough) => {
                if passthrough.kind == MessageKind::Execute {
                    skipped_bind = false;
                    skipped_parse = false;
                    replays_or_captures.push(ReplayOrCapture::NonCacheConfigured)
                } else if passthrough.kind == MessageKind::Query {
                    replays_or_captures.push(ReplayOrCapture::NonCacheConfigured)
                }
            }
            CommandSlot::Replay(replay) => replays_or_captures.push(ReplayOrCapture::Replay {
                replay_content: replay.clone(),
                synthesize_parse_complete: skipped_parse,
                synthesize_bind_complete: skipped_bind,
            }),
            CommandSlot::Capture(capture) => {
                replays_or_captures.push(ReplayOrCapture::Capture(capture.clone()))
            }
            CommandSlot::Skip(skip) => {
                if skip.kind == MessageKind::Parse {
                    skipped_parse = true
                }
                if skip.kind == MessageKind::Bind {
                    skipped_bind = true
                }
            }
        }
    }

    replays_or_captures
}
