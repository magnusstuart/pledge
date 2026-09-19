use std::collections::BTreeMap;

use crate::{
    cache::lfu::CachedResponse,
    wire::{
        messages::DBMessageContent,
        types::{
            CommandSlotCapture, CommandSlotPassthrough, CommandSlotReplay, CommandSlotSkip, Cycle,
            DescribeKind,
        },
    },
};

use super::{
    reader::ByteReader,
    types::{ClientState, CommandSlot, DBState},
    writer::ByteWriter,
};
use tokio::{
    io::AsyncReadExt,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::mpsc::Sender,
};

pub(super) async fn handle_client(
    client_state: &mut ClientState,
    db_write: &OwnedWriteHalf,
    tx: &Sender<Vec<Cycle>>,
) {
    match super::cache_planner::find_command_slot_messages(client_state).await {
        Ok(cycles) => {
            for cycle in &cycles {
                for slot in &cycle.slots {
                    match slot {
                        CommandSlot::Passthrough(CommandSlotPassthrough { bytes })
                        | CommandSlot::Capture(CommandSlotCapture { bytes, .. }) => {
                            super::stream_try_write(db_write, bytes).await;
                        }
                        _ => {}
                    }
                }
                if cycle.synthesize_sync {
                    super::stream_try_write(db_write, &[b'S', 0, 0, 0, 4]).await;
                }
            }

            if let Err(err) = tx.send(cycles).await {
                // TODO! Either add a retry, or just terminate program
                eprintln!("failed to send command slots: {err}");
            }
        }
        Err(err) => {
            eprintln!("{err}")
        }
    }

    let _ = client_state
        .buffer_state
        .consume(&client_state.buffer_state.pending_data_len());
}

pub(super) async fn handle_db(
    cycles: Vec<Cycle>,
    db_state: &mut DBState,
    client_write: &OwnedWriteHalf,
    db_read: &mut OwnedReadHalf,
) -> Result<(), String> {
    println!("got cycles: {:?}", cycles);
    'read_loop: loop {
        let _ = db_state.buffer_state.read_from_stream(db_read).await;

        // super::stream_try_write(client_write, db_state.buffer_state.pending_data()).await;
        db_state
            .framer
            .add_buffer(db_state.buffer_state.pending_data());
        let _ = db_state
            .buffer_state
            .consume(&db_state.buffer_state.pending_data_len());

        while let Ok(Some(msg)) = db_state.framer.next_message() {
            let type_byte = msg[0];
            match type_byte {
                // CommandComplete
                b'C' => {
                    println!("CommandComplete message: {:?}", msg);
                }
                b'Z' => {
                    break 'read_loop;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

pub(super) async fn handle_db_read(
    db_state: &mut DBState,
    client_write: &OwnedWriteHalf,
) -> Result<(), String> {
    super::stream_try_write(client_write, db_state.buffer_state.pending_data()).await;
    if let Err(err) = db_state
        .buffer_state
        .consume(&db_state.buffer_state.pending_data_len())
    {
        eprintln!("Error occurred: {}", err);
        return Err(err.to_string());
    }
    Ok(())
}

pub(super) fn cycle_contains_cache_interaction(cycle: Cycle) -> bool {
    for slot in cycle.slots {
        if let CommandSlot::Passthrough(_) | CommandSlot::Capture(_) = slot {
            return true;
        }
    }
    false
}

// pub(super) async fn handle_db_cache_command(
//     cycles: Vec<Cycle>,
//     should_hit_db: bool,
//     db_state: &mut DBState,
//     client_write: &OwnedWriteHalf,
//     db_read: &mut OwnedReadHalf,
// ) -> Result<(), String> {
//     if slot_commands.len() > 0 {
//         // let mut capture_target: Option<(String, String)> = None;
//         // let mut has_cache_miss: bool = false;
//         for (_, command) in &slot_commands {
//             match command {
//                 CommandSlot::Replay(cmd) => {
//                     println!("replay: {:?}", cmd.data);
//                     let mut buf = Vec::new();
//                     // THE ORDER MATTERS IN THE MATCH BELOW!!
//                     match cmd.describe_kind {
//                         DescribeKind::Statement => {
//                             if let Some(param_desc) = &cmd.data.param_desc
//                                 && let Some(row_desc) = &cmd.data.row_desc
//                             {
//                                 buf.extend_from_slice(param_desc);
//                                 buf.extend_from_slice(row_desc);
//                             } else {
//                                 return Err(format!(
//                                     "Statement cache replay missing param_desc or row_desc"
//                                 ));
//                             }
//                         }
//                         DescribeKind::Portal => {
//                             if let Some(row_desc) = &cmd.data.row_desc {
//                                 buf.extend_from_slice(row_desc);
//                             } else {
//                                 return Err(format!("Portal cache replay missing row_desc"));
//                             }
//                         }
//                         _ => {}
//                     }
//                     buf.extend_from_slice(&cmd.data.data);
//                     buf.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
//                     super::stream_try_write(client_write, &buf).await;
//                 }
//                 CommandSlot::Capture(cmd) => {
//                     println!("capture: {:?}", cmd.key);
//                     let _ = handle_db_capture_command(cmd, db_state, client_write, db_read).await;
//                     break;
//                 }
//                 _ => {}
//             }
//         }
//     }
//     Ok(())
// }

// pub(super) async fn handle_db_capture_command(
//     cache_command: &CommandSlotCapture,
//     db_state: &mut DBState,
//     client_write: &OwnedWriteHalf,
//     db_read: &mut OwnedReadHalf,
// ) -> Result<(), String> {
//     if let Err(err) = db_state.buffer_state.read_from_stream(db_read).await {
//         eprintln!("Error occurred: {}", err);
//         return Err(err.to_string());
//     };

//     super::stream_try_write(client_write, db_state.buffer_state.pending_data()).await;

//     db_state
//         .framer
//         .add_buffer(db_state.buffer_state.pending_data());

//     let mut cached_response = CachedResponse {
//         data: Vec::new(),
//         param_desc: None,
//         row_desc: None,
//     };
//     let mut success = false;
//     loop {
//         match db_state.framer.next_message() {
//             Ok(option) => match option {
//                 Some(bytes) => {
//                     println!("framed type byte:{}", bytes[0]);
//                     match bytes[0] {
//                         // ReadyForQuery
//                         b'Z' => {
//                             break;
//                         }
//                         //ParameterDescription
//                         b't' => cached_response.param_desc = Some(bytes),
//                         //RowDescription
//                         b'T' => cached_response.row_desc = Some(bytes),
//                         //CommandComplete
//                         b'C' => {
//                             cached_response.data.extend_from_slice(&bytes);
//                             success = true;
//                         }
//                         _ => {
//                             cached_response.data.extend_from_slice(&bytes);
//                         }
//                     }
//                 }
//                 None => break,
//             },
//             Err(err) => {
//                 eprintln!("Something went wrong while framing: {}", err);
//                 return Err(format!("Something went wrong while framing: {}", err));
//             }
//         }
//     }
//     if success {
//         super::cache_planner::set_in_cache(
//             &db_state.app_state,
//             cache_command.ttl,
//             &cache_command.key,
//             cached_response,
//         );
//     }

//     if let Err(err) = db_state
//         .buffer_state
//         .consume(&db_state.buffer_state.pending_data().len())
//     {
//         eprintln!("Error occurred: {}", err);
//         return Err(err.to_string());
//     }
//     Ok(())
// }
