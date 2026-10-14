use crate::wire::types::{
    CommandSlotCapture, CommandSlotPassthrough, Cycle, MessageKind, ProtocolMode,
};

use super::types::{ClientState, CommandSlot, DBState};
use tokio::{
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::mpsc::Sender,
};

pub(super) async fn handle_client(
    client_state: &mut ClientState,
    db_write: &OwnedWriteHalf,
    tx: &Sender<Vec<Cycle>>,
) -> Result<(), String> {
    match super::cache_planner::find_command_slots(client_state).await {
        Ok(cycles) => {
            println!("This amount of cycles were found: {}", cycles.len());
            for cycle in &cycles {
                // TODO! Add graceful shutdown, right now it just goes haywire
                for slot in &cycle.slots {
                    if let CommandSlot::Passthrough(passthrough, ..) = slot
                        && passthrough.kind == MessageKind::Terminate
                    {
                        return Err("Connection closed".to_string());
                    }
                    match slot {
                        CommandSlot::Passthrough(CommandSlotPassthrough { bytes, .. })
                        | CommandSlot::Capture(CommandSlotCapture { bytes, .. }) => {
                            super::stream_try_write(db_write, bytes).await;
                        }
                        _ => (),
                    }
                }
                if cycle.needs_db && cycle.protocol_mode == ProtocolMode::Extended {
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
    Ok(())
}

pub(super) async fn handle_db(
    cycles: Vec<Cycle>,
    db_state: &mut DBState,
    client_write: &OwnedWriteHalf,
    db_read: &mut OwnedReadHalf,
) -> Result<(), String> {
    // TODO! Add graceful shutdown, right now it just goes haywire and keeps spamming
    // this function

    for cycle in &cycles {
        super::stream_try_write(
            client_write,
            &super::response_handler::handle_command_slot_messages(db_state, db_read, cycle)
                .await?,
        )
        .await;
    }
    Ok(())
}
