use super::{
    message_framer::MessageFramer,
    types::{ClientState, DBState, WireProtocolStates},
};
use tokio::{
    io::AsyncReadExt,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

pub(super) async fn startup_state_handling(
    client_state: &mut ClientState,
    db_state: &mut DBState,
    client_read: &mut OwnedReadHalf,
    client_write: &OwnedWriteHalf,
    db_read: &mut OwnedReadHalf,
    db_write: &OwnedWriteHalf,
) {
    let mut state = WireProtocolStates::WaitingForSSL;
    let mut startup_framer = MessageFramer::new();

    'start_up_loop: loop {
        match state {
            WireProtocolStates::WaitingForSSL => {
                let client_buffer_data_length = match client_read
                    .read(&mut client_state.buffer_state.buffer[0..])
                    .await
                {
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!("Error occurred: {}", err);
                        return;
                    }
                };
                super::stream_try_write(
                    &db_write,
                    &client_state.buffer_state.buffer[..client_buffer_data_length],
                )
                .await;

                let db_buffer_data_length =
                    match db_read.read(&mut db_state.buffer_state.buffer[0..]).await {
                        Ok(n) => n,
                        Err(err) => {
                            eprintln!("Error occurred: {}", err);
                            return;
                        }
                    };
                super::stream_try_write(
                    &client_write,
                    &db_state.buffer_state.buffer[..db_buffer_data_length],
                )
                .await;
                if db_state.buffer_state.buffer[0] == b'N' {
                    state = WireProtocolStates::WaitingForStartup;
                    println!("Transitioning to WaitingForStartup")
                } else if db_state.buffer_state.buffer[0] == b'S' {
                    eprintln!(
                        "Pledge can't inspect the bytes if SSL is on, this feature will be enabled in the future however"
                    );
                    return;
                }
            }
            WireProtocolStates::WaitingForStartup => {
                let client_buffer_data_length = match client_read
                    .read(&mut client_state.buffer_state.buffer[0..])
                    .await
                {
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!("Error occurred: {}", err);
                        return;
                    }
                };
                super::stream_try_write(
                    &db_write,
                    &client_state.buffer_state.buffer[..client_buffer_data_length],
                )
                .await;

                let db_buffer_data_length =
                    match db_read.read(&mut db_state.buffer_state.buffer[0..]).await {
                        Ok(n) => n,
                        Err(err) => {
                            eprintln!("Error occurred: {}", err);
                            return;
                        }
                    };
                super::stream_try_write(
                    &client_write,
                    &db_state.buffer_state.buffer[..db_buffer_data_length],
                )
                .await;

                startup_framer.add_buffer(&db_state.buffer_state.buffer[..db_buffer_data_length]);
                while let Ok(Some(msg)) = startup_framer.next_message() {
                    if msg[0] == b'Z' {
                        // This indicates the startup is complete as the 'Z' means ReadyForQuery
                        state = WireProtocolStates::ReadyForQuery;
                        println!("Transitioning to ReadyForQuery");
                        break 'start_up_loop;
                    }
                }
            }
            WireProtocolStates::ReadyForQuery => break 'start_up_loop,
        }
    }
}
