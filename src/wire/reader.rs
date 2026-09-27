use super::{
    Decode, WireProtocolStates,
    messages::{Bind, ClientMessageContent, DBMessageContent, Describe, Execute, Parse, Query},
};

pub(super) enum ByteReaderErrorKind {
    EndOfBuffer,
    ParsingString,
    OutOfBounds,
    NegativeLength,
    ProtocolError,
}

pub(super) struct ByteReaderError {
    pub kind: ByteReaderErrorKind,
    pub message: String,
}

pub(super) struct ByteReader {
    buffer: Vec<u8>,
    cursor: usize,
}

impl ByteReader {
    pub(super) fn new(buffer: Vec<u8>, cursor: usize) -> Self {
        Self { buffer, cursor }
    }
    pub(super) fn set_buffer(&mut self, buffer: Vec<u8>) {
        self.buffer = buffer;
    }

    pub(super) fn set_cursor(&mut self, new_cursor: usize) {
        self.cursor = new_cursor;
    }
    pub(super) fn get_cursor_size(&self) -> usize {
        self.cursor
    }

    pub(super) fn peek_total_message_length(
        &self,
        state: WireProtocolStates,
    ) -> Result<usize, ByteReaderError> {
        match state {
            WireProtocolStates::WaitingForSSL | WireProtocolStates::WaitingForStartup => {
                if self.cursor + 4 > self.buffer.len() {
                    return Err(self.get_out_of_bounds_error(4));
                }
                let bytes: [u8; 4] = self.buffer[self.cursor..self.cursor + 4]
                    .try_into()
                    .map_err(|_| ByteReaderError {
                        kind: ByteReaderErrorKind::OutOfBounds,
                        message: "length value exceeds buffer bounds, expected 4 bytes. this shouldn't happen"
                            .to_string(),
                    })?;
                Ok(i32::from_be_bytes(bytes) as usize)
            }
            WireProtocolStates::ReadyForQuery => {
                if self.cursor + 5 > self.buffer.len() {
                    return Err(self.get_out_of_bounds_error(5));
                }
                let bytes: [u8; 4] = self.buffer[self.cursor + 1..self.cursor + 5]
                    .try_into()
                    .map_err(|_| ByteReaderError {
                        kind: ByteReaderErrorKind::OutOfBounds,
                        message: "length value exceeds buffer bounds, expected 5 bytes. this shouldn't happen"
                            .to_string(),
                    })?;
                Ok(1 + i32::from_be_bytes(bytes) as usize)
            }
        }
    }

    pub(super) fn crawl_and_find_messages_client(
        &mut self,
    ) -> Result<Vec<ClientMessageContent>, ByteReaderError> {
        let mut messages: Vec<ClientMessageContent> = Vec::new();
        loop {
            if self.cursor >= self.buffer.len() {
                break;
            }
            let type_byte = self.buffer[self.cursor];
            self.cursor += 1;
            let slice_length = self.read_i32_as_usize()? - 4;
            match type_byte {
                b'Q' => {
                    let decoded = Query {
                        bytes: self.buffer[self.cursor..(self.cursor + slice_length)].to_vec(),
                    }
                    .decode()?;
                    messages.push(ClientMessageContent::QueryMessage {
                        data: decoded,
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    })
                }
                b'P' => {
                    let decoded = Parse {
                        bytes: self.buffer[self.cursor..(self.cursor + slice_length)].to_vec(),
                    }
                    .decode()?;
                    messages.push(ClientMessageContent::ParseMessage {
                        data: decoded,
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    })
                }
                b'B' => {
                    let decoded = Bind {
                        bytes: self.buffer[self.cursor..(self.cursor + slice_length)].to_vec(),
                    }
                    .decode()?;
                    messages.push(ClientMessageContent::BindMessage {
                        data: decoded,
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    })
                }
                b'D' => {
                    let decoded = Describe {
                        bytes: self.buffer[self.cursor..(self.cursor + slice_length)].to_vec(),
                    }
                    .decode()?;
                    messages.push(ClientMessageContent::DescribeMessage {
                        data: decoded,
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    })
                }
                b'E' => {
                    let decoded = Execute {
                        bytes: self.buffer[self.cursor..(self.cursor + slice_length)].to_vec(),
                    }
                    .decode()?;
                    messages.push(ClientMessageContent::ExecuteMessage {
                        data: decoded,
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    })
                }
                b'S' => {
                    messages.push(ClientMessageContent::SyncMessage {
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    });
                }
                b'C' => messages.push(ClientMessageContent::CloseMessage), // Close
                b'X' => messages.push(ClientMessageContent::TerminateMessage), // Terminate
                _ => messages.push(ClientMessageContent::UnknownMessage),
            }
            self.cursor += slice_length;
        }

        Ok(messages)
    }

    pub(super) fn crawl_and_find_messages_db(
        &mut self,
    ) -> Result<Vec<DBMessageContent>, ByteReaderError> {
        let mut messages: Vec<DBMessageContent> = Vec::new();
        loop {
            if self.cursor >= self.buffer.len() {
                break;
            }
            let type_byte = self.buffer[self.cursor];
            self.cursor += 1;
            let slice_length = self.read_i32_as_usize()? - 4;
            match type_byte {
                b'1' => {
                    messages.push(DBMessageContent::ParseComplete {
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    });
                }
                b'2' => {
                    messages.push(DBMessageContent::BindComplete {
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    });
                }
                b't' => {
                    let bytes = self.buffer[self.cursor - 5..(self.cursor + slice_length)].to_vec();
                    messages.push(DBMessageContent::ParameterDescription {
                        data: bytes,
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    });
                }
                b'T' => {
                    let bytes = self.buffer[self.cursor - 5..(self.cursor + slice_length)].to_vec();
                    messages.push(DBMessageContent::RowDescription {
                        data: bytes,
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    });
                }
                b'D' => {
                    let bytes = self.buffer[self.cursor - 5..(self.cursor + slice_length)].to_vec();
                    messages.push(DBMessageContent::DataRow {
                        data: bytes,
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    });
                }
                b'C' => messages.push(DBMessageContent::CommandComplete {
                    start: self.cursor - 5,
                    end: self.cursor + slice_length,
                }),
                b'Z' => {
                    messages.push(DBMessageContent::ReadyForQuery {
                        data: self.buffer[self.cursor - 5..(self.cursor + slice_length)].to_vec(),
                        start: self.cursor - 5,
                        end: self.cursor + slice_length,
                    });
                }
                b'R' => messages.push(DBMessageContent::AuthenticationOk {
                    start: self.cursor - 5,
                    end: self.cursor + slice_length,
                }),
                _ => messages.push(DBMessageContent::UnknownMessage),
            }
            self.cursor += slice_length;
        }
        Ok(messages)
    }

    pub(super) fn read_cstring(&mut self) -> Result<String, ByteReaderError> {
        let start_cursor = self.cursor;
        loop {
            if self.cursor >= self.buffer.len() {
                return Err(self.get_end_of_buffer_error());
            }
            // 0u8 is the normal cstring null terminator (basically just a byte with the value 0)
            else if self.buffer[self.cursor] == 0u8 {
                self.cursor += 1;
                break;
            }
            self.cursor += 1;
        }

        match String::from_utf8(self.buffer[start_cursor..self.cursor - 1].to_vec()) {
            Ok(string) => Ok(string),
            Err(err) => Err(ByteReaderError {
                kind: ByteReaderErrorKind::ParsingString,
                message: format!("{}", err),
            }),
        }
    }
    pub(super) fn read_u8(&mut self) -> Result<u8, ByteReaderError> {
        if self.cursor >= self.buffer.len() {
            return Err(self.get_end_of_buffer_error());
        }
        let value = self.buffer[self.cursor];
        self.cursor += 1;
        Ok(value)
    }

    pub(super) fn read_i16(&mut self) -> Result<i16, ByteReaderError> {
        if (self.cursor) >= self.buffer.len() {
            return Err(self.get_end_of_buffer_error());
        }
        if (self.cursor + 2) > self.buffer.len() {
            return Err(self.get_out_of_bounds_error(2));
        }
        let value: [u8; 2] = self.buffer[self.cursor..(self.cursor + 2)]
            .try_into()
            .expect("bounds exceeded, this should not happen");
        let value: i16 = i16::from_be_bytes(value);
        self.cursor += 2;
        Ok(value)
    }

    pub(super) fn read_i32(&mut self) -> Result<i32, ByteReaderError> {
        if (self.cursor) >= self.buffer.len() {
            return Err(self.get_end_of_buffer_error());
        }
        if (self.cursor + 4) > self.buffer.len() {
            return Err(self.get_out_of_bounds_error(4));
        }
        let value: [u8; 4] = self.buffer[self.cursor..(self.cursor + 4)]
            .try_into()
            .expect("bounds exceeded, this should not happen");

        let value: i32 = i32::from_be_bytes(value);
        self.cursor += 4;
        Ok(value)
    }

    pub(super) fn read_bytes(&mut self, bytes_len: usize) -> Result<Vec<u8>, ByteReaderError> {
        if (self.cursor) >= self.buffer.len() {
            return Err(self.get_end_of_buffer_error());
        }
        if (self.cursor + bytes_len) > self.buffer.len() {
            return Err(self.get_out_of_bounds_error(bytes_len));
        }

        let value = self.buffer[self.cursor..(self.cursor + bytes_len)].to_vec();
        self.cursor += bytes_len;
        Ok(value)
    }

    fn get_end_of_buffer_error(&self) -> ByteReaderError {
        ByteReaderError {
            kind: ByteReaderErrorKind::EndOfBuffer,
            message: "cursor is at end of buffer, no more data to read".to_string(),
        }
    }

    fn get_out_of_bounds_error(&self, extra_bytes: usize) -> ByteReaderError {
        ByteReaderError {
            kind: ByteReaderErrorKind::OutOfBounds,
            message: format!(
                "taking the required {} bytes exceeds buffer length",
                extra_bytes
            ),
        }
    }

    pub(super) fn read_i16_as_usize(&mut self) -> Result<usize, ByteReaderError> {
        self.read_i16()?.try_into().map_err(|_| ByteReaderError {
            kind: ByteReaderErrorKind::NegativeLength,
            message: "length value was negative (i16 → usize conversion failed)".to_string(),
        })
    }

    pub(super) fn read_i32_as_usize(&mut self) -> Result<usize, ByteReaderError> {
        self.read_i32()?.try_into().map_err(|_| ByteReaderError {
            kind: ByteReaderErrorKind::NegativeLength,
            message: "length value was negative (i32 → usize conversion failed)".to_string(),
        })
    }
}
