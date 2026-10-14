pub(super) struct MessageFramer {
    buffer: Vec<u8>,
    cursor: usize,
}

impl MessageFramer {
    pub(super) fn new() -> Self {
        Self {
            buffer: Vec::new(),
            cursor: 0,
        }
    }

    pub(super) fn set_buffer(&mut self, buffer: Vec<u8>) {
        self.buffer = buffer;
        self.cursor = 0;
    }

    pub(super) fn add_buffer(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    fn compact(&mut self) {
        if self.cursor <= self.buffer.len() {
            self.buffer = self.buffer[self.cursor..].to_vec();
            self.cursor = 0;
        }
    }

    pub(super) fn next_message(&mut self) -> Result<Option<Vec<u8>>, String> {
        if self.cursor >= 9_000 || self.cursor == self.buffer.len() {
            self.compact();
        }
        if self.buffer.len() < self.cursor + 5 {
            return Ok(None); // The message is probably seperated by two streams
        }
        let length_bytes: [u8; 4] = self.buffer[self.cursor + 1..self.cursor + 5]
            .try_into()
            .expect("bounds checked above");

        let length: usize = match i32::from_be_bytes(length_bytes).try_into() {
            Ok(length) => length,
            Err(_) => return Err("couldn't parse i32 to usize".to_string()),
        };
        if length < 4 {
            return Err("length below protocol minimum, stream desynced".to_string());
        }
        if self.buffer.len() < self.cursor + length + 1 {
            return Ok(None); // The message is probably seperated by two streams
        }
        let message = self.buffer[self.cursor..(self.cursor + length + 1)].to_vec();
        self.cursor = self.cursor + length + 1;
        Ok(Some(message))
    }
}
