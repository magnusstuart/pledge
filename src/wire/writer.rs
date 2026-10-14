use std::ops::Range;

use crate::wire::BufferState;

use super::ReplayTrim;

pub(super) struct ByteWriter<'a> {
    buffer: &'a mut Vec<u8>,
    cursor: usize,
}

impl<'a> ByteWriter<'a> {
    pub(super) fn new(buffer: &'a mut Vec<u8>, cursor: usize) -> Self {
        Self { buffer, cursor }
    }
    pub(super) fn get_buffer(&self) -> &Vec<u8> {
        &self.buffer
    }

    pub(super) fn slice_out_section(&self, start: usize, end: usize) -> Vec<u8> {
        let len = end - start;
        let mut result = Vec::with_capacity(len);
        for i in 0..len {
            result.push(self.buffer[start + i]);
        }
        result
    }

    pub(super) fn trim_from_pending_commands(&mut self, pending_commands: &'a [ReplayTrim]) {
        let mut removed_indexes = 0;
        for command in pending_commands {
            match command {
                ReplayTrim::Extended(command) => {
                    if let Some(parse) = &command.parse {
                        println!("parse:");
                        removed_indexes = self.drain_section(parse, removed_indexes);
                    }
                    if let Some(bind) = &command.bind {
                        println!("bind:");
                        removed_indexes = self.drain_section(bind, removed_indexes);
                    }
                    if let Some(describe) = &command.describe {
                        println!("describe:");
                        removed_indexes = self.drain_section(describe, removed_indexes);
                    }
                    println!("execute:");
                    removed_indexes = self.drain_section(&command.execute, removed_indexes);

                    if let Some(sync) = &command.sync {
                        println!("sync:");
                        removed_indexes = self.drain_section(sync, removed_indexes);
                    }
                }
                ReplayTrim::Simple(command) => {
                    println!("simple:");
                    removed_indexes = self.drain_section(&command.query, removed_indexes);
                }
            }
        }
    }
    fn drain_section(&mut self, section: &Range<usize>, offset: usize) -> usize {
        if section.start < offset
            || section.end < offset
            || ((section.start - offset) >= self.buffer.len())
            || ((section.end - offset) > self.buffer.len())
        {
            return offset;
        }

        self.buffer
            .drain((section.start - offset)..(section.end - offset));
        return offset + section.end - section.start;
    }
    // pub(super) fn merge_cache_commands(&mut self, commands: &BTreeMap<u16, CacheCommand>) {
    //     let reader_buffer = self.buffer.clone();
    //     let mut reader = ByteReader::new(reader_buffer, 0);
    //     match reader.crawl_and_find_messages_db() {
    //         Ok(messages) => {
    //             for (order, command) in commands {
    //                 match command {
    //                     CacheCommand::Replay {
    //                         data,
    //                         describe_kind,
    //                         protocol_mode,
    //                     } => {}
    //                     CacheCommand::Capture {
    //                         key,
    //                         describe_kind,
    //                         protocol_mode,
    //                     } => {}
    //                 }
    //             }
    //         }
    //         Err(_) => {}
    //     }
    // }
}
