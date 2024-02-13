use std::io::Write;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len() + (self.offsets.len() * 2) + 2
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.len() + key.len() + value.len() + 4 > self.block_size {
            if !self.is_empty() {
                return false;
            }
        }
        self.offsets.push(self.data.len() as u16);
        self.data
            .write_all((key.len() as u16).to_be_bytes().as_ref())
            .unwrap();
        self.data.write_all(key.raw_ref()).unwrap();
        self.data
            .write_all((value.len() as u16).to_be_bytes().as_ref())
            .unwrap();
        self.data.write_all(value).unwrap();
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
