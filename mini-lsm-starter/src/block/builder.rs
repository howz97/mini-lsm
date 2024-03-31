use super::Block;
use crate::key::{KeySlice, KeyVec};
use nom::Slice;
use std::cmp::min;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::with_capacity(block_size / 8),
            data: Vec::with_capacity(block_size * 2),
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
        let m = if self.is_empty() {
            assert!(self.first_key.is_empty());
            self.first_key = key.to_key_vec();
            0
        } else if self.len() + key.raw_len() + value.len() + 4 > self.block_size {
            return false;
        } else {
            let mut m = min(self.first_key.key_len(), key.key_len());
            for i in 0..m {
                if self.first_key.key_ref()[i] != key.key_ref()[i] {
                    m = i;
                    break;
                }
            }
            m as u16
        };
        self.offsets.push(self.data.len() as u16);
        self.data.extend(m.to_be_bytes());
        self.data.extend(((key.key_len() as u16) - m).to_be_bytes());
        self.data.extend(key.key_ref().slice(m as usize..));
        self.data.extend(key.ts().to_be_bytes());
        self.data.extend((value.len() as u16).to_be_bytes());
        self.data.extend(value);
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
