#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use super::Block;
use crate::key::{KeySlice, KeyVec};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let idx = block.offsets.len();
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx,
            first_key: KeyVec::new(),
        }
    }

    pub fn dummy() -> Self {
        Self {
            block: Arc::new(Block {
                data: Vec::new(),
                offsets: Vec::new(),
            }),
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let (k, lo, hi) = block.entry_i(0);
        let first_key = KeyVec::from_vec_with_ts(k.key_ref().to_vec(), k.ts());
        Self {
            block,
            key: first_key.clone(),
            value_range: (lo, hi),
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut blk_it = Self::new(block);
        blk_it.seek_to_key(key);
        blk_it
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.idx < self.block.offsets.len()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_key(self.first_key.clone().as_key_slice());
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.reindex(self.idx + 1)
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.reindex(self.block.index(key))
    }

    fn reindex(&mut self, idx: usize) {
        self.idx = idx;
        if self.idx < self.block.offsets.len() {
            let (k, lo, hi) = self.block.entry_i(self.idx);
            self.key = KeyVec::from_vec_with_ts(k.key_ref().to_vec(), k.ts());
            self.value_range = (lo, hi);
        }
    }
}
