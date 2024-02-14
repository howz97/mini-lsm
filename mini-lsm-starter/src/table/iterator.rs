#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_iter = BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?);
        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        let block = self.table.read_block_cached(self.blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut iter = Self {
            table,
            blk_iter: BlockIterator::dummy(),
            blk_idx: 0,
        };
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        self.blk_idx = self.table.find_block_idx(key);
        if self.blk_idx < self.table.num_of_blocks() {
            let block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() && self.blk_idx + 1 < self.table.num_of_blocks() {
            self.blk_idx += 1;
            let block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(())
    }
}
