#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

const MAX_NUM_BLOCKS: usize = 1024;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::with_capacity(block_size * MAX_NUM_BLOCKS),
            meta: Vec::with_capacity(MAX_NUM_BLOCKS),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.builder.add(key, value) {
            return;
        }
        self.split();
        let ok = self.builder.add(key, value);
        assert!(ok);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.split();
        }
        let offs = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        let mut buf = BytesMut::new();
        buf.put_u32(offs as u32);
        self.data.extend(buf);

        let file = FileObject::create(path.as_ref(), self.data)?;
        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: offs,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    fn split(&mut self) {
        let block =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size)).build();
        let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(block.first_key()));
        let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(block.last_key()));
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key,
            last_key,
        });
        self.data.extend(block.encode());
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
