use std::path::Path;
use std::sync::Arc;

use super::bloom::Bloom;
use super::{BlockMeta, FileObject, SsTable};
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;

const MAX_NUM_BLOCKS: usize = 1024;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    keys_hash: Vec<u32>,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            data: Vec::with_capacity(block_size * MAX_NUM_BLOCKS),
            meta: Vec::with_capacity(MAX_NUM_BLOCKS),
            block_size,
            keys_hash: Vec::with_capacity(64 * MAX_NUM_BLOCKS),
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.keys_hash.push(farmhash::fingerprint32(key.key_ref()));
        self.max_ts = self.max_ts.max(key.ts());
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
        self.data.len() + self.builder.len()
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
        let block_meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        let checksum = crc32fast::hash(&self.data[block_meta_offset..]);
        self.data.extend(checksum.to_be_bytes());
        self.data.extend((block_meta_offset as u32).to_be_bytes());

        let bloom_offs = self.data.len();
        let nbits = Bloom::bloom_bits_per_key(self.keys_hash.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.keys_hash, nbits);
        bloom.encode(&mut self.data);
        let checksum = crc32fast::hash(&self.data[bloom_offs..]);
        self.data.extend(checksum.to_be_bytes());
        self.data.extend((bloom_offs as u32).to_be_bytes());

        self.data.extend(self.max_ts.to_be_bytes());

        let file = FileObject::create(path.as_ref(), self.data)?;
        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();

        let sst = SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: self.max_ts,
        };
        Ok(sst)
    }

    fn split(&mut self) {
        let block =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size)).build();
        let first_key = block.first_key();
        let last_key = block.last_key();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key,
            last_key,
        });
        let encoded = block.encode();
        let checksum = crc32fast::hash(&encoded);
        self.data.extend(encoded);
        self.data.extend(checksum.to_be_bytes());
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
