pub(crate) mod bloom;
mod builder;
mod iterator;

use std::cmp::Ordering;
use std::fs::File;
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::SsTableIterator;
use log::warn;

use crate::block::Block;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;
use crate::mem_table::map_bound;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut bbuf = BytesMut::new();
        for blk in block_meta {
            bbuf.put_u32(blk.offset as u32);
            bbuf.put_u16(blk.first_key.key_len() as u16);
            bbuf.put(blk.first_key.key_ref());
            bbuf.put_u64(blk.first_key.ts());
            bbuf.put_u16(blk.last_key.key_len() as u16);
            bbuf.put(blk.last_key.key_ref());
            bbuf.put_u64(blk.last_key.ts());
        }
        buf.extend(bbuf);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut metas = vec![];
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            // first key
            let n = buf.get_u16() as usize;
            let key = buf.copy_to_bytes(n);
            let ts = buf.get_u64();
            let first_key = KeyBytes::from_bytes_with_ts(key, ts);
            // last key
            let n = buf.get_u16() as usize;
            let key = buf.copy_to_bytes(n);
            let ts = buf.get_u64();
            let last_key = KeyBytes::from_bytes_with_ts(key, ts);
            metas.push(BlockMeta {
                offset,
                first_key,
                last_key,
            })
        }
        metas
    }

    pub fn compare(&self, key: KeySlice) -> Ordering {
        if self.first_key.as_key_slice().cmp(&key) == Ordering::Greater {
            Ordering::Greater
        } else if self.last_key.as_key_slice().cmp(&key) == Ordering::Less {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.0.as_ref().unwrap().read_exact_at(buf, offset)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

pub struct ReverseReader {
    file: File,
    size: u64,
    offset: u64,
}

impl ReverseReader {
    pub fn new(file: FileObject) -> Self {
        Self {
            file: file.0.unwrap(),
            size: file.1,
            offset: file.1,
        }
    }

    pub fn to_file_obj(self) -> FileObject {
        FileObject(Some(self.file), self.size)
    }

    pub fn position(&self) -> u64 {
        self.offset
    }

    pub fn get_u32(&mut self) -> Result<u32> {
        self.offset -= 4;
        let mut buf = [0u8; 4];
        self.file.read_exact_at(&mut buf, self.offset)?;
        Ok(u32::from_be_bytes(buf))
    }

    pub fn get_u64(&mut self) -> Result<u64> {
        self.offset -= 8;
        let mut buf = [0u8; 8];
        self.file.read_exact_at(&mut buf, self.offset)?;
        Ok(u64::from_be_bytes(buf))
    }

    pub fn get_bytes(&mut self, len: u64) -> Result<Vec<u8>> {
        self.offset -= len;
        let mut data = vec![0; len as usize];
        self.file.read_exact_at(&mut data[..], self.offset)?;
        Ok(data)
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let mut reader = ReverseReader::new(file);
        // FIXME: This should be covered by checksum
        let max_ts = reader.get_u64()?;
        let bloom_offs = reader.get_u32()? as u64;
        let checksum = reader.get_u32()?;
        let bloom_content = reader.get_bytes(reader.position() - bloom_offs)?;
        let bloom = if crc32fast::hash(&bloom_content) == checksum {
            Some(Bloom::decode(&bloom_content)?)
        } else {
            // TODO: repair
            warn!("SsTable {id} bloom filter is corrupted");
            None
        };

        let meta_offs = reader.get_u32()? as u64;
        let checksum = reader.get_u32()?;
        let meta = Bytes::from(reader.get_bytes(reader.position() - meta_offs)?);
        if crc32fast::hash(&meta) != checksum {
            return Err(anyhow!("SsTable {id} metadata is corrupted"));
        }
        let block_meta = BlockMeta::decode_block_meta(meta);
        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();
        Ok(Self {
            file: reader.to_file_obj(),
            block_meta,
            block_meta_offset: meta_offs as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom,
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.num_of_blocks() {
            bail!(
                "block_idx {} out of range 0..{}",
                block_idx,
                self.num_of_blocks()
            )
        }
        let offs = self.block_meta[block_idx].offset;
        let sz = if block_idx + 1 < self.num_of_blocks() {
            self.block_meta[block_idx + 1].offset - offs
        } else {
            self.block_meta_offset - offs
        };
        let raw = self
            .file
            .read(offs as u64, sz as u64)
            .map_err(|e| anyhow!("read block failed: {e}"))?;
        let checksum = Bytes::copy_from_slice(&raw[raw.len() - 4..]).get_u32();
        let data = &raw[..raw.len() - 4];
        if crc32fast::hash(data) != checksum {
            return Err(anyhow!(
                "SST {} block {block_idx} is corrupted",
                self.sst_id()
            ));
        }
        Ok(Arc::new(Block::decode(data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = self.block_cache.as_ref() {
            let block = cache
                .try_get_with((self.sst_id(), block_idx), || self.read_block(block_idx))
                .map_err(|e| Arc::into_inner(e).unwrap_or(anyhow!("read block failed")))?;
            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        match self.block_meta.binary_search_by(|bm| bm.compare(key)) {
            Ok(i) => i,
            Err(i) => i,
        }
    }

    pub fn get(&self, key: KeySlice) -> Result<Option<Bytes>> {
        if let Some(bloom) = &self.bloom {
            let h = farmhash::fingerprint32(key.key_ref());
            if !bloom.may_contain(h) {
                return Ok(None);
            }
        }
        let idx = self.find_block_idx(key);
        if idx < self.num_of_blocks() {
            let blk = self.read_block_cached(idx)?;
            let offs = blk.index(key);
            if offs < blk.offsets.len() {
                let (k, lo, hi) = blk.entry_i(offs);
                if key.key_ref() == k.key_ref() {
                    return Ok(Some(Bytes::copy_from_slice(&blk.data[lo..hi])));
                }
            }
        }
        Ok(None)
    }

    pub fn scan(
        table: Arc<Self>,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<SsTableIterator> {
        let mut it = match lower {
            Bound::Included(lo) => SsTableIterator::create_and_seek_to_key(table, lo)?,
            Bound::Excluded(lo) => {
                let mut it = SsTableIterator::create_and_seek_to_key(table, lo)?;
                if it.key().cmp(&lo) == Ordering::Equal {
                    it.next()?;
                }
                it
            }
            Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
        };
        it.set_upper(map_bound(upper));
        Ok(it)
    }

    pub fn scan_all(table: Arc<Self>) -> Result<SsTableIterator> {
        Self::scan(table, Bound::Unbounded, Bound::Unbounded)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn overlap(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> bool {
        match lower {
            Bound::Included(lo) => {
                if self.last_key().as_key_slice().cmp(&lo) == Ordering::Less {
                    return false;
                }
            }
            Bound::Excluded(lo) => {
                if self.last_key().as_key_slice().cmp(&lo) != Ordering::Greater {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }
        match upper {
            Bound::Included(up) => {
                if self.first_key().as_key_slice().cmp(&up) == Ordering::Greater {
                    return false;
                }
            }
            Bound::Excluded(up) => {
                if self.first_key().as_key_slice().cmp(&up) != Ordering::Less {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }
        true
    }

    pub fn compare(&self, key: &[u8]) -> Ordering {
        if self.first_key.key_ref().cmp(key) == Ordering::Greater {
            Ordering::Greater
        } else if self.last_key.key_ref().cmp(key) == Ordering::Less {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}
