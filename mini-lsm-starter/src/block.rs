mod builder;
mod iterator;

use crate::key::KeySlice;
pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.data.len() + (self.offsets.len() * 2));
        buf.put(self.data.as_slice());
        self.offsets.iter().for_each(|n| buf.put_u16(*n));
        buf.put_u16(self.offsets.len() as u16);
        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let hi = data.len() - 2;
        let n = Bytes::copy_from_slice(&data[hi..]).get_u16();
        let lo = hi - (n as usize * 2);
        let mut raw_offs = Bytes::copy_from_slice(&data[lo..hi]);
        let mut offsets = vec![];
        for _ in 0..n {
            offsets.push(raw_offs.get_u16());
        }
        let data = data[..lo].to_vec();
        Self { data, offsets }
    }

    pub fn index_ge(&self, key: KeySlice) -> usize {
        match self
            .offsets
            .binary_search_by_key(&key.raw_ref(), |&off| self.key_at(off))
        {
            Ok(i) => i,
            Err(i) => i,
        }
    }

    pub fn key_at(&self, offset: u16) -> &[u8] {
        let offset = offset as usize;
        let off_k = offset + 2;
        let sz_k = Bytes::copy_from_slice(&self.data[offset..off_k]).get_u16() as usize;
        &self.data[off_k..off_k + sz_k]
    }

    pub fn first_key(&self) -> &[u8] {
        self.key_at(0)
    }

    pub fn last_key(&self) -> &[u8] {
        self.key_at(self.offsets[self.offsets.len() - 1])
    }

    pub fn entry_i(&self, idx: usize) -> (&[u8], usize, usize) {
        let mut offset = self.offsets[idx] as usize;
        let key = self.key_at(offset as u16);
        offset = offset + 2 + key.len();
        let sz_v = Bytes::copy_from_slice(&self.data[offset..offset + 2]).get_u16() as usize;
        (key, offset + 2, offset + 2 + sz_v)
    }
}
