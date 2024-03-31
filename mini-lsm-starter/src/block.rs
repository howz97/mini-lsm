mod builder;
mod iterator;

use crate::key::{KeyBytes, KeySlice};
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

    pub fn index(&self, key: KeySlice) -> usize {
        let key = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key.key_ref()), key.ts());
        match self
            .offsets
            .binary_search_by_key(&key, |&off| self.key_at(off))
        {
            Ok(i) => i,
            Err(i) => i,
        }
    }

    pub fn key_at(&self, offset: u16) -> KeyBytes {
        self.key_at2(offset as usize).0
    }

    fn key_at2(&self, offset: usize) -> (KeyBytes, usize) {
        let offs_k = offset + 4;
        let mut lens = Bytes::copy_from_slice(&self.data[offset..offs_k]);
        let m_len = lens.get_u16() as usize;
        let k_len = lens.get_u16() as usize;
        let offs_ver = offs_k + k_len;
        let mut key = BytesMut::with_capacity(m_len + k_len);
        if m_len > 0 {
            key.put_slice(&self.data[4..4 + m_len]);
        }
        if k_len > 0 {
            key.put_slice(&self.data[offs_k..offs_ver]);
        }
        let mut ts = [0u8; 8];
        ts.copy_from_slice(&self.data[offs_ver..offs_ver + 8]);
        let ts = u64::from_be_bytes(ts);
        let key = KeyBytes::from_bytes_with_ts(key.freeze(), ts);
        (key, offs_ver + 8)
    }

    pub fn first_key(&self) -> KeyBytes {
        self.key_at(0)
    }

    pub fn last_key(&self) -> KeyBytes {
        self.key_at(*self.offsets.last().unwrap())
    }

    pub fn entry_i(&self, idx: usize) -> (KeyBytes, usize, usize) {
        let offset = self.offsets[idx] as usize;
        let (key, offset) = self.key_at2(offset);
        let v_len = Bytes::copy_from_slice(&self.data[offset..offset + 2]).get_u16() as usize;
        (key, offset + 2, offset + 2 + v_len)
    }
}
