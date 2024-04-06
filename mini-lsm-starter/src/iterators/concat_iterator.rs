use super::StorageIterator;
use crate::{
    key::{KeyBytes, KeySlice},
    mem_table::map_bound,
    table::{SsTable, SsTableIterator},
};
use anyhow::Result;
use std::{ops::Bound, sync::Arc};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
    upper: Bound<KeyBytes>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let current = if let Some(sst) = sstables.first() {
            let it = SsTableIterator::create_and_seek_to_first(sst.clone())?;
            Some(it)
        } else {
            None
        };
        Ok(Self {
            current,
            next_sst_idx: 1,
            sstables,
            upper: Bound::Unbounded,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        Self::new(sstables, Bound::Included(key), Bound::Unbounded)
    }

    pub fn new(
        sstables: Vec<Arc<SsTable>>,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<Self> {
        let mut current = None;
        let mut next_sst_idx = sstables.len();
        for (i, sst) in sstables.iter().enumerate() {
            let itr = SsTable::scan(sst.clone(), lower, upper)?;
            if itr.is_valid() {
                current = Some(itr);
                next_sst_idx = i + 1;
                break;
            }
        }
        Ok(Self {
            current,
            next_sst_idx,
            sstables,
            upper: map_bound(upper),
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(it) = &self.current {
            it.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        if !self.current.as_ref().unwrap().is_valid() && self.next_sst_idx < self.sstables.len() {
            let next_sst = self.sstables[self.next_sst_idx].clone();
            self.next_sst_idx += 1;
            let mut it = SsTableIterator::create_and_seek_to_first(next_sst)?;
            it.set_upper(self.upper.clone());
            self.current = Some(it);
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        if let Some(it) = &self.current {
            it.num_active_iterators()
        } else {
            0
        }
    }
}
