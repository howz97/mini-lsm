use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};
use anyhow::Result;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    MergeIterator<MemTableIterator>,
    TwoMergeIterator<MergeIterator<SsTableIterator>, MergeIterator<SstConcatIterator>>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    snapshot: u64,
}

impl LsmIterator {
    pub(crate) fn new(inner: LsmIteratorInner, ts: u64) -> Result<Self> {
        let mut iter = Self {
            inner,
            snapshot: ts,
        };
        iter.next_key(Vec::new())?;
        Ok(iter)
    }

    pub(crate) fn next_key(&mut self, mut prev: Vec<u8>) -> Result<()> {
        while self.inner.is_valid() {
            let key = self.inner.key();
            if key.key_ref() == prev {
                self.inner.next()?;
                continue;
            }
            if key.ts() > self.snapshot {
                self.inner.next()?;
                continue;
            }
            if self.inner.value().is_empty() {
                prev = Vec::from(key.key_ref());
                self.inner.next()?;
                continue;
            }
            break;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_key(Vec::from(self.inner.key().key_ref()))
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            anyhow::bail!("error already encountered");
        }
        if self.iter.is_valid() {
            if let Err(err) = self.iter.next() {
                self.has_errored = true;
                return Err(err);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
