use std::{
    collections::HashSet,
    ops::{Add, Bound},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{maybe_found, LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound_v,
};

use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn ssi_set(&self, key: &[u8], is_write: bool) {
        if let Some(hset) = &self.key_hashes {
            let h = farmhash::hash32(key);
            if is_write {
                hset.lock().0.insert(h);
            } else {
                hset.lock().1.insert(h);
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.ssi_set(key, false);
        maybe_found!(self.local_storage.get(key).map(|ent| ent.value().clone()));
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let mut txn = TxnLocalIterator::new(
            self.local_storage.clone(),
            |m| m.range((map_bound_v(lower), map_bound_v(upper))),
            (Bytes::new(), Bytes::new()),
        );
        txn.next()?;
        TxnIterator::create(self.clone(), TwoMergeIterator::create(txn, iter)?)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.ssi_set(key, true);
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.put(key, &[]);
    }

    pub fn commit(&self) -> Result<()> {
        if self.committed.swap(true, Ordering::Relaxed) {
            bail!("transaction has already been commited");
        }
        let mvcc = self.inner.mvcc();
        let _lock = mvcc.commit_lock.lock();
        if let Some(rwset) = &self.key_hashes {
            let rwset = rwset.lock();
            if rwset.0.is_empty() {
                return Ok(());
            }
            let commit_ts = mvcc.latest_commit_ts() + 1;
            let mut committed = mvcc.committed_txns.lock();
            for (_, txn) in committed.range(self.read_ts.add(1)..commit_ts) {
                if rwset.1.intersection(&txn.key_hashes).count() > 0 {
                    bail!("conflict with txn {}-{}", txn.read_ts, txn.commit_ts);
                }
            }
            committed.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: rwset.0.clone(),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );

            let wm = mvcc.watermark();
            while *committed.first_entry().unwrap().key() <= wm {
                committed.pop_first();
            }
        }

        let mut batch = Vec::with_capacity(self.local_storage.len());
        self.local_storage.iter().for_each(|ent| {
            if ent.value().is_empty() {
                batch.push(WriteBatchRecord::Del(ent.key().clone()));
            } else {
                batch.push(WriteBatchRecord::Put(
                    ent.key().clone(),
                    ent.value().clone(),
                ));
            }
        });
        self.inner.write_batch(&batch)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc.ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let (mut k, mut v) = (Bytes::new(), Bytes::new());
        self.with_iter_mut(|it| {
            if let Some(ent) = it.fuse().next() {
                k = ent.key().to_owned();
                v = ent.value().to_owned();
            }
        });
        self.with_item_mut(|item| {
            item.0 = k;
            item.1 = v;
        });
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter };
        if iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }
        if iter.is_valid() {
            iter.txn.ssi_set(iter.key(), false);
        }
        Ok(iter)
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        loop {
            self.iter.next()?;
            if !self.is_valid() {
                break;
            }
            if !self.value().is_empty() {
                self.txn.ssi_set(self.key(), false);
                break;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
