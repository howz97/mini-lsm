use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT, TS_MAX, TS_MIN};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

pub(crate) fn map_bound_test<'a>(
    lower: Bound<&'a [u8]>,
    upper: Bound<&'a [u8]>,
) -> (Bound<KeySlice<'a>>, Bound<KeySlice<'a>>) {
    let lower = match lower {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, TS_MAX)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, TS_MIN)),
        Bound::Unbounded => Bound::Unbounded,
    };
    let upper = match upper {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, TS_MIN)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, TS_MAX)),
        Bound::Unbounded => Bound::Unbounded,
    };
    (lower, upper)
}

pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(x.to_key_bytes()),
        Bound::Excluded(x) => Bound::Excluded(x.to_key_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn map_bound_v(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(MemTable {
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path)?),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let skm = SkipMap::new();
        let wal = Wal::recover(path, &skm)?;
        Ok(MemTable {
            map: Arc::new(skm),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<usize> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        let (lower, upper) = map_bound_test(lower, upper);
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key = key.to_key_bytes();
        self.map
            .lower_bound(Bound::Included(&key))
            .filter(|ent| ent.key().key_ref() == key.key_ref())
            .map(|ent| ent.value().to_owned())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<usize> {
        let sz = self
            .approximate_size
            .fetch_add(key.raw_len() + value.len(), Ordering::Relaxed);
        self.map
            .insert(key.to_key_bytes(), Bytes::copy_from_slice(value));
        if let Some(ref wal) = self.wal {
            wal.put(key, value)?;
        }
        Ok(sz)
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    pub fn freeze(&mut self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        self.wal = None;
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let lower = map_bound(lower);
        let upper = map_bound(upper);
        let mut it = MemTableIterator::new(
            self.map.clone(),
            |m| m.range((lower, upper)),
            (KeyBytes::new(), Bytes::new()),
        );
        it.next().unwrap();
        it
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        self.map.iter().for_each(|e| {
            builder.add(e.key().as_key_slice(), e.value());
        });
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn max_ts(&self) -> Option<u64> {
        self.map.iter().map(|ent| ent.key().ts()).max()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let (mut k, mut v) = (KeyBytes::new(), Bytes::new());
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
