use std::collections::{HashMap, VecDeque};
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use log::info;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeySlice, TS_MAX};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound_test, MemTable};
use crate::mvcc::txn::Transaction;
use crate::mvcc::{CommittedTxnData, LsmMvccInner};
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: VecDeque<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl<T: AsRef<[u8]>> WriteBatchRecord<T> {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put(k, _) => k.as_ref(),
            Self::Del(k) => k.as_ref(),
        }
    }
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)), // ignore options.enable_wal now
            imm_memtables: VecDeque::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }

    pub fn index_of_table(&self, lv: usize, id: usize) -> Option<usize> {
        self.levels[lv - 1]
            .1
            .iter()
            .enumerate()
            .find(|(_, &e)| e == id)
            .map(|(i, _)| i)
    }

    pub fn get_table(&self, id: usize) -> Arc<SsTable> {
        self.sstables.get(&id).unwrap().clone()
    }

    pub fn get_tables(&self, ids: &[usize]) -> Vec<Arc<SsTable>> {
        ids.iter().map(|id| self.get_table(*id)).collect()
    }

    pub fn tables_overlap(
        &self,
        lv: usize,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Vec<usize> {
        self.levels[lv - 1]
            .1
            .iter()
            .filter_map(|tid| {
                let sst = self.sstables.get(tid).unwrap();
                if sst.overlap(lower, upper) {
                    Some(*tid)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn dump(&self, head: &str) {
        println!("{head}");
        let imm = self
            .imm_memtables
            .iter()
            .map(|t| t.id())
            .collect::<Vec<usize>>();
        println!("mem: {} -> {:?}", self.memtable.id(), imm);
        println!("L0: {:?}", self.l0_sstables);
        self.levels.iter().for_each(|(lv, tables)| {
            println!("L{lv}: {:?}", tables);
        });
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: RwLock<LsmStorageState>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Manifest,
    pub(crate) mvcc: LsmMvccInner,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.maybe_flush()?;
        self.compaction_notifier.send(())?;
        self.flush_notifier.send(())?;
        if let Some(h) = self.flush_thread.lock().take() {
            h.join().unwrap();
        }
        if let Some(h) = self.compaction_thread.lock().take() {
            h.join().unwrap();
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.txn_write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        self.inner.force_flush()
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

macro_rules! maybe_found {
    ($optv: expr) => {
        if let Some(v) = $optv {
            if v.is_empty() {
                return Ok(None);
            }
            return Ok(Some(v));
        }
    };
}

pub(crate) use maybe_found;

impl Drop for LsmStorageInner {
    fn drop(&mut self) {
        self.maybe_flush().expect("LsmStorageInner flush failed");
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let compact_ctl = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let path = path.as_ref();
        let mut next_sst_id = 0;
        let mut max_ts = 0;
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1024));
        let manifest;
        let manif_path = {
            let mut p = path.to_path_buf();
            p.push("MANIFEST");
            p
        };
        if manif_path.exists() {
            info!("recover from path {:?}", path);
            let (m, records) = Manifest::recover(manif_path)?;
            manifest = m;
            let mut mem_id = 0;
            let mut imm_ids: VecDeque<usize> = VecDeque::with_capacity(options.num_memtable_limit);
            for rec in records {
                match rec {
                    ManifestRecord::Flush(id) => {
                        let poped = imm_ids.pop_back().unwrap();
                        assert!(poped == id);
                        if compact_ctl.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            state.levels.insert(0, (id, vec![id]));
                        }
                    }
                    ManifestRecord::NewMemtable(id) => {
                        imm_ids.push_front(mem_id);
                        mem_id = id;
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (s, _) = compact_ctl.apply_compaction_result(&state, &task, &output);
                        state = s;
                    }
                }
            }
            let l0_ids = state.l0_sstables.iter();
            let lv_ids = state.levels.iter().flat_map(|(_, ids)| ids);
            for &id in l0_ids.chain(lv_ids) {
                let p = Self::path_of_sst_static(path, id);
                let f = FileObject::open(&p)?;
                let sst = SsTable::open(id, Some(block_cache.clone()), f)?;
                max_ts = max_ts.max(sst.max_ts());
                state.sstables.insert(id, Arc::new(sst));
                next_sst_id = next_sst_id.max(id);
            }
            next_sst_id = next_sst_id.max(mem_id);
            let memtable = if options.enable_wal {
                let imm_memtables: Result<Vec<MemTable>> = imm_ids
                    .into_iter()
                    .map(|id| MemTable::recover_from_wal(id, Self::path_of_wal_static(path, id)))
                    .collect();
                state.imm_memtables = imm_memtables?.into_iter().map(Arc::new).collect();
                MemTable::recover_from_wal(mem_id, Self::path_of_wal_static(path, mem_id))?
            } else {
                MemTable::create(mem_id)
            };
            state.memtable = Arc::new(memtable);
            // recover latest commited timestamp
            if let Some(ts) = state.memtable.max_ts() {
                max_ts = max_ts.max(ts);
            }
            if let Some(ts) = state
                .imm_memtables
                .iter()
                .map(|imm| imm.max_ts().unwrap())
                .max()
            {
                max_ts = max_ts.max(ts);
            }
        } else {
            info!("new database path {:?}", path);
            std::fs::create_dir_all(path)?;
            manifest = Manifest::create(manif_path)?;
            let memtable = if options.enable_wal {
                MemTable::create_with_wal(next_sst_id, Self::path_of_wal_static(path, next_sst_id))?
            } else {
                MemTable::create(next_sst_id)
            };
            state.memtable = Arc::new(memtable);
        }
        next_sst_id += 1;

        let storage = Self {
            state: RwLock::new(state),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller: compact_ctl,
            manifest,
            options: options.into(),
            mvcc: LsmMvccInner::new(max_ts),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        if self.options.enable_wal {
            self.state.read().memtable.sync_wal()
        } else {
            Err(anyhow::anyhow!("can't sync when wal is not enabled"))
        }
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_with_ts(key, TS_MAX)
    }

    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let key = KeySlice::from_slice(key, ts);
        let state = self.state.read();
        maybe_found!(state.memtable.get(key));
        for imt in &state.imm_memtables {
            maybe_found!(imt.get(key));
        }
        for tid in &state.l0_sstables {
            let sst = state.sstables.get(tid).unwrap();
            maybe_found!(sst.get(key)?);
        }
        for (_, level) in &state.levels {
            if let Ok(i) = level.binary_search_by(|tid| {
                let sst = state.sstables.get(tid).unwrap();
                sst.compare(key.key_ref())
            }) {
                let sst = state.sstables.get(&level[i]).unwrap();
                maybe_found!(sst.get(key)?);
            }
        }
        Ok(None)
    }

    pub fn txn_write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        let _lock = self.mvcc.commit_lock.lock();
        if self.options.serializable {
            let commit_ts = self.mvcc().latest_commit_ts() + 1;
            let key_hashes = batch
                .iter()
                .map(|rec| farmhash::hash32(rec.key()))
                .collect();
            let txn_data = CommittedTxnData {
                key_hashes,
                read_ts: 0,
                commit_ts,
            };
            self.mvcc()
                .committed_txns
                .lock()
                .insert(commit_ts, txn_data);
        }
        self.write_batch(batch)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let guard = self.mvcc.write_lock.lock();
        let ts = self.mvcc.latest_commit_ts() + 1;
        let mut sz = 0;
        {
            let state = self.state.read();
            for rec in batch {
                sz = match rec {
                    WriteBatchRecord::Put(key, val) => {
                        let key = KeySlice::from_slice(key.as_ref(), ts);
                        state.memtable.put(key, val.as_ref())?
                    }
                    WriteBatchRecord::Del(key) => {
                        let key = KeySlice::from_slice(key.as_ref(), ts);
                        state.memtable.put(key, b"")?
                    }
                };
            }
        }
        self.mvcc.update_commit_ts(ts);
        drop(guard);
        if sz > self.options.target_sst_size {
            let guard = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() > self.options.target_sst_size {
                return self.force_freeze_memtable(&guard);
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        let txn = self.new_txn()?;
        txn.put(key, value);
        txn.commit()
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        let txn = self.new_txn()?;
        txn.delete(key);
        txn.commit()
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    #[allow(dead_code)]
    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn force_flush(&self) -> Result<()> {
        if !self.state.read().memtable.is_empty() {
            self.force_freeze_memtable(&self.state_lock.lock())?;
        }
        while !self.state.read().imm_memtables.is_empty() {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn maybe_flush(&self) -> Result<()> {
        if self.options.enable_wal {
            Ok(())
        } else {
            self.force_flush()
        }
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            // self.state.read().memtable.sync_wal()?;
            MemTable::create_with_wal(id, self.path_of_wal(id))?
        } else {
            MemTable::create(id)
        };
        self.manifest
            .add_record(_state_lock_observer, ManifestRecord::NewMemtable(id))?;
        {
            let mut state = self.state.write();
            let imm = state.memtable.clone();
            // imm.freeze()?;
            imm.sync_wal()?;
            state.imm_memtables.push_front(imm);
            state.memtable = Arc::new(memtable);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let imm_table = match self.state.read().imm_memtables.back().cloned() {
            Some(t) => t,
            None => return Ok(()),
        };
        let imm_id = imm_table.id();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        imm_table.flush(&mut builder)?;
        let sst = builder.build(
            imm_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(imm_id),
        )?;
        if self.options.enable_wal {
            fs::remove_file(self.path_of_wal(imm_id))?;
        }
        self.manifest
            .add_record(&_state_lock, ManifestRecord::Flush(imm_id))?;
        {
            let mut state = self.state.write();
            state.sstables.insert(imm_id, Arc::new(sst));
            if self.compaction_controller.flush_to_l0() {
                state.l0_sstables.insert(0, imm_id);
            } else {
                state.levels.insert(0, (imm_id, vec![imm_id]));
            }
            let poped = state.imm_memtables.pop_back().unwrap();
            assert!(poped.id() == imm_id);
        };
        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc.new_txn(self.clone(), self.options.serializable))
    }

    pub fn mvcc(&self) -> &LsmMvccInner {
        &self.mvcc
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let ts = self.mvcc.latest_commit_ts();
        self.scan_with_ts(lower, upper, ts)
    }

    /// Create an iterator over a range of keys.
    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = self.state.read();
        let (lower, upper) = map_bound_test(lower, upper);
        let mut mem_iters = vec![];
        mem_iters.push(Box::new(state.memtable.scan(lower, upper)));
        for imm in &state.imm_memtables {
            mem_iters.push(Box::new(imm.scan(lower, upper)));
        }
        let mem_merge = MergeIterator::create(mem_iters);

        let l0_iters: Result<Vec<Box<SsTableIterator>>> = state
            .l0_sstables
            .iter()
            .filter_map(|tid| {
                let table = state.sstables.get(tid).unwrap().clone();
                if !table.overlap(lower, upper) {
                    return None;
                }
                Some(SsTable::scan(table, lower, upper).map(Box::new))
            })
            .collect();
        let l0_merge = MergeIterator::create(l0_iters?);

        let mut levels_concat = Vec::with_capacity(state.levels.len());
        for lv in 1..=state.levels.len() {
            let tables = state.get_tables(&state.tables_overlap(lv, lower, upper));
            let concat = SstConcatIterator::new(tables, lower, upper)?;
            levels_concat.push(Box::new(concat));
        }
        let levels_merge = MergeIterator::create(levels_concat);

        let disk_merge = TwoMergeIterator::create(l0_merge, levels_merge)?;
        let global_merge = TwoMergeIterator::create(mem_merge, disk_merge)?;
        Ok(FusedIterator::new(LsmIterator::new(global_merge, ts)?))
    }
}
