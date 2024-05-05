mod leveled;
mod simple_leveled;
mod tiered;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{CompactionFilter, LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use log::debug;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
use std::sync::Arc;
use std::time::Duration;
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        debug!("compaction task {:?} output {:?}", task, output);
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let bottom = task.compact_to_bottom_level();
        match task {
            CompactionTask::Leveled(task) => {
                if task.upper_level_sst_ids.len() == 1 {
                    let up_id = task.upper_level_sst_ids.first().unwrap();
                    let up_table = self.state.read().get_table(*up_id);
                    let upper_iter = SsTableIterator::create_and_seek_to_first(up_table)?;
                    let lower_ids = self.state.read().get_tables(&task.lower_level_sst_ids);
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ids)?;
                    self.compact_core(TwoMergeIterator::create(upper_iter, lower_iter)?, bottom)
                } else {
                    assert!(task.upper_level.is_none());
                    assert!(task.upper_level_sst_ids.len() > 1);
                    self.compact_l0(&task.upper_level_sst_ids, &task.lower_level_sst_ids, bottom)
                }
            }
            CompactionTask::Tiered(task) => {
                let mut iters = vec![];
                for (_, tier) in &task.tiers {
                    let tier_tables = self.state.read().get_tables(tier);
                    let it = SstConcatIterator::create_and_seek_to_first(tier_tables)?;
                    iters.push(Box::new(it));
                }
                let iter = MergeIterator::create(iters);
                self.compact_core(iter, bottom)
            }
            CompactionTask::Simple(task) => {
                if task.upper_level.is_none() {
                    self.compact_l0(&task.upper_level_sst_ids, &task.lower_level_sst_ids, bottom)
                } else {
                    let upper_tables = self.state.read().get_tables(&task.upper_level_sst_ids);
                    let lower_tables = self.state.read().get_tables(&task.lower_level_sst_ids);
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_tables)?;
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_tables)?;
                    self.compact_core(TwoMergeIterator::create(upper_iter, lower_iter)?, bottom)
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.compact_l0(l0_sstables, l1_sstables, bottom),
        }
    }

    fn compact_l0(
        &self,
        l0: &[usize],
        lower: &[usize],
        to_bottom: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let iters: Result<Vec<_>> = self
            .state
            .read()
            .get_tables(l0)
            .into_iter()
            .map(SsTable::scan_all)
            .collect();
        let l0_iter = MergeIterator::create(iters?.into_iter().map(Box::new).collect());
        let lo_sstables = self.state.read().get_tables(lower);
        let lo_iter = SstConcatIterator::create_and_seek_to_first(lo_sstables)?;
        self.compact_core(TwoMergeIterator::create(l0_iter, lo_iter)?, to_bottom)
    }

    fn compact_core<I>(&self, mut iter: I, to_bottom: bool) -> Result<Vec<Arc<SsTable>>>
    where
        I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> + 'static,
    {
        let sst_limit = self.options.target_sst_size;
        let mut output = vec![];
        while iter.is_valid() {
            let mut builder = SsTableBuilder::new(self.options.block_size);
            while iter.is_valid() && builder.estimated_size() < sst_limit {
                let latest = iter.key().to_key_vec();
                let mut once = true;
                let filters = self.compaction_filters.lock();
                for f in filters.iter() {
                    let kickout = match f {
                        CompactionFilter::Prefix(p) => {
                            latest.key_ref().len() >= p.len()
                                && latest.key_ref()[..p.len()] == p[..]
                        }
                    };
                    if kickout {
                        once = false;
                        break;
                    }
                }
                while iter.is_valid() && iter.key().key_ref() == latest.key_ref() {
                    if iter.key().ts() > self.mvcc.watermark() {
                        builder.add(iter.key(), iter.value());
                    } else if once {
                        once = false;
                        if !(iter.value().is_empty() && to_bottom) {
                            builder.add(iter.key(), iter.value());
                        }
                    }
                    iter.next()?;
                }
            }
            assert!(builder.estimated_size() >= sst_limit || !iter.is_valid());
            if builder.estimated_size() > 0 {
                // generate new SST
                let id = self.next_sst_id();
                let path = self.path_of_sst(id);
                let sst = builder.build(id, Some(self.block_cache.clone()), path)?;
                output.push(Arc::new(sst));
            }
        }
        Ok(output)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let l0_num = l0_sstables.len();
        let mut dels = l0_sstables.clone();
        dels.extend(l1_sstables.clone());
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        };
        let new_ssts = self.compact(&task)?;
        {
            // update in-memory state
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            // remove compressed L0 sstables
            for _ in 0..l0_num {
                let id = state.l0_sstables.pop().unwrap();
                state.sstables.remove(&id);
            }
            // replace all L1 sstables
            while let Some(id) = state.levels[0].1.pop() {
                state.sstables.remove(&id);
            }
            new_ssts.into_iter().for_each(|sst| {
                state.levels[0].1.push(sst.sst_id());
                state.sstables.insert(sst.sst_id(), sst);
            });
        };
        for id in dels {
            std::fs::remove_file(self.path_of_sst(id))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let mut snapshot = self.state.read().clone();
        let mut dels = vec![];
        while let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        {
            let new_ssts = self.compact(&task)?;
            let mut output = Vec::with_capacity(new_ssts.len());
            new_ssts.into_iter().for_each(|sst| {
                let id = sst.sst_id();
                output.push(id);
                snapshot.sstables.insert(id, sst);
            });
            let (mut result, del) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output);
            del.iter().for_each(|id| {
                result.sstables.remove(id);
            });
            snapshot = result;
            *self.state.write() = snapshot.clone();
            self.manifest
                .add_record(&_state_lock, ManifestRecord::Compaction(task, output))?;
            dels.extend(del);
        }
        dels.into_iter().for_each(|id| {
            let path = self.path_of_sst(id);
            if let Err(e) = std::fs::remove_file(&path) {
                eprintln!("clean compacted file {:?}: {e}", path);
            }
        });
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.options.num_memtable_limit <= self.state.read().imm_memtables.len() {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
