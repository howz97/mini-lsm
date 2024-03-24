use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let lv1 = &snapshot.levels[0];
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: lv1.0,
                lower_level_sst_ids: lv1.1.clone(),
                is_lower_level_bottom_level: lv1.0 == self.options.max_levels,
            });
        }
        for (l, lower) in snapshot.levels.iter().skip(1) {
            let (u, upper) = &snapshot.levels[l - 2];
            assert!(*u + 1 == *l);
            if upper.is_empty() {
                continue;
            }
            if ((lower.len() * 100) as f64) / (upper.len() as f64)
                < (self.options.size_ratio_percent as f64)
            {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(*u),
                    upper_level_sst_ids: upper.clone(),
                    lower_level: *l,
                    lower_level_sst_ids: lower.clone(),
                    is_lower_level_bottom_level: *l == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut del = vec![];
        if let Some(u) = task.upper_level {
            let upper = &mut snapshot.levels[u - 1].1;
            assert!(*upper == task.upper_level_sst_ids);
            del.append(upper);
        } else {
            let n = snapshot.l0_sstables.len() - task.upper_level_sst_ids.len();
            del.extend(snapshot.l0_sstables.drain(n..));
        }
        let lower = &mut snapshot.levels[task.lower_level - 1].1;
        del.append(lower);
        *lower = output.to_vec();
        (snapshot, del)
    }
}
