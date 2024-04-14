use std::ops::Bound;

use crate::lsm_storage::LsmStorageState;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub level_size_multiplier: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut target = snapshot
            .levels
            .last()
            .unwrap()
            .1
            .len()
            .max(self.options.base_level_size_mb);
        let mut targets = vec![0; self.options.max_levels];
        for t in targets.iter_mut().rev() {
            *t = target;
            if target <= self.options.base_level_size_mb {
                break;
            }
            target /= self.options.level_size_multiplier;
        }
        assert!(targets[self.options.max_levels - 1] > 0);

        let idx = targets.partition_point(|t| *t == 0);
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // TODO: limit to overlaped tables
            let (lower_level, lower_level_sst_ids) = snapshot.levels[idx].clone();
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level: lower_level == self.options.max_levels,
            });
        }

        let mut max_lv = None;
        let mut max_p = 1.0;
        for (l, lv) in snapshot.levels.iter().skip(idx) {
            let p = lv.len() as f64 / targets[l - 1] as f64;
            if p > max_p {
                max_p = p;
                max_lv = Some(*l);
            }
        }
        if let Some(l) = max_lv {
            let upper_id = snapshot.levels[l - 1].1.iter().min().unwrap().to_owned();
            let upper_table = snapshot.get_table(upper_id);
            let lower_lv = l + 1;
            let lower_level_sst_ids = snapshot.tables_overlap(
                lower_lv,
                Bound::Included(upper_table.first_key().as_key_slice()),
                Bound::Included(upper_table.last_key().as_key_slice()),
            );
            Some(LeveledCompactionTask {
                upper_level: max_lv,
                upper_level_sst_ids: vec![upper_id],
                lower_level: lower_lv,
                lower_level_sst_ids,
                is_lower_level_bottom_level: lower_lv == self.options.max_levels,
            })
        } else {
            None
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut dels = vec![];
        let mut snapshot = snapshot.clone();
        if let Some(up_lv) = task.upper_level {
            assert!(task.upper_level_sst_ids.len() == 1);
            let upper_id = task.upper_level_sst_ids[0];
            let i = snapshot.index_of_table(up_lv, upper_id).unwrap();
            snapshot.levels[up_lv - 1].1.remove(i);
            dels.push(upper_id);
        } else {
            assert!(snapshot.l0_sstables == task.upper_level_sst_ids);
            dels.append(&mut snapshot.l0_sstables);
            assert!(snapshot.l0_sstables.is_empty());
        }
        if task.lower_level_sst_ids.is_empty() {
            let lower_level = &mut snapshot.levels[task.lower_level - 1].1;
            if lower_level.is_empty() {
                *lower_level = output.to_vec();
            } else {
                let left_key = snapshot
                    .sstables
                    .get(&output[0])
                    .unwrap()
                    .clone()
                    .first_key()
                    .clone();
                // let left_key = snapshot.get_table(output[0]).first_key().clone();
                let p = lower_level
                    .binary_search_by(|tid| {
                        let sst = snapshot.sstables.get(tid).unwrap();
                        sst.compare(left_key.key_ref())
                    })
                    .unwrap_err();
                let tail = lower_level.drain(p..).collect::<Vec<usize>>();
                lower_level.extend(output);
                lower_level.extend(tail);
            }
        } else {
            let l = snapshot
                .index_of_table(
                    task.lower_level,
                    task.lower_level_sst_ids.first().unwrap().to_owned(),
                )
                .unwrap();
            let r = snapshot
                .index_of_table(
                    task.lower_level,
                    task.lower_level_sst_ids.last().unwrap().to_owned(),
                )
                .unwrap();
            let lower_level = &mut snapshot.levels[task.lower_level - 1].1;
            let tail = lower_level.drain(r + 1..).collect::<Vec<usize>>();
            let lower_del = lower_level.drain(l..).collect::<Vec<usize>>();
            assert!(lower_del == task.lower_level_sst_ids);
            dels.extend(lower_del);
            lower_level.extend(output);
            lower_level.extend(tail);
        }
        (snapshot, dels)
    }
}
