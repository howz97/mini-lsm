use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.is_empty() {
            return None;
        }
        let mut it = snapshot.levels.iter().rev();
        let bottom_sz = it.next().unwrap().1.len();
        let sum = it.map(|lv| lv.1.len()).sum::<usize>();
        if bottom_sz == 0
            || ((sum as f64) / (bottom_sz as f64)
                > (self.options.max_size_amplification_percent as f64 / 100.0))
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        if snapshot.levels.len() > self.options.min_merge_width {
            let mut sum = snapshot.levels[0].1.len();
            for i in 1..snapshot.levels.len() {
                let len = snapshot.levels[i].1.len();
                if (sum as f64) / (len as f64) > (100.0 + self.options.size_ratio as f64) / 100.0 {
                    return Some(TieredCompactionTask {
                        tiers: snapshot.levels[..=i].to_owned(),
                        bottom_tier_included: i == snapshot.levels.len() - 1,
                    });
                }
                sum += len;
            }
        }
        if snapshot.levels.len() > self.options.num_tiers {
            let i = snapshot.levels.len() - self.options.num_tiers;
            return Some(TieredCompactionTask {
                tiers: snapshot.levels[..=i].to_owned(),
                bottom_tier_included: i == snapshot.levels.len() - 1,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        assert!(snapshot.levels[0] == task.tiers[0]);
        let dels = snapshot
            .levels
            .drain(..task.tiers.len())
            .flat_map(|(_, tier)| tier)
            .collect::<Vec<usize>>();
        snapshot.levels.insert(0, (output[0], output.to_vec()));
        (snapshot, dels)
    }
}
