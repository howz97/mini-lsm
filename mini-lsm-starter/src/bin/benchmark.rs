use std::{path::PathBuf, time};

use clap::{command, Parser, ValueEnum};
use log::info;
use mini_lsm_starter::{
    compact::{
        CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
        TieredCompactionOptions,
    },
    lsm_storage::{LsmStorageOptions, MiniLsm},
};
use rand::Rng;

#[derive(Debug, Clone, ValueEnum)]
enum CompactionStrategy {
    Simple,
    Leveled,
    Tiered,
    None,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "lsm.db")]
    path: PathBuf,
    #[arg(long, default_value = "leveled")]
    compaction: CompactionStrategy,
    #[arg(long, default_value_t = false)]
    enable_wal: bool,
    #[arg(long, default_value_t = false)]
    serializable: bool,
    #[arg(long, default_value_t = 1)]
    num_op: u16,
    #[arg(long, default_value_t = 1)]
    parallel: u16,
    #[arg(long, default_value_t = 1)]
    iter: u16,
}

fn bench_put(storage: &MiniLsm, op_cnt: u32) {
    let gen_key = |i| format!("{:14}", i);
    let gen_value = |i| format!("{:114}", i);
    let mut rng = rand::thread_rng();
    for _ in 0..op_cnt {
        let i = rng.gen::<u16>();
        let key = gen_key(i);
        let val = gen_value(i);
        storage.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
}

fn main() {
    env_logger::init();
    log::set_max_level(log::LevelFilter::Info);

    let args = Args::parse();
    let compact_opts = match args.compaction {
        CompactionStrategy::None => CompactionOptions::NoCompaction,
        CompactionStrategy::Simple => CompactionOptions::Simple(SimpleLeveledCompactionOptions {
            size_ratio_percent: 200,
            level0_file_num_compaction_trigger: 2,
            max_levels: 4,
        }),
        CompactionStrategy::Tiered => CompactionOptions::Tiered(TieredCompactionOptions {
            num_tiers: 3,
            max_size_amplification_percent: 200,
            size_ratio: 1,
            min_merge_width: 2,
        }),
        CompactionStrategy::Leveled => CompactionOptions::Leveled(LeveledCompactionOptions {
            level0_file_num_compaction_trigger: 2,
            max_levels: 4,
            base_level_size_mb: 128,
            level_size_multiplier: 2,
        }),
    };
    let opts = LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 4 * 1024 * 1024,
        num_memtable_limit: 4,
        compaction_options: compact_opts,
        enable_wal: args.enable_wal,
        serializable: args.serializable,
    };

    let storage = MiniLsm::open(&args.path, opts).unwrap();
    let op_cnt = (args.num_op as u32) * 1024;
    let num_thd = args.parallel as u32;
    info!("benchmark start...");
    for _ in 0..args.iter {
        let start = time::SystemTime::now();
        std::thread::scope(|s| {
            (0..num_thd).for_each(|_| {
                s.spawn(|| bench_put(&storage, op_cnt));
            })
        });
        let dur_secs = start.elapsed().unwrap().as_secs_f32();
        let ops = (op_cnt * num_thd) as f32 / dur_secs;
        info!("OPS: {ops}");
    }
    storage.dump_structure();
}
