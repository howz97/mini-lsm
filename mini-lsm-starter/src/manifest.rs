use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Manifest {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let file = File::options().read(true).append(true).open(path)?;
        let records: serde_json::Result<Vec<ManifestRecord>> =
            serde_json::Deserializer::from_reader(&file)
                .into_iter::<ManifestRecord>()
                .collect();
        let manifest = Manifest {
            file: Arc::new(Mutex::new(file)),
        };
        Ok((manifest, records?))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        // TODO: compact records when needed
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let js = serde_json::to_string_pretty(&record)?;
        self.file.lock().write_all(js.as_bytes())?;
        Ok(())
    }
}
