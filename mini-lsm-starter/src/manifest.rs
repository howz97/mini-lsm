use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
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

#[derive(Serialize, Deserialize)]
pub struct ChecksumRecord {
    record: ManifestRecord,
    checksum: u32,
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Manifest {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut records = Vec::new();
        let mut buf = {
            let mut file = File::open(&path)?;
            let mut content = Vec::with_capacity(file.metadata()?.size() as usize);
            file.read_to_end(&mut content)?;
            Bytes::from(content)
        };
        while buf.has_remaining() {
            let sz = buf.get_u16() as usize;
            let jsn = buf.slice(..sz);
            let record = serde_json::from_slice::<ManifestRecord>(&jsn)?;
            buf.advance(sz);
            let checksum = buf.get_u32();
            if crc32fast::hash(&jsn) != checksum {
                return Err(anyhow!("Manifest is corrupted"));
            }
            records.push(record);
        }
        let file = File::options().append(true).open(path)?;
        let manifest = Manifest {
            file: Arc::new(Mutex::new(file)),
        };
        Ok((manifest, records))
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
        let jsn = serde_json::to_string(&record)?;
        let checksum = crc32fast::hash(jsn.as_bytes());
        let mut buf = BytesMut::with_capacity(jsn.len() + 6);
        buf.put_u16(jsn.len() as u16);
        buf.put_slice(jsn.as_bytes());
        buf.put_u32(checksum);
        self.file.lock().write_all(&buf.freeze())?;
        Ok(())
    }
}
