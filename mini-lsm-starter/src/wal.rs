use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use log::warn;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    fn read_kvs(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<()> {
        // let size = file.metadata()?.len();
        // let pos = reader.stream_position()?;
        let file = File::open(&path)?;
        let mut len_buf = [0; 2];
        let mut reader = BufReader::new(file);
        loop {
            // read key
            reader.read_exact(&mut len_buf)?;
            let len = u16::from_be_bytes(len_buf);
            let mut buf = vec![0; len as usize];
            reader.read_exact(&mut buf)?;
            let key = Bytes::from(buf);
            // read value
            reader.read_exact(&mut len_buf)?;
            let len = u16::from_be_bytes(len_buf);
            let mut buf = vec![0; len as usize];
            reader.read_exact(&mut buf)?;
            let val = Bytes::from(buf);
            skiplist.insert(key, val);
        }
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        match Self::read_kvs(&path, skiplist) {
            Ok(_) => unreachable!(),
            Err(err) => {
                warn!("WAL recover: {}", err);
            }
        }
        let file = File::options().append(true).open(path)?;
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut writer = self.file.lock();
        let len = (key.len() as u16).to_be_bytes();
        writer.write_all(&len)?;
        writer.write_all(key)?;
        let len = (value.len() as u16).to_be_bytes();
        writer.write_all(&len)?;
        writer.write_all(value)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.flush()?;
        writer.get_mut().sync_all()?;
        Ok(())
    }
}
