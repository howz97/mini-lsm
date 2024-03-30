use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
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
        let file = File::open(&path)?;
        let mut lenbuf = [0; 2];
        let mut crcbuf = [0; 4];
        let mut reader = BufReader::new(file);
        loop {
            let mut hasher = crc32fast::Hasher::default();
            // read key
            reader.read_exact(&mut lenbuf)?;
            hasher.update(&lenbuf);
            let len = u16::from_be_bytes(lenbuf);
            let mut buf = vec![0; len as usize];
            reader.read_exact(&mut buf)?;
            hasher.update(&buf);
            let key = Bytes::from(buf);
            // read value
            reader.read_exact(&mut lenbuf)?;
            hasher.update(&lenbuf);
            let len = u16::from_be_bytes(lenbuf);
            let mut buf = vec![0; len as usize];
            reader.read_exact(&mut buf)?;
            hasher.update(&buf);
            let val = Bytes::from(buf);
            // read checksum
            reader.read_exact(&mut crcbuf)?;
            let checksum = u32::from_be_bytes(crcbuf);
            if hasher.finalize() != checksum {
                return Err(anyhow!("WAL record is corrupted"));
            }
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
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_u16(key.len() as u16);
        buf.extend(key);
        buf.put_u16(value.len() as u16);
        buf.extend(value);
        buf.put_u32(crc32fast::hash(&buf));
        self.file.lock().write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.flush()?;
        writer.get_mut().sync_all()?;
        Ok(())
    }
}
