use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        if let Some(cnt) = self.readers.get_mut(&ts) {
            *cnt += 1;
        } else {
            self.readers.insert(ts, 1);
        }
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let cnt = self.readers.get_mut(&ts).expect("reader not found");
        *cnt -= 1;
        if *cnt == 0 {
            self.readers.remove(&ts).unwrap();
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(&ts, _)| ts)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
