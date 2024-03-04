use super::StorageIterator;
use anyhow::Result;
use std::cmp::{min, Ordering};

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        Ok(Self { a, b })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if !self.a.is_valid() {
            self.b.key()
        } else if !self.b.is_valid() {
            self.a.key()
        } else {
            min(self.a.key(), self.b.key())
        }
    }

    fn value(&self) -> &[u8] {
        if !self.a.is_valid() {
            self.b.value()
        } else if !self.b.is_valid() {
            self.a.value()
        } else if self.a.key().cmp(&self.b.key()) == Ordering::Greater {
            self.b.value()
        } else {
            self.a.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if !self.a.is_valid() {
            self.b.next()
        } else if !self.b.is_valid() {
            self.a.next()
        } else {
            let ord = self.a.key().cmp(&self.b.key());
            match ord {
                Ordering::Less => self.a.next(),
                Ordering::Equal => self.a.next().and(self.b.next()),
                Ordering::Greater => self.b.next(),
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
