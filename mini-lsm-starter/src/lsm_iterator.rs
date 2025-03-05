// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Result;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};
use std::ops::Bound;

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,

    upper_bound: Bound<Vec<u8>>,
    reached_upper_bound: bool,
}

/// NOTE: one extra thing that `LsmIterator` does is that it filters out deleted
/// key-value pairs.
impl LsmIterator {
    pub(crate) fn new(mut iter: LsmIteratorInner, upper_bound: Bound<&[u8]>) -> Result<Self> {
        let upper_bound = match upper_bound {
            Bound::Excluded(slice) => Bound::Excluded(slice.to_vec()),
            Bound::Included(slice) => Bound::Included(slice.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        if !iter.is_valid() {
            return Ok(Self {
                inner: iter,
                upper_bound,
                reached_upper_bound: false,
            });
        }

        // filter deleted key-value pairs
        loop {
            if !iter.value().is_empty() {
                break;
            }

            iter.next()?;
            if !iter.is_valid() {
                return Ok(Self {
                    inner: iter,
                    upper_bound,
                    reached_upper_bound: false,
                });
            }
        }

        let reached_upper_bound = match &upper_bound {
            Bound::Excluded(bound) => iter.key().raw_ref() >= bound.as_slice(),
            Bound::Included(bound) => iter.key().raw_ref() > bound.as_slice(),
            Bound::Unbounded => false,
        };

        Ok(Self {
            inner: iter,
            upper_bound,
            reached_upper_bound,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && !self.reached_upper_bound
    }

    fn key(&self) -> &[u8] {
        self.inner.key().into_inner()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        let inner = &mut self.inner;

        // filter deleted key-value pairs
        loop {
            inner.next()?;
            if !inner.is_valid() {
                return Ok(());
            }

            if !inner.value().is_empty() {
                break;
            }
        }

        self.reached_upper_bound = match &self.upper_bound {
            Bound::Excluded(bound) => inner.key().raw_ref() >= bound.as_slice(),
            Bound::Included(bound) => inner.key().raw_ref() > bound.as_slice(),
            Bound::Unbounded => false,
        };

        Ok(())
    }
}

/// A wrapper around existing iterator, will
///
/// 1. prevent users from calling `next` when the iterator is invalid. (conflicts with requirement 2)
/// 2. If an iterator is already invalid, `next` does not do anything.
/// 3. If `next` returns an error, `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            return false;
        }

        self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("invalid iterator");
        }

        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid iterator");
        }

        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        // If an iterator is already invalid, `next` does not do anything.
        if self.has_errored {
            return Err(anyhow::anyhow!("error"));
        }

        if !self.iter.is_valid() {
            return Ok(());
        }

        let result = self.iter.next();
        if result.is_err() {
            self.has_errored = true;
            return result;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{key::KeySlice, mem_table::MemTable, table::SsTableBuilder};
    use bytes::Bytes;
    use crossbeam_skiplist::SkipMap;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_task3_lsm_iterator_ignore_deleted_values() {
        let temp_dir = tempdir().unwrap();

        let skiplist = SkipMap::new();
        skiplist.insert(Bytes::from("a"), Bytes::from(""));
        skiplist.insert(Bytes::from("b"), Bytes::from("b"));
        skiplist.insert(Bytes::from("c"), Bytes::from(""));
        skiplist.insert(Bytes::from("d"), Bytes::from("d"));
        let memtable = MemTable::for_testing_from_skiplist(skiplist);
        let mem_iter = memtable.scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded);
        let merge_iter_a = MergeIterator::create(vec![Box::new(mem_iter)]);

        let mut sstable_builder = SsTableBuilder::new(4096);
        sstable_builder.add(KeySlice::from_slice("c".as_bytes()), "sstable".as_bytes());
        sstable_builder.add(KeySlice::from_slice("e".as_bytes()), "sstable".as_bytes());
        let sstable = sstable_builder
            .build(0, None, temp_dir.path().join("sstable"))
            .unwrap();
        let sstable_iter = SsTableIterator::create_and_seek_to_first(Arc::new(sstable)).unwrap();
        let merge_iter_b = MergeIterator::create(vec![Box::new(sstable_iter)]);

        let two_merge_iter = TwoMergeIterator::create(merge_iter_a, merge_iter_b).unwrap();

        let mut lsm_iter = LsmIterator::new(two_merge_iter, Bound::Unbounded).unwrap();

        let mut values = Vec::new();
        loop {
            let key = std::str::from_utf8(lsm_iter.key()).unwrap().to_string();
            let value = std::str::from_utf8(lsm_iter.value()).unwrap().to_string();
            values.push((key, value));

            lsm_iter.next().unwrap();

            if !lsm_iter.is_valid() {
                break;
            }
        }

        assert_eq!(
            values,
            vec![
                ("b".to_string(), "b".to_string()),
                ("d".to_string(), "d".to_string()),
                ("e".to_string(), "sstable".to_string())
            ]
        )
    }
}
