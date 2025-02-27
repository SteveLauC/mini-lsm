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

use super::Block;
use crate::key::{KeySlice, KeyVec};
use std::ops::Range;
use std::sync::Arc;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    ///
    /// Since we don't allow empty key, if this field is empty, this iterator
    /// is invalid.
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    ///
    /// end is exclusive
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let (key, value_range) = block
            .ith_key_and_value_range(0)
            .expect("0 should be in range");
        let key = key.to_key_vec();

        Self {
            block,
            key: key.clone(),
            value_range: (value_range.start, value_range.end),
            idx: 0,
            first_key: key,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        // Since `Self::new` will put the cursor on the first element, nothing
        // needs to be handled here.
        Self::new(block)
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);

        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let range = Range {
            start: self.value_range.0,
            end: self.value_range.1,
        };
        &self.block.data[range]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_ith(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let next = self.idx + 1;

        if next >= self.block.number_of_elements() {
            self.invalidate_iterator();
            return;
        }

        self.seek_to_ith(next);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        if key < self.first_key.as_key_slice() {
            self.seek_to_first();
            return;
        }

        if key > self.block.last_key() {
            self.invalidate_iterator();
            return;
        }

        // do binary search
        let index = match self.block.offsets.binary_search_by(|offset| {
            unsafe { self.block.decode_key_at_offset(*offset) }.cmp(&key)
        }) {
            Ok(i) => i,
            Err(i) => i,
        };

        self.seek_to_ith(index);
    }

    fn seek_to_ith(&mut self, i: usize) {
        let Some((key, value_range)) = self.block.ith_key_and_value_range(i) else {
            panic!("i out of range");
        };

        if self.idx == i {
            return;
        }

        self.key = key.to_key_vec();
        self.idx = i;
        self.value_range = (value_range.start, value_range.end);
    }

    fn invalidate_iterator(&mut self) {
        self.key.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::block::BlockBuilder;

    use super::*;

    #[test]
    fn test_name() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        let mut iter = BlockIterator::new(Arc::new(block));

        loop {
            if !iter.is_valid() {
                break;
            }

            let key = iter.key();
            let value = iter.value();
            println!("{:?}:{:?}", key, value);

            iter.next();
        }
    }

    #[test]
    #[should_panic]
    fn test_seek_ith_out_of_range() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();
        let mut iter = BlockIterator::new(Arc::new(block));

        iter.seek_to_ith(100);
    }

    #[test]
    fn test_seek_key() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();
        let mut iter = BlockIterator::new(Arc::new(block));

        iter.seek_to_key(KeySlice::from_slice("a".as_bytes()));
        assert_eq!(iter.idx, 0);

        iter.seek_to_key(KeySlice::from_slice("b".as_bytes()));
        assert_eq!(iter.idx, 1);

        iter.seek_to_key(KeySlice::from_slice("c".as_bytes()));
        assert_eq!(iter.idx, 2);

        iter.seek_to_key(KeySlice::from_slice("d".as_bytes()));
        assert!(!iter.is_valid());

        iter.seek_to_key(KeySlice::from_slice(&[0])); // smaller than "a"
        assert_eq!(iter.idx, 0);

        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("e".as_bytes()), "e".as_bytes()));
        let block = builder.build();
        let mut iter = BlockIterator::new(Arc::new(block));

        iter.seek_to_key(KeySlice::from_slice("b".as_bytes()));
        assert_eq!(iter.idx, 1);

        iter.seek_to_key(KeySlice::from_slice("d".as_bytes()));
        assert_eq!(iter.idx, 2);
    }

    #[test]
    fn test_invalidate_iterator() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();
        let mut iter = BlockIterator::new(Arc::new(block));

        assert!(iter.is_valid());
        iter.invalidate_iterator();
        assert!(!iter.is_valid());
    }
}
