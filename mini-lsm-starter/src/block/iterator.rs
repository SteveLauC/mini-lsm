use bytes::Buf;
use std::{cmp::Ordering, sync::Arc};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The block we are iterating on.
    block: Arc<Block>,
    /// Key of the current element.
    key: Vec<u8>,
    /// Value of the current element.
    value: Vec<u8>,
    /// Which element we are currently on.
    idx: usize,
}

impl BlockIterator {
    /// Create a `BlockIterator` with everything uninitialized.
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Seek to `idx` element.
    fn seek_to_idx_element(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            return;
        }

        let offset = self.block.offsets[idx] as usize;
        let mut data_raw = &self.block.data[offset..];
        // read key
        let key_len = data_raw.get_u16_ne() as usize;
        self.key = data_raw[..key_len].to_vec();
        data_raw.advance(key_len);

        // read value
        let value_len = data_raw.get_u16_ne() as usize;
        self.value = data_raw[..value_len].to_vec();
        data_raw.advance(value_len);

        // update cursor
        self.idx = idx;
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut ret = Self::new(block);
        ret.seek_to_first();
        ret
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut ret = Self::new(block);
        ret.seek_to_key(key);
        ret
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx_element(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx_element(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    ///
    /// Since the elements in a block is sorted, we use binary search to speed
    /// up our seek.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low_idx = 0;
        let mut high_idx = self.block.offsets.len();

        while low_idx < high_idx {
            let mid_idx = (low_idx + high_idx) / 2;
            self.seek_to_idx_element(mid_idx);

            match self.key().cmp(key) {
                Ordering::Less => low_idx = mid_idx + 1,
                _ => high_idx = mid_idx,
            }
        }

        self.seek_to_idx_element(low_idx);
    }
}
