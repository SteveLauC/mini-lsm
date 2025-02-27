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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    ///
    /// TODO(steve): what is this for? I think we can use it to skip a whole
    /// block (min-max index)
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            // Use an empty key here
            first_key: KeyVec::new(),
        }
    }

    fn size_with_extra_key_value(&self, key: &KeySlice, value: &[u8]) -> usize {
        let data_len = self.data.len() + 4 + key.len() + value.len();
        let offsets_len = (self.offsets.len() + 1) * 2;

        data_len + offsets_len + 2
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if key.is_empty() {
            panic!("key should not be empty");
        }

        if self.is_empty() {
            // If this is the first key-value, we accept it regardless of its size.
            self.first_key = key.to_key_vec();
        } else if self.size_with_extra_key_value(&key, value) > self.block_size {
            return false;
        }

        let key_len = key.len() as u16;
        let value_len = value.len() as u16;

        let offset = self.data.len() as u16;

        self.data.put_u16_ne(key_len);
        self.data.put_slice(key.raw_ref());
        self.data.put_u16_ne(value_len);
        self.data.put_slice(value);
        self.offsets.push(offset);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
