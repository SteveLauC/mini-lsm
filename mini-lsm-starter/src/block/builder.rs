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
    /// 
    /// future steve: 
    /// 1. This field is for the week1/day7/task3
    /// 2. Block Pruning will use BlockMeta 
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

    pub(crate) fn size(&self) -> usize {
        let data_len = self.data.len();
        let offsets_len = self.offsets.len() * 2;

        data_len + offsets_len
    }

    fn size_with_extra_key_value(&self, rest_key_len: usize, value: &[u8]) -> usize {
        use std::mem::size_of;

        let data_len = self.data.len() + 2 + 2 + rest_key_len + 2+ value.len();
        let offsets_len = (self.offsets.len() + 1) * size_of::<u16>(); 

        data_len + offsets_len + 2
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if key.is_empty() {
            panic!("key should not be empty");
        }


        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        let offset = self.data.len() as u16;

        if self.is_empty() {
            // If this is the first key-value, we accept it regardless of its size.
            self.first_key = key.to_key_vec();

            self.data.put_u16_ne(key_len);
            self.data.put_slice(key.raw_ref());
        } else {
            let first_key = self.first_key.as_key_slice().raw_ref();
            let common_prefix_len = find_common_prefix_len(first_key, key.raw_ref());
            // common_prefix_len should be smaller than key_len
            let rest_key_len = key_len - common_prefix_len;

            if self.size_with_extra_key_value(rest_key_len as usize, value) > self.block_size {
                return false
            }

            self.data.put_u16_ne(common_prefix_len);
            self.data.put_u16_ne(rest_key_len);
            self.data.put_slice(&key.raw_ref()[common_prefix_len as usize..]);
        }


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
            first_key: self.first_key.into_inner()
        }
    }
}


/// Helper function to find the length of the common prefix between `first_key` and `key`.
fn find_common_prefix_len(first_key: &[u8], key: &[u8]) -> u16 {
    let mut common_prefix_len = 0;
    let min_len = std::cmp::min(first_key.len(), key.len());

    for possible_common_prefix_len in 1..min_len {
        let byte_index = possible_common_prefix_len - 1;

        if first_key[byte_index] == key[byte_index] {
            common_prefix_len = possible_common_prefix_len;
        } else {
            break;
        }
    }


    common_prefix_len as u16
}