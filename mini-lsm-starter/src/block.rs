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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
pub use iterator::BlockIterator;

use std::ops::Range;
use bytes::{Buf, BufMut, Bytes};
use crate::key::KeyVec;

const VALUE_LEN_SIZE: usize = 2;
const COMMON_PREFIX_LEN_SIZE: usize = 2;
const REST_KEY_LEN_SIZE: usize = 2;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Debug, PartialEq)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    // This enables us to do binary search even though a block entry is not
    // fixed-sized
    pub(crate) offsets: Vec<u16>,
    pub(crate) first_key: Vec<u8>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes: Vec<u8> = Vec::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        bytes.extend_from_slice(&self.data);
        for offset in self.offsets.iter() {
            bytes.put_u16_ne(*offset);
        }
        let number_of_elements = self.offsets.len() as u16;
        bytes.put_u16_ne(number_of_elements);

        Bytes::from(bytes)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(bytes: &[u8]) -> Self {
        let bytes_len = bytes.len();

        let number_of_elements = u16::from_ne_bytes(bytes[bytes_len - 2..].try_into().unwrap());
        let data = bytes[..bytes_len - (number_of_elements as usize + 1) * 2].to_vec();
        let mut offsets = Vec::with_capacity(number_of_elements as _);

        let cursor = bytes_len - 2;
        for i in (0..number_of_elements as usize).rev() {
            let end_exclusive = cursor - i * 2;
            let start_inclusive = end_exclusive - 2;
            let offset_bytes: [u8; 2] = bytes[start_inclusive..end_exclusive].try_into().unwrap();
            let offset = u16::from_ne_bytes(offset_bytes);

            offsets.push(offset);
        }

        let first_key = Self::read_first_key(data.as_slice());

        Self { data, offsets, first_key }
    }

    pub(crate) fn number_of_elements(&self) -> usize {
        self.offsets.len()
    }

    pub(crate) fn ith_key(&self, i: usize) -> Option<KeyVec> {
        if i >= self.number_of_elements() {
            return None;
        }

        let offset = self.offsets[i] as usize;
        Some(self.decode_key_at_offset(offset))
    }

    #[cfg(test)] // Currently only used in tests
    pub(crate) fn ith_key_value(&self, i: usize) -> Option<(KeyVec, &[u8])> {
        let (key, value_range) = self.ith_key_and_value_range(i)?;
        let value = &self.data[value_range];

        Some((key, value))
    }

    pub(crate) fn ith_key_and_value_range(&self, i: usize) -> Option<(KeyVec, Range<usize>)> {
        if i >= self.number_of_elements() {
            return None;
        }

        if i == 0 {
            const FIRST_KEN_LEN_SIZE: usize = 2;

            let mut value_slice = &self.data[FIRST_KEN_LEN_SIZE + self.first_key.len()..];
            let value_len = value_slice.get_u16_ne();
            
            let value_range_start = FIRST_KEN_LEN_SIZE + self.first_key.len() + VALUE_LEN_SIZE;
            let value_range_end = value_range_start + value_len as usize;
            let value_range = Range {
                start: value_range_start,
                end: value_range_end
            };

            return Some((KeyVec::from_vec(self.first_key.clone()), value_range));
        }

        let offset = self.offsets[i] as usize;
        let mut kv_slice = &self.data[offset..];

        let common_prefix_len = kv_slice.get_u16_ne();
        let rest_key_len = kv_slice.get_u16_ne();
        let rest_key = &kv_slice[..rest_key_len as usize];
        kv_slice = &kv_slice[rest_key_len as usize..];
        let mut key_prefix = self.first_key[..common_prefix_len as usize].to_vec();
        key_prefix.extend_from_slice(rest_key);
        let key = key_prefix;

        let value_len = kv_slice.get_u16_ne();

        let value_range_start = offset + COMMON_PREFIX_LEN_SIZE + REST_KEY_LEN_SIZE + rest_key_len as usize + VALUE_LEN_SIZE;
        let value_range_end = value_range_start + value_len as usize;
        let value_range = Range {
            start: value_range_start,
            end: value_range_end,
        };

        Some((KeyVec::from_vec(key), value_range))
    }

    pub(crate) fn last_key(&self) -> KeyVec {
        let number_of_elements = self.number_of_elements();
        let i = number_of_elements - 1;

        self.ith_key(i).expect("i should be in range")
    }

    fn read_first_key(self_data: &[u8]) -> Vec<u8> {
        const ENCODED_FIRST_KEN_LEN_SIZE: usize = 2;

        let key_len = u16::from_ne_bytes(self_data[..ENCODED_FIRST_KEN_LEN_SIZE].try_into().unwrap()) as usize;
        let first_key_range = ENCODED_FIRST_KEN_LEN_SIZE..ENCODED_FIRST_KEN_LEN_SIZE+key_len;
        
        self_data[first_key_range].to_vec() 
    }

    /// You have to ensure the bytes at `offset` is a key.
    fn decode_key_at_offset(&self, offset: usize) -> KeyVec {
        let mut kv_slice = &self.data[offset..];

        if offset == 0 {
            return KeyVec::from_vec(self.first_key.clone());
        }

        let common_prefix_len = kv_slice.get_u16_ne();
        let rest_key_len = kv_slice.get_u16_ne();
        let rest_key = &kv_slice[..rest_key_len as usize];
        let mut key_prefix = self.first_key[..common_prefix_len as usize].to_vec();

        key_prefix.extend_from_slice(rest_key);

        KeyVec::from_vec(key_prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::KeySlice;

    #[test]
    fn test_last_key() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        let last_key = block.last_key();
        assert_eq!(last_key.as_key_slice(), KeySlice::from_slice("c".as_bytes()));
    }

    #[test]
    fn test_ith_key() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        assert_eq!(block.ith_key(0).unwrap().as_key_slice(), (KeySlice::from_slice(b"a")));
        assert_eq!(block.ith_key(1).unwrap().as_key_slice(), (KeySlice::from_slice(b"b")));
        assert_eq!(block.ith_key(2).unwrap().as_key_slice(), (KeySlice::from_slice(b"c")));
        assert_eq!(block.ith_key(3), None);
    }

    #[test]
    fn test_ith_key_and_value_range() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        assert_eq!(
            block.ith_key(0),
            block.ith_key_and_value_range(0).map(|(k, _v)| k)
        );
        assert_eq!(
            block.ith_key(1),
            block.ith_key_and_value_range(1).map(|(k, _v)| k)
        );
        assert_eq!(
            block.ith_key(2),
            block.ith_key_and_value_range(2).map(|(k, _v)| k)
        );
        assert_eq!(
            block.ith_key(3),
            block.ith_key_and_value_range(3).map(|(k, _v)| k)
        );

        assert_eq!(
            block.ith_key_and_value_range(0).map(|(_k, v)| v),
            Some(Range { start: 5, end: 6 })
        );
        assert_eq!(
            block.ith_key_and_value_range(1).map(|(_k, v)| v),
            Some(Range { start: 13, end: 14 })
        );
        assert_eq!(
            block.ith_key_and_value_range(2).map(|(_k, v)| v),
            Some(Range { start: 21, end: 22 })
        );
        assert_eq!(block.ith_key_and_value_range(3).map(|(_k, v)| v), None);
    }

    #[test]
    fn test_decode_key_at_offset() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        assert_eq!(
            block.decode_key_at_offset(block.offsets[0] as usize).as_key_slice().raw_ref(),
            b"a",
        );
        assert_eq!(
            block.decode_key_at_offset(block.offsets[1] as usize).as_key_slice().raw_ref(),
            b"b",
        );
        assert_eq!(
            block.decode_key_at_offset(block.offsets[2] as usize).as_key_slice().raw_ref(),
            b"c",
        );
    }


    #[test]
    fn test_decode_key_at_offset_keys_have_common_prefix() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("prefix-a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("prefix-b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("prefix-c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        assert_eq!(
            block.decode_key_at_offset(block.offsets[0] as usize).as_key_slice().raw_ref(),
            b"prefix-a",
        );
        assert_eq!(
            block.decode_key_at_offset(block.offsets[1] as usize).as_key_slice().raw_ref(),
            b"prefix-b",
        );
        assert_eq!(
            block.decode_key_at_offset(block.offsets[2] as usize).as_key_slice().raw_ref(),
            b"prefix-c",
        );
    }

    #[test]
    fn test_with_common_prefix() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("prefix-a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("prefix-b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("prefix-c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        let mut index = 0;
        let mut kv_pairs = Vec::new();
        while let Some(pair) = block.ith_key_value(index) {
            kv_pairs.push(pair);
            index += 1;
        }
        
        assert_eq!(kv_pairs.len(), 3);
        // Assertions against key
        assert_eq!(kv_pairs[0].0.as_key_slice().raw_ref(), b"prefix-a");
        assert_eq!(kv_pairs[1].0.as_key_slice().raw_ref(), b"prefix-b");
        assert_eq!(kv_pairs[2].0.as_key_slice().raw_ref(), b"prefix-c");
        // Assertions against key
        assert_eq!(kv_pairs[0].1, b"a");
        assert_eq!(kv_pairs[1].1, b"b");
        assert_eq!(kv_pairs[2].1, b"c");
    }

    #[test]
    fn test_encode_decode() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("prefix-a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("prefix-b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("prefix-c".as_bytes()), "c".as_bytes()));
        let block = builder.build();


        let bytes = block.encode();
        let block_decoded = Block::decode(&bytes);

        assert_eq!(block, block_decoded);

        let mut index = 0;
        let mut kv_pairs = Vec::new();
        while let Some(pair) = block_decoded.ith_key_value(index) {
            kv_pairs.push(pair);
            index += 1;
        }
        
        assert_eq!(kv_pairs.len(), 3);
        // Assertions against key
        assert_eq!(kv_pairs[0].0.as_key_slice().raw_ref(), b"prefix-a");
        assert_eq!(kv_pairs[1].0.as_key_slice().raw_ref(), b"prefix-b");
        assert_eq!(kv_pairs[2].0.as_key_slice().raw_ref(), b"prefix-c");
        // Assertions against key
        assert_eq!(kv_pairs[0].1, b"a");
        assert_eq!(kv_pairs[1].1, b"b");
        assert_eq!(kv_pairs[2].1, b"c");
    }
}
