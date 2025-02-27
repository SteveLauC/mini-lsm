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

use std::ops::Range;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

use crate::key::KeySlice;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    // This enables us to do binary search even though a block entry is not
    // fixed-sized
    pub(crate) offsets: Vec<u16>,
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

        Self { data, offsets }
    }

    pub(crate) fn number_of_elements(&self) -> usize {
        self.offsets.len()
    }

    pub(crate) fn ith_key(&self, i: usize) -> Option<KeySlice> {
        if i >= self.number_of_elements() {
            return None;
        }

        let data = self.data.as_slice();
        let offset = self.offsets[i] as usize;

        let key_len = (&self.data[offset..]).get_u16_ne();
        let key_range = std::ops::Range {
            start: offset + 2,
            end: offset + 2 + key_len as usize,
        };

        Some(KeySlice::from_slice(&data[key_range]))
    }

    pub(crate) fn ith_key_and_value_range(&self, i: usize) -> Option<(KeySlice, Range<usize>)> {
        if i >= self.number_of_elements() {
            return None;
        }
        let data = self.data.as_slice();
        let offset = self.offsets[i] as usize;

        let key_len = (&self.data[offset..]).get_u16_ne();
        let key_range = std::ops::Range {
            start: offset + 2,
            end: offset + 2 + key_len as usize,
        };
        let key = KeySlice::from_slice(&data[key_range]);

        let value_len = (&data[offset + 2 + key_len as usize..]).get_u16_ne();
        let value_range_start = offset + 2 + key_len as usize + 2;
        let value_range_end = value_range_start + value_len as usize;
        let value_range = Range {
            start: value_range_start,
            end: value_range_end,
        };

        Some((key, value_range))
    }

    fn last_key(&self) -> KeySlice {
        let number_of_elements = self.number_of_elements();
        let i = number_of_elements - 1;

        self.ith_key(i).expect("i should be in range")
    }

    /// Safety:
    ///
    /// You have to ensure the bytes at `offset` is a key.
    unsafe fn decode_key_at_offset(&self, offset: u16) -> KeySlice {
        let offset = offset as usize;

        let key_len = (&self.data[offset..]).get_u16_ne();
        let key_range = std::ops::Range {
            start: offset + 2,
            end: offset + 2 + key_len as usize,
        };
        KeySlice::from_slice(&self.data[key_range])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_last_key() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        let last_key = block.last_key();
        assert_eq!(last_key, KeySlice::from_slice("c".as_bytes()));
    }

    #[test]
    fn test_ith_key() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        assert_eq!(block.ith_key(0), Some(KeySlice::from_slice(b"a")));
        assert_eq!(block.ith_key(1), Some(KeySlice::from_slice(b"b")));
        assert_eq!(block.ith_key(2), Some(KeySlice::from_slice(b"c")));
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

        // [_, _,  _,  _, _,  _]
        //  k_len  k   v_len  v
        assert_eq!(
            block.ith_key_and_value_range(0).map(|(_k, v)| v),
            Some(Range { start: 5, end: 6 })
        );
        assert_eq!(
            block.ith_key_and_value_range(1).map(|(_k, v)| v),
            Some(Range { start: 11, end: 12 })
        );
        assert_eq!(
            block.ith_key_and_value_range(2).map(|(_k, v)| v),
            Some(Range { start: 17, end: 18 })
        );
        assert_eq!(block.ith_key_and_value_range(3).map(|(_k, v)| v), None);
    }

    #[test]
    fn test_decode_offset() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(KeySlice::from_slice("a".as_bytes()), "a".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("b".as_bytes()), "b".as_bytes()));
        assert!(builder.add(KeySlice::from_slice("c".as_bytes()), "c".as_bytes()));
        let block = builder.build();

        unsafe {
            assert_eq!(
                block.ith_key(0),
                Some(block.decode_key_at_offset(block.offsets[0]))
            );
            assert_eq!(
                block.ith_key(1),
                Some(block.decode_key_at_offset(block.offsets[1]))
            );
            assert_eq!(
                block.ith_key(2),
                Some(block.decode_key_at_offset(block.offsets[2]))
            );
        }
    }
}
