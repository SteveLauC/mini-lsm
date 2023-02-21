mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
use std::ops::{Index, Range};

pub const SIZE_OF_NUM_OF_ELEMENTS: usize = std::mem::size_of::<u16>();
pub const SIZE_OF_AN_OFFSET: usize = std::mem::size_of::<u16>();
pub const SIZE_OF_KEY_LEN: usize = std::mem::size_of::<u16>();
pub const SIZE_OF_VALUE_LEN: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a
/// collection of **sorted** key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut ret = self.data.clone();
        let num_of_elements = self.offsets.len();
        let offsets = unsafe {
            std::slice::from_raw_parts(
                self.offsets.as_ptr().cast(),
                num_of_elements * SIZE_OF_NUM_OF_ELEMENTS,
            )
        };
        ret.put_slice(offsets);
        ret.put_u16_ne(num_of_elements.try_into().expect("65535 elements at most"));

        Bytes::from(ret)
    }

    pub fn decode(data: &[u8]) -> Self {
        let bytes_len = data.len();
        let num_of_elements = (&data[bytes_len - SIZE_OF_NUM_OF_ELEMENTS..]).get_u16_ne() as usize;
        let offsets_range = Range {
            start: bytes_len - SIZE_OF_NUM_OF_ELEMENTS - (num_of_elements * SIZE_OF_AN_OFFSET),
            end: bytes_len - SIZE_OF_NUM_OF_ELEMENTS,
        };
        let offsets_slice = data.index(offsets_range.clone());
        let offsets: Vec<u16> = offsets_slice
            .chunks(SIZE_OF_AN_OFFSET)
            .map(|mut offset_slice| offset_slice.get_u16_ne())
            .collect();

        assert_eq!(num_of_elements, offsets.len());

        Self {
            data: data[..offsets_range.start].to_vec(),
            offsets,
        }
    }
}

#[cfg(test)]
mod tests;
