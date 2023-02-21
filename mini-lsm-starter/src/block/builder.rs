use super::{Block, SIZE_OF_KEY_LEN, SIZE_OF_NUM_OF_ELEMENTS, SIZE_OF_VALUE_LEN, SIZE_OF_AN_OFFSET};
use bytes::BufMut;

/// Builds a block.
pub struct BlockBuilder {
    /// Maximum bytes a block can hold (aka. target_size in the tutorial).
    block_size: usize,
    /// data section.
    data: Vec<u8>,
    /// Offsets section.
    offsets: Vec<u16>,
    /// Length in bytes.
    len: usize,
}

impl BlockBuilder {
    /// Return how many bytes will be added when this new entry is inserted.
    #[inline]
    fn size_increased_by_one_element(key: &[u8], value: &[u8]) -> usize {
        SIZE_OF_AN_OFFSET + SIZE_OF_KEY_LEN + SIZE_OF_VALUE_LEN + key.len() + value.len()
    }

    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            data: Vec::new(),
            offsets: Vec::new(),
            // Two bytes for `num_of_elements` field.
            len: SIZE_OF_NUM_OF_ELEMENTS,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    ///
    /// This method is not responsible for maintaining the elements in this block
    /// is sorted.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");

        let len_increased = Self::size_increased_by_one_element(key, value);
        // limit check
        if self.len + len_increased > self.block_size && !self.is_empty() {
            return false;
        }

        self.len += len_increased;
        self.offsets.push(
            self.data
                .len()
                .try_into()
                .expect("offset should be in range of u16"),
        );
        self.data
            .put_u16_ne(key.len().try_into().expect("max len of key: 65536"));
        self.data.put_slice(key);
        self.data
            .put_u16_ne(value.len().try_into().expect("max len of value: 65535"));
        self.data.put_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    #[inline]
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
