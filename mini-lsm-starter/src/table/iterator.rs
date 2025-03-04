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

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
    key::KeySlice,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Common part of `create_and_seek_to_first()` and `seek_to_first()`.
    fn seek_to_first_key_iter(table: &SsTable) -> Result<BlockIterator> {
        let block = table.read_block_cached(0)?;
        let block_iter = BlockIterator::create_and_seek_to_first(block);

        Ok(block_iter)
    }

    /// Common part of `create_and_seek_to_key()` and `seek_to_key()`.
    fn seek_to_key_iter(table: &SsTable, key: KeySlice) -> Result<Option<(BlockIterator, usize)>> {
        let block_idx = table
            .block_meta
            .binary_search_by(|block_meta| block_meta.last_key.as_key_slice().cmp(&key))
            .unwrap_or_else(|idx| idx);
        if block_idx >= table.num_of_blocks() {
            return Ok(None);
        }

        let block = table.read_block_cached(block_idx)?;
        let mut block_iter = BlockIterator::create_and_seek_to_first(block);
        block_iter.seek_to_key(key);

        assert!(block_iter.is_valid());

        Ok(Some((block_iter, block_idx)))
    }

    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_iter = Self::seek_to_first_key_iter(&table)?;

        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let blk_iter = Self::seek_to_first_key_iter(&self.table)?;

        self.blk_iter = blk_iter;
        self.blk_idx = 0;

        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let Some((blk_iter, blk_idx)) = Self::seek_to_key_iter(&table, key)? else {
            let mut iter = Self::create_and_seek_to_first(table)?;
            iter.blk_iter.invalidate_iterator();
            return Ok(iter);
        };

        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let Some((blk_iter, blk_idx)) = Self::seek_to_key_iter(&self.table, key)? else {
            self.blk_iter.invalidate_iterator();
            return Ok(());
        };

        self.blk_iter = blk_iter;
        self.blk_idx = blk_idx;

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        if !self.is_valid() {
            panic!("key() called on an invalid iterator");
        }

        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("value() called on an invalid iterator");
        }

        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();

        if !self.blk_iter.is_valid() {
            let block_idx = self.blk_idx + 1;
            if block_idx >= self.table.num_of_blocks() {
                return Ok(());
            }

            let block_meta = self
                .table
                .block_meta
                .get(block_idx)
                .expect("should not be out of range");
            let block_offset = block_meta.offset;
            let block_bytes_len =
                if let Some(next_block_meta) = self.table.block_meta.get(block_idx + 1) {
                    next_block_meta.offset - block_offset
                } else {
                    // block_idx points to the last block
                    self.table.block_meta_offset - block_offset
                };
            let block_bytes = self
                .table
                .file
                .read(block_offset as u64, block_bytes_len as u64)?;
            let block = Block::decode(&block_bytes);
            let block_iter = BlockIterator::create_and_seek_to_first(Arc::new(block));

            self.blk_iter = block_iter;
            self.blk_idx = block_idx;
        }

        Ok(())
    }
}
