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

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use bytes::Bytes;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            // 256 MB
            data: Vec::with_capacity(1024 * 1024 * 256),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        loop {
            let added = self.builder.add(key, value);

            if added {
                if self.first_key.is_empty() {
                    self.first_key = key.raw_ref().to_vec();
                }
                self.last_key = key.raw_ref().to_vec();

                break;
            } else {
                // this block is full
                let builder =
                    std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

                let block = builder.build();
                let block_bytes = block.encode();

                let block_first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
                    block.ith_key(0).expect("0 should be in range").raw_ref(),
                ));
                let block_last_key =
                    KeyBytes::from_bytes(Bytes::copy_from_slice(block.last_key().raw_ref()));

                let block_meta = BlockMeta {
                    offset: self.data.len(),
                    first_key: block_first_key,
                    last_key: block_last_key,
                };

                self.data.extend_from_slice(&block_bytes);
                self.meta.push(block_meta);

                // now let's add this key-value again
                continue;
            }
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.builder.size()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if self.builder.size() != 0 {
            let block = self.builder.build();
            let block_bytes = block.encode();

            let block_first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
                block.ith_key(0).expect("0 should be in range").raw_ref(),
            ));
            let block_last_key =
                KeyBytes::from_bytes(Bytes::copy_from_slice(block.last_key().raw_ref()));

            let block_meta = BlockMeta {
                offset: self.data.len(),
                first_key: block_first_key,
                last_key: block_last_key,
            };

            self.data.extend_from_slice(&block_bytes);
            self.meta.push(block_meta);
        }

        let mut sst_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path)
            .expect("open");
        let data = self.data;
        sst_file.write_all(data.as_slice()).expect("write");
        let block_meta_offset = data.len() as u32;
        drop(data);

        if self.meta.is_empty() {
            panic!("trying to build an empty SsTable");
        }

        let mut meta_bytes = Vec::new();
        BlockMeta::encode_block_meta(self.meta.as_slice(), &mut meta_bytes);
        sst_file.write_all(&meta_bytes).expect("write");

        sst_file
            .write_all(&block_meta_offset.to_le_bytes())
            .expect("write");

        let file_len = sst_file.metadata().expect("stat").len();

        SsTable::open(id, block_cache, super::FileObject(Some(sst_file), file_len))
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
