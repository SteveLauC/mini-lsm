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

use anyhow::Result;


pub struct Bloom(pub fastbloom::BloomFilter<xxhash_rust::xxh64::Xxh64Builder>);


impl std::fmt::Debug for Bloom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bloom")
    }
}

impl Bloom {
    pub fn new() -> Self {
        Bloom(fastbloom::BloomFilter::with_num_bits(1024).hasher(xxhash_rust::xxh64::Xxh64Builder::new(0)).expected_items(1024))
    }

    pub fn encode(&self, mut to: impl Write) -> Result<()> {
        let slice: &[u64] = self.0.as_slice();
        // slice.len() * 8 because slice contains u64 but we need u8
        let bytes =
            unsafe { std::slice::from_raw_parts(slice.as_ptr().cast::<u8>(), slice.len() * 8) };

        to.write_all(bytes)?;

        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Self {
        // Ensure the bytes length is a multiple of 8 (size of u64)
        assert_eq!(bytes.len() % 8, 0, "Bytes length must be a multiple of 8");

        let slice: Vec<u64> = bytes
            .chunks_exact(8)
            .map(|chunk| u64::from_ne_bytes(chunk.try_into().unwrap()))
            .collect();

        let bloom = fastbloom::BloomFilter::from_vec(slice).hasher(xxhash_rust::xxh64::Xxh64Builder::new(0)).expected_items(1024);

        Self(bloom)
    }
}
