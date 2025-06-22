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

use tempfile::tempdir;

use crate::{
    key::{KeySlice, TS_ENABLED},
    table::SsTableBuilder,
};

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:010}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    100
}

#[test]
fn test_task3_block_key_compression() {
    let mut builder = SsTableBuilder::new(128);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(KeySlice::for_testing_from_slice_no_ts(&key[..]), &value[..]);
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    let sst = builder.build_for_test(path).unwrap();
    if TS_ENABLED {
        assert!(
            sst.block_meta.len() <= 34,
            "you have {} blocks, expect 34",
            sst.block_meta.len()
        );
    } else {
        assert!(
            sst.block_meta.len() <= 25,
            "you have {} blocks, expect 25",
            sst.block_meta.len()
        );
    }
}
