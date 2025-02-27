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

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::ops::{Deref, DerefMut};

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

/// `usize` is the index number of the underlying iterator, smaller index means
/// newer data.
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            // When encountering 2 keys with the same value, prefer the newer
            // one
            .then(self.0.cmp(&other.0))
            // `a` is smaller than `b`, but `a` should comes first(SkipMap Range
            // iter does this). Since `std::collections::BinaryHeap` is a max-heap,
            // reverse the ordering.
            //
            // The same applies to iterator index number as well.
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    // Will be None only if the `iters` arg of `create()` is empty
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    // TODO(steve): why Box here?
    //
    // future steve: I think this may be changed to a trait object in the future
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<HeapWrapper<I>> = iters
            .into_iter()
            // filter out invalid iterators
            .filter(|iter| iter.is_valid())
            .enumerate()
            .map(|(idx, iter)| HeapWrapper(idx, iter))
            .collect();

        let current = iters.pop();

        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().expect("current is None").1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().expect("current is None").1.value()
    }

    fn is_valid(&self) -> bool {
        match self.current {
            Some(ref current) => current.1.is_valid(),
            None => false,
        }
    }

    fn next(&mut self) -> Result<()> {
        let key = self.key().into_inner().to_vec();

        // There will be duplicate values in multiple iterators, we need to clear them.
        while let Some(mut max_iter) = self.iters.peek_mut() {
            if key == max_iter.1.key().into_inner() {
                let result = max_iter.1.next();
                if result.is_err() {
                    // max_iter is invalid, remove it
                    PeekMut::pop(max_iter);
                    // NOTE: an error occurred, but the iterator will not be set to invalid!
                    return result;
                }

                if !max_iter.1.is_valid() {
                    // max_iter is invalid, remove it
                    PeekMut::pop(max_iter);
                    continue;
                }
            } else {
                break;
            }
        }

        self.current.as_mut().unwrap().1.next()?;
        // there is no more entires in current
        if !self.current.as_ref().unwrap().1.is_valid() {
            self.current = self.iters.pop();
            return Ok(());
        }

        if let Some(mut max_in_iters) = self.iters.peek_mut() {
            if max_in_iters.deref() > self.current.as_ref().unwrap() {
                std::mem::swap(self.current.as_mut().unwrap(), max_in_iters.deref_mut());
            }
        }

        Ok(())
    }
}
