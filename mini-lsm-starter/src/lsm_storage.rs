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

use std::collections::HashMap;
use std::ffi::OsStr;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    // TODO(steve): figure out the use of `RwLock<Arc<T>>` here
    // why not `RwLock<LsmStorageState>`
    //
    // future steve: it is used for read snapshot, and it can reduce the time
    // for which the read lock will be held.
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    /// NOTE: With this lock, there will be ONLY 1 thread that will acquire the
    /// write lock on `state`: `state.write()`
    ///
    /// If we don't use this state_lock, then multiple threads will see the mutable
    /// MemTable is full and try to froze it (`state.write()), which would affect writes.
    pub(crate) state_lock: Mutex<()>,

    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        self.flush_thread
            .lock()
            .take()
            .expect("should be Some")
            .join()
            .expect("failed to wait for the flush thread to exit");

        // TODO: do the same to the compaction thread.

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if path.try_exists()? {
            let mut l0_sstables = Vec::new();
            let mut sstables = HashMap::new();
            let sst_file_extension = OsStr::new("sst");

            let dir = std::fs::read_dir(path)?;
            for res_entry in dir {
                let entry = res_entry?;
                let entry_path = entry.path();
                if Some(sst_file_extension) == entry_path.extension() {
                    let sstable_id = entry_path
                        .file_stem()
                        .expect("should be some since it has file extension")
                        .to_str()
                        .expect("utf8 encoded")
                        .parse::<usize>()
                        .unwrap_or_else(|err| {
                            panic!(
                                "failed to parse '{:?}' as a number: {}",
                                entry_path.file_stem().unwrap(),
                                err
                            )
                        });

                    let file_object = FileObject::open(&entry_path)?;
                    let sstable = Arc::new(SsTable::open(
                        sstable_id,
                        Some(Arc::new(BlockCache::new(4096))),
                        file_object,
                    )?);

                    l0_sstables.push(sstable_id);
                    sstables.insert(sstable_id, sstable);
                }
            }

            // sort the IDs in descending order
            l0_sstables.sort_by(|a, b| b.cmp(a));

            let memtable_id = l0_sstables.first().copied().unwrap_or(0);
            let next_sst_id = memtable_id + 1;

            let state = Arc::new(RwLock::new(Arc::new(LsmStorageState {
                memtable: Arc::new(MemTable::create(memtable_id)),
                imm_memtables: Vec::new(),
                l0_sstables,
                levels: Vec::new(),
                sstables,
            })));

            Ok(Self {
                state,
                state_lock: Mutex::new(()),
                path: path.to_path_buf(),
                block_cache: Arc::new(BlockCache::new(1024)),
                next_sst_id: AtomicUsize::new(next_sst_id),
                compaction_controller,
                manifest: None,
                options: options.into(),
                mvcc: None,
                compaction_filters: Arc::new(Mutex::new(Vec::new())),
            })
        } else {
            std::fs::create_dir_all(path)?;
            let state = LsmStorageState::create(&options);

            let storage = Self {
                state: Arc::new(RwLock::new(Arc::new(state))),
                state_lock: Mutex::new(()),
                path: path.to_path_buf(),
                block_cache: Arc::new(BlockCache::new(1024)),
                next_sst_id: AtomicUsize::new(1),
                compaction_controller,
                manifest: None,
                options: options.into(),
                mvcc: None,
                compaction_filters: Arc::new(Mutex::new(Vec::new())),
            };

            Ok(storage)
        }
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state_snapshot = Arc::clone(&self.state.read());

        if let Some(bytes) = state_snapshot.memtable.get(key) {
            if bytes.is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(bytes));
            }
        }

        for imm_memtable in state_snapshot.imm_memtables.iter() {
            if let Some(bytes) = imm_memtable.get(key) {
                if bytes.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(bytes));
                }
            }
        }

        for sstable_id in state_snapshot.l0_sstables.iter() {
            let sstable = state_snapshot
                .sstables
                .get(sstable_id)
                .expect("SsTable ID not found");

            if let Some(ref bloom_filter) = sstable.bloom {
                if !bloom_filter.0.contains(key) {
                    continue;
                }
            }

            if sstable.first_key().raw_ref() > key || sstable.last_key().raw_ref() < key {
                continue;
            }

            let sstable_iter = SsTableIterator::create_and_seek_to_key(
                Arc::clone(sstable),
                KeySlice::from_slice(key),
            )?;

            if sstable_iter.key().raw_ref() != key {
                continue;
            }

            let value = sstable_iter.value();

            if value.is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(Bytes::copy_from_slice(value)));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let state_read_lock = self.state.read();
        state_read_lock.memtable.put(key, value)?;
        let memtable_approximate_size = state_read_lock.memtable.approximate_size();
        // hold the state_read_lock after `memtable.put()` to ensure that no threads will
        // write to a frozen immutable memtable.
        //
        // This lock has to be released before `self.state_lock.lock()`, or the
        // threads that get blocked by `self.state_lock.lock()` will hold a read lock
        // to `self.state` and stop the first thread that acquires `self.state_lock.lock()`
        // from freezing the mutable memtable (as `force_freeze_memtable()` needs
        // a write lock to `self.state`).
        drop(state_read_lock);

        if memtable_approximate_size >= self.options.target_sst_size {
            let mutex_guard = self.state_lock.lock();

            // re-check
            let state_snapshot = Arc::clone(&self.state.read());
            if state_snapshot.memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&mutex_guard)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_mutable_memtable = Arc::new(MemTable::create(self.next_sst_id()));

        let mut state_write_guard = self.state.write();

        let mut current_state = LsmStorageState::clone(&state_write_guard);
        let new_immutable_memtale = Arc::clone(&current_state.memtable);
        current_state.imm_memtables.insert(0, new_immutable_memtale);
        current_state.memtable = new_mutable_memtable;

        *state_write_guard = Arc::new(current_state);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_write_guard = self.state_lock.lock();

        let state_snapshot = Arc::clone(&self.state.read());
        let earliest_immutable_memtable = state_snapshot
            .imm_memtables
            .last()
            .expect("no immutable memtable found");

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        earliest_immutable_memtable.flush(&mut sst_builder)?;
        let sst_id = earliest_immutable_memtable.id();
        let sstable = sst_builder.build(
            sst_id,
            Some(Arc::new(BlockCache::new(4096))),
            self.path_of_sst(sst_id),
        )?;

        let mut new_state = LsmStorageState::clone(&state_snapshot);
        new_state.imm_memtables.pop();
        new_state.l0_sstables.insert(0, sst_id);
        new_state.sstables.insert(sst_id, Arc::new(sstable));
        *self.state.write() = Arc::new(new_state);

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = Arc::clone(&self.state.read());

        let n_memtable = state.imm_memtables.len() + 1;
        let mut memtable_iters = Vec::with_capacity(n_memtable);
        memtable_iters.push(Box::new(state.memtable.scan(lower, upper)));
        for imm_memtable in state.imm_memtables.iter() {
            memtable_iters.push(Box::new(imm_memtable.scan(lower, upper)));
        }
        let memtable_merge_iter = MergeIterator::create(memtable_iters);

        let n_sstables = state.l0_sstables.len();
        let mut sstable_iters = Vec::with_capacity(n_sstables);
        for sstable_idx in state.l0_sstables.iter() {
            let sstable = state
                .sstables
                .get(sstable_idx)
                .expect("SsTable index not found");

            // filter out SsTables that do not overlap with the specified range
            if !sstable.range_overlap(lower, upper) {
                continue;
            }

            let sstable_iter = match lower {
                Bound::Excluded(bound) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        Arc::clone(sstable),
                        KeySlice::from_slice(bound),
                    )?;
                    if iter.key().raw_ref() == bound {
                        iter.next()?;
                    }

                    iter
                }
                Bound::Included(bound) => SsTableIterator::create_and_seek_to_key(
                    Arc::clone(sstable),
                    KeySlice::from_slice(bound),
                )?,
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(Arc::clone(sstable))?,
            };

            sstable_iters.push(Box::new(sstable_iter));
        }
        let sstable_merge_iter = MergeIterator::create(sstable_iters);

        let two_merge_iter = TwoMergeIterator::create(memtable_merge_iter, sstable_merge_iter)?;
        let lsm_iter = LsmIterator::new(two_merge_iter, upper)?;

        Ok(FusedIterator::new(lsm_iter))
    }
}
