use mini_lsm_starter::compact::CompactionOptions;
use mini_lsm_starter::iterators::StorageIterator;
use mini_lsm_starter::lsm_storage::MiniLsm;
use mini_lsm_starter::lsm_storage::LsmStorageOptions;
use mini_lsm_starter::utils::display_utf8_bytes;
use std::ops::Bound;

fn main() {
    let opt = LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 4096 * 10,
        num_memtable_limit: 5,
        compaction_options: CompactionOptions::NoCompaction,
        enable_wal: true,
        serializable: true,
    };
    let lsm = MiniLsm::open("data", opt).unwrap();
    
    lsm.put(b"a", b"a").unwrap();
    let mut iter = lsm.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    
    loop {
        if !iter.is_valid() {
            break;
        }
        
        let key = iter.key();
        let value = iter.value();
        
        println!("{}:{}", display_utf8_bytes(key), display_utf8_bytes(value));
        
        iter.next().unwrap();
    }
}
