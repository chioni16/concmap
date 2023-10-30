#![feature(let_chains)]

use crossbeam::epoch::{self, Guard, Owned, Shared};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::RwLock;

use bucket::Bucket;

use crate::node::Node;

mod bucket;
mod node;

pub struct Map<K: Hash, V> {
    bucket_size: u8,
    log_num_buckets: AtomicU32,
    count: AtomicUsize,
    buckets: RwLock<Vec<RwLock<Bucket<K, V>>>>,
    // iteration fields
}

impl<K, V> Map<K, V>
where
    K: Hash + PartialEq,
{
    pub fn new(bucket_size: u8) -> Self {
        assert!(bucket_size >= 6);
        let guard = &epoch::pin();
        let bucket = Bucket::new(0, 0);
        let mut fence = bucket.fence.load(Ordering::SeqCst, guard);
        let next = epoch::Owned::from(Node::new_fence(0));
        unsafe {
            fence.deref_mut().link_to(next.into_shared(guard));
        }
        Self {
            bucket_size,
            log_num_buckets: AtomicU32::new(0),
            count: AtomicUsize::new(0),
            buckets: RwLock::new(vec![RwLock::new(bucket)]),
        }
    }

    pub fn len(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    #[inline]
    fn get_bucket_index(&self, h: u64) -> usize {
        let log_num_buckets = self.log_num_buckets.load(Ordering::SeqCst);
        (h >> (64 - log_num_buckets)) as usize
    }

    pub fn get<'g>(&'g self, key: &'g K, guard: &'g Guard) -> Option<&'g V> {
        let hash = calculate_hash(key);
        let buckets = self.buckets.read().unwrap();
        let bucket_index = self.get_bucket_index(hash);
        let bucket = buckets[bucket_index].read().unwrap();

        let val: Option<Shared<'g, V>> = bucket.get(key, hash, guard);
        // SAFETY: same as above
        // TODO
        val.and_then(|v| unsafe { v.as_ref() })
    }

    pub fn set<'g>(&'g self, key: K, val: V, guard: &'g Guard) {
        let hash = calculate_hash(&key);
        let node = Node::new_data(hash, key, Owned::from(val));

        let buckets = self.buckets.read().unwrap();
        let bucket_index = self.get_bucket_index(hash);
        let bucket = buckets[bucket_index].read().unwrap();

        if bucket.upsert(node, guard) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }

        if self.is_overflow() {
            self.expand()
        }
    }

    pub fn delete<'g>(&'g self, key: K, guard: &'g Guard) {
        let hash = calculate_hash(&key);

        let buckets = self.buckets.read().unwrap();
        let bucket_index = self.get_bucket_index(hash);
        let mut bucket = buckets[bucket_index].write().unwrap();

        if bucket.delete(key, hash, guard) {
            self.count.fetch_sub(1, Ordering::SeqCst);
        }

        if self.is_underflow() {
            self.shrink()
        }
    }

    fn is_overflow(&self) -> bool {
        let count = self.count.load(Ordering::SeqCst);
        let log_num_buckets = self.log_num_buckets.load(Ordering::SeqCst);
        (count >> log_num_buckets) > (self.bucket_size as usize)
    }

    fn is_underflow(&self) -> bool {
        let count = self.count.load(Ordering::SeqCst);
        let log_num_buckets = self.log_num_buckets.load(Ordering::SeqCst);
        log_num_buckets > 4 && (count >> log_num_buckets) <= (self.bucket_size / 3) as usize
    }

    fn expand(&self) {
        if !self.is_overflow() {
            return;
        }

        self.log_num_buckets.fetch_add(1, Ordering::SeqCst);
    }

    fn shrink(&self) {
        if !self.is_underflow() {
            return;
        }
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
