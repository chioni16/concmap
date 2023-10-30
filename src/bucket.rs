use crossbeam::epoch::{Atomic, CompareExchangeError, Guard, Owned, Shared};
use std::sync::atomic::{AtomicU32, Ordering};

use crate::node::{Node, NodeT};

pub struct Bucket<K, V> {
    count: AtomicU32,
    pub fence: Atomic<Node<K, V>>,
}

impl<K, V> Drop for Bucket<K, V> {
    fn drop(&mut self) {
        let guard = &crossbeam::epoch::pin();
        let fence = self.fence.swap(Shared::null(), Ordering::SeqCst, guard);
        if !fence.is_null() {
            unsafe { guard.defer_destroy(fence) };
        }
    }
}

impl<K, V> Bucket<K, V>
where
    K: PartialEq,
{
    pub(super) fn new(count: u32, hash: u64) -> Self {
        Self {
            count: AtomicU32::new(count),
            fence: Atomic::from(Node::new_fence(hash)),
        }
    }

    pub(super) fn size(&self) -> u32 {
        self.count.load(Ordering::SeqCst)
    }

    pub(super) fn get<'g>(
        &'g self,
        key: &'g K,
        hash: u64,
        guard: &'g Guard,
    ) -> Option<Shared<'g, V>> {
        // SAFETY1:
        // why is it okay to deref the shared pointer here?
        // prove that the pointer is still valid and not already freed.
        // still being part of the concurrent data structure means that we haven't freed it yet
        // destructuring is run only on CAS operations when the value is replaced by another value and hence not part of the hash map anymore.
        // and the `Shared` reference is created from the value we saw in the data structure
        // even if it is removed by another thread later (after we read it),
        // this shouldn't be deleted as we have the older epoch and the epoch based GC guarantees this.

        // SAFETY2: `unwrap` is okay in this situation as there can be no `null` nodes in the chain.
        // there has to be a node whose hash is greater than or equal to the one provided after the current node
        // This holds true for the `data` nodes.
        // What about the last `fence` node?
        // All chain traverals stop right before the next fence node, so we will not look for the `next` node of the next `fence` node
        // The list of buckets will not contain the last `fence` node as the start of a new bucket.
        // So, we will never call `next` on the last `fence` node.
        let mut curr = unsafe {
            let fence = self.fence.load(Ordering::SeqCst, guard).as_ref().unwrap();
            fence.next(guard).as_ref().unwrap()
        };

        while matches!(curr.inner, NodeT::Data(_)) {
            if hash == curr.hash && key == curr.key() {
                return Some(curr.value(guard));
            }

            curr = unsafe { curr.next(guard).as_ref().unwrap() };
        }

        None
    }

    pub(super) fn upsert<'g>(
        &'g self,
        // mut key: K,
        // mut val: Owned<V>,
        // hash: u64,
        mut node: Node<K, V>,
        guard: &'g Guard,
    ) -> bool {
        loop {
            // let (curr, next, insert) = self.search(&key, hash, guard);
            let (curr, next, insert) = self.search(&node.key(), node.hash, guard);

            if insert {
                // let mut node: Node<K, V> = Node::new_data(hash, key, val);
                node.link_to(next);

                let curr = unsafe { curr.as_ref().unwrap() };
                // insert `node` in between `curr` and `next`
                // after insertion, curr -> node -> next
                match curr.comp_swap_next(next, Owned::from(node), guard) {
                    Ok(_) => {
                        self.count.fetch_add(1, Ordering::SeqCst);
                        return true;
                    }
                    // the "new" current value is ignored as we get the next suitable to insert our `node`
                    // through a call to the `search` function in the next iteration
                    Err(CompareExchangeError { new, .. }) => {
                        // let new = *new.into_box();
                        // let NodeT::Data(data) = new.inner else {
                        //     panic!("Expected data node")
                        // };
                        // key = data.key;
                        // // SAFETY: this value has never been added to the hashmap
                        // // so we can guarantee that this is the only handle present
                        // val = unsafe { data.val.into_owned() };
                        node = *new.into_box();
                    }
                }
            } else {
                let val = {
                    let atomic_val = node.atomic_value();
                    let val = atomic_val.swap(Shared::null(), Ordering::SeqCst, guard);
                    // SAFETY: not yet part of the hashmap, so this is the unique reference
                    // hence safe to be converted to owned type
                    unsafe { val.into_owned() }
                };

                let next = unsafe { next.as_ref().unwrap() };
                match next.comp_swap_value(next.value(guard), val, guard) {
                    Ok(_) => return false,
                    Err(CompareExchangeError { new, .. }) => {
                        // val = new;
                        let atomic_val = node.atomic_value();
                        let val = atomic_val.swap(new.into_shared(guard), Ordering::SeqCst, guard);
                        assert!(val.is_null());
                    }
                }
            }
        }
    }

    pub(super) fn delete<'g>(&'g mut self, key: K, hash: u64, guard: &'g Guard) -> bool {
        let (mut curr, next, insert) = self.search(&key, hash, guard);
        if insert {
            return false;
        }

        unsafe {
            // doing this without exclusive access to the linked list requires use of double CAS (NOT double width CAS)
            // so we take the exclusive access to this bucket and do it without using CAS.
            curr.deref_mut().next = next.as_ref().unwrap().next.clone();

            // usually destructors are called from within the `swap` functions.
            // but here we don't invoke `swap` functions
            // so we will have to call the destructor ourselves here
            guard.defer_destroy(Shared::from(next));
        }

        // TODO TODO TODO: can we use `Relaxed` for all count related stuff?
        // as no other atomic variable needs to know about this?
        self.count.fetch_sub(1, Ordering::SeqCst);
        true
    }

    pub(super) fn split<'g>(&'g mut self, hash: u64, guard: &'g Guard) -> Self {
        // Requires exclusive access to the bucket

        let (curr, next, count) = self.pivot(hash, guard);
        let mut curr = Shared::from(curr as *const Node<K, V>);
        let next = Shared::from(next as *const Node<K, V>);

        // prepare the new bucket
        let newb = Bucket::new(self.count.load(Ordering::SeqCst) - count, hash);
        let fence = unsafe { newb.fence.load(Ordering::SeqCst, guard).deref_mut() };
        fence.link_to(next);

        // reduce the count of old bucket
        self.count.store(count, Ordering::SeqCst);

        unsafe {
            curr.deref_mut().next = newb.fence.clone();
        }

        newb
    }

    pub(super) fn merge<'g>(&'g mut self, other: &'g mut Self, guard: &'g Guard) {
        // Requires exclusive access to both the buckets

        // we have exclusive access to the two buckets
        // so no need for the two following operations to be performed as one atomic operation
        let oc = other.count.load(Ordering::SeqCst);
        self.count.fetch_add(oc, Ordering::SeqCst);

        let on = unsafe { other.fence.load(Ordering::SeqCst, guard).as_ref().unwrap() };
        let last = unsafe { self.last(guard).deref_mut() };
        last.link_to(on.next.load(Ordering::SeqCst, guard));

        unsafe {
            // usually destructors are called from within the `swap` functions.
            // but here we don't invoke `swap` functions
            // so we will have to call the destructor ourselves here
            // TODO TODO TODO
            // But this is a fence node
            // So, what about the bucket that had this node as its fence?
            // Will that bucket be destroyed as well?
            guard.defer_destroy(Shared::from(on as *const _));
        }
    }

    fn last<'g>(&'g self, guard: &'g Guard) -> Shared<'g, Node<K, V>> {
        let mut curr = unsafe { self.fence.load(Ordering::SeqCst, guard).as_ref().unwrap() };
        let mut next = unsafe { curr.next(guard).as_ref().unwrap() };

        while matches!(next.inner, NodeT::Data(_)) {
            (curr, next) = (next, unsafe { next.next(guard).as_ref().unwrap() });
        }

        Shared::from(curr as *const _)
    }

    fn pivot<'g>(&'g self, hash: u64, guard: &'g Guard) -> (&'g Node<K, V>, &'g Node<K, V>, u32) {
        let mut curr = unsafe { self.fence.load(Ordering::SeqCst, guard).as_ref().unwrap() };
        let mut next = unsafe { curr.next(guard).as_ref().unwrap() };
        let mut count = 0;

        while hash > next.hash {
            (curr, next) = (next, unsafe { next.next(guard).as_ref().unwrap() });
            count += 1;
        }

        // curr.hash < hash <= next.hash
        // count = number of nodes with node.hash < hash
        (curr, next, count)
    }

    fn search<'g, 'k>(
        &'g self,
        key: &'k K,
        hash: u64,
        guard: &'g Guard,
    ) -> (Shared<'g, Node<K, V>>, Shared<'g, Node<K, V>>, bool) {
        let (mut curr, mut next, _) = self.pivot(hash, guard);

        // true if the node we are looking for is absent and vice-versa
        while matches!(next.inner, NodeT::Data(_)) && hash == next.hash {
            if next.key() == key {
                // curr.hash <= hash == next.hash
                // key == next.key
                return (
                    Shared::from(curr as *const _),
                    Shared::from(next as *const _),
                    false,
                );
            }

            (curr, next) = (next, unsafe { next.next(guard).as_ref().unwrap() });
        }

        // curr.hash <= hash < next.hash
        (
            Shared::from(curr as *const _),
            Shared::from(next as *const _),
            true,
        )
    }
}
