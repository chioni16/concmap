use crossbeam::epoch::{Atomic, CompareExchangeError, Guard, Owned, Shared};
use std::sync::atomic::Ordering;

#[derive(Debug, Default)]
pub enum NodeT<K, V> {
    Data(DataNode<K, V>),
    #[default]
    Fence,
}

// No need to implement `Drop` for `Node`.
// The automatically generated “drop glue” which recursively calls the destructors of all the fields of this value.
// This means that the `Drop` implementation for `inner` field will be called.
// We don't want to destroy the `Node` pointed to by the `next` field.
// This works out well as the `Drop` implementation for `Atomic` doesn't destroy the value that it points to.
#[derive(Debug)]
pub struct Node<K, V> {
    pub next: Atomic<Self>,
    pub hash: u64,
    pub inner: NodeT<K, V>,
}

impl<K, V> Node<K, V> {
    pub fn new_fence(hash: u64) -> Self {
        Self {
            hash,
            next: Default::default(),
            inner: Default::default(),
        }
    }

    pub fn new_data(hash: u64, key: K, val: Owned<V>) -> Self {
        let data = DataNode {
            key,
            val: Atomic::from(val),
        };
        Self {
            hash,
            next: Default::default(),
            inner: NodeT::Data(data),
        }
    }

    // because we make use of exclusive lock,
    // it should be okay that we don't use CAS to modify the `next` node?
    pub fn link_to(&mut self, next: Shared<Self>) {
        self.next = Atomic::from(next);
    }

    pub fn next<'g>(&'g self, guard: &'g Guard) -> Shared<'g, Self> {
        self.next.load(Ordering::SeqCst, guard)
    }

    pub fn key(&self) -> &K {
        let NodeT::Data(data) = &self.inner else {
            panic!("Expected data node")
        };
        data.key()
    }

    pub fn atomic_value(&self) -> &Atomic<V> {
        let NodeT::Data(data) = &self.inner else {
            panic!("Expected data node")
        };
        data.atomic_value()
    }

    pub fn value<'g>(&'g self, guard: &'g Guard) -> Shared<'g, V> {
        let NodeT::Data(data) = &self.inner else {
            panic!("Expected data node")
        };
        data.value(guard)
    }

    // pub fn owned_key_value(self) -> (K, Owned<V>) {
    //     let NodeT::Data(data) = self.inner else {
    //         panic!("Expected data node")
    //     };
    //     data.owned_key_value()
    // }

    // pub fn next_ref<'g>(&'g self, guard: &'g Guard) -> &'g Self {
    //     let next = self.next(guard);
    //     // SAFETY: TODO
    //     let next_ref = unsafe { next.as_ref() };
    //     next_ref.unwrap()
    // }

    pub fn comp_swap_next<'g>(
        &'g self,
        current: Shared<'g, Self>,
        new: Owned<Self>,
        guard: &'g Guard,
    ) -> Result<(), CompareExchangeError<Self, Owned<Self>>> {
        let res =
            self.next
                .compare_exchange(current, new, Ordering::SeqCst, Ordering::Relaxed, guard);
        res.map(|garbage| {
            // SAFETY: TODO
            // 1. no other way to get this
            // 2. once removed, not destroyed till all threads with the reference (obtained in previous epochs)
            // aren't done with the job.
            //
            // the above statements are true if the `next` node is a `DataNode`.
            // What happens in the case of `Fence` nodes?
            // The `buckets` field can hold the reference to the `Fence` nodes.
            // TODO TODO TODO
            if !garbage.is_null() {
                unsafe { guard.defer_destroy(garbage) };
            }
        })
    }

    pub fn comp_swap_value<'g>(
        &'g self,
        current: Shared<'_, V>,
        new: Owned<V>,
        guard: &'g Guard,
    ) -> Result<(), CompareExchangeError<V, Owned<V>>> {
        let NodeT::Data(data) = &self.inner else {
            panic!("Expected data node")
        };
        data.comp_swap_value(current, new, guard)
    }
}

#[derive(Debug)]
pub struct DataNode<K, V> {
    pub key: K,
    pub val: Atomic<V>,
}

impl<K, V> Drop for DataNode<K, V> {
    fn drop(&mut self) {
        let guard = &crossbeam::epoch::pin();
        let val = self.val.swap(Shared::null(), Ordering::SeqCst, guard);
        if !val.is_null() {
            unsafe { guard.defer_destroy(val) };
        }
    }
}

impl<K, V> DataNode<K, V> {
    #[inline]
    fn key(&self) -> &K {
        &self.key
    }

    #[inline]
    fn atomic_value<'g>(&'g self) -> &Atomic<V> {
        &self.val
    }

    #[inline]
    fn value<'g>(&'g self, guard: &'g Guard) -> Shared<'g, V> {
        self.val.load(Ordering::SeqCst, guard)
    }

    // #[inline]
    // fn owned_key_value(self) -> (K, Owned<V>) {
    //     // SAFETY: we have exclusive access to the node here
    //     (self.key, unsafe { self.val.into_owned() })
    // }

    #[inline]
    fn comp_swap_value<'g>(
        &'g self,
        current: Shared<'_, V>,
        new: Owned<V>,
        guard: &'g Guard,
    ) -> Result<(), CompareExchangeError<V, Owned<V>>> {
        let res =
            self.val
                .compare_exchange(current, new, Ordering::SeqCst, Ordering::Relaxed, guard);
        res.map(|garbage| {
            // SAFETY: TODO
            // 1. no other way to get this
            // 2. once removed, not destroyed till all threads with the reference (obtained in previous epochs)
            // aren't done with the job.
            if !garbage.is_null() {
                unsafe { guard.defer_destroy(garbage) };
            }
        })
    }
}
