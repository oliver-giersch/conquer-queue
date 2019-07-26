mod faa;
mod faa2;

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};

use reclaim::prelude::*;
use reclaim::GlobalReclaim;

pub use faa::Queue as FAAQueue;

type Atomic<T, R> = reclaim::Atomic<T, R, reclaim::typenum::U0>;
type Owned<T, R> = reclaim::Owned<T, R, reclaim::typenum::U0>;
type Shared<'g, T, R> = reclaim::Shared<'g, T, R, reclaim::typenum::U0>;

////////////////////////////////////////////////////////////////////////////////////////////////////
// Queue
////////////////////////////////////////////////////////////////////////////////////////////////////

/// TODO: Docs...
pub struct Queue<T, R: GlobalReclaim> {
    head: Atomic<Node<T, R>, R>,
    tail: Atomic<Node<T, R>, R>,
}

/********** impl Default **************************************************************************/

impl<T, R: GlobalReclaim> Default for Queue<T, R> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/********** impl inherent *************************************************************************/

impl<T, R: GlobalReclaim> Queue<T, R> {
    #[inline]
    pub fn new() -> Self {
        let sentinel: Owned<Node<T, R>, R> = Owned::new(Node::sentinel());
        let leaked = Owned::into_marked_ptr(sentinel);

        // this is unsafe, because calling `take` on either head or tail would invalidate the other
        // one, so drop MUST only touch the head field
        unsafe {
            Self {
                head: Atomic::from_raw(leaked),
                tail: Atomic::from_raw(leaked),
            }
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        let guard = &mut R::guard();
        // head must be de-referenced in order to check its contents
        let head = self.head.load(Acquire, guard).unwrap();
        head.is_sentinel() && head.next.load_unprotected(Relaxed).is_none()
    }

    #[inline]
    pub fn push(&self, elem: T) {
        let node = Owned::leak_unprotected(Owned::new(Node::new(elem)));

        let mut guard = R::guard();

        let tail = loop {
            let tail = self.load_tail(Acquire, &mut guard);
            let next = tail.next.load_unprotected(Relaxed);

            // tail has changed since it was first loaded - retry
            if self.tail.load_raw(Acquire) != tail.as_marked_ptr() {
                continue;
            }

            // try to insert `node` at the tail position
            if next.is_none() {
                if tail
                    .next
                    .compare_exchange(next, node, Release, Relaxed)
                    .is_ok()
                {
                    break tail;
                }
            } else {
                // try to swing tail to next - retry
                let _ = self.tail.compare_exchange(tail, next, Release, Relaxed);
            }
        };

        // after `node` has been inserted, swing tail to the new node
        let _ = self.tail.compare_exchange(tail, node, Release, Relaxed);
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        let mut head_guard = R::guard();
        let mut next_guard = R::guard();

        let (unlinked, res) = loop {
            let head = self.load_head(Acquire, &mut head_guard);
            // tail is never de-referenced and doesn't need protection
            let tail = self.tail.load_unprotected(Acquire).unwrap();
            let next = head.next.load(Acquire, &mut next_guard);

            // head has changed since it was first loaded - retry
            if self.head.load_raw(Relaxed) != head.as_marked_ptr() {
                continue;
            }

            // head and tail point to the sentinel node
            if head.as_marked_ptr() == tail.as_marked_ptr() {
                if next.is_none() {
                    // the queue is empty - return
                    return None;
                } else {
                    // try to swing the tail node to next - retry
                    let _ = self.tail.compare_exchange(tail, next, Release, Relaxed);
                }
            } else {
                // head and tail are separate pointers, so next can not be null and can be
                // safely de-referenced; next's elem is tentatively read, but may also be read
                // concurrently by other pop operations, so it must not be dropped if the following
                // CAS fails
                let res = unsafe { ptr::read(&next.unwrap_unchecked().elem) };
                if let Ok(unlinked) = self.head.compare_exchange(head, next, Release, Relaxed) {
                    break (unlinked, ManuallyDrop::into_inner(res));
                }
            }
        };

        // the previous head node was successfully unlinked, so we can return the element read from
        // its former next node; since node's element is `ManuallyDrop`, it can be safely reclaimed
        // at any time, regardless of an lifetime-bound references in `T`
        unsafe { unlinked.retire_unchecked() };
        res
    }

    #[inline]
    fn load_head<'g>(&self, order: Ordering, guard: &'g mut R::Guard) -> Shared<'g, Node<T, R>, R> {
        unsafe { self.head.load(order, guard).unwrap_unchecked() }
    }

    #[inline]
    fn load_tail<'g>(&self, order: Ordering, guard: &'g mut R::Guard) -> Shared<'g, Node<T, R>, R> {
        unsafe { self.tail.load(order, guard).unwrap_unchecked() }
    }
}

/********** impl Drop *****************************************************************************/

impl<T, R: GlobalReclaim> Drop for Queue<T, R> {
    #[inline]
    fn drop(&mut self) {
        let mut curr = self.head.take();
        while let Some(mut node) = curr {
            unsafe { ManuallyDrop::drop(&mut node.elem) };
            curr = node.next.take();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Node
////////////////////////////////////////////////////////////////////////////////////////////////////

struct Node<T, R> {
    elem: ManuallyDrop<Option<T>>,
    next: Atomic<Node<T, R>, R>,
}

/********** impl inherent *************************************************************************/

impl<T, R> Node<T, R> {
    #[inline]
    fn sentinel() -> Self {
        Self {
            elem: ManuallyDrop::new(None),
            next: Atomic::null(),
        }
    }

    #[inline]
    fn new(elem: T) -> Self {
        Self {
            elem: ManuallyDrop::new(Some(elem)),
            next: Atomic::null(),
        }
    }

    #[inline]
    fn is_sentinel(&self) -> bool {
        self.elem.is_none()
    }
}
