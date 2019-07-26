mod faa;
mod faa2;

use std::mem::MaybeUninit;
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

/// A concurrent lock-free Michael-Scott queue with generic memory reclamation.
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
    /// Creates a new empty [`Queue`].
    #[inline]
    pub fn new() -> Self {
        let sentinel: Owned<Node<T, R>, R> = Owned::new(Node::sentinel());
        let leaked = Owned::into_marked_ptr(sentinel);

        // this is safe as long as methods like `Atomic::take` that assume the
        // the Atomic to be the owner of its value are only called on EITHER the
        // head or the tail
        unsafe {
            Self {
                head: Atomic::from_raw(leaked),
                tail: Atomic::from_raw(leaked),
            }
        }
    }

    /// Returns `true` if the [`Queue`] was currently empty at the time of the
    /// call.
    #[inline]
    pub fn is_empty(&self) -> bool {
        let mut guard = R::guard();

        let head = self.load_head(Acquire, &mut guard);
        let tail = self.tail.load_raw(Acquire);

        head.as_marked_ptr() == tail && head.next.load_raw(Relaxed).is_null()
    }

    /// Pushes `elem` to the tail of the queue.
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

    /// Attempts to pop an element from the head of the queue and returns
    /// [`None`] if the queue is empty.
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
                    break (unlinked, unsafe { res.assume_init() });
                }
            }
        };

        // the previous head node was successfully unlinked, so we can return the element read from
        // its former next node; since node's element is `ManuallyDrop`, it can be safely reclaimed
        // at any time, regardless of an lifetime-bound references in `T`
        unsafe { unlinked.retire_unchecked() };
        Some(res)
    }

    #[inline]
    fn load_head<'g>(&self, order: Ordering, guard: &'g mut R::Guard) -> Shared<'g, Node<T, R>, R> {
        // both head and tail must point to a node (the sentinel)
        unsafe { self.head.load(order, guard).unwrap_unchecked() }
    }

    #[inline]
    fn load_tail<'g>(&self, order: Ordering, guard: &'g mut R::Guard) -> Shared<'g, Node<T, R>, R> {
        // both head and tail must point to a node (the sentinel)
        unsafe { self.tail.load(order, guard).unwrap_unchecked() }
    }
}

/********** impl Drop *****************************************************************************/

impl<T, R: GlobalReclaim> Drop for Queue<T, R> {
    #[inline]
    fn drop(&mut self) {
        // the sentinel is always present
        let mut sentinel = self.head.take().unwrap();

        let mut curr = sentinel.next.take();
        while let Some(mut node) = curr {
            // all non-sentinel nodes contain an initialized `T`, which can be dropped in place
            unsafe { ptr::drop_in_place(node.elem.as_mut_ptr()) };
            curr = node.next.take();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Node
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A queue node containing a generic element and an atomic next pointer.
struct Node<T, R> {
    elem: MaybeUninit<T>,
    next: Atomic<Node<T, R>, R>,
}

/********** impl inherent *************************************************************************/

impl<T, R> Node<T, R> {
    /// Creates a new sentinel [`Node`] with an uninitialized element that is
    /// never accessed or used.
    #[inline]
    fn sentinel() -> Self {
        Self {
            elem: MaybeUninit::uninit(),
            next: Atomic::null(),
        }
    }

    /// Creates a new [`Node`] with the given `elem`.
    #[inline]
    fn new(elem: T) -> Self {
        Self {
            elem: MaybeUninit::new(elem),
            next: Atomic::null(),
        }
    }
}
