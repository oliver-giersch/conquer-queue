use std::cell::UnsafeCell;
use std::cmp;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{
    AtomicBool, AtomicUsize,
    Ordering::{Acquire, Relaxed, Release, SeqCst},
};

use reclaim::prelude::*;
use reclaim::typenum::U0;
use reclaim::GlobalReclaim;

type Atomic<T, R> = reclaim::Atomic<T, R, U0>;
type Owned<T, R> = reclaim::Owned<T, R, U0>;
type Shared<'g, T, R> = reclaim::Shared<'g, T, R, U0>;

////////////////////////////////////////////////////////////////////////////////////////////////////
// Queue
////////////////////////////////////////////////////////////////////////////////////////////////////

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
        let head: Owned<Node<T, R>, R> = Owned::new(Node::new());
        let ptr = Owned::into_marked_ptr(head);

        unsafe {
            Self {
                head: Atomic::from_raw(ptr),
                tail: Atomic::from_raw(ptr),
            }
        }
    }

    #[inline]
    pub fn push(&self, mut elem: T) {
        let mut guard = R::guard();
        loop {
            let tail = self.tail.load(Relaxed, &mut guard).unwrap();
            let idx: usize = tail.push_idx.fetch_add(1, SeqCst); // Acquire?
            if idx >= NODE_SIZE {
                if self.tail.load_raw(Relaxed) != tail.as_marked_ptr() {
                    continue;
                }

                match self.push_new_node(tail, elem) {
                    Ok(_) => return,
                    Err(e) => {
                        elem = e;
                        continue;
                    }
                };
            } else {
                let slot = &tail.elements[idx];
                unsafe { slot.write(elem) };
                slot.init.store(true, Release);
                return;
            }
        }
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        let mut guard = R::guard();
        loop {
            let head = self.head.load(SeqCst, &mut guard).unwrap();

            let pop_idx = head.pop_idx.load(SeqCst);
            let push_idx = head.push_idx.load(SeqCst);

            if push_idx >= pop_idx && head.next.load_unprotected(SeqCst).is_none() {
                return None;
            }

            let idx: usize = head.pop_idx.fetch_add(1, SeqCst);

            if idx >= NODE_SIZE {
                match head.next.load_unprotected(SeqCst) {
                    Some(next) => {
                        if let Ok(unlinked) =
                            self.head.compare_exchange(head, next, SeqCst, Relaxed)
                        {
                            unsafe { unlinked.retire_unchecked() };
                        }
                    }
                    None => return None,
                };
            } else {
                let slot = &head.elements[idx];
                if !slot.init.swap(false, Acquire) {
                    continue;
                }

                return Some(unsafe { slot.read() });
            }
        }
    }

    #[inline]
    fn push_new_node(&self, tail: Shared<Node<T, R>, R>, elem: T) -> Result<(), T> {
        match tail.next.load_unprotected(SeqCst) {
            None => {
                let node: Owned<Node<T, R>, R> = unsafe { Owned::new(Node::with_tentative(&elem)) };
                match tail
                    .next
                    .compare_exchange(Shared::none(), node, SeqCst, Relaxed)
                {
                    Ok(_) => {
                        mem::forget(elem);
                        Ok(())
                    }
                    Err(fail) => {
                        // if the insert fails, the tentative write is reversed and the node is
                        // de-allocated again
                        Owned::into_inner(fail.input).reset_tentative_and_drop();
                        Err(elem)
                    }
                }
            }
            Some(next) => {
                let _ = self.tail.compare_exchange(tail, next, SeqCst, Relaxed);
                Err(elem)
            }
        }
    }
}

impl<T, R: GlobalReclaim> Drop for Queue<T, R> {
    #[inline]
    fn drop(&mut self) {
        let mut curr = self.head.take();
        while let Some(mut node) = curr {
            curr = node.next.take();
            mem::drop(node);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Node
////////////////////////////////////////////////////////////////////////////////////////////////////

const NODE_SIZE: usize = 1024;

struct Node<T, R> {
    push_idx: AtomicUsize,       // CacheAligned
    pop_idx: AtomicUsize,        // CacheAligned
    next: Atomic<Node<T, R>, R>, // CacheAligned
    elements: [Slot<T>; NODE_SIZE],
}

impl<T, R> Node<T, R> {
    #[inline]
    fn new() -> Self {
        Self {
            push_idx: AtomicUsize::new(0),
            pop_idx: AtomicUsize::new(0),
            next: Atomic::null(),
            elements: unsafe { Self::init_elements() },
        }
    }

    #[inline]
    unsafe fn with_tentative(elem: &T) -> Self {
        let elements = Self::init_elements();

        let first = &elements[0];
        (&mut *first.inner.get())
            .as_mut_ptr()
            .copy_from_nonoverlapping(elem, 1);
        first.init.store(true, Relaxed);

        Self {
            push_idx: AtomicUsize::new(1),
            pop_idx: AtomicUsize::new(0),
            next: Atomic::null(),
            elements,
        }
    }

    #[inline]
    fn reset_tentative_and_drop(self) {
        self.push_idx.store(0, Relaxed);
    }

    unsafe fn init_elements() -> [Slot<T>; NODE_SIZE] {
        let mut uninit: MaybeUninit<[Slot<T>; NODE_SIZE]> = MaybeUninit::uninit();
        let first = uninit.as_mut_ptr() as *mut Slot<T>;

        for i in 0..NODE_SIZE {
            first.add(i).write(Slot::new());
        }

        uninit.assume_init()
    }
}

impl<T, R> Drop for Node<T, R> {
    #[inline]
    fn drop(&mut self) {
        let start: usize = self.pop_idx.load(Relaxed);
        let end: usize = cmp::min(self.push_idx.load(Relaxed), NODE_SIZE);

        // TODO: what if panic?
        for slot in &mut self.elements[start..end] {
            debug_assert!(slot.init.load(Relaxed));
            unsafe {
                let inner = &mut *slot.inner.get();
                ptr::drop_in_place(inner.as_mut_ptr());
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Slot
////////////////////////////////////////////////////////////////////////////////////////////////////

struct Slot<T> {
    inner: UnsafeCell<MaybeUninit<T>>,
    init: AtomicBool,
}

/********** impl inherent *************************************************************************/

impl<T> Slot<T> {
    #[inline]
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(MaybeUninit::uninit()),
            init: AtomicBool::new(false),
        }
    }

    #[inline]
    unsafe fn read(&self) -> T {
        (&*self.inner.get()).as_ptr().read()
    }

    #[inline]
    unsafe fn write(&self, elem: T) {
        (&mut *self.inner.get()).as_mut_ptr().write(elem)
    }
}
