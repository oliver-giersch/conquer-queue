use std::cell::UnsafeCell;
use std::cmp;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{
    AtomicPtr, AtomicUsize,
    Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
// Queue
////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Queue<T> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
}

/********** impl Default **************************************************************************/

impl<T> Default for Queue<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/********** impl inherent *************************************************************************/

impl<T> Queue<T> {
    const RETRY_READ: usize = 128;

    #[inline]
    pub fn new() -> Self {
        let head = Box::leak(box Node::new());

        Self {
            head: AtomicPtr::new(head),
            tail: AtomicPtr::new(head),
        }
    }

    #[inline]
    pub fn push(&self, mut elem: T) {
        loop {
            let tail_ptr = self.tail.load(Acquire);
            let tail = unsafe { &*tail_ptr };
            let idx: usize = tail.push_idx.fetch_add(1, AcqRel);

            if idx >= NODE_SIZE {
                if self.tail.load(Relaxed) != tail_ptr {
                    continue;
                }

                match unsafe { self.insert_new_node(tail_ptr, elem) } {
                    Ok(_) => return,
                    Err(e) => {
                        elem = e;
                        continue;
                    }
                };
            } else {
                let slot = &tail.elements[idx];
                // FIXME:
                //  - write tentative
                //  - CAS state(UNINIT, INIT)
                //  - success: forget elem, return
                //  - failure: state == ABANDONED, retry
                unsafe { slot.write(elem) };
                // FIXME: slot.state.store(INIT; release); // or fetch_or? deps on pop
                //   slot.init.store(true, Release);
                return;
            }
        }
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        loop {
            let head_ptr = self.head.load(Acquire);
            let head = unsafe { &*head_ptr };

            let pop_idx = head.pop_idx.load(AcqRel);
            let push_idx = head.push_idx.load(AcqRel);

            if push_idx >= pop_idx && head.next.load(SeqCst).is_null() {
                return None;
            }

            let idx: usize = head.pop_idx.fetch_add(1, SeqCst);

            if idx < NODE_SIZE - 1 {
                let slot = &head.elements[idx];

                // loop x times while state UNINIT
                // if init -> fetch_or(CONSUMED, AcqRel) + read value;
                // else CAS(UNINIT, ABANDONED)
                //   success: continue; // slot is abandoned, won't be written to ever (init destruction)
                //   failure: fetch_or(CONSUMED, AcqRel) + read value

                for _ in 0..Self::RETRY_READ {
                    const RDY: usize = 88;
                    if slot.state.load(Acquire) == RDY {
                        break;
                    }

                    // back-off
                }

            //

            // what if node uninit? -> spin a few times, else abandon the slot
            // read value and destroy slot, may take over node destruction (cold)
            } else if idx == NODE_SIZE - 1 {
                // read value, initiate node destruction, try to unlink node
            } else {
                // try help to unlink
            }

            if idx >= NODE_SIZE {
                match head.next.load(Acquire) {
                    ptr if ptr.is_null() => return None,
                    next => {
                        if self.head.compare_and_swap(head_ptr, next, Release) == head_ptr {
                            unimplemented!()
                        }
                    }
                }
            }

            // head node is already drained, try to unlink and retry, or return None,
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

    unsafe fn insert_new_node(&self, tail: *mut Node<T>, elem: T) -> Result<(), T> {
        match (*tail).next.load(Acquire) {
            ptr if ptr.is_null() => {
                let node = Box::leak(box Node::with_tentative(&elem));
                if (*tail)
                    .next
                    .compare_exchange(ptr::null_mut(), node, SeqCst, Relaxed)
                    .is_ok()
                {
                    mem::forget(elem);
                    Ok(())
                } else {
                    Box::from_raw(node).reset_tentative_and_drop();
                    Err(elem)
                }
            }
            next => {
                self.tail.compare_and_swap(tail, next, Release);
                Err(elem)
            }
        }
    }
}

impl<T> Drop for Queue<T> {
    #[inline]
    fn drop(&mut self) {
        unimplemented!()

        /*let mut curr = self.head.take();
        while let Some(mut node) = curr {
            curr = node.next.take();
            mem::drop(node);
        }*/
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Node
////////////////////////////////////////////////////////////////////////////////////////////////////

const NODE_SIZE: usize = 1024;

struct Node<T> {
    push_idx: AtomicUsize,    // CacheAligned
    pop_idx: AtomicUsize,     // CacheAligned
    next: AtomicPtr<Node<T>>, // CacheAligned
    elements: [Slot<T>; NODE_SIZE],
}

impl<T> Node<T> {
    #[inline]
    fn new() -> Self {
        Self {
            push_idx: AtomicUsize::new(0),
            pop_idx: AtomicUsize::new(0),
            next: AtomicPtr::new(ptr::null_mut()),
            elements: unsafe { Self::init_elements_arr() },
        }
    }

    #[inline]
    unsafe fn with_tentative(elem: &T) -> Self {
        let elements = Self::init_elements_arr();

        let first = &elements[0];
        (&mut *first.inner.get())
            .as_mut_ptr()
            .copy_from_nonoverlapping(elem, 1);
        first.init.store(true, Relaxed);

        Self {
            push_idx: AtomicUsize::new(1),
            pop_idx: AtomicUsize::new(0),
            next: AtomicPtr::new(ptr::null_mut()),
            elements,
        }
    }

    #[inline]
    fn reset_tentative_and_drop(self) {
        self.push_idx.store(0, Relaxed);
    }

    #[inline]
    unsafe fn init_elements_arr() -> [Slot<T>; NODE_SIZE] {
        let mut uninit: MaybeUninit<[Slot<T>; NODE_SIZE]> = MaybeUninit::uninit();
        let first = uninit.as_mut_ptr() as *mut Slot<T>;

        for i in 0..NODE_SIZE {
            first.add(i).write(Slot::new());
        }

        uninit.assume_init()
    }
}

impl<T> Drop for Node<T> {
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

const UNINIT: usize = 0;

struct Slot<T> {
    inner: UnsafeCell<MaybeUninit<T>>,
    state: AtomicUsize,
}

/********** impl inherent *************************************************************************/

impl<T> Slot<T> {
    #[inline]
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(MaybeUninit::uninit()),
            state: AtomicUsize::new(UNINIT),
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
