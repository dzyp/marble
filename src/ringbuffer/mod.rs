#![allow(dead_code)]
// Copyright 2012-2014 Dustin Hiatt. See the COPYRIGHT
// file at the top-level directory.
// 
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
//
//! This buffer is similar to the buffer
//! described here: http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
//! with some minor additions.
//!
//! A statically-sized ring buffer that acts as an MPMC queue.  Differs
//! from the ring buffer-backed VecDeque in that this structure
//! is lockless and threadsafe.  Threadsafety is achieved using only
//! CAS operations, making this queue ideal for high throughput.  This
//! queue is not ideal for situations where a long wait is expected
//! as any getter is blocked in a spinlock until an item is put.  Locks
//! due to resource starvation are avoided by yielding the spin lock to
//! the scheduler every few thousand iterations.  This queue is cache-aware
//! and performs best with cache line sizes of 64 bytes.
//!
//! Because this queue is threadsafe, it is safe to pass an Arc around
//! to spawned threads and call put, without wrapping the struct itself
//! in a mutex.

//! Benchmarks:
//! test ringbuffer::rbtest::bench_rb_batch     ... bench:    443876 ns/iter (+/- 19276)
//! ringbuffer::rbtest::bench_rb_get       ... bench:        42 ns/iter (+/- 2)
//! ringbuffer::rbtest::bench_rb_lifecycle ... bench:        24 ns/iter (+/- 0)
//! ringbuffer::rbtest::bench_rb_put       ... bench:        36 ns/iter (+/- 5)
//! ringbuffer::rbtest::bench_vecdeque     ... bench:      2934 ns/iter (+/- 394)
//!
//! The final benchmark is comparable to the bench_rb_lifecycle in terms
//! of functionality.

/// #Examples
///
/// ```
/// use marble::ringbuffer::RingBuffer;
/// 
/// use std::sync::Arc;
/// use std::thread;
/// 
/// let rb = Arc::new(RingBuffer::new(1));
///
/// let rbc = rb.clone();
/// let join = thread::spawn(move || {
/// 	loop {
/// 		let result = rbc.get();
/// 		match result {
/// 			Err(_) => break,
/// 			_ => ()
///			}	
///		}
/// });
///
/// let _ = rb.put(1).unwrap();
/// rb.dispose();
/// join.join().ok().unwrap();
/// ```

use std::cell::UnsafeCell;
use std::default::Default;
use std::marker;
use std::mem;
use std::ptr;
use std::sync::atomic::{ATOMIC_BOOL_INIT, ATOMIC_USIZE_INIT, AtomicBool, AtomicUsize, Ordering};
use std::thread::yield_now;
use std::vec::Vec;

// Node will wrap items in the buffer and keep track of enough
// meta data to know if queue and dequere are referencing the
// correct item.  This is achieved through the atomic position
// field.
struct Node<T> {
	item: 	  UnsafeCell<T>,
	position: AtomicUsize,
}

impl<T> Node<T> {
	// new creates a new node with its atomic position set to
	// position.  No item by default.  Unsafe because it uses uninitialized memory.
	unsafe fn new(position: usize) -> Node<T> {
		Node {
			position: AtomicUsize::new(position),
			item: mem::uninitialized(),
		}
	}
}

/// RingBuffer is a threadsafe MPMC queue that's statically-sized.
/// Puts are blocked on a full queue and gets are blocked on an empty
/// queue.  In either case, calling dispose will free any blocked threads
/// and return an error.
#[repr(C)]
pub struct RingBuffer<T> {
	queue: 	   AtomicUsize,
	// padding is here to ensure that queue and dequeue end on different
	// cache lines to prevent false sharing.
	_padding0: [u64;8],
	dequeue:   AtomicUsize,
	_padding1: [u64;8],
	disposed:  AtomicBool,
	_padding2: [u64;8],
	mask:	  usize,
	positions: Vec<Node<T>>,
}

impl<T> Drop for RingBuffer<T> {
	fn drop(&mut self) {
		let mut start = self.queue.load(Ordering::Relaxed) & self.mask;
		let end = self.dequeue.load(Ordering::Relaxed) & self.mask;
		unsafe {
			// Setting the length to 0 will prevent Vec from trying to run destructors for nodes
			// that aren't actually initialized.  We free the elements explicitly instead.  We do
			// it before dropping any elements in case dropping the element's destructor leads to a
			// panic.
			self.positions.set_len(0);
			while start != end {
				ptr::read((self.positions.get_unchecked(start)).item.get());
				start = start.wrapping_add(1) & self.mask;
			}
		}
	}
}

// these implementations are required to access a ringbuffer in a separate
// lifetime and thread using only a wrapping Arc.
unsafe impl<T> marker::Send for RingBuffer<T> where T: Send {}
unsafe impl<T> marker::Sync for RingBuffer<T> where T: Send {}

impl<T> Default for RingBuffer<T> {
	// this is convenient so we don't have to create 0 for atomics
	// and empty arrays with every constructor call.
	fn default() -> RingBuffer<T> {
		RingBuffer::new(0)
	}
}

/// RingBufferError is returned when a thread gets or puts on a disposed
/// RingBuffer.  Any blocked threads will be freed and will received this
/// error.
#[derive(Copy,Clone,Debug)]
pub enum RingBufferError { Disposed }

impl<T> RingBuffer<T> {
	/// new creates a new RingBuffer with the specified capacity.  It
	/// is important to note that capacity will be rounded up to the next
	/// power of two.  This is done to improve performance by avoiding a 
	/// costly modulo call.  To get the actual capacity of the ring buffer
	/// call cap().
	pub fn new(cap: usize) -> RingBuffer<T> {
		let calculated_capacity = if cap < 2 {
			// This special case is necessary because of how we encode filled cells; in particular,
			// we assume that x + 1 != x + capacity, since position = x + 1 means a cell is filled
			// and the next cell we attempt to look at when calling put is at index (x + 1) & mask.
			// It also can't be zero because mask = cap - 1.
			2
		} else {
			cap.next_power_of_two()
		};

		
			// We really just want to use raw allocation, but unfortunately that isn't stable yet.
			let mut positions = Vec::with_capacity(calculated_capacity);

			// It's important that this not panic, since it would lead to uninitialized memory
			// being freed.
			for i in 0..calculated_capacity {
				unsafe { positions.push(Node::new(i)) };
			}

			RingBuffer{
				queue:	 ATOMIC_USIZE_INIT,
				_padding0: [0;8],
				dequeue:   ATOMIC_USIZE_INIT,
				_padding1: [0;8],
				disposed:  ATOMIC_BOOL_INIT,
				_padding2: [0;8],
				mask: calculated_capacity-1,
				positions: positions,
			}
		
	}

	/// cap returns the actual capacity of the ring buffer.  This
	/// different than length, which returns the actual number of
	/// items in the ring buffer.  If cap == len, then the ring
	/// buffer is full.
	#[inline]
	pub fn cap(&self) -> usize {
		self.mask + 1
	}

	/// len returns the number of items in the ring buffer at this
	/// moment.
	pub fn len(&self) -> usize {
		// Saturating subtract is required, because we might see a negative value here (there is no
		// order dependency between the load of the queue length and dequeue length).
		self.queue.load(Ordering::Relaxed).saturating_sub(self.dequeue.load(Ordering::Relaxed))
	}

	/// dispose frees any resources consumed by this ring buffer and
	/// releases any threads that are currently in spin locks.  Any subsequent
	/// calls to put or get will return an error.
	pub fn dispose(&self) {
		self.disposed.store(true, Ordering::Relaxed);
	}

	/// Convenience method shared by both get and put that ensures exclusive access to a node's
	/// item.  If the ring buffer is full the operation blocks until it can be executed.  Returns
	/// an error if this ring buffer is disposed.
	fn with_unique<F,G,U>(&self, queue: &AtomicUsize, unlocked: F, op: G)
						 -> Result<U, RingBufferError>
		where F: Fn(usize) -> usize,
			  G: FnOnce(&Node<T>, usize) -> U,
	{
		let mut position = queue.load(Ordering::Relaxed);

		while !self.disposed.load(Ordering::Relaxed) {
			const MAX_SPINS: u16 = 10000;

			let mut spins = MAX_SPINS - 1;
			while spins != 0 {
				let n = unsafe {
					// We know the index is in bounds because mask = cap - 1.
					self.positions.get_unchecked(position & self.mask)
				};
				if n.position.load(Ordering::Acquire) == unlocked(position) {
					let next = position.wrapping_add(1);
					let old = queue.compare_and_swap(position, next, Ordering::Relaxed);
					if old == position {
						return Ok(op(n, next));
					}
				} else {
					position = queue.load(Ordering::Relaxed);
				}
				spins -= 1;
			}

			yield_now();
		}

		Err(RingBufferError::Disposed)
	}

	/// put will add item to the ring buffer.  If the ring buffer is full
	/// this operation blocks until it can be put.  Returns an error if
	/// this ring buffer is disposed.
	pub fn put(&self, data: T) -> Result<(), RingBufferError> {
		self.with_unique(&self.queue, |p| p, |n, p| {
			unsafe { ptr::write(n.item.get(), data) };
			n.position.store(p, Ordering::Release);
		})
	}

	/// get retrieves the next item from the ring buffer.  This method blocks
	/// if the ring buffer is empty until an item is placed.  Returns an error
	/// if the ring buffer is disposed.
	pub fn get(&self) -> Result<T, RingBufferError> {
		self.with_unique(&self.dequeue, |p| p.wrapping_add(1), |n, p| {
			let data = unsafe { ptr::read(n.item.get()) };
			n.position.store(p.wrapping_add(self.mask), Ordering::Release);
			data
		})
	}
}

#[cfg(test)]
#[allow(unused_must_use)]
mod rbtest {
	extern crate test;

	use self::test::Bencher;

	use std::collections::vec_deque::VecDeque;
	use std::sync::{Arc, Mutex};
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::mpsc::channel;
	use std::thread;

	use super::*;

   	#[test]
	fn test_simple_put_get() {
		let rb = RingBuffer::new(10);
		let number = 5;
		rb.put(number);
		assert_eq!(1, rb.len());
		let result = rb.get();
		match result {
			Ok(x) => assert_eq!(x, 5),
			Err(x) => panic!(x)
		}
	}

	#[test]
	fn test_fill_and_empty() {
		let rb = RingBuffer::new(8);
		for i in 0..rb.cap() {
			rb.put(i);
		}

		for i in 0..rb.cap() {
			let result = rb.get();
			match result {
				Ok(x) => assert_eq!(x, i),
				Err(x) => panic!(x)
			}
		}

		assert_eq!(0, rb.len());
	}

	#[test]
	fn test_fill_and_dispose() {
		let rb = RingBuffer::new(8);
		let arb = Arc::new(rb);
		let mut vec = vec![];

		for i in 0..arb.cap()+1 {
			let trb = arb.clone();
			let join = thread::spawn(move || {
				trb.put(i);
			});

			vec.push(join);
		}

		arb.dispose();
		for j in vec {
			j.join();
		}
	}

	#[test]
	fn test_get_put_on_dispose() {
		let rb = RingBuffer::new(2);
		rb.dispose();

		let result = rb.get();
		match result {
			Ok(_) => panic!("Should return error."),
			_ => () 
		}

		let result = rb.put(());
		match result {
			Ok(_) => panic!("Should return error."),
			_ => ()
		}
	}

	#[bench]
	fn bench_rb_put(b: &mut Bencher) {
		b.iter(|| {
			let rb = RingBuffer::new(2);
			rb.put(());
		});
	}

	#[bench]
	fn bench_rb_get(b: &mut Bencher) {
		b.iter(|| {
			let rb = RingBuffer::new(2);
			rb.put(());
			rb.get();
		});
	}

	#[bench]
	fn bench_rb_batch(b: &mut Bencher) {
		b.iter(|| {
			const NUM_ITEMS:usize = 1000;
			let rb = Arc::new(RingBuffer::new(NUM_ITEMS));
			let num_done = Arc::new(AtomicUsize::new(0));
			let num_sent = Arc::new(AtomicUsize::new(0));
			let (tx, rx) = channel();

			for _ in 0..8 {
				let rb = rb.clone();
				let tx = tx.clone();
				let num_done = num_done.clone();
				thread::spawn(move || {
					loop {
						let result = rb.get();

						match result {
							Ok(x) => {
								num_done.fetch_add(1, Ordering::SeqCst);
								if x == NUM_ITEMS-2 {
									tx.send(()).unwrap();
								}
							},
							_ => break
						}
					}
				});
			}

			for _ in 0..8 {
				let rb = rb.clone();
				let num_sent = num_sent.clone();
				thread::spawn(move || {
					loop {
						let previous = num_sent.fetch_add(1, Ordering::SeqCst);
						if previous >= NUM_ITEMS {
							break
						}
						rb.put(previous);
					}
				});
			}

			rx.recv().unwrap();
			rb.dispose();
		});
	}

	#[bench]
	fn bench_rb_lifecycle(b: &mut Bencher) {
		let rb = Arc::new(RingBuffer::new(1));

		let rbc = rb.clone();
		let join = thread::spawn(move || {
			loop {
				let result = rbc.get();
				match result {
					Err(_) => break,
					_ => ()
				}
			}
		});

		b.iter( || {
			let rb = rb.clone();
			rb.put(());
		});

		rb.dispose();
		join.join();
	}

	#[bench]
	fn bench_vecdeque(b: &mut Bencher) {
		let rb = VecDeque::new();
		let arc = Arc::new(Mutex::new(rb));

		enum Msg { NoOp, Stop }

		let clone = arc.clone();
		thread::spawn(move || {
			loop {
				let mut rb = clone.lock().unwrap();
				if let Some(Msg::Stop) = rb.pop_front() {
					break
				}
			}
		});

		b.iter(|| {
			let mut rb = arc.lock().unwrap();
			rb.push_back(Msg::NoOp);
		});

		let mut rb = arc.lock().unwrap();
		rb.push_back(Msg::Stop);
	}
}
