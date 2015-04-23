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
/// rb.put(1);
/// rb.dispose();
/// join.join();
/// ```

use std::cell::UnsafeCell;
use std::default::Default;
use std::marker;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};
use std::thread::yield_now;
use std::vec::Vec;

// Node will wrap items in the buffer and keep track of enough
// meta data to know if queue and dequere are referencing the
// correct item.  This is achieved through the atomic position
// field.
struct Node<T> {
	item: 	  Option<T>,
	position: AtomicUsize,
}

impl<T> Node<T> {
	// new creates a new node with its atomic position set to
	// position.  No item by default.
	fn new(position: usize) -> Node<T> {
		Node {
			position: AtomicUsize::new(position),
			item: None,
		}
	}
}

/// RingBuffer is a threadsafe MPMC queue that's statically-sized.
/// Puts are blocked on a full queue and gets are blocked on an empty
/// queue.  In either case, calling dispose will free any blocked threads
/// and return an error.
pub struct RingBuffer<T> {
	queue: 	   AtomicUsize,
	// padding is here to ensure that queue and dequeue end on different
	// cache lines to prevent false sharing.
	_padding0: [u64;8],
	dequeue:   AtomicUsize,
	_padding1: [u64;8],
	disposed:  AtomicUsize,
	_padding2: [u64;8],
	mask:      usize,
	// positions is unsafecell so we can grab a node mutably in the
	// put and get functions, which themselves can be called on an
	// immutable ring buffer.
	positions: UnsafeCell<Vec<Node<T>>>,
}

// these implementations are required to access a ringbuffer in a separate
// lifetime and thread using only a wrapping Arc.
unsafe impl<T: Send> marker::Send for RingBuffer<T> {}
unsafe impl<T: Sync> marker::Sync for RingBuffer<T> {}

impl<T> Default for RingBuffer<T> {
	// this is convenient so we don't have to create 0 for atomics
	// and empty arrays with every constructor call.
	fn default() -> RingBuffer<T> {
		RingBuffer{
			queue:     ATOMIC_USIZE_INIT,
			_padding0: [0;8],
			dequeue:   ATOMIC_USIZE_INIT,
			_padding1: [0;8],
			disposed:  ATOMIC_USIZE_INIT,
			_padding2: [0;8],
			mask: 	   0,
			positions: UnsafeCell::new(vec![]),
		}
	}
}

/// RingBufferError is returned when a thread gets or puts on a disposed
/// RingBuffer.  Any blocked threads will be freed and will received this
/// error.
pub enum RingBufferError { Disposed }

impl<T> RingBuffer<T> {
	/// new creates a new RingBuffer with the specified capacity.  It
	/// is important to note that capacity will be rounded up to the next
	/// power of two.  This is done to improve performance by avoiding a 
	/// costly modulo call.  To get the actual capacity of the ring buffer
	/// call cap().
	pub fn new(cap: usize) -> RingBuffer<T> {
		let calculated_capacity = cap.next_power_of_two();
		let mut positions = Vec::<Node<T>>::with_capacity(calculated_capacity);

		for i in 0..calculated_capacity {
			positions.push(Node::new(i as usize));
		}

		RingBuffer{
			mask: calculated_capacity-1,
			positions: UnsafeCell::new(positions),
			..Default::default()
		}
	}

	/// cap returns the actual capacity of the ring buffer.  This
	/// different than length, which returns the actual number of
	/// items in the ring buffer.  If cap == len, then the ring
	/// buffer is full.
	pub fn cap(&self) -> usize {
		unsafe {
			let positions = self.positions.get();
			(*positions).len()
		}
	}

	/// len returns the number of items in the ring buffer at this
	/// moment.
	pub fn len(&self) -> usize {
		self.queue.load(Ordering::Relaxed) - self.dequeue.load(Ordering::Relaxed)
	}

	/// dispose frees any resources consumed by this ring buffer and
	/// releases any threads that are currently in spin locks.  Any subsequent
	/// calls to put or get will return an error.
	pub fn dispose(&self) {
		self.disposed.store(1, Ordering::Relaxed);
	}

	/// put will add item to the ring buffer.  If the ring buffer is full
	/// this operation blocks until it can be put.  Returns an error if
	/// this ring buffer is disposed.
	pub fn put(&self, item: T) -> Result<(), RingBufferError> {
		unsafe {
			let mut position = self.queue.load(Ordering::Relaxed);
			let mut i = 0;

			loop {
				if self.disposed.load(Ordering::Relaxed) == 1 {
					return Err(RingBufferError::Disposed);
				}

				let positions = self.positions.get();
				let n = (*positions).get_unchecked_mut(position&self.mask);
				let diff = n.position.load(Ordering::Acquire) - position;
				if diff == 0 {
					if self.queue.compare_and_swap(position, position+1, Ordering::Relaxed) == position {
						n.item = Some(item);
						n.position.store(position+1, Ordering::Release);
						return Ok(());
					}
				} else {
					position = self.queue.load(Ordering::Relaxed);
				}

				if i == 10000 {
					i = 0;
					yield_now();
				} else {
					i += 1;
				}
			}
		}
	}

	/// get retrieves the next item from the ring buffer.  This method blocks
	/// if the ring buffer is empty until an item is placed.  Returns an error
	/// if the ring buffer is disposed.
	pub fn get(&self) -> Result<T, RingBufferError> {
		unsafe {
			let mut position = self.dequeue.load(Ordering::Relaxed);
			let mut i = 0;
			loop {
				if self.disposed.load(Ordering::SeqCst) == 1 {
					return Err(RingBufferError::Disposed);
				}
				let positions = self.positions.get();
				let n = (*positions).get_unchecked_mut(position&self.mask);
				let diff = n.position.load(Ordering::Acquire) - (position + 1);
				if diff == 0 {
					if self.dequeue.compare_and_swap(position, position+1, Ordering::Relaxed) == position {
						let data = n.item.take().unwrap();
						n.position.store(position+self.mask+1, Ordering::Release);
						return Ok(data);
					}
				} else {
					position = self.dequeue.load(Ordering::Relaxed);
				}

				if i == 10000 {
					i = 0;
					yield_now();
				} else {
					i += 1;
				}
			}
		}
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

		let result = rb.put(3);
		match result {
			Ok(_) => panic!("Should return error."),
			_ => ()
		}
	}

	#[bench]
	fn bench_rb_put(b: &mut Bencher) {
		b.iter(|| {
			let rb = RingBuffer::new(2);
			rb.put(1);
		});
	}

	#[bench]
	fn bench_rb_get(b: &mut Bencher) {
		b.iter(|| {
			let rb = RingBuffer::new(2);
			rb.put(1);
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

		b.iter(|| {
			let rb = rb.clone();
			rb.put(1);
		});

		rb.dispose();
		join.join();
	}

	#[bench]
	fn bench_vecdeque(b: &mut Bencher) {
		let rb = VecDeque::new();
		let arc = Arc::new(Mutex::new(rb));

		let clone = arc.clone();
		thread::spawn(move || {
			loop {
				let mut rb = clone.lock().unwrap();
				let result = rb.pop_front();
				match result {
					Some(x) => {if x == 2 { break }},
					None => ()
				}
			}
		});

		b.iter(|| {
			let mut rb = arc.lock().unwrap();
			rb.push_back(1);
		});

		let mut rb = arc.lock().unwrap();
		rb.push_back(2);
	}
}