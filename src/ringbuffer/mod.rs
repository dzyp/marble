#![allow(dead_code)]
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::thread::yield_now;
use std::vec::Vec;
use std::cell::UnsafeCell;
use std::marker;
use std::default::Default;

struct Node<T> {
	item: 	  Option<T>,
	position: AtomicUsize,
}

impl<T> Node<T> {
	fn new(position: usize) -> Node<T> {
		Node {
			position: AtomicUsize::new(position),
			item: None,
		}
	}
}

pub struct RingBuffer<T> {
	queue: 	   AtomicUsize,
	_padding0: [u64;8],
	dequeue:   AtomicUsize,
	_padding1: [u64;8],
	disposed:  AtomicUsize,
	_padding2: [u64;8],
	mask:      usize,
	positions: UnsafeCell<Vec<Node<T>>>,
}

unsafe impl<T> marker::Send for RingBuffer<T> {}
unsafe impl<T> marker::Sync for RingBuffer<T> {}

impl<T> Default for RingBuffer<T> {
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

enum RingBufferError { Disposed }

impl<T> RingBuffer<T> {
	fn new(cap: usize) -> RingBuffer<T> {
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

	fn cap(&self) -> usize {
		unsafe {
			let positions = self.positions.get();
			(*positions).len()
		}
	}

	fn len(&self) -> usize {
		self.queue.load(Ordering::SeqCst) - self.dequeue.load(Ordering::SeqCst)
	}

	fn dispose(&self) {
		self.disposed.store(1, Ordering::Relaxed);
	}

	fn put(&self, item: T) {
		unsafe {
			let mut position = self.queue.load(Ordering::Relaxed);
			let mut i = 0;

			loop {
				if self.disposed.load(Ordering::Relaxed) == 1 {
					return;
				}

				let positions = self.positions.get();
				let n = (*positions).get_unchecked_mut(position&self.mask);
				let diff = n.position.load(Ordering::Acquire) - position;
				if diff == 0 {
					if self.queue.compare_and_swap(position, position+1, Ordering::Relaxed) == position {
						n.item = Some(item);
						n.position.store(position+1, Ordering::Release);
						return;
					}
				} else {
					position = self.queue.load(Ordering::Relaxed);
				}

				if i == 10000 {
					yield_now();
				} else {
					i += 1;
				}
			}
		}
	}

	fn get(&self) -> Result<T, RingBufferError> {
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
    use super::*;
    use std::collections::vec_deque::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use self::test::Bencher;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::channel;

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