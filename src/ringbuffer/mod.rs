#![feature(test)]
#![feature(scoped)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::thread::yield_now;
use std::collections::vec_deque::VecDeque;
use std::vec::Vec;
use std::cell::UnsafeCell;
use std::cell::RefCell;
use std::clone::Clone;
//extern crate test;

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
	dequeue:   AtomicUsize,
	disposed:  AtomicUsize,
	mask:      usize,
	positions: UnsafeCell<Vec<Node<T>>>,
}

enum DisposedError { RingBufferDisposed }

impl<T> RingBuffer<T> {
	fn new(cap: usize) -> RingBuffer<T> {
		let calculated_capacity = cap.next_power_of_two();
		let mut positions = Vec::<Node<T>>::with_capacity(calculated_capacity);

		for i in 0..calculated_capacity {
			positions.push(Node::new(i as usize));
		}

		RingBuffer{
			queue: ATOMIC_USIZE_INIT,
			dequeue: ATOMIC_USIZE_INIT,
			disposed: ATOMIC_USIZE_INIT,
			mask: calculated_capacity-1,
			positions: UnsafeCell::new(positions),
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

	fn put(&self, item: T) {
		unsafe {
			let mut position = self.queue.load(Ordering::SeqCst);
			let mut i = 0;

			loop {
				if self.disposed.load(Ordering::SeqCst) == 1 {
					return;
				}

				let positions = self.positions.get();
				let n = (*positions).get_unchecked_mut(position&self.mask);
				let diff = n.position.load(Ordering::SeqCst) - position;
				if diff == 0 {
					if self.queue.compare_and_swap(position, position+1, Ordering::SeqCst) == position {
						n.item = Some(item);
						n.position.store(position+1, Ordering::SeqCst);
						return;
					}
				} else if diff < 0 {
					panic!("Ring buffer is in a compromised state in a put.");
				} else {
					position = self.queue.load(Ordering::SeqCst);
				}

				if i == 10000 {
					yield_now();
				} else {
					i += 1;
				}
			}
		}
	}

	fn get(&self) -> Result<T, DisposedError> {
		unsafe {
			let mut position = self.dequeue.load(Ordering::SeqCst);
			let mut i = 0;
			loop {
				if self.disposed.load(Ordering::SeqCst) == 1 {
					return Err(DisposedError::RingBufferDisposed);
				}
				let positions = self.positions.get();
				let n = (*positions).get_unchecked_mut(position&self.mask);
				let diff = n.position.load(Ordering::SeqCst) - (position + 1);
				if diff == 0 {
					if self.dequeue.compare_and_swap(position, position+1, Ordering::SeqCst) == position {
						let data = n.item.take().unwrap();
						n.position.store(position+self.mask+1, Ordering::SeqCst);
						return Ok(data);
					}
				} else if diff < 0 {
					panic!("Ring buffer in a compromised state during get.");
				} else {
					position = self.dequeue.load(Ordering::SeqCst);
				}
			}
		}
	}
}

#[test]
fn test_simple_put_get_works() {
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

#[cfg(test)]
mod durr {
    //extern crate test;
    use super::*;
    use std::collections::vec_deque::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::sync::mpsc;
    use std::sync::mpsc::{Sender, Receiver};



    fn rb_put() {
    	unsafe {
	    	//let mut rb = RingBuffer::new(10);
    		//rb.put(10);
    	}
    }

    /*
    #[bench]
    fn bench_rb_put(b: &mut Bencher) {
    	unsafe {
	    	b.iter(|| {
	    		let mut rb = RingBuffer::new(2);
		    	rb.put(1);
    		});
    	}
    }

    #[bench]
    fn bench_rb_puts(b: &mut Bencher) {
    	unsafe {
	    	b.iter(|| {
		    	let mut rb = RingBuffer::new(100);
	    		let mut guards = vec![];
	    		for i in 0..8 {
	    			let rb = &rb;
		    		let guard = thread::scoped(move || {
		    			for i in 0..10 {
		    				rb.put(i);
		    			}
	    			});
	    			guards.push(guard);
	    		}

	    		for g in guards {
	    			g.join();
	    		}
    		});
    	}
    }

    #[bench]
    fn bench_add_items(b: &mut Bencher) {
    	let mut vd = VecDeque::new();
    	let lock = Arc::new(Mutex::new(vd));

    	unsafe {
	    	b.iter(|| {
	    		let n = test::black_box(1000);
	    		let mut guards = vec![];
	    		for i in 0..8 {
	    			let lock = lock.clone();
		    		let guard = thread::scoped(move || {
		    			let mut vd = lock.lock().unwrap();
		    			for i in 0..1000 {
		    				vd.push_back(i);
		    			}

	    			});
	    			guards.push(guard);
	    		}

	    		for g in guards {
	    			g.join();
	    		}
    		});
    	}
    }

    #[bench]
    fn bench_add_item(b: &mut Bencher) {
    	b.iter(|| {
    		let mut vd = VecDeque::new();
    		let n = test::black_box(1000);

    		vd.push_back(1);
    	});
    }*/
}