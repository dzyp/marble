extern crate num_cpus;

use self::num_cpus::get;

use std::cmp::Ordering;
use std::sync::Arc;
use std::thread;
use std::vec::Vec;


const SYM_MERGE_AT: usize = 100;

fn get_indices(len_vec: usize, num_chunks: usize) -> Vec<usize> {
	let chunk_size = len_vec/num_chunks;
	let remainder = len_vec - chunk_size*num_chunks; // faster than modulo

	let mut first = Vec::with_capacity(num_chunks/2);
	let mut second = Vec::with_capacity(num_chunks/2);
	let mut placed = 0;
	for i in 0..num_chunks {
		let mut iter_chunk_size = chunk_size;
		if placed < remainder {
			iter_chunk_size += 1;
			placed += 1;
		}
		if i % 2 == 0 {
			first.push(iter_chunk_size);
		} else {
			second.push(iter_chunk_size);
		}
	}

	first.append(&mut second);
	first
}

pub fn symsort<T, F: FnMut(&T, &T) -> Ordering>(vec: &mut Vec<T>, compare: F) {
	if vec.len() < SYM_MERGE_AT {
		vec.sort_by(compare);
		return;
	}

	let mut num_cpu = get();
	if !num_cpu.is_power_of_two() {
		num_cpu = num_cpu.next_power_of_two();
	}

	let sizes = get_indices(vec.len(), num_cpu);
	let mut slices = vec![];
	let mut index = 0;
	for size in sizes {
		slices.push(&vec[index+size]);
		index += size;
	}

	let mut i = 0;
	let mut j = 0;
	let mut guards = vec![];
	loop {
		while j < slices.len() {
			let guard = thread::scoped(move || {

			});

			guards.push(guard);
			for guard in guards.drain() {
				guard.join();
			}
			j *= 2;
		}
	}

}

#[cfg(test)]
mod sorttest {
	use super::*;

	#[test]
	fn test_simple_chunk() {
		
	}
}