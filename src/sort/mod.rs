extern crate num_cpus;

use self::num_cpus::get;

use std::cmp::Ordering;
use std::thread;
use std::vec::Vec;


const SYM_MERGE_AT: usize = 4;

// sym_binarySearch will perform a binary search between the provided
// indices and find the index at which a rotation should occur.
fn sym_binary_search<T: Sync+Send+Ord>(slice: &mut [T], start: usize, stop: usize, total: usize) -> usize {
	let mut a = start;
	let mut b = stop;
	while a < b {
		let mid = (a + b) / 2;
		if slice[mid].cmp(&slice[total-mid]) != Ordering::Greater {
			a = mid + 1;
		} else {
			b = mid;
		}
	}

	a
}

// sym_swap will perform a rotation or swap between the provided
// indices.  Again, there is duplication here with swap, but
// we are buying performance.
fn sym_swap<T: Sync+Send+Ord>(slice: &mut [T], start1: usize, start2: usize, end: usize) {
	let mut i = 0;
	while i < end {
		slice.swap(start1+i, start2+i);
		i += 1;
	}
}

// sym_rotate determines the indices to use in a symSwap and
// performs the swap.
fn sym_rotate<T: Ord+Sync+Send>(slice: &mut [T], start1: usize, start2: usize, end: usize) {
	let mut i = start2 - start1;
	if i == 0 {
		return;
	}

	let mut j = end - start2;
	if j == 0 {
		return;
	}

	if i == j {
		sym_swap(slice, start1, start2, i);
		return;
	}

	let p = start1 + i;
	while i != j {
		if i > j {
			sym_swap(slice, p-i, p, j);
			i -= j;
		} else {
			sym_swap(slice, p-i, p+j-i, i);
			j -= i;
		}
	}
	sym_swap(slice, p-i, p, i);
}

fn create_and_sort_buckets<'a, T: Sync+Send+Ord>(vec: &'a mut [T], sizes: &Vec<usize>) {
	let mut slices: Vec<&'a mut [T]> = vec![];
	slices.push(vec);
	for size in sizes {
		let end = slices.pop().unwrap();
		let (chunk, rest) = end.split_at_mut(*size);
		slices.push(chunk);
		slices.push(rest);
	}
 
	let mut guards = vec![];
	while let Some(chunk) = slices.pop() {
		let guard = thread::scoped(move || {
			chunk.sort();
		});
		guards.push(guard);
	}

	for guard in guards {
		guard.join();
	}
}

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

// sym_merge is the recursive and internal form of SymMerge.
#[allow(unused_assignments)]
fn sym_merge<T: Sync+Send+Ord>(slice: &mut [T], start1: usize, start2: usize, last: usize) {
	if start1 < start2 && start2 < last {
		let mid = (start1 + last) / 2;
		let n = mid + start2;
		let mut start = 0;
		if start2 > mid {
			start = sym_binary_search(slice, n-last, mid, n-1);
		} else {
			start = sym_binary_search(slice, start1, start2, n-1);
		}
		let end = n - start;

		sym_rotate(slice, start, start2, end);
		sym_merge(slice, start1, start, mid);
		sym_merge(slice, mid, end, last);
	}
}

pub fn symsort<T: Sync+Send+Ord>(slice: &mut [T]) {
	if slice.len() == 0 {
		return;
	}

	if slice.len() < SYM_MERGE_AT {
		slice.sort();
		return;
	}

	let mut num_cpu = get();
	if !num_cpu.is_power_of_two() {
		num_cpu = num_cpu.next_power_of_two();
	}

	let mut sizes = get_indices(slice.len(), num_cpu);
	
	create_and_sort_buckets(slice, &sizes);
	let slice_len = slice.len();

	loop {
		let mut i = 0;
		let mut guards = vec![];
		{
			let mut slices: Vec<&mut [T]> = vec![];
			slices.push(slice);
			while i < slice_len {
				let end = slices.pop().unwrap();
				let left_size = sizes.remove(0);
				let right_size = sizes.remove(0);
				let (mut chunk, rest) = end.split_at_mut(left_size+right_size);
				slices.push(rest);
				sizes.push(left_size+right_size);
				i += left_size+right_size;
				let guard = thread::scoped(move || {
					sym_merge(&mut chunk, 0, left_size, left_size+right_size);
				});
				guards.push(guard);
			}
		}

		for guard in guards.drain() {
			guard.join();
		}

		if sizes.len() == 1 {
			break
		}
	}
}

#[cfg(test)]
#[allow(unused_attributes, dead_code)]
mod sorttest {
	extern crate test;
	extern crate quickcheck;
	extern crate rand;

	use self::quickcheck::*;
	use self::test::Bencher;

	use std::vec::Vec;

	use super::*;

	fn sort(items: Vec<isize>) -> bool {
		if items.len() == 0 {
			let mut cp1 = items.to_vec();
			symsort(&mut cp1);
			return true;
		}

		let mut cp1 = items.to_vec();
		let mut cp2 = items.to_vec();
		symsort(cp2.as_mut_slice());
		cp1.sort();
		for i in 0..cp1.len() {
			if cp1[i] != cp2[i] {
				return false;
			}
		}

		return true;
	}

	fn generate_random_vector(number: usize) -> Vec<isize> {
		let mut rands = Vec::with_capacity(number);
		for _ in 0..number {
			rands.push(rand::random());
		}

		rands
	}

	#[test]
	fn test_check_power_2_sort() {
		quickcheck(sort as fn(Vec<isize>) -> bool);
	}

	#[bench]
	fn bench_symsort(b: &mut Bencher) {
		const NUM_ITEMS:usize = 1000;
		b.iter(|| {
			let mut to_sort = generate_random_vector(NUM_ITEMS);
			symsort(to_sort.as_mut_slice());
		});
	}

	#[bench]
	fn bench_std_sort(b: &mut Bencher) {
		const NUM_ITEMS:usize = 1000;
		b.iter(|| {
			let mut to_sort = generate_random_vector(NUM_ITEMS);
			to_sort.sort();
		});
	}
}