#![allow(dead_code, unused_assignments)]
// Copyright 2012-2014 Dustin Hiatt. See the COPYRIGHT
// file at the top-level directory.
// 
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// n defines the maximum power of 2 that can define a bound,
// this is the value for 2-d space if you want to support
// all hilbert ids with a single integer variable
const N: i64 = 1 << 31;

fn bool_to_int(value: &bool) -> i32 {
	if *value {
		return 1i32;
	}

	return 0i32;
}

fn rotate(s: &i32, rx: &i32, ry: &i32, x: &mut i32, y: &mut i32) {
	if *ry == 0 {
		if *rx == 1 {
			*x = *s - 1 - *x;
			*y = *s - 1 - *y;
		}

		let t = *x;
		*x = *y;
		*y = t;
	}
}

// Encode will encode the provided x and y coordinates into a Hilbert
// distance.
pub fn encode(x: &i32, y: &i32) -> i64 {
	let mut xc = *x;
	let mut yc = *y;
	let mut rx = 0i32;
	let mut ry = 0i32;
	let mut d = 0i64;
	let mut s = (N/2) as i32;

	while s > 0 {
		rx = bool_to_int(&(x&s > 0));
		ry = bool_to_int(&(y&s > 0));
		d += s as i64 * s as i64 * ((3*rx) ^ ry) as i64;
		rotate(&s, &rx, &ry, &mut xc, &mut yc);
		s /= 2;
	}

	d
}

// Decode will decode the provided Hilbert distance into a corresponding
// x and y value, respectively.
pub fn decode(h: &i64) -> (i32, i32) {
	let mut ry = 0i64;
	let mut rx = 0i64;
	let mut x = 0i32;
	let mut y = 0i32;
	let mut t = *h;
	let mut s = 1i64;

	while s < N as i64 {
		rx = 1 & (t/2);
		ry = 1 & (t ^ rx);
		rotate(&(s as i32), &(rx as i32), &(ry as i32), &mut x, &mut y);
		x += (s * rx) as i32;
		y += (s * ry) as i32;
		t /= 4; 
		s *= 2;
	}

	(x, y)
}

#[cfg(test)]
mod hilberttest {
	extern crate test;

	use self::test::{black_box, Bencher};

	use std::i32::MAX;

	use super::*;

	#[test]
	fn test_hilbert() {
		let h = encode(&0, &0);
		let (x, y) = decode(&h);
		assert_eq!(0, h);
		assert_eq!(0, x);
		assert_eq!(0, y);

		let h = encode(&1, &0);
		let (x, y) = decode(&h);
		assert_eq!(3, h);
		assert_eq!(1, x);
		assert_eq!(0, y);

		let h = encode(&1, &1);
		let (x, y) = decode(&h);
		assert_eq!(2, h);
		assert_eq!(1, x);
		assert_eq!(1, y);

		let h = encode(&0, &1);
		let (x, y) = decode(&h);
		assert_eq!(1, h);
		assert_eq!(0, x);
		assert_eq!(1, y);
	}

	#[test]
	fn test_hilbert_at_max_range() {
		let x = MAX;
		let y = MAX;
		let h = encode(&x, &y);
		let (resultx, resulty) = decode(&h);
		assert_eq!(x, resultx);
		assert_eq!(y, resulty);
	}

	#[bench]
	fn bench_encode(b: &mut Bencher) {
		let mut a = 0;
		b.iter(|| {
			let h = encode(&a, &1);
			black_box(h);
			a += 1;
		});
	}

	#[bench]
	fn bench_decode(b: &mut Bencher) {
		b.iter(|| {
			let (x, _) = decode(&(MAX as i64));
			black_box(x);
		});
	}
}