// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[cfg(not(feature = "std"))]
#[allow(unused_imports)]
use alloc::{
    borrow::ToOwned,
    boxed::Box,
    string::{String, ToString},
    vec::Vec,
};

#[macro_use]
pub mod bit_util;
mod bit_pack;
#[cfg(feature = "std")]
pub(crate) mod interner;

pub mod push_buffers;
#[cfg(any(test, feature = "test_common"))]
pub(crate) mod test_common;
pub mod utf8;

#[cfg(any(test, feature = "test_common"))]
pub use self::test_common::page_util::{
    DataPageBuilder, DataPageBuilderImpl, InMemoryPageIterator,
};

/// The largest decimal precision `bits` bits of two's-complement magnitude hold,
/// that is `floor(log10(2^(bits - 1) - 1))`.
///
/// A `no_std` build has no `f64::powi`/`log10`, and beyond 128 bits no integer
/// type holds the intermediate either, so the decimal digits of `2^(bits - 1)`
/// are carried directly. `2^k` ends in 2, 4, 6 or 8 for every `k >= 1`, so
/// subtracting the one keeps every digit and the count is the same.
pub(crate) fn max_precision_for_bits(bits: i32) -> u32 {
    if bits < 2 {
        return 0;
    }
    // Decimal digits of the running power of two, least significant first.
    let mut digits = alloc::vec![1_u8];
    for _ in 1..bits {
        let mut carry = 0_u8;
        for digit in digits.iter_mut() {
            let doubled = *digit * 2 + carry;
            *digit = doubled % 10;
            carry = doubled / 10;
        }
        if carry != 0 {
            digits.push(carry);
        }
    }
    digits.len() as u32 - 1
}
