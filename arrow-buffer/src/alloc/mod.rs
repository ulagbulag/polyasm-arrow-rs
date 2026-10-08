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

//! Defines the low-level [`Allocation`] API for shared memory regions

use alloc_crate::sync::Arc;
use core::alloc::Layout;
use core::fmt::{Debug, Formatter};
use core::panic::RefUnwindSafe;

mod alignment;

pub use alignment::ALIGNMENT;

/// The owner of an allocation.
/// The trait implementation is responsible for dropping the allocations once no more references exist.
pub trait Allocation: RefUnwindSafe + Send + Sync {}

impl<T: RefUnwindSafe + Send + Sync> Allocation for T {}

/// Mode of deallocating memory regions
pub(crate) enum Deallocation {
    /// An allocation using [`std::alloc`]
    Standard(Layout),
    /// An allocation from an external source like the FFI interface
    /// Deallocation will happen on `Allocation::drop`
    /// The size of the allocation is tracked here separately only
    /// for memory usage reporting via `Array::get_buffer_memory_size`
    Custom(Arc<dyn Allocation>, usize),
}

impl Debug for Deallocation {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        match self {
            Deallocation::Standard(layout) => {
                write!(f, "Deallocation::Standard {layout:?}")
            }
            Deallocation::Custom(_, size) => {
                write!(f, "Deallocation::Custom {{ capacity: {size} }}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::alloc::Deallocation;

    #[test]
    fn test_size_of_deallocation() {
        assert_eq!(
            core::mem::size_of::<Deallocation>(),
            3 * core::mem::size_of::<usize>()
        );
    }
}
