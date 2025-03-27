// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::cmp::Ordering::Less;
use std::intrinsics::assume;
use std::mem;
use std::ptr;

use databend_common_base::runtime::drop_guard;
use databend_common_column::bitmap::MutableBitmap;
use fixed_size::fixed_size32_ord;

use crate::types::fixed_size::try_downcast_fixed_size32;
use crate::types::fixed_size::FixedSize32;
use crate::types::*;
use crate::with_number_mapped_type;
use crate::Column;
use crate::Scalar;

#[derive(Clone)]
pub struct TopKSorter {
    data: Vec<FixedSize32>,
    limit: usize,
    asc: bool,
}

impl TopKSorter {
    pub fn new(limit: usize, asc: bool) -> Self {
        Self {
            data: Vec::with_capacity(limit),
            limit,
            asc,
        }
    }

    // Push the column into this sorted and update the bitmap
    // The bitmap could be used in filter
    pub fn push_column(&mut self, col: &Column, bitmap: &mut MutableBitmap) {
        let fixed_size = try_downcast_fixed_size32(col).unwrap();
        let cmp = fixed_size32_ord(&col.data_type()).unwrap();
        self.push_column_internal(&fixed_size, bitmap, cmp);
    }

    fn push_column_internal(
        &mut self,
        col: &Buffer<FixedSize32>,
        bitmap: &mut MutableBitmap,
        cmp: fn(&FixedSize32, &FixedSize32) -> std::cmp::Ordering,
    ) {
        for (i, value) in col.iter().copied().enumerate() {
            if !bitmap.get(i) {
                continue;
            }

            if self.data.len() < self.limit {
                self.data.push(value);
                if self.data.len() == self.limit {
                    self.make_heap(cmp);
                }
                continue;
            }

            if !self.push_value(value, cmp) {
                bitmap.set(i, false);
            }
        }
    }

    #[inline]
    fn push_value(
        &mut self,
        value: FixedSize32,
        cmp: fn(&FixedSize32, &FixedSize32) -> std::cmp::Ordering,
    ) -> bool {
        let order = self.ordering();
        unsafe {
            assume(self.data.len() == self.limit);
        }

        if cmp(&self.data[0], &value) != order {
            self.data[0] = value;
            self.adjust(cmp);
            true
        } else {
            false
        }
    }

    pub fn never_match(
        &self,
        (min, max): &(FixedSize32, FixedSize32),
        cmp: fn(&FixedSize32, &FixedSize32) -> std::cmp::Ordering,
    ) -> bool {
        if self.data.len() != self.limit {
            return false;
        }
        (self.asc && cmp(&self.data[0], min) == Ordering::Less)
            || (!self.asc && cmp(&self.data[0], max) == Ordering::Greater)
    }

    fn make_heap(&mut self, cmp: impl Fn(&FixedSize32, &FixedSize32) -> std::cmp::Ordering) {
        let ordering = self.ordering();
        let data = self.data.as_mut_slice();
        make_heap(data, &|a, b| cmp(a, b) == ordering);
    }

    fn adjust(&mut self, cmp: impl Fn(&FixedSize32, &FixedSize32) -> std::cmp::Ordering) {
        let ordering = self.ordering();
        let data = self.data.as_mut_slice();
        adjust_heap(data, 0, data.len(), &|a, b| cmp(a, b) == ordering);
    }

    fn ordering(&self) -> Ordering {
        if self.asc {
            Less
        } else {
            Less.reverse()
        }
    }
}

#[inline]
fn make_heap<T, F>(v: &mut [T], is_less: &F)
where F: Fn(&T, &T) -> bool {
    let len = v.len();

    if len < 2 {
        // no need to adjust heap
        return;
    }

    let mut parent = (len - 2) / 2;

    loop {
        adjust_heap(v, parent, len, is_less);
        if parent == 0 {
            return;
        }
        parent -= 1;
    }
}

/// adjust_heap is a shift up adjust op for the heap
#[inline]
fn adjust_heap<T, F>(v: &mut [T], hole_index: usize, len: usize, is_less: &F)
where F: Fn(&T, &T) -> bool {
    let mut left_child = hole_index * 2 + 1;

    // SAFETY: we ensure hole_index point to a properly initialized value of type T
    let mut tmp = unsafe { mem::ManuallyDrop::new(ptr::read(&v[hole_index])) };
    let mut hole = InsertionHole {
        src: &mut *tmp,
        dest: &mut v[hole_index],
    };
    // Panic safety:
    //
    // If `is_less` panics at any point during the process, `hole` will get dropped and fill the
    // hole in `v` with the unconsumed range in `buf`, thus ensuring that `v` still holds every
    // object it initially held exactly once.

    // SAFETY:
    // we ensure src/dest point to a properly initialized value of type T
    // src is valid for reads of `count * size_of::()` bytes.
    // dest is valid for reads of `count * size_of::()` bytes.
    // Both `src` and `dst` are properly aligned.

    unsafe {
        while left_child < len {
            // SAFETY:
            // we ensure left_child and left_child + 1 are between [0, len)
            if left_child + 1 < len {
                left_child +=
                    is_less(v.get_unchecked(left_child), v.get_unchecked(left_child + 1)) as usize;
            }

            // SAFETY:
            // left_child and hole.dest point to a properly initialized value of type T
            if is_less(&*tmp, v.get_unchecked(left_child)) {
                ptr::copy_nonoverlapping(&v[left_child], hole.dest, 1);
                hole.dest = &mut v[left_child];
            } else {
                break;
            }

            left_child = left_child * 2 + 1;
        }
    }

    // These codes is from std::sort_by
    // When dropped, copies from `src` into `dest`.
    struct InsertionHole<T> {
        src: *mut T,
        dest: *mut T,
    }

    impl<T> Drop for InsertionHole<T> {
        fn drop(&mut self) {
            drop_guard(move || {
                // SAFETY:
                // we ensure src/dest point to a properly initialized value of type T
                // src is valid for reads of `count * size_of::()` bytes.
                // dest is valid for reads of `count * size_of::()` bytes.
                // Both `src` and `dst` are properly aligned.
                unsafe {
                    ptr::copy_nonoverlapping(self.src, self.dest, 1);
                }
            })
        }
    }
}
