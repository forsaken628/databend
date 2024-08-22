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

use s2::point::Point;

#[derive(Debug, Default)]
pub struct Edge {
    pub v0: Point,
    pub v1: Point,
}

pub struct Polyline(pub Vec<s2::point::Point>);

impl Polyline {
    pub fn iter_edge(&self) -> EdgeIterator<'_> {
        EdgeIterator {
            iter: self.0.windows(2),
        }
    }
}

pub struct EdgeIterator<'a> {
    iter: std::slice::Windows<'a, s2::point::Point>,
}

impl Iterator for EdgeIterator<'_> {
    type Item = Edge;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|pair| Edge {
            v0: pair[0],
            v1: pair[1],
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl ExactSizeIterator for EdgeIterator<'_> {}

unsafe impl std::iter::TrustedLen for EdgeIterator<'_> {}
