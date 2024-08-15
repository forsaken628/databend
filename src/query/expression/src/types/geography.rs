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

use super::array::ArrayColumn;
use super::array::ArrayColumnBuilder;
use super::array::ArrayType;
use super::map::KvColumn;
use super::map::KvColumnBuilder;
use super::map::KvPair;
use super::BinaryType;
use super::Float64Type;

type Tuple2<A, B> = KvPair<A, B>;
type Tuple2Column<A, B> = KvColumn<A, B>;
type Tuple2ColumnBuilder<A, B> = KvColumnBuilder<A, B>;

pub type CoordType = Tuple2<Float64Type, Float64Type>;
pub type PointType = CoordType;
pub type LineStringType = ArrayType<CoordType>;
pub type PolygonType = ArrayType<ArrayType<CoordType>>;
pub type MultiPointType = ArrayType<PointType>;
pub type MultiLineStringType = ArrayType<LineStringType>;
pub type MultiPolygonType = ArrayType<PolygonType>;
pub type AnyGeography = Tuple2<BinaryType, ArrayType<PointType>>;

pub type CoordColumn = Tuple2Column<Float64Type, Float64Type>;
pub type PointColumn = CoordColumn;
pub type LineStringColumn = ArrayColumn<CoordType>;
pub type PolygonColumn = ArrayColumn<ArrayType<CoordType>>;
pub type MultiPointColumn = ArrayColumn<PointType>;
pub type MultiLineStringColumn = ArrayColumn<LineStringType>;
pub type MultiPolygonColumn = ArrayColumn<PolygonType>;
pub type AnyGeographyColumn = Tuple2Column<BinaryType, ArrayType<PointType>>;

pub type CoordBuilder = Tuple2ColumnBuilder<Float64Type, Float64Type>;
pub type PointBuilder = CoordBuilder;
pub type LineStringBuilder = ArrayColumnBuilder<CoordType>;
pub type PolygonBuilder = ArrayColumnBuilder<ArrayType<CoordType>>;
pub type MultiPointBuilder = ArrayColumnBuilder<PointType>;
pub type MultiLineStringBuilder = ArrayColumnBuilder<LineStringType>;
pub type MultiPolygonBuilder = ArrayColumnBuilder<PolygonType>;
pub type AnyGeographyBuilder = Tuple2ColumnBuilder<BinaryType, ArrayType<PointType>>;
