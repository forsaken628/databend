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

use std::fmt::Debug;
use std::hash::Hash;
use std::io;
use std::marker::PhantomData;
use std::ops::Range;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::trusted_len::TrustedLen;
use databend_common_geobuf::FeatureKind;
use databend_common_geobuf::Geometry;
use databend_common_geobuf::GeometryRef;
use databend_common_geobuf::ObjectKind;
use serde::Deserialize;
use serde::Serialize;

use super::array::*;
use super::binary::BinaryType;
use super::number::Float64Type;
use crate::property::Domain;
use crate::types::binary::BinaryColumn;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::types::F64;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::ColumnBuilder;

pub const LATITUDE_MIN: f64 = -90.0;
pub const LATITUDE_MAX: f64 = 90.0;
pub const LONGITUDE_MIN: f64 = -180.0;
pub const LONGITUDE_MAX: f64 = 180.0;

#[derive(Clone, Default, Debug, PartialOrd)]
pub struct Geography(pub Geometry);

impl Geography {
    pub fn as_ref(&self) -> GeographyRef<'_> {
        GeographyRef(self.0.as_ref())
    }
}

impl Serialize for Geography {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        Serialize::serialize(&self.0.as_ref(), serializer)
    }
}

impl<'de> Deserialize<'de> for Geography {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        Ok(Geography(Deserialize::deserialize(deserializer)?))
    }
}

impl BorshSerialize for Geography {
    fn serialize<W: io::prelude::Write>(&self, writer: &mut W) -> io::Result<()> {
        BorshSerialize::serialize(&self.0.as_ref(), writer)
    }
}

impl BorshDeserialize for Geography {
    fn deserialize_reader<R: io::prelude::Read>(reader: &mut R) -> io::Result<Self> {
        Ok(Geography(Geometry::deserialize_reader(reader)?))
    }
}

impl PartialEq for Geography {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Geography {}

type CoordType = Pair<Float64Type, Float64Type>;
pub type PolygonType = ArrayType<ArrayType<CoordType>>;

type Item = (Vec<u8>, ArrayColumn<CoordType>);
type ItemRef<'a> = (&'a [u8], ArrayColumn<CoordType>);

impl Geography {
    fn new((buf, polygon): Item) -> Self {
        let x = unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(polygon.values.0) };
        let y = unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(polygon.values.1) };
        Self(Geometry::new(buf, polygon.offsets, x, y))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd)]
pub struct GeographyRef<'a>(pub GeometryRef<'a>);

impl<'a> GeographyRef<'a> {
    pub fn new((buf, polygon): ItemRef<'a>) -> Self {
        let lon = unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(polygon.values.0) };
        let lat = unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(polygon.values.1) };
        GeographyRef(GeometryRef::new(buf, polygon.offsets, lon, lat))
    }

    pub fn to_owned(&self) -> Geography {
        Geography(self.0.to_owned())
    }

    pub fn check(&self) -> Result<(), String> {
        if !self
            .0
            .x()
            .iter()
            .all(|longitude| (LONGITUDE_MIN..=LONGITUDE_MAX).contains(longitude))
        {
            return Err("longitude is out of range".to_string());
        }
        if !self
            .0
            .y()
            .iter()
            .all(|latitude| (LATITUDE_MIN..=LATITUDE_MAX).contains(latitude))
        {
            return Err("latitude is out of range".to_string());
        }
        Ok(())
    }

    fn as_item_ref(&'a self) -> ItemRef<'a> {
        let buf = self.0.buf();

        let offsets = self.0.offsets();
        let lon = unsafe { std::mem::transmute::<Buffer<f64>, Buffer<F64>>(self.0.x()) };
        let lat = unsafe { std::mem::transmute::<Buffer<f64>, Buffer<F64>>(self.0.y()) };

        let polygon = ArrayColumn {
            values: PairColumn(lon, lat),
            offsets,
        };

        (buf, polygon)
    }
}

impl<'a> Hash for GeographyRef<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeographyType;

impl ValueType for GeographyType {
    type Scalar = Geography;
    type ScalarRef<'a> = GeographyRef<'a>;
    type Column = GeographyColumn;
    type Domain = ();
    type ColumnIterator<'a> = GeographyIterator<'a>;
    type ColumnBuilder = GeographyColumnBuilder;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: GeographyRef<'long>) -> GeographyRef<'short> {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_owned()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.as_ref()
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_geography().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_geography().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        if domain.is_undefined() {
            Some(())
        } else {
            None
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Geography(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Geography(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Geography(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Geography(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Geography(col)
    }

    fn upcast_domain(_domain: Self::Domain) -> Domain {
        Domain::Undefined
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        GeographyColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item)
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(item, n)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default()
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.append_column(other)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.0.memory_size()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }

    fn compare(a: Self::ScalarRef<'_>, b: Self::ScalarRef<'_>) -> Option<std::cmp::Ordering> {
        a.partial_cmp(&b)
    }
}

impl ArgType for GeographyType {
    fn data_type() -> DataType {
        DataType::Geography
    }

    fn full_domain() -> Self::Domain {}

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        GeographyColumnBuilder::with_capacity(capacity, 0)
    }
}

impl GeographyType {
    pub fn point(lon: f64, lat: f64) -> Geography {
        Geography(Geometry::point(lon, lat))
    }

    pub fn point_column(lon: Buffer<F64>, lat: Buffer<F64>) -> GeographyColumn {
        debug_assert_eq!(lon.len(), lat.len());

        let n = lon.len();
        let buf_item = [FeatureKind::Geometry(ObjectKind::Point).as_u8()];
        let buf = BinaryColumnBuilder::repeat(&buf_item, n).build();
        let offsets: Buffer<_> = (0u64..=n as u64).collect();

        GeographyColumn::new(buf, lon, lat, offsets.clone(), offsets)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GeographyColumn(pub PairColumn<BinaryType, PolygonType>);

impl GeographyColumn {
    pub fn new(
        buf: BinaryColumn,
        lon: Buffer<F64>,
        lat: Buffer<F64>,
        ring_offsets: Buffer<u64>,
        polygon_offsets: Buffer<u64>,
    ) -> Self {
        GeographyColumn(PairColumn(buf, ArrayColumn {
            values: ArrayColumn {
                values: PairColumn(lon, lat),
                offsets: ring_offsets,
            },
            offsets: polygon_offsets,
        }))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn memory_size(&self) -> usize {
        self.0.memory_size()
    }

    pub fn index(&self, index: usize) -> Option<GeographyRef<'_>> {
        self.0.index(index).map(GeographyRef::new)
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> GeographyRef<'_> {
        GeographyRef::new(self.0.index_unchecked(index))
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        GeographyColumn(self.0.slice(range))
    }

    pub fn iter(&self) -> GeographyIterator<'_> {
        GeographyIterator(self.0.iter())
    }

    pub fn lon(&self) -> &[F64] {
        &self.0.1.values.values.0
    }

    pub fn lat(&self) -> &[F64] {
        &self.0.1.values.values.1
    }
}

pub struct GeographyIterator<'a>(PairIterator<'a, BinaryType, PolygonType>);

unsafe impl<'a> TrustedLen for GeographyIterator<'a> {}

unsafe impl<'a> std::iter::TrustedLen for GeographyIterator<'a> {}

impl<'a> Iterator for GeographyIterator<'a> {
    type Item = GeographyRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(GeographyRef::new)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

#[derive(Debug, Clone)]
pub struct GeographyColumnBuilder(pub PairColumnBuilder<BinaryType, PolygonType>);

impl GeographyColumnBuilder {
    pub fn with_capacity(len: usize, _: usize) -> Self {
        GeographyColumnBuilder(PairColumnBuilder::with_capacity(len, &[]))
    }

    pub fn from_column(col: GeographyColumn) -> Self {
        GeographyColumnBuilder(PairColumnBuilder::from_column(col.0))
    }

    pub fn repeat(item: &GeographyRef<'_>, n: usize) -> Self {
        let (buf, array) = item.as_item_ref();
        GeographyColumnBuilder(PairColumnBuilder(
            BinaryColumnBuilder::repeat(buf, n),
            ArrayColumnBuilder::repeat(&array, n),
        ))
    }

    pub fn repeat_default(n: usize) -> Self {
        let item = Geography::default();
        let item = item.as_ref();
        Self::repeat(&item, n)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn push(&mut self, item: GeographyRef<'_>) {
        self.0.push(item.as_item_ref())
    }

    pub fn push_repeat(&mut self, item: GeographyRef<'_>, n: usize) {
        self.0.push_repeat(item.as_item_ref(), n)
    }

    pub fn push_default(&mut self) {
        self.push(Geography::default().as_ref())
    }

    pub fn append_column(&mut self, other: &GeographyColumn) {
        self.0.append_column(&other.0);
    }

    pub fn build(self) -> GeographyColumn {
        GeographyColumn(self.0.build())
    }

    pub fn build_scalar(self) -> Geography {
        assert_eq!(self.len(), 1);
        Geography::new(self.0.build_scalar())
    }

    pub fn pop(&mut self) -> Option<Geography> {
        if self.len() > 0 {
            let buf = &mut self.0.0;
            let polygon_offsets = &mut self.0.1.offsets;
            let ring_offsets = &mut self.0.1.builder.offsets;
            let x = &mut self.0.1.builder.builder.0;
            let y = &mut self.0.1.builder.builder.1;

            let mut ring_offsets_tails =
                ring_offsets.split_off(polygon_offsets[polygon_offsets.len() - 2] as usize);
            let start = ring_offsets_tails[0];
            ring_offsets.push(start);

            let x = x.split_off(start as usize);
            let y = y.split_off(start as usize);

            let buf = buf.pop().unwrap();
            polygon_offsets.pop().unwrap();

            for v in ring_offsets_tails.iter_mut() {
                *v -= start
            }

            Some(Geography::new((buf, ArrayColumn {
                values: PairColumn(x.into(), y.into()),
                offsets: ring_offsets_tails.into(),
            })))
        } else {
            None
        }
    }

    pub fn memory_size(&self) -> usize {
        self.0.0.memory_size()
            + self.0.1.offsets.len() * 8
            + self.0.1.builder.offsets.len() * 8
            + self.0.1.builder.builder.0.len() * 8
            + self.0.1.builder.builder.1.len() * 8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pair<T: ValueType, U: ValueType>(PhantomData<(T, U)>);

impl<T: ValueType, U: ValueType> ValueType for Pair<T, U> {
    type Scalar = (T::Scalar, U::Scalar);
    type ScalarRef<'a> = (T::ScalarRef<'a>, U::ScalarRef<'a>);
    type Column = PairColumn<T, U>;
    type Domain = (T::Domain, U::Domain);
    type ColumnIterator<'a> = PairIterator<'a, T, U>;
    type ColumnBuilder = PairColumnBuilder<T, U>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: Self::ScalarRef<'long>) -> Self::ScalarRef<'short> {
        (T::upcast_gat(long.0), U::upcast_gat(long.1))
    }

    fn to_owned_scalar((t, u): Self::ScalarRef<'_>) -> Self::Scalar {
        (T::to_owned_scalar(t), U::to_owned_scalar(u))
    }

    fn to_scalar_ref((t, u): &Self::Scalar) -> Self::ScalarRef<'_> {
        (T::to_scalar_ref(t), U::to_scalar_ref(u))
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Tuple(fields) if fields.len() == 2 => Some((
                T::try_downcast_scalar(&fields[0])?,
                U::try_downcast_scalar(&fields[1])?,
            )),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        match col {
            Column::Tuple(fields) if fields.len() == 2 => Some(PairColumn(
                T::try_downcast_column(&fields[0])?,
                U::try_downcast_column(&fields[1])?,
            )),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Tuple(fields) if fields.len() == 2 => Some((
                T::try_downcast_domain(&fields[0])?,
                U::try_downcast_domain(&fields[1])?,
            )),
            _ => None,
        }
    }

    fn try_downcast_builder(_builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        None
    }

    fn try_downcast_owned_builder<'a>(_builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        None
    }

    fn try_upcast_column_builder(
        _builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        None
    }

    fn upcast_scalar((k, v): Self::Scalar) -> Scalar {
        Scalar::Tuple(vec![T::upcast_scalar(k), U::upcast_scalar(v)])
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Tuple(vec![T::upcast_column(col.0), U::upcast_column(col.1)])
    }

    fn upcast_domain((t, u): Self::Domain) -> Domain {
        Domain::Tuple(vec![T::upcast_domain(t), U::upcast_domain(u)])
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        PairColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(item, n)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default();
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other_builder: &Self::Column) {
        builder.append_column(other_builder);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }

    fn scalar_memory_size((t, u): &Self::ScalarRef<'_>) -> usize {
        T::scalar_memory_size(t) + U::scalar_memory_size(u)
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }
}

impl<K: ArgType, V: ArgType> ArgType for Pair<K, V> {
    fn data_type() -> DataType {
        DataType::Tuple(vec![K::data_type(), V::data_type()])
    }

    fn full_domain() -> Self::Domain {
        (K::full_domain(), V::full_domain())
    }

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        PairColumnBuilder::with_capacity(capacity, generics)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PairColumn<T: ValueType, U: ValueType>(pub T::Column, pub U::Column);

impl<T: ValueType, U: ValueType> PairColumn<T, U> {
    pub fn len(&self) -> usize {
        T::column_len(&self.0)
    }

    pub fn index(&self, index: usize) -> Option<(T::ScalarRef<'_>, U::ScalarRef<'_>)> {
        Some((
            T::index_column(&self.0, index)?,
            U::index_column(&self.1, index)?,
        ))
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> (T::ScalarRef<'_>, U::ScalarRef<'_>) {
        (
            T::index_column_unchecked(&self.0, index),
            U::index_column_unchecked(&self.1, index),
        )
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        PairColumn(
            T::slice_column(&self.0, range.clone()),
            U::slice_column(&self.1, range),
        )
    }

    pub fn iter(&self) -> PairIterator<T, U> {
        PairIterator(T::iter_column(&self.0), U::iter_column(&self.1))
    }

    pub fn memory_size(&self) -> usize {
        T::column_memory_size(&self.0) + U::column_memory_size(&self.1)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PairColumnBuilder<T: ValueType, U: ValueType>(
    pub T::ColumnBuilder,
    pub U::ColumnBuilder,
);

impl<T: ValueType, U: ValueType> PairColumnBuilder<T, U> {
    pub fn from_column(col: PairColumn<T, U>) -> Self {
        Self(T::column_to_builder(col.0), U::column_to_builder(col.1))
    }

    pub fn len(&self) -> usize {
        T::builder_len(&self.0)
    }

    pub fn push(&mut self, (t, u): (T::ScalarRef<'_>, U::ScalarRef<'_>)) {
        T::push_item(&mut self.0, t);
        U::push_item(&mut self.1, u);
    }

    pub fn push_repeat(&mut self, (t, u): (T::ScalarRef<'_>, U::ScalarRef<'_>), n: usize) {
        T::push_item_repeat(&mut self.0, t, n);
        U::push_item_repeat(&mut self.1, u, n);
    }

    pub fn push_default(&mut self) {
        T::push_default(&mut self.0);
        U::push_default(&mut self.1);
    }

    pub fn append_column(&mut self, other: &PairColumn<T, U>) {
        T::append_column(&mut self.0, &other.0);
        U::append_column(&mut self.1, &other.1);
    }

    pub fn build(self) -> PairColumn<T, U> {
        PairColumn(T::build_column(self.0), U::build_column(self.1))
    }

    pub fn build_scalar(self) -> (T::Scalar, U::Scalar) {
        (T::build_scalar(self.0), U::build_scalar(self.1))
    }
}

impl<T: ArgType, U: ArgType> PairColumnBuilder<T, U> {
    pub fn with_capacity(capacity: usize, generics: &GenericMap) -> Self {
        Self(
            T::create_builder(capacity, generics),
            U::create_builder(capacity, generics),
        )
    }
}

unsafe impl<'a, T: ValueType, U: ValueType> TrustedLen for PairIterator<'a, T, U> {}

unsafe impl<'a, T: ValueType, U: ValueType> std::iter::TrustedLen for PairIterator<'a, T, U> {}

pub struct PairIterator<'a, T: ValueType, U: ValueType>(
    T::ColumnIterator<'a>,
    U::ColumnIterator<'a>,
);

impl<'a, T: ValueType, U: ValueType> Iterator for PairIterator<'a, T, U> {
    type Item = (T::ScalarRef<'a>, U::ScalarRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.0.next()?, self.1.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        assert_eq!(self.0.size_hint(), self.1.size_hint());
        self.0.size_hint()
    }
}
