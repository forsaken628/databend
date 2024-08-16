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
use std::marker::PhantomData;

use serde::Deserialize;
use serde::Serialize;

use super::ArgType;
use super::DataType;
use super::LogicalTypeIdent;
use super::ValueType;

#[derive(PartialEq, Clone, Debug, Serialize)]
struct LogicalType<const IDENT: LogicalTypeIdent, P: ArgType>(PhantomData<P>);

impl<const IDENT: LogicalTypeIdent, P: ArgType> ArgType for LogicalType<IDENT, P> {
    fn data_type() -> DataType {
        DataType::LogicalType((Box::new(P::data_type()), IDENT))
    }

    fn full_domain() -> Self::Domain {
        P::full_domain()
    }

    fn create_builder(capacity: usize, generics: &super::GenericMap) -> Self::ColumnBuilder {
        P::create_builder(capacity, generics)
    }
}

impl<const IDENT: LogicalTypeIdent, P: ArgType> ValueType for LogicalType<IDENT, P> {
    type Scalar = P::Scalar;

    type ScalarRef<'a> = P::ScalarRef<'a>;

    type Column = P::Column;

    type Domain = P::Domain;

    type ColumnIterator<'a> = P::ColumnIterator<'a>;

    type ColumnBuilder = P::ColumnBuilder;

    fn upcast_gat<'short, 'long: 'short>(long: Self::ScalarRef<'long>) -> Self::ScalarRef<'short> {
        todo!()
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        todo!()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        todo!()
    }

    fn try_downcast_scalar<'a>(scalar: &'a crate::ScalarRef) -> Option<Self::ScalarRef<'a>> {
        todo!()
    }

    fn try_downcast_column(col: &crate::Column) -> Option<Self::Column> {
        todo!()
    }

    fn try_downcast_domain(domain: &crate::Domain) -> Option<Self::Domain> {
        todo!()
    }

    fn try_downcast_builder(
        builder: &mut crate::ColumnBuilder,
    ) -> Option<&mut Self::ColumnBuilder> {
        todo!()
    }

    fn try_downcast_owned_builder(builder: crate::ColumnBuilder) -> Option<Self::ColumnBuilder> {
        todo!()
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        decimal_size: Option<super::DecimalSize>,
    ) -> Option<crate::ColumnBuilder> {
        todo!()
    }

    fn upcast_scalar(scalar: Self::Scalar) -> crate::Scalar {
        todo!()
    }

    fn upcast_column(col: Self::Column) -> crate::Column {
        todo!()
    }

    fn upcast_domain(domain: Self::Domain) -> crate::Domain {
        todo!()
    }

    fn column_len(col: &Self::Column) -> usize {
        todo!()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        todo!()
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        todo!()
    }

    fn slice_column(col: &Self::Column, range: std::ops::Range<usize>) -> Self::Column {
        todo!()
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        todo!()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        todo!()
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        todo!()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        todo!()
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        todo!()
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        todo!()
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        todo!()
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        todo!()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        todo!()
    }
}
