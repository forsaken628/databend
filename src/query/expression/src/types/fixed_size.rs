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

use databend_common_column::buffer::Buffer;

use super::*;
use crate::types::ValueType;
use crate::Column;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct FixedSize32(u32);

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct FixedSize64(u64);

pub trait KindFixedSize {
    type Item;
    type Column;
    fn upcast_column(col: Self::Column) -> Buffer<Self::Item>;
}

impl KindFixedSize for DateType {
    type Item = FixedSize32;
    type Column = <Self as ValueType>::Column;

    fn upcast_column(col: Self::Column) -> Buffer<Self::Item> {
        unsafe { std::mem::transmute(col) }
    }
}

impl From<FixedSize32> for i32 {
    fn from(value: FixedSize32) -> Self {
        unsafe { std::mem::transmute(value.0) }
    }
}

impl From<FixedSize32> for u32 {
    fn from(value: FixedSize32) -> Self {
        value.0
    }
}

impl KindFixedSize for Int32Type {
    type Item = FixedSize32;
    type Column = <Self as ValueType>::Column;

    fn upcast_column(col: Self::Column) -> Buffer<Self::Item> {
        unsafe { std::mem::transmute(col) }
    }
}

impl KindFixedSize for UInt32Type {
    type Item = FixedSize32;
    type Column = <Self as ValueType>::Column;

    fn upcast_column(col: Self::Column) -> Buffer<Self::Item> {
        unsafe { std::mem::transmute(col) }
    }
}

pub fn try_downcast_fixed_size32(column: &Column) -> Option<Buffer<FixedSize32>> {
    match column.data_type() {
        DataType::Number(NumberDataType::Int32) => {
            Some(<Int32Type as KindFixedSize>::upcast_column(
                Int32Type::try_downcast_column(column).unwrap(),
            ))
        }
        DataType::Number(NumberDataType::UInt32) => {
            Some(<UInt32Type as KindFixedSize>::upcast_column(
                UInt32Type::try_downcast_column(column).unwrap(),
            ))
        }
        DataType::Date => Some(<DateType as KindFixedSize>::upcast_column(
            DateType::try_downcast_column(column).unwrap(),
        )),
        _ => None,
    }
}

pub fn fixed_size32_ord(
    data_type: &DataType,
) -> Option<fn(&FixedSize32, &FixedSize32) -> std::cmp::Ordering> {
    match data_type {
        DataType::Number(NumberDataType::Int32) | DataType::Date => {
            Some(|a: &FixedSize32, b: &FixedSize32| Ord::cmp(&i32::from(*a), &i32::from(*b)))
        }
        DataType::Number(NumberDataType::UInt32) => {
            Some(|a: &FixedSize32, b: &FixedSize32| Ord::cmp(&u32::from(*a), &u32::from(*b)))
        }
        _ => None,
    }
}

pub fn fixed_size32_upcast_scalar(data_type: &DataType) -> Option<fn(FixedSize32) -> Scalar> {
    match data_type {
        DataType::Number(NumberDataType::Int32) => {
            Some(|v| Scalar::Number(NumberScalar::Int32(v.into())))
        }
        DataType::Number(NumberDataType::UInt32) => {
            Some(|v| Scalar::Number(NumberScalar::UInt32(v.0)))
        }
        DataType::Date => Some(|v| Scalar::Date(v.into())),
        _ => None,
    }
}
