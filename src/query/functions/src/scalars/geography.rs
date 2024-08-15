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

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_expression::types::*;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    // aliases
    registry.register_aliases("st_makepoint", &["st_point"]);

    registry
        .register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, PointType, _, _>(
            "st_makepoint",
            |_, _, _| FunctionDomain::MayThrow,
            |lon, lat, _| {
                let col = match (lon, lat) {
                    (ValueRef::Scalar(lon), ValueRef::Scalar(lat)) => {
                        return Value::Scalar((lon, lat));
                    }
                    (ValueRef::Scalar(lon), ValueRef::Column(lat)) => PointColumn {
                        keys: Buffer::from(vec![lon; lat.len()]),
                        values: lat,
                    },
                    (ValueRef::Column(lon), ValueRef::Scalar(lat)) => PointColumn {
                        values: Buffer::from(vec![lat; lon.len()]),
                        keys: lon,
                    },
                    (ValueRef::Column(lon), ValueRef::Column(lat)) => PointColumn {
                        keys: lon,
                        values: lat,
                    },
                };
                Value::Column(col)
            },
        );
}
