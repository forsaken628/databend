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

use std::alloc::Layout;
use std::fmt;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::number::NumberColumnBuilder;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::utils::column_merge_validity;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;

use super::aggregate_function::SyncAggregateFunction;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::borsh_deserialize_state;
use super::borsh_serialize_state;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_variadic_arguments;

struct AggregateCountState {
    count: u64,
}

#[derive(Clone)]
pub struct AggregateCountFunction {
    display_name: String,
}

impl AggregateCountFunction {
    pub fn try_create(
        display_name: &str,
        _params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<Arc<dyn SyncAggregateFunction>> {
        assert_variadic_arguments(display_name, arguments.len(), (0, 1))?;
        Ok(Arc::new(AggregateCountFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> AggregateFunctionDescription {
        let features = super::aggregate_function_factory::AggregateFunctionFeatures {
            returns_default_when_only_null: true,
            is_decomposable: true,
            ..Default::default()
        };
        AggregateFunctionDescription::creator_with_features(Box::new(Self::try_create), features)
    }
}

impl SyncAggregateFunction for AggregateCountFunction {
    fn name(&self) -> &str {
        "AggregateCountFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Number(NumberDataType::UInt64))
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateCountState { count: 0 });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateCountState>()
    }

    // columns may be nullable
    // if not we use validity as the null signs
    fn accumulate(
        &self,
        place: StateAddr,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let nulls = if columns.is_empty() {
            validity.map(|v| v.null_count()).unwrap_or(0)
        } else {
            match &columns[0] {
                Column::Nullable(c) => validity
                    .map(|v| v & (&c.validity))
                    .unwrap_or_else(|| c.validity.clone())
                    .null_count(),
                _ => validity.map(|v| v.null_count()).unwrap_or(0),
            }
        };
        state.count += (input_rows - nulls) as u64;
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        let validity = columns
            .iter()
            .fold(None, |acc, col| column_merge_validity(col, acc));

        match validity {
            Some(v) => {
                // all nulls
                if v.null_count() == v.len() {
                    return Ok(());
                }
                for (valid, place) in v.iter().zip(places.iter()) {
                    if valid {
                        let state = place.next(offset).get::<AggregateCountState>();
                        state.count += 1;
                    }
                }
            }

            _ => {
                for place in places {
                    let state = place.next(offset).get::<AggregateCountState>();
                    state.count += 1;
                }
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, _columns: InputColumns, _row: usize) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        state.count += 1;
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        borsh_serialize_state(writer, &state.count)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let other: u64 = borsh_deserialize_state(reader)?;
        state.count += other;
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let other = rhs.get::<AggregateCountState>();
        state.count += other.count;
        Ok(())
    }

    fn batch_merge_result(
        &self,
        places: &[StateAddr],
        offset: usize,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        match builder {
            ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) => {
                for place in places {
                    let state = place.next(offset).get::<AggregateCountState>();
                    builder.push(state.count);
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        match builder {
            ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) => {
                let state = place.get::<AggregateCountState>();
                builder.push(state.count);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn get_own_null_adaptor(
        &self,
        _nested_function: super::SyncAggregateFunctionRef,
        _params: Vec<Scalar>,
        _arguments: Vec<DataType>,
    ) -> Result<Option<super::SyncAggregateFunctionRef>> {
        Ok(Some(Arc::new(self.clone())))
    }
}

impl fmt::Display for AggregateCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
