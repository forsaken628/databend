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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_functions::aggregates::get_layout_offsets;
use databend_common_functions::aggregates::AggregateFunctionRef;
use databend_common_sql::IndexType;
use itertools::Itertools;

pub struct AggregatorParams {
    pub input_schema: DataSchemaRef,
    pub group_columns: Vec<IndexType>,
    pub group_data_types: Vec<DataType>,

    pub aggregate_functions: Vec<AggregateFunctionRef>,
    pub aggregate_functions_arguments: Vec<Vec<usize>>,

    // about function state memory layout
    // If there is no aggregate function, layout is None
    pub layout: Option<Layout>,
    pub offsets_aggregate_states: Vec<usize>,

    pub enable_experimental_aggregate_hashtable: bool,
    pub cluster_aggregator: bool,
    pub max_block_size: usize,
    pub max_spill_io_requests: usize,
}

impl AggregatorParams {
    pub fn try_create(
        input_schema: DataSchemaRef,
        group_data_types: Vec<DataType>,
        group_columns: Vec<usize>,
        agg_funcs: Vec<AggregateFunctionRef>,
        agg_args: Vec<Vec<usize>>,
        enable_experimental_aggregate_hashtable: bool,
        cluster_aggregator: bool,
        max_block_size: usize,
        max_spill_io_requests: usize,
    ) -> Result<Arc<AggregatorParams>> {
        let mut states_offsets: Vec<usize> = Vec::with_capacity(agg_funcs.len());
        let mut states_layout = None;
        if !agg_funcs.is_empty() {
            states_offsets = Vec::with_capacity(agg_funcs.len());
            states_layout = Some(get_layout_offsets(&agg_funcs, &mut states_offsets)?);
        }

        Ok(Arc::new(AggregatorParams {
            input_schema,
            group_columns,
            group_data_types,
            aggregate_functions: agg_funcs,
            aggregate_functions_arguments: agg_args,
            layout: states_layout,
            offsets_aggregate_states: states_offsets,
            enable_experimental_aggregate_hashtable,
            cluster_aggregator,
            max_block_size,
            max_spill_io_requests,
        }))
    }

    pub fn has_distinct_combinator(&self) -> bool {
        self.aggregate_functions
            .iter()
            .any(|f| f.name().contains("DistinctCombinator"))
    }

    pub fn empty_result_block(&self) -> DataBlock {
        let columns = self
            .aggregate_functions
            .iter()
            .map(|f| ColumnBuilder::with_capacity(&f.return_type().unwrap(), 0).build())
            .chain(
                self.group_data_types
                    .iter()
                    .map(|t| ColumnBuilder::with_capacity(t, 0).build()),
            )
            .collect_vec();
        DataBlock::new_from_columns(columns)
    }

    pub fn has_async_aggregate_function(&self) -> bool {
        !self
            .aggregate_functions
            .iter()
            .all(AggregateFunctionRef::is_sync)
    }
}
