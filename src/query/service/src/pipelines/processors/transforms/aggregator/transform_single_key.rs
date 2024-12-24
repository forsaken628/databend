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
use std::time::Instant;

use async_trait::async_trait;
use bumpalo::Bump;
use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::aggregates::AggregateFunctionRef;
use databend_common_functions::aggregates::StateAddr;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

/// SELECT COUNT | SUM FROM table;
pub struct PartialSingleStateAggregator {
    #[allow(dead_code)]
    arena: Bump,
    places: Vec<StateAddr>,
    arg_indices: Vec<Vec<usize>>,
    funcs: Vec<AggregateFunctionRef>,

    start: Instant,
    first_block_start: Option<Instant>,
    rows: usize,
    bytes: usize,
}

impl PartialSingleStateAggregator {
    pub fn try_new(params: &Arc<AggregatorParams>) -> Result<Self> {
        assert!(!params.offsets_aggregate_states.is_empty());

        let arena = Bump::new();
        let layout = params
            .layout
            .ok_or_else(|| ErrorCode::LayoutError("layout shouldn't be None"))?;

        let place: StateAddr = arena.alloc_layout(layout).into();
        let temp_place: StateAddr = arena.alloc_layout(layout).into();
        let mut places = Vec::with_capacity(params.offsets_aggregate_states.len());

        for (idx, func) in params.aggregate_functions.iter().enumerate() {
            let arg_place = place.next(params.offsets_aggregate_states[idx]);
            func.init_state(arg_place);
            places.push(arg_place);

            let state_place = temp_place.next(params.offsets_aggregate_states[idx]);
            func.init_state(state_place);
        }

        Ok(PartialSingleStateAggregator {
            arena,
            places,
            funcs: params.aggregate_functions.clone(),
            arg_indices: params.aggregate_functions_arguments.clone(),
            start: Instant::now(),
            first_block_start: None,
            rows: 0,
            bytes: 0,
        })
    }

    fn is_agg_index_block(&self, block: &DataBlock) -> bool {
        block
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .map(|index| index.is_agg)
            .unwrap_or_default()
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Option<DataBlock>> {
        let generate_data_block = if generate_data {
            let mut columns = Vec::with_capacity(self.funcs.len());

            for (idx, func) in self.funcs.iter().enumerate() {
                let place = self.places[idx];

                let mut data = Vec::with_capacity(4);
                func.serialize(place, &mut data)?;
                columns.push(BlockEntry::new(
                    DataType::Binary,
                    Value::Scalar(Scalar::Binary(data)),
                ));
            }

            Some(DataBlock::new(columns, 1))
        } else {
            None
        };

        // destroy states
        for (place, func) in self.places.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(*place) }
            }
        }

        log::info!(
            "Aggregated {} to 1 rows in {} sec (real: {}). ({} rows/sec, {}/sec, {})",
            self.rows,
            self.start.elapsed().as_secs_f64(),
            if let Some(t) = &self.first_block_start {
                t.elapsed().as_secs_f64()
            } else {
                self.start.elapsed().as_secs_f64()
            },
            convert_number_size(self.rows as f64 / self.start.elapsed().as_secs_f64()),
            convert_byte_size(self.bytes as f64 / self.start.elapsed().as_secs_f64()),
            convert_byte_size(self.bytes as _),
        );

        Ok(generate_data_block)
    }
}

impl AccumulatingTransform for PartialSingleStateAggregator {
    const NAME: &'static str = "AggregatorPartialTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if self.first_block_start.is_none() {
            self.first_block_start = Some(Instant::now());
        }

        let is_agg_index_block = self.is_agg_index_block(&block);
        let block = block.consume_convert_to_full();

        for (idx, (func, place)) in self
            .funcs
            .iter()
            .zip(self.places.iter().cloned())
            .enumerate()
        {
            if is_agg_index_block {
                // Aggregation states are in the back of the block.
                let agg_index = block.num_columns() - self.funcs.len() + idx;
                let agg_state = block.get_by_offset(agg_index).value.as_column().unwrap();

                func.as_sync()
                    .unwrap()
                    .batch_merge_single(place, agg_state)?;
            } else {
                let columns =
                    InputColumns::new_block_proxy(self.arg_indices[idx].as_slice(), &block);
                func.as_sync()
                    .unwrap()
                    .accumulate(place, columns, None, block.num_rows())?;
            }
        }

        self.rows += block.num_rows();
        self.bytes += block.memory_size();

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        Ok(self
            .on_finish(generate_data)?
            .map(|block| vec![block])
            .unwrap_or_default())
    }
}

#[async_trait]
impl AsyncAccumulatingTransform for PartialSingleStateAggregator {
    const NAME: &'static str = "AggregatorPartialTransform";

    async fn transform(&mut self, block: DataBlock) -> Result<Option<DataBlock>> {
        if self.first_block_start.is_none() {
            self.first_block_start = Some(Instant::now());
        }

        let is_agg_index_block = self.is_agg_index_block(&block);
        let block = block.consume_convert_to_full();

        for (idx, (func, place)) in self
            .funcs
            .iter()
            .zip(self.places.iter().cloned())
            .enumerate()
        {
            if is_agg_index_block {
                // Aggregation states are in the back of the block.
                let agg_index = block.num_columns() - self.funcs.len() + idx;
                let agg_state = block.get_by_offset(agg_index).value.as_column().unwrap();

                match func {
                    AggregateFunctionRef::Sync(func) => {
                        func.batch_merge_single(place, agg_state)?;
                    }
                    AggregateFunctionRef::Async(func) => {
                        let agg_state = vec![agg_state.clone()];
                        func.batch_merge_single(place, &agg_state).await? // todo parallelism
                    }
                }
            } else {
                let columns =
                    InputColumns::new_block_proxy(self.arg_indices[idx].as_slice(), &block);
                match func {
                    AggregateFunctionRef::Sync(func) => {
                        func.accumulate(place, columns, None, block.num_rows())?;
                    }
                    AggregateFunctionRef::Async(func) => {
                        func.accumulate(place, columns, None, block.num_rows())
                            .await? // todo parallelism
                    }
                }
            }
        }

        self.rows += block.num_rows();
        self.bytes += block.memory_size();

        Ok(None)
    }

    async fn on_finish(&mut self, generate_data: bool) -> Result<Option<DataBlock>> {
        self.on_finish(generate_data)
    }
}

/// SELECT COUNT | SUM FROM table;
pub struct FinalSingleStateAggregator {
    arena: Bump,
    layout: Layout,
    to_merge_data: Vec<Vec<Column>>,
    funcs: Vec<AggregateFunctionRef>,
    offsets_aggregate_states: Vec<usize>,
}

impl FinalSingleStateAggregator {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: &Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        assert!(!params.offsets_aggregate_states.is_empty());

        let arena = Bump::new();
        let layout = params
            .layout
            .ok_or_else(|| ErrorCode::LayoutError("layout shouldn't be None"))?;

        let inner = FinalSingleStateAggregator {
            arena,
            layout,
            funcs: params.aggregate_functions.clone(),
            to_merge_data: vec![vec![]; params.aggregate_functions.len()],
            offsets_aggregate_states: params.offsets_aggregate_states.clone(),
        };

        if params.has_async_aggregate_function() {
            Ok(AsyncAccumulatingTransformer::create(input, output, inner))
        } else {
            Ok(AccumulatingTransformer::create(input, output, inner))
        }
    }

    fn new_places(&self) -> Vec<StateAddr> {
        let place: StateAddr = self.arena.alloc_layout(self.layout).into();
        self.funcs
            .iter()
            .enumerate()
            .map(|(idx, func)| {
                let arg_place = place.next(self.offsets_aggregate_states[idx]);
                func.init_state(arg_place);
                arg_place
            })
            .collect()
    }

    fn transform(&mut self, block: DataBlock) -> Result<()> {
        if !block.is_empty() {
            let block = block.consume_convert_to_full();

            for (index, _) in self.funcs.iter().enumerate() {
                let binary_array = block.get_by_offset(index).value.as_column().unwrap();
                self.to_merge_data[index].push(binary_array.clone());
            }
        }

        Ok(())
    }
}

impl AccumulatingTransform for FinalSingleStateAggregator {
    const NAME: &'static str = "AggregatorFinalTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.transform(block)?;
        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        if !generate_data {
            return Ok(vec![]);
        }

        let main_places = self.new_places();
        for ((func, main_place), data) in self
            .funcs
            .iter()
            .zip(main_places.iter().cloned())
            .zip(self.to_merge_data.iter())
        {
            for col in data.iter() {
                func.as_sync()
                    .unwrap()
                    .batch_merge_single(main_place, col)?;
            }
        }

        let columns = self
            .funcs
            .iter()
            .zip(main_places.iter().cloned())
            .map(|(func, main_place)| {
                let data_type = func.return_type()?;
                let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
                func.as_sync()
                    .unwrap()
                    .merge_result(main_place, &mut builder)?;
                Ok(builder.build())
            })
            .collect::<Result<Vec<_>>>()?;

        // destroy states
        for (place, func) in main_places.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(*place) }
            }
        }

        Ok(vec![DataBlock::new_from_columns(columns)])
    }
}

#[async_trait]
impl AsyncAccumulatingTransform for FinalSingleStateAggregator {
    const NAME: &'static str = "AggregatorFinalTransform";

    async fn transform(&mut self, block: DataBlock) -> Result<Option<DataBlock>> {
        self.transform(block)?;
        Ok(None)
    }

    async fn on_finish(&mut self, generate_data: bool) -> Result<Option<DataBlock>> {
        if !generate_data {
            return Ok(None);
        }

        let main_places = self.new_places();
        for ((func, main_place), data) in self
            .funcs
            .iter()
            .zip(main_places.iter().cloned())
            .zip(self.to_merge_data.iter())
        {
            match func {
                AggregateFunctionRef::Sync(func) => {
                    for col in data.iter() {
                        func.batch_merge_single(main_place, col)?;
                    }
                }
                AggregateFunctionRef::Async(func) => {
                    func.batch_merge_single(main_place, data).await?; // todo parallelism
                }
            }
        }

        let mut columns = Vec::with_capacity(self.funcs.len());
        for (func, main_place) in self.funcs.iter().zip(main_places.iter().cloned()) {
            let data_type = func.return_type()?;
            let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
            match func {
                AggregateFunctionRef::Sync(func) => func.merge_result(main_place, &mut builder)?,
                AggregateFunctionRef::Async(func) => {
                    func.merge_result(main_place, &mut builder).await? // todo parallelism
                }
            }
            columns.push(builder.build());
        }

        // destroy states
        for (place, func) in main_places.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(*place) }
            }
        }

        Ok(Some(DataBlock::new_from_columns(columns)))
    }
}
