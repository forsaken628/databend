// Copyright 2023 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;

use crate::api::ExchangeShuffleMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::HashTablePayload;
use crate::pipelines::processors::transforms::aggregator::estimated_key_size;
use crate::pipelines::processors::transforms::aggregator::serde::transform_group_by_serializer::serialize_group_by;
use crate::pipelines::processors::transforms::aggregator::serde::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;

pub struct TransformScatterGroupBySerializer<Method: HashMethodBounds> {
    method: Method,
}

impl<Method: HashMethodBounds> TransformScatterGroupBySerializer<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformScatterGroupBySerializer { method },
        )))
    }
}

impl<Method> BlockMetaTransform<ExchangeShuffleMeta> for TransformScatterGroupBySerializer<Method>
where Method: HashMethodBounds
{
    const NAME: &'static str = "TransformScatterGroupBySerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<DataBlock> {
        let mut new_blocks = Vec::with_capacity(meta.blocks.len());

        for mut block in meta.blocks {
            if let Some(meta) = block
                .take_meta()
                .and_then(AggregateMeta::<Method, ()>::downcast_from)
            {
                new_blocks.push(match meta {
                    AggregateMeta::Spilling(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::Serialized(_) => unreachable!(),
                    AggregateMeta::Spilled(payload) => {
                        DataBlock::empty_with_meta(AggregateSerdeMeta::create_spilled(
                            payload.bucket,
                            payload.location,
                            payload.columns_layout,
                        ))
                    }
                    AggregateMeta::HashTable(payload) => {
                        let bucket = payload.bucket;
                        let data_block = serialize_group_by(&self.method, payload)?;
                        data_block.add_meta(Some(AggregateSerdeMeta::create(bucket)))?
                    }
                });

                continue;
            }

            return Err(ErrorCode::Internal(
                "Internal, TransformScatterGroupBySerializer only recv AggregateMeta.",
            ));
        }

        Ok(DataBlock::empty_with_meta(ExchangeShuffleMeta::create(
            new_blocks,
        )))
    }
}
