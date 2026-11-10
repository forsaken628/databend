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

//! Conservative scalar rewrites after semantic binding has completed.
//! Each rewrite preserves its result type, so parents need no retyping.

use databend_common_ast::Span;
use databend_common_exception::Result;
use databend_common_expression::FoldRules;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;
use crate::plans::WindowFuncType;

mod auto_cast;

/// Keep the binder's externally reported type, which specialized binders may
/// intentionally make wider than `scalar.data_type()`.
pub fn optimize(
    mut scalar: ScalarExpr,
    data_type: DataType,
    func_ctx: &FunctionContext,
) -> Result<(ScalarExpr, DataType)> {
    rewrite(&mut scalar, func_ctx);
    Ok((scalar, data_type))
}

#[recursive::recursive]
fn rewrite(scalar: &mut ScalarExpr, func_ctx: &FunctionContext) {
    match scalar {
        ScalarExpr::FunctionCall(call) => {
            for argument in &mut call.arguments {
                rewrite(argument, func_ctx);
            }
            auto_cast::move_cast_to_constant(call, func_ctx);
        }
        ScalarExpr::WindowFunction(window) => {
            for partition in &mut window.partition_by {
                rewrite(partition, func_ctx);
            }
            for order in &mut window.order_by {
                rewrite(&mut order.expr, func_ctx);
            }
            match &mut window.func {
                WindowFuncType::Aggregate(aggregate) => {
                    for argument in aggregate.exprs_mut() {
                        rewrite(argument, func_ctx);
                    }
                }
                WindowFuncType::LagLead(function) => {
                    rewrite(&mut function.arg, func_ctx);
                    if let Some(default) = &mut function.default {
                        rewrite(default, func_ctx);
                    }
                }
                WindowFuncType::NthValue(function) => rewrite(&mut function.arg, func_ctx),
                WindowFuncType::RowNumber
                | WindowFuncType::Rank
                | WindowFuncType::DenseRank
                | WindowFuncType::PercentRank
                | WindowFuncType::Ntile(_)
                | WindowFuncType::CumeDist => {}
            }
        }
        ScalarExpr::CastExpr(cast) => rewrite(&mut cast.argument, func_ctx),
        ScalarExpr::AutoCast(cast) => rewrite(&mut cast.argument, func_ctx),
        ScalarExpr::LambdaFunction(lambda) => {
            for argument in &mut lambda.args {
                rewrite(argument, func_ctx);
            }
        }
        ScalarExpr::UDFLambdaCall(udf) => rewrite(&mut udf.scalar, func_ctx),
        // Standalone aggregate/subquery/runtime UDF bodies are not scalar inputs.
        // Window inputs are rewritten before extraction into WindowFunctionInfo;
        // lambda bodies are processed before their RemoteExpr compilation.
        _ => {}
    }

    // Fold the implicit conversion only when its input can be evaluated as a
    // constant. This also handles nullable/TRY wrappers around literals left
    // by binding, without stripping or reinterpreting those wrappers first.
    // eval_constant rejects runtime inputs and failures; standalone explicit
    // casts remain untouched.
    if matches!(scalar, ScalarExpr::AutoCast(_))
        && let Ok(checked) = scalar.as_expr()
        && checked.data_type() == scalar.data_type().as_ref()
        && checked.is_deterministic(&BUILTIN_FUNCTIONS)
        && let Some(value) = FoldRules::eval_constant(func_ctx, &BUILTIN_FUNCTIONS, &checked)
    {
        *scalar = constant(scalar.span(), value, checked.into_data_type());
    }
}

fn constant(span: Span, value: Scalar, data_type: DataType) -> ScalarExpr {
    let inferred_type = value.as_ref().infer_data_type();
    let constant = ConstantExpr { span, value };
    if inferred_type == data_type {
        constant.into()
    } else {
        ScalarExpr::TypedConstantExpr(constant, data_type)
    }
}
