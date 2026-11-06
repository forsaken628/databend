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

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::type_check;
use databend_common_expression::type_check::CheckOptions;
use databend_common_expression::type_check::CheckedNode;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::plans::AutoCastExpr;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

/// Infer the whole expression once, then materialize its source-node types.
/// The annotation walk never searches for overloads or freezes child types.
pub(super) fn retype(scalar: ScalarExpr, options: CheckOptions) -> Result<(ScalarExpr, DataType)> {
    let (checked, nodes) =
        type_check::check_with_types(&scalar.as_raw_expr(), &BUILTIN_FUNCTIONS, options)?;
    let mut nodes = nodes.into_iter();
    let scalar = annotate(scalar, &mut nodes)?;
    if nodes.next().is_some() {
        return Err(ErrorCode::Internal("unused scalar type annotations"));
    }
    Ok((scalar, checked.into_data_type()))
}

#[recursive::recursive]
fn annotate(
    scalar: ScalarExpr,
    nodes: &mut impl Iterator<Item = CheckedNode>,
) -> Result<ScalarExpr> {
    // Lowering erases this wrapper; there is no source-node annotation for it.
    if let ScalarExpr::UDFLambdaCall(mut udf) = scalar {
        udf.scalar = Box::new(annotate(*udf.scalar, nodes)?);
        return Ok(udf.into());
    }
    let mut scalar = match scalar {
        ScalarExpr::FunctionCall(mut call) => {
            call.arguments = call
                .arguments
                .into_iter()
                .map(|arg| annotate(arg, nodes))
                .collect::<Result<_>>()?;
            call.into()
        }
        ScalarExpr::CastExpr(mut cast) => {
            cast.argument = Box::new(annotate(*cast.argument, nodes)?);
            cast.into()
        }
        ScalarExpr::AutoCast(mut cast) => {
            cast.argument = Box::new(annotate(*cast.argument, nodes)?);
            ScalarExpr::AutoCast(cast)
        }
        ScalarExpr::LambdaFunction(mut lambda) => {
            lambda.args = lambda
                .args
                .into_iter()
                .map(|arg| annotate(arg, nodes))
                .collect::<Result<_>>()?;
            lambda.into()
        }
        // These nodes lower to a single input, not their contained expressions.
        other => other,
    };
    let checked = nodes
        .next()
        .ok_or_else(|| ErrorCode::Internal("missing scalar type annotation"))?;
    if let ScalarExpr::FunctionCall(call) = &mut scalar {
        call.return_type = Box::new(checked.data_type);
        if let Some(arguments) = checked.arguments {
            if arguments.len() != call.arguments.len() {
                return Err(ErrorCode::Internal(
                    "scalar argument annotation count mismatch",
                ));
            }
            call.arguments = std::mem::take(&mut call.arguments)
                .into_iter()
                .zip(arguments)
                .map(|(arg, checked)| {
                    if let Some(value) = checked.constant {
                        return constant(arg.span(), value, checked.data_type);
                    }
                    if arg.data_type().as_ref() == &checked.data_type {
                        return arg;
                    }
                    if checked.is_try {
                        CastExpr {
                            span: arg.span(),
                            is_try: true,
                            argument: Box::new(arg),
                            target_type: Box::new(checked.data_type),
                        }
                        .into()
                    } else {
                        ScalarExpr::AutoCast(AutoCastExpr::new(arg, checked.data_type))
                    }
                })
                .collect();
        }
    }
    Ok(scalar)
}

pub(super) fn constant(span: Span, value: Scalar, data_type: DataType) -> ScalarExpr {
    let inferred_type = value.as_ref().infer_data_type();
    let constant = ConstantExpr { span, value };
    if inferred_type == data_type {
        constant.into()
    } else {
        ScalarExpr::TypedConstantExpr(constant, data_type)
    }
}
