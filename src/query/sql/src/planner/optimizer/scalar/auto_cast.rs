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

use databend_common_expression::FoldRules;
use databend_common_expression::FunctionContext;
use databend_common_expression::conversion::classify_conversion;
use databend_common_expression::type_check::CheckOptions;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::ScalarRewrite;
use super::ScalarRule;
use super::children;
use super::children_mut;
use super::contains_column_ref;
use crate::plans::AutoCastExpr;
use crate::plans::ScalarExpr;

/// Move a lossless numeric comparison conversion from a runtime input to a
/// literal, if that literal keeps its value in the original comparison domain.
/// The rule proposes syntax, not an overload or a locally committed type.
pub(super) struct MoveAutoCastToConstants;

struct RewriteSite {
    path: Vec<usize>,
    column: usize,
    column_type: DataType,
}

impl ScalarRule for MoveAutoCastToConstants {
    fn candidates<'a>(
        &'a self,
        expression: &'a ScalarExpr,
        func_ctx: &'a FunctionContext,
    ) -> Box<dyn Iterator<Item = ScalarRewrite> + 'a> {
        let mut sites = Vec::new();
        find_rewrites(expression, func_ctx, &mut Vec::new(), &mut sites);
        Box::new(sites.into_iter().map(|site| {
            let mut candidate = expression.clone();
            let mut node = &mut candidate;
            for index in site.path {
                node = &mut children_mut(node)[index];
            }
            let ScalarExpr::FunctionCall(call) = node else {
                unreachable!("rewrite paths name function calls")
            };
            call.arguments = std::mem::take(&mut call.arguments)
                .into_iter()
                .enumerate()
                .map(|(index, arg)| {
                    if index == site.column {
                        let ScalarExpr::AutoCast(cast) = arg else {
                            unreachable!("rewrite sites name inferred conversions")
                        };
                        *cast.argument
                    } else {
                        ScalarExpr::AutoCast(AutoCastExpr::new(
                            strip_auto_casts(arg),
                            site.column_type.clone(),
                        ))
                    }
                })
                .collect();
            ScalarRewrite {
                expression: candidate,
                options: CheckOptions {
                    forbid_column_casts: true,
                    prefer_exact_arguments: true,
                },
            }
        }))
    }
}

#[recursive::recursive]
fn find_rewrites(
    scalar: &ScalarExpr,
    func_ctx: &FunctionContext,
    path: &mut Vec<usize>,
    sites: &mut Vec<RewriteSite>,
) {
    if let ScalarExpr::FunctionCall(call) = scalar
        && matches!(
            call.func_name.as_str(),
            "eq" | "noteq" | "gt" | "gte" | "lt" | "lte"
        )
        && call.arguments.len() == 2
    {
        for column in 0..2 {
            let ScalarExpr::AutoCast(cast) = &call.arguments[column] else {
                continue;
            };
            let column_type = cast.argument.data_type();
            if !contains_column_ref(&cast.argument)
                || !matches!(
                    column_type.remove_nullable(),
                    DataType::Number(_) | DataType::Decimal(_)
                )
                || !matches!(
                    cast.target_type.remove_nullable(),
                    DataType::Number(_) | DataType::Decimal(_)
                )
                || !classify_conversion(&column_type, &cast.target_type).is_lossless_injective()
            {
                continue;
            }
            let other = &call.arguments[1 - column];
            if other.data_type().as_ref() != cast.target_type.as_ref() {
                continue;
            }
            let literal = auto_cast_argument(other);
            if !matches!(
                literal,
                ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _)
            ) {
                continue;
            }
            let converted = ScalarExpr::AutoCast(AutoCastExpr::new(
                literal.clone(),
                column_type.clone().into_owned(),
            ));
            let round_trip =
                ScalarExpr::AutoCast(AutoCastExpr::new(converted, *cast.target_type.clone()));
            let same_value = other.as_expr().ok().and_then(|original| {
                let original = FoldRules::eval_constant(func_ctx, &BUILTIN_FUNCTIONS, &original)?;
                let round_trip = round_trip.as_expr().ok()?;
                let converted =
                    FoldRules::eval_constant(func_ctx, &BUILTIN_FUNCTIONS, &round_trip)?;
                Some(original == converted)
            });
            if same_value == Some(true) {
                sites.push(RewriteSite {
                    path: path.clone(),
                    column,
                    column_type: column_type.into_owned(),
                });
            }
        }
    }
    for (index, child) in children(scalar).iter().enumerate() {
        path.push(index);
        find_rewrites(child, func_ctx, path, sites);
        path.pop();
    }
}

fn auto_cast_argument(mut scalar: &ScalarExpr) -> &ScalarExpr {
    while let ScalarExpr::AutoCast(cast) = scalar {
        scalar = &cast.argument;
    }
    scalar
}

fn strip_auto_casts(mut scalar: ScalarExpr) -> ScalarExpr {
    while let ScalarExpr::AutoCast(cast) = scalar {
        scalar = *cast.argument;
    }
    scalar
}
