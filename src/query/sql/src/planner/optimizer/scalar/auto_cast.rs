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

use databend_common_expression::Constant;
use databend_common_expression::Expr;
use databend_common_expression::FoldRules;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::conversion::classify_conversion;
use databend_common_expression::type_check;
use databend_common_expression::type_check::CheckOptions;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

/// Match `cmp(AutoCast<U>(e:T), k:U)` (or the reversed arguments) after
/// literal AutoCasts have folded. Change it to `cmp(e:T, k':T)` only when
/// widening T to U is lossless and k' round-trips to k in U.
pub(super) fn move_cast_to_constant(call: &mut FunctionCall, func_ctx: &FunctionContext) {
    if !matches!(
        call.func_name.as_str(),
        "eq" | "noteq" | "gt" | "gte" | "lt" | "lte"
    ) || call.arguments.len() != 2
    {
        return;
    }
    for runtime in 0..2 {
        let ScalarExpr::AutoCast(cast) = &call.arguments[runtime] else {
            continue;
        };
        let source_type = cast.argument.data_type();
        if !matches!(
            source_type.remove_nullable(),
            DataType::Number(_) | DataType::Decimal(_)
        ) || !matches!(
            cast.target_type.remove_nullable(),
            DataType::Number(_) | DataType::Decimal(_)
        ) || !classify_conversion(&source_type, &cast.target_type).is_lossless_injective()
        {
            continue;
        }
        let other = &call.arguments[1 - runtime];
        let (ScalarExpr::ConstantExpr(literal) | ScalarExpr::TypedConstantExpr(literal, _)) = other
        else {
            continue;
        };
        if other.data_type().as_ref() != cast.target_type.as_ref() {
            continue;
        }
        let original: Expr<usize> = Constant {
            span: other.span(),
            scalar: literal.value.clone(),
            data_type: *cast.target_type.clone(),
        }
        .into();
        let Some(converted) = type_check::check_cast(
            other.span(),
            false,
            original,
            &source_type,
            &BUILTIN_FUNCTIONS,
        )
        .ok()
        .and_then(|expr| FoldRules::eval_constant(func_ctx, &BUILTIN_FUNCTIONS, &expr)) else {
            continue;
        };
        let narrowed: Expr<usize> = Constant {
            span: other.span(),
            scalar: converted.clone(),
            data_type: source_type.clone().into_owned(),
        }
        .into();
        let round_trip = type_check::check_cast(
            other.span(),
            false,
            narrowed,
            &cast.target_type,
            &BUILTIN_FUNCTIONS,
        )
        .ok()
        .and_then(|expr| FoldRules::eval_constant(func_ctx, &BUILTIN_FUNCTIONS, &expr));
        if round_trip.as_ref() != Some(&literal.value) {
            continue;
        }

        // Only this comparison changes. Check its proposed argument types,
        // not the bound runtime subtree, and reject any new runtime coercion
        // or result nullability change before mutating the call.
        let raw: RawExpr<usize> = RawExpr::FunctionCall {
            span: call.span,
            name: call.func_name.clone(),
            params: call.params.clone(),
            args: (0..2)
                .map(|id| RawExpr::ColumnRef {
                    span: None,
                    id,
                    data_type: source_type.clone().into_owned(),
                    display_name: id.to_string(),
                })
                .collect(),
        };
        let Ok(checked) = type_check::check_with_options(&raw, &BUILTIN_FUNCTIONS, CheckOptions {
            forbid_column_casts: true,
            prefer_exact_arguments: true,
        }) else {
            continue;
        };
        if checked.data_type() != call.return_type.as_ref() {
            continue;
        }
        let replacement = super::constant(other.span(), converted, source_type.into_owned());
        let ScalarExpr::AutoCast(cast) = call.arguments.remove(runtime) else {
            unreachable!()
        };
        call.arguments.insert(runtime, *cast.argument);
        call.arguments[1 - runtime] = replacement;
        return;
    }
}
