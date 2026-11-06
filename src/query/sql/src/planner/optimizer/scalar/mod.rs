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

//! Optimization of a complete bound scalar expression.
//!
//! Binding keeps name resolution, context-sensitive semantics and existing
//! bind-time folding. This first phase only separates implicit conversion
//! choices and their constant folding. Rules propose expressions; the driver
//! owns whole-expression inference, acceptance and rollback. It uses a strict
//! improvement schedule for now, without committing the rule API to it.

use databend_common_exception::Result;
use databend_common_expression::FoldRules;
use databend_common_expression::FunctionContext;
use databend_common_expression::type_check::CheckOptions;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::plans::ScalarExpr;

mod auto_cast;
mod type_check;

/// A proposed rewrite, not an in-place mutation of the accepted expression.
pub struct ScalarRewrite {
    pub expression: ScalarExpr,
    pub options: CheckOptions,
}

/// Rule application and exploration strategy are deliberately separate.
/// Implementations must establish evaluation equivalence, including NULL and
/// failure behavior; the driver's type and cost checks do not prove it.
pub trait ScalarRule {
    fn candidates<'a>(
        &'a self,
        expression: &'a ScalarExpr,
        func_ctx: &'a FunctionContext,
    ) -> Box<dyn Iterator<Item = ScalarRewrite> + 'a>;
}

/// Finish a bound expression. The externally reported type is the contract;
/// intermediate function types may change when the whole tree is re-inferred.
pub fn optimize(
    scalar: ScalarExpr,
    data_type: DataType,
    func_ctx: &FunctionContext,
) -> Result<(ScalarExpr, DataType)> {
    let (current, inferred_type) = type_check::retype(scalar, CheckOptions {
        prefer_exact_arguments: true,
        ..Default::default()
    })?;
    // Some specialized binders intentionally report a wider type than the
    // scalar itself (e.g. nullable tuple -> Variant). Preserve that boundary.
    let rule = auto_cast::MoveAutoCastToConstants;
    let mut current = apply_rules(current, &inferred_type, func_ctx, &[&rule]);
    fold_implicit_constants(&mut current, func_ctx);
    Ok((current, data_type))
}

fn apply_rules(
    mut current: ScalarExpr,
    inferred_type: &DataType,
    func_ctx: &FunctionContext,
    rules: &[&dyn ScalarRule],
) -> ScalarExpr {
    loop {
        let cost = column_casts(&current);
        if cost == 0 {
            break;
        }
        let improved = rules.iter().find_map(|rule| {
            rule.candidates(&current, func_ctx).find_map(|rewrite| {
                let (candidate, candidate_type) =
                    type_check::retype(rewrite.expression, rewrite.options).ok()?;
                if &candidate_type != inferred_type || column_casts(&candidate) >= cost {
                    return None;
                }
                // Lowering stores names, not resolved FunctionIDs. Check the final
                // tree too: materialized argument types must bind to the same type.
                let lowered = candidate.as_expr().ok()?;
                (lowered.data_type() == inferred_type).then_some(candidate)
            })
        });
        match improved {
            Some(candidate) => current = candidate,
            None => break,
        }
    }
    current
}

#[recursive::recursive]
fn fold_implicit_constants(scalar: &mut ScalarExpr, func_ctx: &FunctionContext) {
    for child in children_mut(scalar) {
        fold_implicit_constants(child, func_ctx);
    }
    if let ScalarExpr::AutoCast(cast) = scalar
        && !contains_column_ref(&cast.argument)
        && let Ok(checked) = scalar.as_expr()
        && checked.is_deterministic(&BUILTIN_FUNCTIONS)
        && let Some(value) = FoldRules::eval_constant(func_ctx, &BUILTIN_FUNCTIONS, &checked)
    {
        *scalar = type_check::constant(scalar.span(), value, checked.into_data_type());
    }
}

#[recursive::recursive]
fn column_casts(scalar: &ScalarExpr) -> usize {
    usize::from(matches!(scalar, ScalarExpr::AutoCast(cast)
        if cast.argument.data_type().as_ref() != cast.target_type.as_ref()
            && contains_column_ref(&cast.argument)))
        + children(scalar).iter().map(column_casts).sum::<usize>()
}

#[recursive::recursive]
fn contains_column_ref(scalar: &ScalarExpr) -> bool {
    match scalar {
        ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _) => false,
        ScalarExpr::FunctionCall(_)
        | ScalarExpr::CastExpr(_)
        | ScalarExpr::AutoCast(_)
        | ScalarExpr::LambdaFunction(_)
        | ScalarExpr::UDFLambdaCall(_) => children(scalar).iter().any(contains_column_ref),
        _ => true,
    }
}

// Match the region traversed by ScalarExpr::as_raw_expr. Aggregate/window/UDF
// operators are runtime inputs, not ordinary scalar function nodes.
fn children(scalar: &ScalarExpr) -> &[ScalarExpr] {
    match scalar {
        ScalarExpr::FunctionCall(call) => &call.arguments,
        ScalarExpr::CastExpr(cast) => std::slice::from_ref(cast.argument.as_ref()),
        ScalarExpr::AutoCast(cast) => std::slice::from_ref(cast.argument.as_ref()),
        ScalarExpr::LambdaFunction(lambda) => &lambda.args,
        ScalarExpr::UDFLambdaCall(udf) => std::slice::from_ref(udf.scalar.as_ref()),
        _ => &[],
    }
}

fn children_mut(scalar: &mut ScalarExpr) -> &mut [ScalarExpr] {
    match scalar {
        ScalarExpr::FunctionCall(call) => &mut call.arguments,
        ScalarExpr::CastExpr(cast) => std::slice::from_mut(cast.argument.as_mut()),
        ScalarExpr::AutoCast(cast) => std::slice::from_mut(cast.argument.as_mut()),
        ScalarExpr::LambdaFunction(lambda) => &mut lambda.args,
        ScalarExpr::UDFLambdaCall(udf) => std::slice::from_mut(udf.scalar.as_mut()),
        _ => &mut [],
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::Symbol;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::plans::AutoCastExpr;
    use crate::plans::BoundColumnRef;
    use crate::plans::ConstantExpr;
    use crate::plans::FunctionCall;

    struct Attempts {
        valid: auto_cast::MoveAutoCastToConstants,
    }

    impl ScalarRule for Attempts {
        fn candidates<'a>(
            &'a self,
            expression: &'a ScalarExpr,
            func_ctx: &'a FunctionContext,
        ) -> Box<dyn Iterator<Item = ScalarRewrite> + 'a> {
            let wrong_type = ConstantExpr {
                span: None,
                value: Scalar::Boolean(true),
            }
            .into();
            let invalid = FunctionCall {
                span: None,
                func_name: "unknown_scalar_rewrite".to_string(),
                params: vec![],
                arguments: vec![],
                return_type: Box::new(DataType::Boolean.wrap_nullable()),
            }
            .into();
            Box::new(
                [wrong_type, invalid]
                    .into_iter()
                    .map(|expression| ScalarRewrite {
                        expression,
                        options: CheckOptions::default(),
                    })
                    .chain(self.valid.candidates(expression, func_ctx)),
            )
        }
    }

    #[test]
    fn failed_attempts_preserve_the_accepted_expression() -> Result<()> {
        let source_type = DataType::Number(NumberDataType::Int32).wrap_nullable();
        let target_type = DataType::Number(NumberDataType::Int64).wrap_nullable();
        let column = BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "c".to_string(),
                Symbol::new(0),
                Box::new(source_type),
                Visibility::Visible,
            )
            .build(),
        }
        .into();
        let constant = ConstantExpr {
            span: None,
            value: Scalar::Number(NumberScalar::Int64(1)),
        }
        .into();
        let original: ScalarExpr = FunctionCall {
            span: None,
            func_name: "gt".to_string(),
            params: vec![],
            arguments: vec![
                ScalarExpr::AutoCast(AutoCastExpr::new(column, target_type.clone())),
                ScalarExpr::AutoCast(AutoCastExpr::new(constant, target_type)),
            ],
            return_type: Box::new(DataType::Boolean.wrap_nullable()),
        }
        .into();
        let snapshot = original.clone();
        let rule = Attempts {
            valid: auto_cast::MoveAutoCastToConstants,
        };
        let optimized = apply_rules(
            original.clone(),
            &DataType::Boolean.wrap_nullable(),
            &FunctionContext::default(),
            &[&rule],
        );
        assert_eq!(original, snapshot);
        assert_eq!(column_casts(&original), 1);
        assert_eq!(column_casts(&optimized), 0);
        assert_eq!(
            optimized.as_expr()?.data_type(),
            &DataType::Boolean.wrap_nullable()
        );
        Ok(())
    }

    struct RemoveInnerCast;

    impl ScalarRule for RemoveInnerCast {
        fn candidates<'a>(
            &'a self,
            expression: &'a ScalarExpr,
            _func_ctx: &'a FunctionContext,
        ) -> Box<dyn Iterator<Item = ScalarRewrite> + 'a> {
            let mut expression = expression.clone();
            let ScalarExpr::CastExpr(root) = &mut expression else {
                unreachable!()
            };
            let ScalarExpr::FunctionCall(call) = root.argument.as_mut() else {
                unreachable!()
            };
            let ScalarExpr::AutoCast(cast) = call.arguments.remove(0) else {
                unreachable!()
            };
            call.arguments.push(*cast.argument);
            // Deliberately keep the stale return_type. Whole-expression
            // inference, not the proposal, is responsible for refreshing it.
            Box::new(std::iter::once(ScalarRewrite {
                expression,
                options: CheckOptions {
                    forbid_column_casts: true,
                    prefer_exact_arguments: true,
                },
            }))
        }
    }

    #[test]
    fn internal_types_are_reinferred_under_the_root_contract() -> Result<()> {
        let source_type = DataType::Number(NumberDataType::Int32).wrap_nullable();
        let target_type = DataType::Number(NumberDataType::Int64).wrap_nullable();
        let column = BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "c".to_string(),
                Symbol::new(0),
                Box::new(source_type),
                Visibility::Visible,
            )
            .build(),
        }
        .into();
        let call: ScalarExpr = FunctionCall {
            span: None,
            func_name: "assume_not_null".to_string(),
            params: vec![],
            arguments: vec![ScalarExpr::AutoCast(AutoCastExpr::new(
                column,
                target_type.clone(),
            ))],
            return_type: Box::new(target_type.remove_nullable()),
        }
        .into();
        let original: ScalarExpr = crate::plans::CastExpr {
            span: None,
            is_try: false,
            argument: Box::new(call),
            target_type: Box::new(target_type.clone()),
        }
        .into();
        let optimized = apply_rules(original, &target_type, &FunctionContext::default(), &[
            &RemoveInnerCast,
        ]);
        assert_eq!(column_casts(&optimized), 0);
        let ScalarExpr::CastExpr(root) = &optimized else {
            unreachable!()
        };
        assert_eq!(
            root.argument.data_type().as_ref(),
            &DataType::Number(NumberDataType::Int32)
        );
        assert_eq!(optimized.data_type().as_ref(), &target_type);
        assert_eq!(optimized.as_expr()?.data_type(), &target_type);
        Ok(())
    }
}
