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
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr as CheckedExpr;
use databend_common_expression::Scalar;
use databend_common_expression::shrink_scalar;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprId;
use super::TypeChecker;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

impl<'a> CoreExprArena<'a> {
    pub(super) fn literal(&mut self, span: Span, value: Literal) -> CoreExprId {
        self.alloc(CoreExpr::Literal {
            span,
            value: literal_value(&value),
        })
    }

    pub(super) fn map(&mut self, span: Span, kvs: &'a [(Literal, Expr)]) -> Result<CoreExprId> {
        let kvs = kvs
            .iter()
            .map(|(key, value)| Ok((key.clone(), self.lower_ast_expr(value)?)))
            .collect::<Result<_>>()?;
        Ok(self.alloc(CoreExpr::Map { span, kvs }))
    }

    pub(super) fn tuple(&mut self, span: Span, exprs: &'a [Expr]) -> Result<CoreExprId> {
        let exprs = self.lower_expr_args(exprs)?;
        Ok(self.alloc(CoreExpr::Tuple { span, exprs }))
    }
}

impl<'a, A> TypeChecker<'a, A> {
    #[inline]
    pub(super) fn resolve_literal(
        &self,
        span: Span,
        literal: &Literal,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let (value, data_type) = infer_literal_data_type(literal_value(literal));

        let scalar_expr = ScalarExpr::ConstantExpr(ConstantExpr { span, value });
        Ok(Box::new((scalar_expr, data_type)))
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    pub(super) fn resolve_array(
        &mut self,
        span: Span,
        arguments: Vec<(ScalarExpr, DataType)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let constants = arguments
            .iter()
            .map(|(argument, data_type)| {
                let (ScalarExpr::ConstantExpr(constant)
                | ScalarExpr::TypedConstantExpr(constant, _)) = argument
                else {
                    return None;
                };
                Some(CheckedExpr::<usize>::Constant(Constant {
                    span: constant.span,
                    scalar: constant.value.clone(),
                    data_type: data_type.clone(),
                }))
            })
            .collect::<Option<Vec<_>>>();

        if let Some(constants) = constants
            && !constants.is_empty()
        {
            // Retain the efficient constant-array builder, but let the checker
            // choose both the common type and each element's cast operation.
            // A destination type alone cannot distinguish strict from TRY.
            let checked =
                type_check::check_function(span, "array", &[], &constants, &BUILTIN_FUNCTIONS)?;
            let CheckedExpr::FunctionCall(call) = checked else {
                unreachable!("a nonempty array is a checked function call")
            };
            let DataType::Array(element_type) = call.return_type else {
                unreachable!("a nonempty array has an Array return type")
            };
            let block = DataBlock::empty();
            let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let casted = call
                .args
                .into_iter()
                .map(|argument| match argument {
                    CheckedExpr::Constant(constant) => Ok(constant.scalar),
                    argument => Ok(evaluator.run(&argument)?.into_scalar().unwrap()),
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Self::build_constant_array(span, *element_type, casted));
        }

        self.resolve_scalar_function_call(
            span,
            "array",
            vec![],
            arguments.into_iter().map(|(scalar, _)| scalar).collect(),
        )
    }

    fn build_constant_array(
        span: Span,
        element_ty: DataType,
        values: Vec<Scalar>,
    ) -> Box<(ScalarExpr, DataType)> {
        let mut builder = ColumnBuilder::with_capacity(&element_ty, values.len());
        for value in &values {
            builder.push(value.as_ref());
        }
        let scalar = Scalar::Array(builder.build());
        Box::new((
            ConstantExpr {
                span,
                value: scalar,
            }
            .into(),
            DataType::Array(Box::new(element_ty)),
        ))
    }

    pub(super) fn resolve_map(
        &mut self,
        span: Span,
        keys: Vec<ScalarExpr>,
        vals: Vec<ScalarExpr>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let deref!((key_arg, _data_type)) =
            self.resolve_scalar_function_call(span, "array", vec![], keys)?;
        let deref!((val_arg, _data_type)) =
            self.resolve_scalar_function_call(span, "array", vec![], vals)?;
        self.resolve_scalar_function_call(span, "map", vec![], vec![key_arg, val_arg])
    }

    pub(super) fn resolve_tuple(
        &mut self,
        span: Span,
        args: Vec<ScalarExpr>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        self.resolve_scalar_function_call(span, "tuple", vec![], args)
    }
}

fn literal_value(literal: &Literal) -> Scalar {
    match literal {
        Literal::UInt64(value) => Scalar::Number(NumberScalar::UInt64(*value)),
        Literal::Decimal256 {
            value,
            precision,
            scale,
        } => Scalar::Decimal(DecimalScalar::Decimal256(
            i256(*value),
            DecimalSize::new_unchecked(*precision, *scale),
        )),
        Literal::Float64(float) => Scalar::Number(NumberScalar::Float64((*float).into())),
        Literal::String(string) => Scalar::String(string.clone()),
        Literal::Binary(bytes) => Scalar::Binary(bytes.clone()),
        Literal::Boolean(boolean) => Scalar::Boolean(*boolean),
        Literal::Null => Scalar::Null,
    }
}

pub(super) fn infer_literal_data_type(value: Scalar) -> (Scalar, DataType) {
    let value = shrink_scalar(value);
    let data_type = value.as_ref().infer_data_type();
    (value, data_type)
}
