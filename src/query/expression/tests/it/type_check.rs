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

use databend_common_exception::Result;
use databend_common_expression::Expr;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::type_check;
use databend_common_expression::type_check::CheckOptions;
use databend_common_expression::type_check::ExprChecker;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;

fn register_predicate(registry: &mut FunctionRegistry, name: &str, argument: DataType) {
    registry.register_function(Function {
        signature: FunctionSignature {
            name: name.to_string(),
            args_type: vec![argument.clone(), argument],
            return_type: DataType::Boolean,
        },
        eval: FunctionEval::Scalar {
            calc_domain: domain_evaluator(|_, _| FunctionDomain::Full),
            eval: scalar_evaluator(|_, _| Value::Scalar(Scalar::Boolean(true))),
            derive_stat: None,
        },
    });
}

#[test]
fn inference_options_cover_nested_calls() -> Result<()> {
    let int32 = DataType::Number(NumberDataType::Int32);
    let int64 = DataType::Number(NumberDataType::Int64);
    let mut registry = FunctionRegistry::empty();
    registry.register_default_cast_rules([(int32.clone(), int64.clone())]);
    // Binding takes the first legal signature, even when a later signature
    // would avoid a runtime cast. That preference belongs to scalar rules.
    register_predicate(&mut registry, "probe", int64.clone());
    register_predicate(&mut registry, "probe", int32.clone());
    register_predicate(&mut registry, "and", DataType::Boolean);
    registry.register_aliases("probe", &["probe_alias"]);
    let predicate = RawExpr::FunctionCall {
        span: None,
        name: "probe_alias".to_string(),
        params: vec![],
        args: vec![
            RawExpr::ColumnRef {
                span: None,
                id: 0_usize,
                data_type: int32.clone(),
                display_name: "c".to_string(),
            },
            RawExpr::Constant {
                span: None,
                scalar: Scalar::from(1_i32),
                data_type: None,
            },
        ],
    };
    let raw = RawExpr::Cast {
        span: None,
        is_try: false,
        dest_type: DataType::String,
        expr: Box::new(RawExpr::FunctionCall {
            span: None,
            name: "and".to_string(),
            params: vec![],
            args: vec![predicate.clone(), predicate],
        }),
    };
    let assert_argument_types = |checked: &Expr<usize>, expected: &DataType| {
        let Expr::Cast(root) = checked else {
            unreachable!()
        };
        let Expr::FunctionCall(conjunction) = root.expr.as_ref() else {
            unreachable!()
        };
        for predicate in &conjunction.args {
            let Expr::FunctionCall(call) = predicate else {
                unreachable!()
            };
            assert_eq!(call.args[0].data_type(), expected);
            assert_eq!(call.args[1].data_type(), expected);
        }
    };
    let checker = ExprChecker::new(&registry, CheckOptions::default());
    let default = checker.check(&raw)?;
    assert_eq!(default, type_check::check(&raw, &registry)?);
    assert_argument_types(&default, &int64);
    let options = CheckOptions {
        forbid_column_casts: true,
        ..Default::default()
    };
    let constrained = ExprChecker::new(&registry, options).check(&raw)?;
    assert_eq!(
        constrained,
        type_check::check_with_options(&raw, &registry, options)?
    );
    assert_ne!(default, constrained);
    assert_argument_types(&constrained, &int32);
    let exact = type_check::check_with_options(&raw, &registry, CheckOptions {
        prefer_exact_arguments: true,
        ..Default::default()
    })?;
    assert_eq!(constrained, exact);
    Ok(())
}
