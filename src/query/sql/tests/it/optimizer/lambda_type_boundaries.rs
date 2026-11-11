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

use std::collections::HashMap;
use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::FormatOptions;
use databend_common_sql::optimizer::ir::ExprVisitor;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::PVisitAction;
use databend_common_sql::optimizer::ir::Physical;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::LambdaFunc;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::plans::Visitor;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

struct LambdaEvaluator<'a> {
    tuple: Scalar,
    n: Option<i32>,
    expected: &'a str,
    count: usize,
}

impl<'a> Visitor<'a> for LambdaEvaluator<'_> {
    fn visit_lambda_function(&mut self, lambda: &'a LambdaFunc) -> Result<()> {
        let expr = ScalarExpr::LambdaFunction(lambda.clone()).as_expr()?;
        let values = expr
            .column_refs()
            .into_keys()
            .map(|column| {
                let value = match column.column_name.as_str() {
                    "t" => self.tuple.clone(),
                    "n" => self.n.map_or(Scalar::Null, Scalar::from),
                    "nn" => Scalar::from(1_i32),
                    "big" => Scalar::from(1_i64),
                    name => panic!("unexpected lambda input: {name}"),
                };
                (column, value)
            })
            .collect::<HashMap<_, _>>();
        let expr = expr.fill_const_column(&values).project_column_ref(|_| {
            unreachable!("all outer lambda inputs must have test values")
        })?;
        let block = DataBlock::empty_with_rows(1);
        let func_ctx = FunctionContext::default();
        let result = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS).run(&expr)?;
        assert_eq!(
            result.index(0).unwrap().to_string(),
            self.expected,
            "{}: tuple={:?}, n={:?}",
            lambda.func_name,
            self.tuple,
            self.n
        );
        self.count += 1;
        Ok(())
    }
}

fn evaluate_logical(expr: &SExpr, evaluator: &mut LambdaEvaluator<'_>) -> Result<()> {
    for scalar in expr.plan().scalar_expr_iter() {
        Visitor::visit(evaluator, scalar)?;
    }
    for child in expr.children() {
        evaluate_logical(child, evaluator)?;
    }
    Ok(())
}

impl ExprVisitor<Physical> for LambdaEvaluator<'_> {
    fn visit(&mut self, expr: &PExpr) -> Result<PVisitAction> {
        for scalar in expr.plan().scalar_expr_iter() {
            Visitor::visit(self, scalar)?;
        }
        Ok(PVisitAction::Continue)
    }
}

async fn write_case(
    file: &mut impl Write,
    case: &SqlTestCase,
    n: Option<i32>,
    expected: [&str; 2],
) -> Result<()> {
    let ctx = setup_context(case).await?;
    let raw = ctx.bind_sql(case.sql).await?;
    let optimized = ctx.optimize_plan(raw.clone()).await?;
    write_case_header(file, case)?;
    let options = FormatOptions {
        show_auto_cast: true,
        ..Default::default()
    };
    writeln!(
        file,
        "raw_plan:\n{}",
        raw.format_indent(options.clone(), &ctx.stat_context()?)?
    )?;
    writeln!(
        file,
        "optimized_plan:\n{}",
        optimized.format_indent(options, &ctx.stat_context()?)?
    )?;

    for (tuple, expected) in [
        Scalar::Null,
        Scalar::Tuple(vec![Scalar::from(7_i32), Scalar::String("v".into())]),
    ]
    .into_iter()
    .zip(expected)
    {
        let mut evaluator = LambdaEvaluator {
            tuple,
            n,
            expected,
            count: 0,
        };
        let Plan::Query {
            s_expr, metadata, ..
        } = &raw
        else {
            panic!("expected query")
        };
        s_expr.logical()?.validate_types(metadata)?;
        evaluate_logical(s_expr.logical()?, &mut evaluator)?;
        assert!(evaluator.count > 0, "raw lambda missing: {}", case.name);
        evaluator.count = 0;
        let Plan::Query {
            s_expr, metadata, ..
        } = &optimized
        else {
            panic!("expected query")
        };
        let physical = s_expr.planned()?.expr();
        physical.validate_types(metadata)?;
        physical.accept(&mut evaluator)?;
        assert!(
            evaluator.count > 0,
            "optimized lambda missing: {}",
            case.name
        );
    }
    Ok(())
}

const SETUP: &[&str] = &[
    "CREATE TABLE lambda_boundary(t Tuple(a INT, b STRING) NULL, n INT NULL, nn INT NOT NULL, big BIGINT NOT NULL)",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_lambda_body_type_boundary() -> Result<()> {
    let object = [
        r#"['{"a":null,"b":null}', '{"a":null,"b":null}']"#,
        r#"['{"a":7,"b":"v"}', '{"a":7,"b":"v"}']"#,
    ];
    let cases = [
        (
            "nullable_tuple_array_body",
            "The compiled array body materializes its reported nullable Variant result without changing JSON null values.",
            "SELECT array_transform([1,2], x -> CAST(t AS VARIANT)) FROM lambda_boundary",
            object,
        ),
        (
            "nullable_tuple_map_body",
            "Map value builders and the compiled body must agree on the exact Variant nullability.",
            "SELECT map_transform_values(map(['k'], [1]), (k,x) -> CAST(t AS VARIANT)) FROM lambda_boundary",
            [
                r#"{'k':'{"a":null,"b":null}'}"#,
                r#"{'k':'{"a":7,"b":"v"}'}"#,
            ],
        ),
        (
            "nullable_tuple_json_path_body",
            "JSON path lambda compilation also materializes the nullable Variant body contract.",
            "SELECT json_path_transform(parse_json('[1,2]'), '$[*]', x -> CAST(t AS VARIANT)) FROM lambda_boundary",
            [
                r#"'[{"a":null,"b":null},{"a":null,"b":null}]'"#,
                r#"'[{"a":7,"b":"v"},{"a":7,"b":"v"}]'"#,
            ],
        ),
        (
            "try_tuple_array_body",
            "TRY already produces a nullable body and must retain its existing value behavior.",
            "SELECT array_transform([1,2], x -> TRY_CAST(t AS VARIANT)) FROM lambda_boundary",
            object,
        ),
        (
            "reduce_body_keeps_runtime_accumulator_cast",
            "Reduce's body result is String, not the accumulator type; runtime conversion remains intentional.",
            "SELECT array_reduce([1,2], (x,y) -> to_string(x + y)) FROM lambda_boundary",
            ["3", "3"],
        ),
    ];
    let mut file = open_golden_file("optimizer", "lambda_body_type_boundary.txt")?;
    for (name, description, sql, expected) in cases {
        write_case(
            &mut file,
            &SqlTestCase {
                name,
                description,
                setup_sqls: SETUP,
                sql,
            },
            Some(1),
            expected,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_lambda_capture_type_boundary() -> Result<()> {
    let cases = [
        (
            "nullable_array_capture",
            "Replacing n with 1 must keep the compiled Nullable(Int32) capture schema.",
            "SELECT n FROM lambda_boundary WHERE n = 1 AND array_any(array_transform([1,2], x -> n + x)) > 0",
            Some(1),
            "[2, 3]",
        ),
        (
            "nullable_map_capture",
            "Map lambda capture replacement must preserve its compiled input type too.",
            "SELECT n FROM lambda_boundary WHERE n = 1 AND map_size(map_transform_values(map(['k'], [1]), (k,x) -> n + x)) > 0",
            Some(1),
            "{'k':2}",
        ),
        (
            "json_path_capture_layout",
            "JSON path uses [json, path, captures...] rather than leading captures; preserve its nullable capture.",
            "SELECT n FROM lambda_boundary WHERE n = 1 AND json_path_transform(parse_json('[1,2]'), '$[*]', x -> to_variant(n)) IS NOT NULL",
            Some(1),
            "'[1,1]'",
        ),
        (
            "nonnullable_array_capture",
            "A same-type non-nullable constant remains substitutable without a redundant conversion.",
            "SELECT nn FROM lambda_boundary WHERE nn = 1 AND array_any(array_transform([1,2], x -> nn + x)) > 0",
            Some(1),
            "[2, 3]",
        ),
        (
            "multiple_capture_types",
            "Nullable and wider numeric captures retain their separate input contracts after propagation.",
            "SELECT n FROM lambda_boundary WHERE n = 1 AND big = 1 AND array_any(array_transform([1,2], x -> n + big + x)) > 0",
            Some(1),
            "[3, 4]",
        ),
        (
            "nonasserted_equality_keeps_null_capture",
            "Equalities observed under IS NULL are not assertions; a real NULL capture must not become 1 or 2.",
            "SELECT n FROM lambda_boundary WHERE is_null(n = 1 AND n = 2) AND array_any(array_transform([1,2], x -> coalesce(n, 0) + x)) > 0",
            None,
            "[1, 2]",
        ),
    ];
    let mut file = open_golden_file("optimizer", "lambda_capture_type_boundary.txt")?;
    for (name, description, sql, n, expected) in cases {
        write_case(
            &mut file,
            &SqlTestCase {
                name,
                description,
                setup_sqls: SETUP,
                sql,
            },
            n,
            [expected; 2],
        )
        .await?;
    }
    Ok(())
}
