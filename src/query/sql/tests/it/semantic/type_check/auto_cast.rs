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

use databend_common_expression::type_check;
use databend_common_expression::type_check::CheckOptions;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ExprDisplay;
use databend_common_sql::FormatOptions;
use databend_common_sql::optimizer::scalar;

use super::*;

// Keep these optimization goldens structural even though user-facing display
// hides implicit casts by default.
fn format_scalar(scalar: &ScalarExpr) -> String {
    ExprDisplay::new(&FormatOptions {
        show_auto_cast: true,
        ..Default::default()
    })
    .format_scalar(scalar)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auto_cast_expression_rules() -> Result<()> {
    init_testing_globals();
    let mut file = open_golden_file("semantic/type_check", "auto_cast.txt")?;
    for (name, sql) in [
        ("lossless_integer_conversion", "delta > 1::Int64"),
        (
            "nested_lossless_comparison",
            "if(delta > 1::Int64, number, 0)",
        ),
        (
            "multiple_lossless_sites",
            "delta > 1::Int64 AND delta < 100::Int64",
        ),
        (
            "out_of_range_constant",
            "delta > 9223372036854775807::Int64",
        ),
        (
            "lossy_integer_float_conversion",
            "number > 9007199254740992::Float64",
        ),
        (
            "explicit_numeric_conversion",
            "CAST(delta AS Int64) > 1::Int64",
        ),
        ("integer_constant_on_left", "1::Int64 < delta"),
        ("less_or_equal", "delta <= 1::Int64"),
        ("greater_or_equal", "delta >= 1::Int64"),
        ("not_equal", "delta <> 1::Int64"),
        (
            "nonnullable_runtime_expression",
            "assume_not_null(delta) > 1::Int64",
        ),
        ("same_type_columns_need_no_widening", "delta > delta"),
        ("mixed_runtime_types_are_not_rewritten", "delta > number"),
        (
            "if_result_type_is_not_changed",
            "if(flag, delta, 1::Int64) > 2::Int64",
        ),
        ("unsigned_integer_equality", "wide = 1::Int64"),
        ("string_column_integer_literal", "text > 1"),
        ("nested_comparison", "if(text > 1, number, 0)"),
        ("multiple_rewrite_sites", "text > 1 AND pattern < 2"),
        ("explicit_cast_boundary", "CAST(text > 1 AS STRING)"),
        ("explicit_column_cast", "CAST(text AS DECIMAL(38, 0)) > 1"),
        ("typed_decimal_literal", "text > 1::DECIMAL(38, 0)"),
        ("two_runtime_inputs", "text > number"),
        ("no_compatible_column_type", "if(flag, text, 1)"),
        ("nullable_predicate", "is_null(text > 1)"),
        ("implicit_timestamp_constant", "ts > '2024-01-01'"),
        ("failed_constant_cast", "number > 'not a number'"),
        ("non_deterministic_conversion", "rand() > try_to_float64(0)"),
        ("fractional_literal_not_narrowed", "delta > 1.5"),
        (
            "lambda_body_region",
            "array_transform([1::Int32, 2::Int32], x -> x > 1::Int64)",
        ),
        (
            "lambda_out_of_range",
            "array_transform([1::Int32, 2::Int32], x -> x > 9223372036854775807::Int64)",
        ),
    ] {
        let settings = Settings::create(Tenant::new_literal("default"));
        let adapter = TestTypeCheckAdapter::new(settings);
        let func_ctx = adapter.func_ctx.clone();
        let mut context = test_bind_context(ExprContext::Unknown);
        add_test_column(
            &mut context,
            7,
            "wide",
            DataType::Number(NumberDataType::UInt64),
        );
        let names = NameResolutionContext::try_from(adapter.settings.as_ref())?;
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let tokens = tokenize_sql(sql)?;
        let ast = parse_expr(&tokens, adapter.sql_dialect()?)?;
        let mut checker =
            TypeChecker::try_create_with_adapter(&mut context, adapter, &names, metadata, &[])?;
        let (bound, bound_type) = *checker.resolve(&ast)?;
        writeln!(file, "=== {name} ===\nsql: {sql}")?;
        writeln!(file, "bound: {}", format_scalar(&bound))?;
        writeln!(file, "bound_type: {bound_type}")?;
        let original = bound.clone();
        let (optimized, result_type) = scalar::optimize(bound, bound_type.clone(), &func_ctx)?;
        assert_eq!(result_type, bound_type, "{sql}");
        assert_eq!(original.data_type(), optimized.data_type(), "{sql}");
        let checked = optimized.as_expr()?;
        assert_eq!(checked.data_type(), &result_type, "{sql}");
        writeln!(file, "optimized: {}", format_scalar(&optimized))?;
        writeln!(file, "lowered: {}", checked.sql_display())?;
        writeln!(file, "type: {result_type}\n")?;

        let (again, again_type) = scalar::optimize(optimized.clone(), result_type, &func_ctx)?;
        assert_eq!(again, optimized, "optimization is idempotent: {sql}");
        assert_eq!(again_type, bound_type);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auto_cast_rules_in_production_pipeline() -> Result<()> {
    use crate::framework::golden::SqlTestCase;
    use crate::framework::golden::setup_context;
    use crate::framework::golden::write_case_header;

    let mut file = open_golden_file("semantic/type_check", "auto_cast_pipeline.txt")?;
    let options = FormatOptions {
        show_auto_cast: true,
        ..Default::default()
    };
    for (name, description, sql) in [
        (
            "filter",
            "Narrow a representable literal before relational filter normalization.",
            "SELECT i FROM cast_pipeline WHERE i > 1::Int64 AND i < 100::Int64",
        ),
        (
            "projection",
            "Keep the enclosing IF expression and its nullable result contract.",
            "SELECT if(i > 1::Int64, big, 0) FROM cast_pipeline",
        ),
        (
            "range_guard",
            "Keep the widening column conversion for an out-of-range literal.",
            "SELECT i FROM cast_pipeline WHERE i < 9223372036854775807::Int64",
        ),
        (
            "lossy_guard",
            "Do not replace the non-injective Int64 to Float64 comparison.",
            "SELECT big FROM cast_pipeline WHERE big = 9007199254740992::Float64",
        ),
        (
            "explicit_guard",
            "The scalar boundary keeps the explicit cast; later normalization may safely eliminate it.",
            "SELECT i FROM cast_pipeline WHERE CAST(i AS Int64) > 1::Int64",
        ),
        (
            "lambda_region",
            "The body is optimized before compilation, independently of lambda inputs.",
            "SELECT array_transform(a, x -> x > 1::Int64) FROM cast_pipeline",
        ),
    ] {
        let case = SqlTestCase {
            name,
            description,
            setup_sqls: &[
                "CREATE TABLE cast_pipeline(i Int32 NULL, big Int64 NULL, a Array(Int32))",
            ],
            sql,
        };
        let ctx = setup_context(&case).await?;
        let raw = ctx.bind_sql(case.sql).await?;
        write_case_header(&mut file, &case)?;
        writeln!(
            file,
            "raw_plan:\n{}",
            raw.format_indent(options.clone(), &ctx.stat_context()?)?
        )?;
        let optimized = ctx.optimize_plan(raw).await?;
        writeln!(
            file,
            "optimized_plan:\n{}",
            optimized.format_indent(options.clone(), &ctx.stat_context()?)?
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_window_auto_cast_rules_in_production_pipeline() -> Result<()> {
    use crate::framework::golden::SqlTestCase;
    use crate::framework::golden::setup_context;
    use crate::framework::golden::write_case_header;

    let mut file = open_golden_file("semantic/type_check", "auto_cast_window.txt")?;
    let options = FormatOptions {
        show_auto_cast: true,
        ..Default::default()
    };
    for (name, description, sql) in [
        (
            "partition_input_reuse",
            "Fold the nullable literal conversion in both SELECT and the partition input.",
            "SELECT i + 1 AS n, row_number() OVER (PARTITION BY i + 1 ORDER BY big) FROM cast_pipeline",
        ),
        (
            "order_input_reuse",
            "Narrow the comparison in both SELECT and ORDER BY, preserving DESC and NULLS FIRST.",
            "SELECT i > 1::Int64 AS n, row_number() OVER (ORDER BY i > 1::Int64 DESC NULLS FIRST) FROM cast_pipeline",
        ),
        (
            "aggregate_argument",
            "Optimize the aggregate window input without changing its frame or return type.",
            "SELECT i + 1 AS n, sum(i + 1) OVER (PARTITION BY big ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM cast_pipeline",
        ),
        (
            "lag_argument_and_default",
            "Optimize both the LAG argument and its default before extracting input columns.",
            "SELECT i > 1::Int64 AS n, lag(i > 1::Int64, 1, i > 1::Int64) OVER (ORDER BY big) FROM cast_pipeline",
        ),
        (
            "nth_value_explicit_cast",
            "Traverse NTH_VALUE inputs but retain the user-written numeric cast.",
            "SELECT CAST(i AS Int64) > 1::Int64 AS n, nth_value(CAST(i AS Int64) > 1::Int64, 2) OVER (ORDER BY big) FROM cast_pipeline",
        ),
        (
            "nondeterministic_input_not_reused",
            "Optimize deterministic inputs while keeping separate RAND evaluations.",
            "SELECT i + 1 AS n, rand() AS r, row_number() OVER (PARTITION BY i + 1 ORDER BY rand()) FROM cast_pipeline",
        ),
    ] {
        let case = SqlTestCase {
            name,
            description,
            setup_sqls: &["CREATE TABLE cast_pipeline(i Int32 NULL, big Int64 NULL)"],
            sql,
        };
        let ctx = setup_context(&case).await?;
        let raw = ctx.bind_sql(case.sql).await?;
        write_case_header(&mut file, &case)?;
        writeln!(
            file,
            "raw_plan:\n{}",
            raw.format_indent(options.clone(), &ctx.stat_context()?)?
        )?;
        let optimized = ctx.optimize_plan(raw).await?;
        writeln!(
            file,
            "optimized_plan:\n{}",
            optimized.format_indent(options.clone(), &ctx.stat_context()?)?
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_numeric_auto_cast_rules_preserve_evaluation() -> Result<()> {
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::Expr;
    use databend_common_expression::types::NumberScalar;

    init_testing_globals();
    for sql in [
        "delta > 1::Int64",
        "1::Int64 < delta",
        "delta > 1::Int64 AND delta < 100::Int64",
        "if(delta > 1::Int64, number, 0)",
        "if(delta > 1::Int64, 1 / 0, 7)",
        "delta > 9223372036854775807::Int64",
        "delta > 1.5",
        "CAST(delta AS Int64) > 1::Int64",
        "number = 9007199254740992::Float64",
        "wide = 1::Int64",
        "delta <= 1::Int64",
        "delta >= 1::Int64",
        "delta <> 1::Int64",
        "assume_not_null(delta) > 1::Int64",
        "delta > delta",
        "delta > number",
        "if(flag, delta, 1::Int64) > 2::Int64",
    ] {
        let settings = Settings::create(Tenant::new_literal("default"));
        let adapter = TestTypeCheckAdapter::new(settings);
        let func_ctx = adapter.func_ctx.clone();
        let names = NameResolutionContext::try_from(adapter.settings.as_ref())?;
        let mut context = test_bind_context(ExprContext::Unknown);
        add_test_column(
            &mut context,
            7,
            "wide",
            DataType::Number(NumberDataType::UInt64),
        );
        let tokens = tokenize_sql(sql)?;
        let ast = parse_expr(&tokens, adapter.sql_dialect()?)?;
        let mut checker = TypeChecker::try_create_with_adapter(
            &mut context,
            adapter,
            &names,
            Arc::new(RwLock::new(Metadata::default())),
            &[],
        )?;
        let (bound, data_type) = *checker.resolve(&ast)?;
        let original = bound
            .as_expr()?
            .project_column_ref(|column| Ok(column.index))?;
        let (optimized, _) = scalar::optimize(bound, data_type, &func_ctx)?;
        let optimized = optimized
            .as_expr()?
            .project_column_ref(|column| Ok(column.index))?;
        for delta in [
            None,
            Some(i32::MIN),
            Some(-1),
            Some(0),
            Some(1),
            Some(2),
            Some(i32::MAX),
        ] {
            let values = HashMap::from([
                (Symbol::new(4), Scalar::Boolean(delta.is_some())),
                (
                    Symbol::new(0),
                    Scalar::Number(NumberScalar::Int64(9007199254740993)),
                ),
                (
                    Symbol::new(7),
                    Scalar::Number(NumberScalar::UInt64(u64::MAX)),
                ),
                (
                    Symbol::new(3),
                    delta.map_or(Scalar::Null, |value| {
                        Scalar::Number(NumberScalar::Int32(value))
                    }),
                ),
            ]);
            let before: Expr = original
                .fill_const_column(&values)
                .project_column_ref(|_| unreachable!("all inputs must have test values"))?;
            let after: Expr = optimized
                .fill_const_column(&values)
                .project_column_ref(|_| unreachable!("all inputs must have test values"))?;
            let block = DataBlock::empty_with_rows(1);
            let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
            match (evaluator.run(&before), evaluator.run(&after)) {
                (Ok(before), Ok(after)) => assert_eq!(before, after, "{sql}, delta={delta:?}"),
                (Err(before), Err(after)) => {
                    assert_eq!(before.code(), after.code(), "{sql}, delta={delta:?}");
                    // Evaluator appends the rendered whole expression, which
                    // is expected to change. Compare the failure itself, not
                    // that diagnostic rendering of the rewritten tree.
                    assert_eq!(
                        before.message().split(", during run expr:").next(),
                        after.message().split(", during run expr:").next(),
                        "{sql}, delta={delta:?}"
                    );
                }
                (before, after) => {
                    panic!("evaluation changed: {sql}, delta={delta:?}: {before:?} -> {after:?}")
                }
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_window_auto_cast_inputs_preserve_evaluation() -> Result<()> {
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::Expr;

    init_testing_globals();
    for sql in [
        "row_number() OVER (PARTITION BY delta + 1 ORDER BY delta > 1::Int64 DESC NULLS FIRST)",
        "sum(delta + 1) OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
        "array_agg(delta + 1) WITHIN GROUP (ORDER BY delta > 1::Int64 DESC NULLS FIRST) OVER ()",
        "lag(delta > 1::Int64, 1, delta > 1::Int64) OVER (ORDER BY number)",
        "lead(delta + 1, 1, delta + 1) OVER (ORDER BY number)",
        "nth_value(CAST(delta AS UInt8), 2) OVER (ORDER BY number)",
        "row_number() OVER (ORDER BY number > 'not a number')",
    ] {
        let settings = Settings::create(Tenant::new_literal("default"));
        let adapter = TestTypeCheckAdapter::new(settings);
        let func_ctx = adapter.func_ctx.clone();
        let names = NameResolutionContext::try_from(adapter.settings.as_ref())?;
        let mut context = test_bind_context(ExprContext::Unknown);
        let tokens = tokenize_sql(sql)?;
        let ast = parse_expr(&tokens, adapter.sql_dialect()?)?;
        let mut checker = TypeChecker::try_create_with_adapter(
            &mut context,
            adapter,
            &names,
            Arc::new(RwLock::new(Metadata::default())),
            &[],
        )?;
        let (bound, data_type) = *checker.resolve(&ast)?;
        let (optimized, optimized_type) =
            scalar::optimize(bound.clone(), data_type.clone(), &func_ctx)?;
        assert_eq!(optimized_type, data_type, "{sql}");
        let (again, again_type) = scalar::optimize(optimized.clone(), optimized_type, &func_ctx)?;
        assert_eq!(again, optimized, "idempotence: {sql}");
        assert_eq!(again_type, data_type, "{sql}");
        let (ScalarExpr::WindowFunction(before), ScalarExpr::WindowFunction(after)) =
            (&bound, &optimized)
        else {
            panic!("expected window function: {sql}");
        };
        assert_eq!(before.frame, after.frame, "{sql}");
        for (before, after) in before.order_by.iter().zip(&after.order_by) {
            assert_eq!(
                (before.asc, before.nulls_first),
                (after.asc, after.nulls_first)
            );
        }
        let before = before
            .partition_by
            .iter()
            .chain(before.order_by.iter().map(|order| &order.expr))
            .chain(before.func.scalar_expr_iter());
        let after = after
            .partition_by
            .iter()
            .chain(after.order_by.iter().map(|order| &order.expr))
            .chain(after.func.scalar_expr_iter());
        for (before, after) in before.zip(after) {
            assert_eq!(before.data_type(), after.data_type(), "{sql}");
            let before = before
                .as_expr()?
                .project_column_ref(|column| Ok(column.index))?;
            let after = after
                .as_expr()?
                .project_column_ref(|column| Ok(column.index))?;
            for delta in [
                None,
                Some(i32::MIN),
                Some(-1),
                Some(0),
                Some(2),
                Some(i32::MAX),
            ] {
                let values = HashMap::from([
                    (Symbol::new(0), Scalar::from(1_i64)),
                    (Symbol::new(3), delta.map_or(Scalar::Null, Scalar::from)),
                ]);
                let before: Expr = before.fill_const_column(&values).project_column_ref(|_| {
                    unreachable!("all window inputs must have test values")
                })?;
                let after: Expr = after.fill_const_column(&values).project_column_ref(|_| {
                    unreachable!("all window inputs must have test values")
                })?;
                let block = DataBlock::empty_with_rows(1);
                let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
                match (evaluator.run(&before), evaluator.run(&after)) {
                    (Ok(before), Ok(after)) => assert_eq!(before, after, "{sql}, {delta:?}"),
                    (Err(before), Err(after)) => {
                        assert_eq!(before.code(), after.code(), "{sql}, {delta:?}");
                        assert_eq!(
                            // The diagnostic renders the input tree, whose
                            // folded constant conversions are allowed to differ.
                            before.message().split(" in expr `").next(),
                            after.message().split(" in expr `").next(),
                            "{sql}, {delta:?}"
                        );
                    }
                    (before, after) => {
                        panic!(
                            "window input evaluation changed: {sql}, {delta:?}: {before:?} -> {after:?}"
                        )
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn test_type_inference_constraints_apply_to_entire_expression() -> Result<()> {
    // Constraints are passed through the whole check, not applied only to the
    // root `and` call over already frozen Boolean placeholders.
    let columns = [
        ("text", DataType::String),
        ("number", DataType::Number(NumberDataType::Int64)),
        ("delta", DataType::Number(NumberDataType::Int32)),
    ];
    let raw = databend_common_expression_test_support::parse_raw_expr(
        "text > 1 AND number > 2",
        &columns,
        &BUILTIN_FUNCTIONS,
    );
    let checked = type_check::check_with_options(&raw, &BUILTIN_FUNCTIONS, CheckOptions {
        forbid_column_casts: true,
        ..Default::default()
    })?;
    assert!(!checked.sql_display().contains("CAST(text"), "{checked:?}");
    let constrained = databend_common_expression_test_support::parse_raw_expr(
        "text > 1 AND delta > 1::Int64",
        &columns,
        &BUILTIN_FUNCTIONS,
    );
    assert!(type_check::check(&constrained, &BUILTIN_FUNCTIONS).is_ok());
    assert!(
        type_check::check_with_options(&constrained, &BUILTIN_FUNCTIONS, CheckOptions {
            forbid_column_casts: true,
            ..Default::default()
        })
        .is_err()
    );
    let explicit = databend_common_expression_test_support::parse_raw_expr(
        "text > 1 AND CAST(delta AS Int64) > 1::Int64",
        &columns,
        &BUILTIN_FUNCTIONS,
    );
    assert!(
        type_check::check_with_options(&explicit, &BUILTIN_FUNCTIONS, CheckOptions {
            forbid_column_casts: true,
            ..Default::default()
        })
        .is_ok()
    );
    Ok(())
}
