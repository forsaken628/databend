use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_constant_arrays_use_checked_coercions() -> Result<()> {
    init_testing_globals();
    for arguments in [
        "parse_json('\"bad\"'), CAST(1 AS Int64 NULL)",
        "parse_json('1'), CAST(2 AS Int64 NULL)",
        "parse_json('null'), CAST(1 AS Int64 NULL)",
        "TRY_CAST('bad' AS Int64), CAST(1 AS Int64 NULL)",
        "CAST('1' AS Int64), CAST(2 AS Int64 NULL)",
        "1, 256, -1, NULL",
    ] {
        let mut results = Vec::new();
        for sql in [format!("[{arguments}]"), format!("array({arguments})")] {
            let settings = Settings::create(Tenant::new_literal("default"));
            let mut context = test_bind_context(ExprContext::Unknown);
            let (scalar, declared) =
                resolve_type_check_sql(&sql, TestTypeCheckAdapter::new(settings), &mut context)?;
            assert_eq!(scalar.data_type().as_ref(), &declared, "{sql}");
            let expr = scalar.as_expr()?;
            let value = databend_common_expression::FoldRules::eval_constant(
                &FunctionContext::default(),
                &databend_common_functions::BUILTIN_FUNCTIONS,
                &expr,
            )
            .unwrap();
            results.push((declared, value));
        }
        assert_eq!(results[0], results[1], "{arguments}");
    }
    // Keep the constant builder even above the ordinary function-fold limit.
    let settings = Settings::create(Tenant::new_literal("default"));
    let mut context = test_bind_context(ExprContext::Unknown);
    let sql = format!("[{}]", vec!["1"; 4097].join(","));
    let (scalar, _) =
        resolve_type_check_sql(&sql, TestTypeCheckAdapter::new(settings), &mut context)?;
    let ScalarExpr::ConstantExpr(constant) = scalar else {
        panic!("large literal arrays must remain constants")
    };
    let Scalar::Array(values) = constant.value else {
        panic!("expected an array")
    };
    assert_eq!(values.len(), 4097);
    // The common checker must not turn explicit strict errors into TRY NULL.
    let settings = Settings::create(Tenant::new_literal("default"));
    let mut context = test_bind_context(ExprContext::Unknown);
    let (scalar, _) = resolve_type_check_sql(
        "[CAST('bad' AS Int64), 1]",
        TestTypeCheckAdapter::new(settings),
        &mut context,
    )?;
    let block = databend_common_expression::DataBlock::empty_with_rows(1);
    let func_ctx = FunctionContext::default();
    assert!(
        databend_common_expression::Evaluator::new(
            &block,
            &func_ctx,
            &databend_common_functions::BUILTIN_FUNCTIONS,
        )
        .run(&scalar.as_expr()?.project_column_ref(|_| Ok(0usize))?)
        .is_err()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_literal() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "float_literal_from_parser_tests_binds",
            description: "A float literal shape from parser tests should resolve as a typed constant.",
            setup_sqls: &[],
            sql: "1.925e-3",
        },
        SqlTestCase {
            name: "binary_literal_from_parser_tests_binds",
            description: "A binary literal shape from parser tests should resolve as a typed constant.",
            setup_sqls: &[],
            sql: "x'deedbeef'",
        },
        SqlTestCase {
            name: "null_literal_binds",
            description: "A NULL literal should stay on the literal type-check path.",
            setup_sqls: &[],
            sql: "NULL",
        },
        SqlTestCase {
            name: "constant_array_from_comparison_tests_binds",
            description: "A constant array from scalar comparison tests should use the constant-array fast path.",
            setup_sqls: &[],
            sql: "[1, 2, 3]",
        },
        SqlTestCase {
            name: "mixed_array_with_column_binds",
            description: "An array with a column element should fall back to the scalar array function path.",
            setup_sqls: &[],
            sql: "[1, delta, NULL]",
        },
        SqlTestCase {
            name: "map_literal_from_sqllogictest_binds",
            description: "A map literal shape from map sqllogictests should resolve through map construction.",
            setup_sqls: &[],
            sql: "{'k1': 1, 'k2': delta}",
        },
        SqlTestCase {
            name: "tuple_literal_from_parser_tests_binds",
            description: "A tuple expression from parser tests should resolve through tuple construction.",
            setup_sqls: &[],
            sql: "(number, text, true)",
        },
    ];

    run_type_check_cases("literal.txt", &cases).await
}
