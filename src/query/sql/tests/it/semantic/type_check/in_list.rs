use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_in_list_coercions_are_independent_of_contains_threshold() -> Result<()> {
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_functions::BUILTIN_FUNCTIONS;

    init_testing_globals();
    for (data_type, value, item, expected) in [
        (
            DataType::Variant,
            Scalar::Variant(jsonb::parse_value(b"1").unwrap().to_vec()),
            "'1'",
            Scalar::Boolean(false),
        ),
        (
            DataType::Variant,
            Scalar::Variant(jsonb::parse_value(br#""1""#).unwrap().to_vec()),
            "'1'",
            Scalar::Boolean(true),
        ),
        (
            DataType::Variant.wrap_nullable(),
            Scalar::Null,
            "'1'",
            Scalar::Null,
        ),
        (
            DataType::Number(NumberDataType::UInt64),
            Scalar::from(1_u64),
            "NULL",
            Scalar::Null,
        ),
        (
            DataType::Number(NumberDataType::UInt64),
            Scalar::from(1_u64),
            "1",
            Scalar::Boolean(true),
        ),
    ] {
        for not in [false, true] {
            for count in [1, 4, 16] {
                let settings = Settings::create(Tenant::new_literal("default"));
                let mut context = test_bind_context(ExprContext::Unknown);
                context.add_column_binding(
                    ColumnBindingBuilder::new(
                        "x".into(),
                        Symbol::new(7),
                        Box::new(data_type.clone()),
                        Visibility::Visible,
                    )
                    .build(),
                );
                let items = vec![item; count].join(",");
                let sql = format!("x {}IN ({items})", if not { "NOT " } else { "" });
                let (scalar, declared) = resolve_type_check_sql(
                    &sql,
                    TestTypeCheckAdapter::new(settings),
                    &mut context,
                )?;
                let expr = scalar.as_expr()?.project_column_ref(|_| Ok(0usize))?;
                assert_eq!(expr.data_type(), &declared, "{sql}");
                let expr = expr.fill_const_column(&HashMap::from([(0, value.clone())]));
                let block = DataBlock::empty_with_rows(1);
                let func_ctx = FunctionContext::default();
                let result = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
                    .run(&expr)?
                    .into_scalar()
                    .unwrap();
                let expected = match &expected {
                    Scalar::Boolean(v) if not => Scalar::Boolean(!v),
                    value => value.clone(),
                };
                assert_eq!(result, expected, "{sql}, {data_type}");
            }
        }
    }
    // Invalid strict conversions must remain errors with either rewrite
    // threshold. Do not silently replace them with TRY or fold them away.
    for threshold in ["0", "64"] {
        let settings = Settings::create(Tenant::new_literal("default"));
        settings.set_setting("max_inlist_to_or".into(), threshold.into())?;
        let mut context = test_bind_context(ExprContext::Unknown);
        context.add_column_binding(
            ColumnBindingBuilder::new(
                "d".into(),
                Symbol::new(7),
                Box::new(DataType::Date),
                Visibility::Visible,
            )
            .build(),
        );
        let (scalar, _) = resolve_type_check_sql(
            "d IN ('1970-01-01','bad date','bad date','bad date')",
            TestTypeCheckAdapter::new(settings),
            &mut context,
        )?;
        let expr = scalar
            .as_expr()?
            .project_column_ref(|_| Ok(0usize))?
            .fill_const_column(&HashMap::from([(0, Scalar::Date(0))]));
        let block = DataBlock::empty_with_rows(1);
        let func_ctx = FunctionContext::default();
        assert!(
            Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
                .run(&expr)
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_in_list_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "small_in_list_rewrites_to_balanced_or",
            description: "A small IN list should stay in scalar type checking rather than using the subquery conversion path.",
            setup_sqls: &[],
            sql: "number IN (1, 2, delta)",
        },
        SqlTestCase {
            name: "not_in_list_wraps_rewritten_predicate",
            description: "NOT IN should wrap the scalar IN-list rewrite with a NOT function.",
            setup_sqls: &[],
            sql: "number NOT IN (1, 2, delta)",
        },
        SqlTestCase {
            name: "constant_in_list_uses_contains_after_or_threshold",
            description: "A constant IN list above max_inlist_to_or should preserve the old contains(array_distinct(...)) rewrite.",
            setup_sqls: &[],
            sql: "number IN (1, 2, 3, 4)",
        },
        SqlTestCase {
            name: "constant_not_in_list_wraps_contains_rewrite",
            description: "NOT IN should wrap the contains rewrite when the constant list exceeds max_inlist_to_or.",
            setup_sqls: &[],
            sql: "number NOT IN (1, 2, 3, 4)",
        },
    ];

    run_type_check_cases("in_list.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_in_list_subquery_conversion() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    settings.set_setting("inlist_to_join_threshold".to_string(), "4".to_string())?;
    let cases = [
        SqlTestCase {
            name: "in_list_at_join_threshold_converts_to_subquery",
            description: "An IN list at inlist_to_join_threshold should preserve the old constant-scan subquery conversion.",
            setup_sqls: &[],
            sql: "number IN (1, 2, 3, 4)",
        },
        SqlTestCase {
            name: "not_in_list_at_join_threshold_wraps_subquery_conversion",
            description: "NOT IN at inlist_to_join_threshold should wrap the converted subquery with NOT.",
            setup_sqls: &[],
            sql: "number NOT IN (1, 2, 3, 4)",
        },
        SqlTestCase {
            name: "typed_constants_of_common_type_convert_to_subquery",
            description: "Folded explicit casts that already have the common type are read as values, not rejected.",
            setup_sqls: &[],
            sql: "number IN (try_to_int64('1'), try_to_int64('2'), try_to_int64('3'), try_to_int64('4'))",
        },
    ];

    run_type_check_cases_with_settings(
        "in_list_subquery.txt",
        &cases,
        settings,
        ExprContext::Unknown,
    )
    .await
}
