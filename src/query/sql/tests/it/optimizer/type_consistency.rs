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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::optimizer::ir::QueryPlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::setup_context;

async fn optimize(case: &SqlTestCase, settings: &[(&str, &str)]) -> Result<()> {
    let ctx = setup_context(case).await?;
    for (name, value) in settings {
        ctx.get_settings()
            .set_setting((*name).to_string(), (*value).to_string())?;
    }
    let plan = ctx.bind_sql(case.sql).await?;
    ctx.optimize_plan(plan).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_optimizer_rewrites_keep_function_return_types_consistent() -> Result<()> {
    let grouping_sets = SqlTestCase {
        name: "grouping_sets_rewrites_refresh_function_return_types",
        description: "",
        setup_sqls: &["CREATE TABLE grouping_type_t(a Int64, b Int64, c Int64)"],
        sql: "SELECT a, b, c, a + 8, b + c \
              FROM grouping_type_t GROUP BY ROLLUP(a, b, c)",
    };
    for grouping_sets_to_union in ["0", "1"] {
        optimize(&grouping_sets, &[(
            "grouping_sets_to_union",
            grouping_sets_to_union,
        )])
        .await?;
    }

    let grouping_array_lambda = SqlTestCase {
        name: "grouping_sets_refresh_lambda_return_type",
        description: "",
        setup_sqls: &["CREATE TABLE grouping_array_t(a Array(Int64) NOT NULL)"],
        sql: "SELECT grouping(a) FROM grouping_array_t GROUP BY CUBE(a) \
              HAVING array_length(array_filter(a, x -> x <> -42)) >= 0",
    };
    for grouping_sets_to_union in ["0", "1"] {
        optimize(&grouping_array_lambda, &[(
            "grouping_sets_to_union",
            grouping_sets_to_union,
        )])
        .await?;
    }

    let cases = [
        SqlTestCase {
            name: "filter_join_rewrite_refreshes_function_return_types",
            description: "",
            setup_sqls: &[
                "CREATE TABLE rewrite_type_t1(a Int64, b Int64)",
                "CREATE TABLE rewrite_type_t2(a Int64, b Int64)",
            ],
            sql: "SELECT * FROM rewrite_type_t1 t1 JOIN rewrite_type_t2 t2 \
                  ON t1.a = t2.a AND t1.b BETWEEN t2.b AND t2.b + 2 WHERE t2.b = 3",
        },
        SqlTestCase {
            name: "in_subquery_rewrite_refreshes_function_return_types",
            description: "",
            setup_sqls: &["CREATE TABLE rewrite_type_t3(a Int64, b Int64)"],
            sql: "SELECT * FROM rewrite_type_t3 \
                  WHERE a IN (SELECT x FROM (VALUES (1), (2), (3), (4), (5), (6)) t(x))",
        },
        SqlTestCase {
            name: "self_join_refreshes_function_return_types",
            description: "",
            setup_sqls: &["CREATE TABLE rewrite_settings(name String, value String)"],
            sql: "SELECT e1.name, e2.name, e1.value, e2.value FROM rewrite_settings e1 \
                  LEFT JOIN rewrite_settings e2 ON e1.name = e2.name \
                  WHERE e1.name = 'max_threads'",
        },
        SqlTestCase {
            name: "nullable_tuple_cast_to_variant_keeps_function_signature",
            description: "",
            setup_sqls: &["CREATE TABLE tuple_variant_t(a Nullable(Tuple(x Int64, y String)))"],
            sql: "SELECT CAST(a AS VARIANT) FROM tuple_variant_t",
        },
        SqlTestCase {
            name: "equivalent_constant_rewrite_refreshes_lambda_return_type",
            description: "",
            setup_sqls: &["CREATE TABLE equivalent_lambda_t(a Nullable(Array(Int64)))"],
            sql: "SELECT * FROM equivalent_lambda_t \
                  WHERE a = CAST([1] AS ARRAY(INT64)) \
                  AND array_length(array_filter(a, x -> x > 0)) > 0",
        },
        SqlTestCase {
            name: "equivalent_empty_array_rewrite_keeps_lambda_type_valid",
            description: "",
            setup_sqls: &["CREATE TABLE equivalent_reduce_t(a Nullable(Array(Int64)))"],
            sql: "SELECT * FROM equivalent_reduce_t \
                  WHERE a = [] AND is_null(array_reduce(a, (x, y) -> x + y))",
        },
        SqlTestCase {
            name: "decorrelated_reused_key_refreshes_function_return_types",
            description: "The nullable outer key is replaced by the non-nullable inner key it equals.",
            setup_sqls: &[
                "CREATE TABLE reuse_a(k BIGINT NOT NULL, v BIGINT NOT NULL)",
                "CREATE TABLE reuse_b(k BIGINT NOT NULL, v BIGINT NOT NULL)",
                "CREATE TABLE reuse_c(k BIGINT NOT NULL, v BIGINT NOT NULL)",
            ],
            sql: "SELECT reuse_a.k FROM reuse_a LEFT JOIN reuse_b ON reuse_a.k = reuse_b.k \
                  JOIN LATERAL (SELECT reuse_c.k FROM reuse_c WHERE reuse_c.v = reuse_b.v \
                  GROUP BY reuse_c.k) t ON reuse_a.k = t.k",
        },
        SqlTestCase {
            name: "binder_folded_argument_refreshes_function_return_types",
            description: "An explicitly nullable constant and the equality signature must agree after binding.",
            setup_sqls: &["CREATE TABLE folded_arg_t(a INT NOT NULL)"],
            sql: "SELECT a + CAST(1 AS INT NULL) FROM folded_arg_t \
                  WHERE a = CAST(1 AS INT NULL) AND a + 1 > 0",
        },
    ];
    for case in &cases {
        optimize(case, &[]).await?;
    }

    let folded_in = SqlTestCase {
        name: "folded_in_predicate_preserves_nullability",
        description: "",
        setup_sqls: &["CREATE TABLE folded_in_type_t(a Nullable(Int64))"],
        sql: "SELECT * FROM folded_in_type_t WHERE a IN (1, 2, 3, 1, 2, 3)",
    };
    optimize(&folded_in, &[
        ("inlist_to_join_threshold", "6"),
        ("max_inlist_to_or", "2"),
    ])
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_inferred_filters_keep_cast_targets_nullable() -> Result<()> {
    // Filter inference moves `CAST(cast_outer.a AS STRING)` through the join key
    // onto the nullable `cast_inner.n`. The type validator does not check casts,
    // so inspect the plan: a non-nullable target would fail on NULL keys.
    let setup_sqls: &[&str] = &[
        "CREATE TABLE cast_outer(a INT NOT NULL)",
        "CREATE TABLE cast_inner(n INT NULL)",
    ];
    let cases = [
        SqlTestCase {
            name: "join_key_filter",
            description: "",
            setup_sqls,
            sql: "SELECT cast_outer.a FROM cast_outer \
                  JOIN (SELECT n FROM cast_inner) q ON q.n = cast_outer.a \
                  WHERE CAST(cast_outer.a AS STRING) <> ''",
        },
        SqlTestCase {
            name: "decorrelated_key_filter",
            description: "",
            setup_sqls,
            sql: "SELECT cast_outer.a FROM cast_outer WHERE EXISTS (SELECT 1 FROM \
                  (SELECT CAST(cast_outer.a AS STRING) AS d, cast_inner.n AS n FROM cast_inner) q \
                  WHERE q.n = cast_outer.a AND q.d <> '')",
        },
    ];
    for case in &cases {
        let ctx = setup_context(case).await?;
        let plan = ctx.bind_sql(case.sql).await?;
        let plan = ctx.optimize_plan(plan).await?.format_indent(
            databend_common_sql::FormatOptions {
                show_auto_cast: true,
                ..Default::default()
            },
            &ctx.stat_context()?,
        )?;
        let inner_casts = plan
            .match_indices("CAST(cast_inner.n (#")
            .map(|(start, _)| &plan[start..])
            .collect::<Vec<_>>();
        assert!(
            !inner_casts.is_empty(),
            "{}: no inferred cast\n{plan}",
            case.name
        );
        for cast in inner_casts {
            let target = &cast[cast.find(" AS ").unwrap() + 4..];
            assert!(
                target.starts_with("String NULL)"),
                "{}: inferred cast must be nullable\n{plan}",
                case.name
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_validator_rejects_non_nullable_try_cast_target() -> Result<()> {
    let case = SqlTestCase {
        name: "try_cast_target",
        description: "",
        setup_sqls: &["CREATE TABLE try_cast_t(s STRING NOT NULL)"],
        sql: "SELECT TRY_CAST(s AS INT) FROM try_cast_t",
    };
    let ctx = setup_context(&case).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = ctx.bind_sql(case.sql).await?
    else {
        unreachable!()
    };
    let QueryPlan::Logical(expr) = *s_expr else {
        unreachable!()
    };
    expr.validate_types(&metadata)?;

    let mut node = &expr;
    let mut eval = loop {
        if let RelOperator::EvalScalar(eval) = node.plan()
            && eval
                .items
                .iter()
                .any(|item| matches!(item.scalar, ScalarExpr::CastExpr(_)))
        {
            break eval.clone();
        }
        node = node.child(0)?;
    };
    let item = eval
        .items
        .iter_mut()
        .find(|item| matches!(item.scalar, ScalarExpr::CastExpr(_)))
        .unwrap();
    let ScalarExpr::CastExpr(cast) = &mut item.scalar else {
        unreachable!()
    };
    assert!(cast.is_try && cast.target_type.is_nullable());
    // Lowering always produces the nullable form, so a non-nullable declared
    // target misstates the type that execution produces.
    *cast.target_type = cast.target_type.remove_nullable();

    let error = node
        .replace_plan(eval)
        .validate_types(&metadata)
        .unwrap_err();
    assert!(error.message().contains("try_cast target type mismatch"));
    Ok(())
}
