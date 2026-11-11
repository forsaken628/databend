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

use std::io::Write;

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::FormatOptions;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auto_cast_rule_pipeline() -> Result<()> {
    let setup_sqls: &[&str] = &[
        "CREATE TABLE ac_left(x BIGINT NULL, y INT NULL, nn INT NOT NULL, s STRING NULL, d DECIMAL(18,2), e DECIMAL(20,3))",
        "CREATE TABLE ac_right(x BIGINT NULL, y INT NULL, s STRING NULL)",
    ];
    let cases = [
        (
            "widened_equality_combines_bounds",
            "Inferred integer widening must not hide contradictory constraints on the source column.",
            "SELECT x FROM ac_left WHERE x = y AND x < 5 AND y >= 5",
        ),
        (
            "widened_equality_keeps_null_rejection",
            "Safe equality inference must still reject NULL even when a reflexive nullable equality is present.",
            "SELECT x FROM ac_left WHERE x = y AND y = y AND y = 5",
        ),
        (
            "decimal_equality_combines_bounds",
            "Lossless Decimal widening shares exact bounds without changing scale semantics.",
            "SELECT d FROM ac_left WHERE d = e AND d < 5.00::DECIMAL(18,2) AND e >= 5.000::DECIMAL(20,3)",
        ),
        (
            "contradiction_preserves_empty_count",
            "Contradictory inferred join bounds eliminate the inputs, not the aggregate's empty-input row.",
            "SELECT count(*) FROM ac_left l JOIN ac_right r ON l.y = r.x WHERE l.y < 5 AND r.x >= 5",
        ),
        (
            "lossy_integer_float_stays_a_boundary",
            "A lossy integer/float equality cannot transfer an exact integer constraint to the other column.",
            "SELECT l.x FROM ac_left l CROSS JOIN ac_right r WHERE l.x = CAST(r.x AS FLOAT64) AND l.x = 9007199254740993 AND r.x = 9007199254740992",
        ),
        (
            "try_cast_stays_a_boundary",
            "A TRY_CAST remains fallible and nullable rather than aliasing the underlying string column.",
            "SELECT l.x FROM ac_left l JOIN ac_right r ON l.x = TRY_CAST(r.s AS BIGINT) WHERE l.x < 5 AND r.s >= '5'",
        ),
        (
            "nullable_boolean_terms_allow_anti_join",
            "A nullable lift around IS NULL must not block anti-join recognition; projected right values remain typed NULL.",
            "SELECT l.x, r.s FROM ac_left l LEFT JOIN ac_right r ON l.x = r.x WHERE r.x IS NULL AND l.y > 0",
        ),
        (
            "non_key_null_test_keeps_outer_join",
            "Testing a nullable non-key value does not prove an unmatched row and must not become an anti join.",
            "SELECT l.x, r.s FROM ac_left l LEFT JOIN ac_right r ON l.x = r.x WHERE r.s IS NULL AND l.y > 0",
        ),
        (
            "nullable_boolean_or_keeps_null_semantics",
            "A predicate under IS NULL observes FALSE versus NULL and must not inherit asserted conjunction constraints.",
            "SELECT x FROM ac_left WHERE is_null(y = 1 AND y = 2) OR nn = 3",
        ),
        (
            "nullable_union_keeps_scan_order",
            "A nullable-only implicit cast keeps scan order on both UNION branches.",
            "SELECT y FROM (SELECT y FROM ac_left UNION ALL SELECT nn FROM ac_left) u ORDER BY y LIMIT 3",
        ),
        (
            "numeric_union_keeps_boundary_types",
            "Even lossless numeric widening cannot push a scan boundary of the union output type into the source type.",
            "SELECT x FROM (SELECT x FROM ac_left UNION ALL SELECT y FROM ac_right) u ORDER BY x LIMIT 3",
        ),
        (
            "try_union_does_not_push_scan_order",
            "TRY_CAST in a UNION branch must not masquerade as nullable-only order preservation.",
            "SELECT y FROM (SELECT y FROM ac_left UNION ALL SELECT TRY_CAST(s AS INT) FROM ac_right) u ORDER BY y LIMIT 3",
        ),
        (
            "ranking_filter_accepts_typed_constant",
            "A typed nullable integer limit still reaches the ranking filter rule.",
            "SELECT x, row_number() OVER (PARTITION BY y ORDER BY x) AS rn FROM ac_left QUALIFY rn <= CAST(3 AS UInt64 NULL)",
        ),
        (
            "grouping_casts_and_typed_nulls",
            "Grouping rewrites retain nullable CAST and TRY_CAST outputs when absent grouping keys become typed NULL.",
            "SELECT CAST(y AS STRING), TRY_CAST(s AS BIGINT), sum(x) FROM ac_left GROUP BY GROUPING SETS ((y, s), (y), ())",
        ),
        (
            "union_filter_refreshes_scalar_inputs",
            "Filter substitution through UNION keeps implicit nullable coercions and their checked function signatures.",
            "SELECT y FROM (SELECT nn AS y FROM ac_left UNION ALL SELECT y FROM ac_right) u WHERE y + 1 = 5",
        ),
    ];
    let mut file = open_golden_file("optimizer", "auto_cast_rules.txt")?;
    for (name, description, sql) in cases {
        let case = SqlTestCase {
            name,
            description,
            setup_sqls,
            sql,
        };
        let ctx = setup_context(&case).await?;
        // Exercise grouping rewrites that replace absent keys with typed NULL.
        ctx.get_settings()
            .set_setting("grouping_sets_to_union".to_string(), "1".to_string())?;
        let raw = ctx.bind_sql(sql).await?;
        let optimized = ctx.optimize_plan(raw.clone()).await?;
        write_case_header(&mut file, &case)?;
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
    }
    Ok(())
}
