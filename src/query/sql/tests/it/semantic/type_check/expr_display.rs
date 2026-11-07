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

use databend_common_sql::ExprDisplay;
use databend_common_sql::FormatOptions;

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_expr_display_options() -> Result<()> {
    init_testing_globals();
    let mut file = open_golden_file("semantic/type_check", "expr_display.txt")?;
    let default_options = FormatOptions::default();
    let explicit_options = FormatOptions {
        show_auto_cast: true,
        ..Default::default()
    };
    let default_display = ExprDisplay::new(&default_options);
    let explicit_display = ExprDisplay::new(&explicit_options);
    for (name, sql) in [
        ("implicit_numeric_cast", "delta > 1::Int64"),
        ("nested_function", "if(delta > 1::Int64, number, 0)"),
        ("explicit_cast", "CAST(delta AS Int64) > 1::Int64"),
        ("explicit_over_implicit", "CAST(delta > 1::Int64 AS String)"),
        ("try_cast", "TRY_CAST(text AS Int64)"),
        ("implicit_over_explicit", "CAST(delta AS Int16) > 1::Int64"),
        ("ordinary_function", "concat(text, pattern)"),
    ] {
        let settings = Settings::create(Tenant::new_literal("default"));
        let adapter = TestTypeCheckAdapter::new(settings);
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
        let (scalar, data_type) = *checker.resolve(&ast)?;
        let original = scalar.clone();
        let default = default_display.format_scalar(&scalar);
        let explicit = explicit_display.format_scalar(&scalar);
        assert_eq!(default, format_scalar(&scalar));
        match name {
            "implicit_numeric_cast" => {
                assert_eq!(default, "gt(delta (#3), 1)");
                assert!(explicit.contains("CAST(delta"));
            }
            "try_cast" => assert!(default.starts_with("TRY_CAST(")),
            "explicit_cast" | "explicit_over_implicit" | "implicit_over_explicit" => {
                assert!(default.contains("CAST("));
            }
            _ => {}
        }
        assert_eq!(scalar, original, "display must not modify the expression");
        writeln!(
            file,
            "=== {name} ===\nsql: {sql}\ndefault: {default}\nshow_auto_cast: {explicit}\ntype: {data_type}\n"
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_expr_display_plan_options() -> Result<()> {
    use crate::framework::golden::SqlTestCase;
    use crate::framework::golden::setup_context;
    use crate::framework::golden::write_case_header;

    let mut file = open_golden_file("semantic/type_check", "expr_display_plan.txt")?;
    for (name, sql, has_default_cast) in [
        (
            "filter",
            "SELECT i FROM display_input WHERE i < 9223372036854775807::Int64",
            false,
        ),
        (
            "aggregate",
            "SELECT sum(i) FROM display_input WHERE i < 9223372036854775807::Int64",
            false,
        ),
        (
            "explicit",
            "SELECT i FROM display_input WHERE CAST(i AS Int64) > 1::Int64",
            true,
        ),
        (
            "root_auto_cast",
            "SELECT i FROM display_input UNION ALL SELECT 0::Int64",
            true,
        ),
        (
            "root_auto_cast_with_nested_casts",
            "SELECT if(i > 1.5, i, 0::Int32) AS value FROM display_input UNION ALL SELECT 0::Int64",
            true,
        ),
        (
            "root_auto_cast_over_explicit",
            "SELECT CAST(i AS Int16) AS value FROM display_input UNION ALL SELECT 0::Int64",
            true,
        ),
    ] {
        let case = SqlTestCase {
            name,
            description: "Display policy changes rendering only, not the bound plan.",
            setup_sqls: &["CREATE TABLE display_input(i Int32 NULL)"],
            sql,
        };
        let ctx = setup_context(&case).await?;
        let plan = ctx.bind_sql(sql).await?;
        let default = plan.format_indent(FormatOptions::default(), &ctx.stat_context()?)?;
        let explicit = plan.format_indent(
            FormatOptions {
                show_auto_cast: true,
                ..Default::default()
            },
            &ctx.stat_context()?,
        )?;
        assert_eq!(default.contains("CAST("), has_default_cast);
        assert!(explicit.contains("CAST("));
        if !has_default_cast || name == "root_auto_cast_with_nested_casts" {
            assert_ne!(default, explicit);
        }
        if name.starts_with("root_auto_cast") {
            assert!(default.contains("left: [CAST("));
        }
        if name == "root_auto_cast_with_nested_casts" {
            assert!(default.contains("if(gt(display_input.i (#0), 1.5)"));
            assert!(!default.contains("gt(CAST("));
            assert!(explicit.contains("gt(CAST("));
        }
        write_case_header(&mut file, &case)?;
        writeln!(file, "default:\n{default}\nshow_auto_cast:\n{explicit}")?;
    }
    Ok(())
}
