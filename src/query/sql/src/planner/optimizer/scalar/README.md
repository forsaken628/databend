# Bound scalar optimization

`ScalarBinder::bind` finishes semantic resolution before calling `optimize`.
Lambda bodies use the same boundary before compilation into `RemoteExpr`.
Binding still owns name/scope resolution, special-function semantics,
bind-required constants and existing bind-time folding. The expression checker's
pre-existing integer-equality literal normalization is retained.

The pass makes one postorder traversal of the bound scalar. It has two concrete
rewrites; there is no candidate iterator, whole-tree reconstruction, cost model,
fixed-point search or backtracking machinery.

## Fold inferred literal conversions

Binding materializes implicit function-argument conversions as `AutoCast`,
retaining source types until the complete expression has been checked. After
visiting a node's children, fold an `AutoCast` if its input is deterministic,
requires no runtime input and evaluates successfully. This includes nullable or
TRY wrappers around literals left by binding: evaluate them as written, rather
than stripping them first. Preserve the exact checked type, including
nullability and decimal precision. Failed conversions and standalone explicit
casts remain unchanged.

For example, binding `ts > '2024-01-01'` produces a comparison with an implicit
String-to-Timestamp conversion of the literal. This pass evaluates that
conversion, not a new interpretation of the comparison. Successful inner
literal conversions fold before their parents, so no cast-chain rule is needed.

## Move a numeric comparison conversion to its literal

Match `cmp(AutoCast<U>(e:T), k:U)`, or the reversed argument order, where `cmp`
is `eq`, `noteq`, `lt`, `lte`, `gt` or `gte`. The constant-side inferred
conversions have already folded. Require:

- T and U are numeric types, and T -> U is lossless and injective;
- converting the literal from U to T and back succeeds with the same value;
- the comparison accepts the proposed argument types without new runtime
  conversions and returns exactly the original result type.

Then replace it with `cmp(e:T, k':T)`. For example, with nullable Int32 `delta`,
`delta > 1::Int64` binds with a widening runtime conversion and becomes an
Int32 comparison. An out-of-range literal or an Int64-to-Float64 runtime
conversion does not qualify. Explicit casts are not matched or stripped.

Only the comparison's new argument types are checked. Both rewrites preserve
their own result type, so enclosing functions and the externally reported bind
type do not need re-inference. Default overload resolution remains registration
order; typed lowering prefers exact arguments, without candidate-cost sorting.

## Deliberate limits

Do not add identity-cast removal or common-widening reductions based on arbitrary
hand-built trees. Ordinary function binding does not create identity casts, and
numeric comparison registration already selects the earlier compatible integer
type. New rules need an actual SQL-to-bound-expression example first.

Aggregate, window, subquery and runtime UDF internals remain opaque inputs.
This module does not change relational optimizer rules. Regression coverage in
`tests/it/semantic/type_check/auto_cast.rs` starts from SQL and includes bound,
optimized and lowered expressions, evaluation equivalence, idempotence and
production raw/optimized plan goldens.
