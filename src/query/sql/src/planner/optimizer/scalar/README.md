# Bound scalar optimization

`ScalarBinder::bind` finishes semantic resolution before calling `optimize`.
Lambda bodies use the same boundary before compilation into `RemoteExpr`.
Window arguments (including aggregate arguments and their ordering), partition
keys and order keys are traversed before `WindowRewriter` extracts input columns.
This keeps their optimized representation consistent with SELECT expressions
for deterministic window-input reuse. Window return types, frames, ordering
options and the restriction against reusing nondeterministic inputs are unchanged.
Binding still owns name/scope resolution, special-function semantics,
bind-required constants and existing bind-time folding. Integer-equality literal
narrowing runs here, not in the expression checker: checking retains literal
types and only selects signatures and required coercions.

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

- T and U are numeric types, and T -> U is lossless and injective, except that
  integer `eq` also permits checked integer conversions that may overflow;
- converting the literal from U to T and back succeeds with the same value;
- the comparison accepts the proposed argument types without new runtime
  conversions and returns exactly the original result type.

Then replace it with `cmp(e:T, k':T)`, retaining a nullable-only lift when the
checked result requires it. For example, with nullable Int32 `delta`,
`delta > 1::Int64` binds with a widening runtime conversion and becomes an
Int32 comparison. Integer `eq` also handles `wide:UInt64 = 1::Int64`: move the
inferred UInt64-to-Int64 conversion onto the literal so that `UInt64::MAX`
compares false instead of overflowing. This preserves the end-to-end behavior
of the former pre-check integer-literal rule, not the transient overflow of the
newly bound expression. A negative literal that cannot convert to UInt64, an
out-of-range literal or an Int64-to-Float64 runtime conversion does not qualify.
Explicit casts are not matched or stripped. An explicitly nullable constant
retains the comparison's checked nullable result type.

Only the comparison's new argument types are checked. Both rewrites preserve
their own result type, so enclosing functions and the externally reported bind
type do not need re-inference. Default overload resolution remains registration
order; typed lowering prefers exact arguments, without candidate-cost sorting.

## Deliberate limits

Do not add identity-cast removal or common-widening reductions based on arbitrary
hand-built trees. Ordinary function binding does not create identity casts, and
numeric comparison registration already selects the earlier compatible integer
type. New rules need an actual SQL-to-bound-expression example first.

Aggregate arguments and ordering, UDF/UDAF/async-call arguments and subquery
comparison inputs use the same traversal; otherwise moving a checker rewrite
would miss the same SQL expression inside those inputs. Subquery relational
plans and runtime UDF bodies remain opaque. This module does not change
relational optimizer rules. Regression coverage in
`tests/it/semantic/type_check/auto_cast.rs` starts from SQL and includes bound,
optimized and lowered expressions, evaluation equivalence, idempotence and
production raw/optimized plan goldens.
