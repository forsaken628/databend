# Bound scalar optimization

This is the first, conservative boundary between semantic binding and scalar
optimization. `ScalarBinder::bind` finishes semantic resolution before calling
`optimize`. Lambda bodies use the same boundary before compilation into
`RemoteExpr`, because the enclosing scalar cannot traverse the compiled body.

Binding still owns name/scope resolution, special-function semantics,
bind-required constants, and existing bind-time folding. The expression checker's
pre-existing integer-equality literal normalization is also retained in this
stage; changing that behavior needs separate signed/unsigned range coverage.
Implicit argument
conversions are materialized as `AutoCast` instead of being folded away with
their source types. This is deliberately not a migration of all folding rules.

## Rules and acceptance

A `ScalarRule` proposes complete candidate expressions and constraints for
checking them. It does not enumerate function overloads, select a signature,
or commit types at the rewrite site. The expression crate's recursive type
checker propagates constraints and returns annotations for rebuilding inferred
conversions and internal return types in one check of the candidate.

The driver owns acceptance: a candidate must type-check, keep the expression's
root type, lower consistently, and strictly reduce runtime implicit conversions
across the expression. Rejected candidates do not mutate the accepted tree.
Internal inferred return types are recomputed, not independently frozen. Explicit
casts remain expression nodes and cannot be stripped by the rule.

The initial driver uses first-improvement, monotone descent. It is **not** a
globally optimal search, and cannot accept temporarily more expensive states or
coordinate rewrites that are only valid together. Separating complete-expression
proposals from acceptance leaves those policies replaceable without moving
optimization back into overload selection.

Lowering and return-type refresh prefer exact argument signatures for an already
typed expression; semantic binding still takes the first legal signature. This
prevents lowering from reintroducing a coercing overload after conversions have
already been materialized.

## First rule and semantic guardrails

`MoveAutoCastToConstants` handles binary numeric comparisons only. It moves an
inferred conversion away from a runtime argument when:

- the runtime conversion is lossless and injective;
- the other argument is a literal (possibly with inferred conversions), with
  the same comparison-domain type;
- converting the literal to the runtime argument's source type and back succeeds
  and preserves its value.

The rule does not remove explicit conversions. Failed, out-of-range, fractional,
and lossy conversions remain unchanged. In particular, moving an `Int64 ->
Float64` conversion would be unsound even for a representable constant: two
integers can compare equal after that conversion.

After rule application, deterministic, column-free inferred conversions are
folded using the bound function context. Typed constants retain their checked
types, including nullability and decimal precision.

## Scope limits

Traversal follows scalar execution/lowering regions. Aggregate, window,
subquery, and runtime-UDF nodes are opaque inputs; their internal semantic
binding remains unchanged in this stage. Lambda inputs belong to the enclosing
region, while the body is optimized independently before compilation. This is
not yet a general expression search framework or a replacement for the existing
relational optimizer's scalar passes.

Regression coverage starts from SQL in `tests/it/semantic/type_check/auto_cast.rs`
(bound/optimized/lowered goldens, evaluation equivalence, and idempotence).
Driver unit coverage checks rejected type-invalid and root-contract-changing
candidates before a valid candidate. SQL logic coverage checks user-visible
results, NULLs, range limits, and non-injective conversions.
