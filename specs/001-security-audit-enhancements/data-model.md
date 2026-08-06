# Data Model: Security Audit & Hardening with Value-Add Enhancements

**Feature**: `001-security-audit-enhancements` | **Date**: 2026-08-06

Additive changes only. Existing types keep every current member and signature.

## Changed types (additive)

### NaturalQueryOptions (extended)

| New field | Type | Default | Purpose |
|-----------|------|---------|---------|
| MaxQuestionLength | int | 2000 | FR-009 input cap (chars) |
| MaxContextTurns | int | 20 | FR-009 conversation-history cap |
| MaxResultRows | int | 10000 | FR-016 result truncation cap |
| QueryTimeoutSeconds | int | 30 | FR-017 execution timeout |
| TenantIdPattern | string (regex) | `^[A-Za-z0-9._-]{1,128}$` | FR-006 tenant character policy |
| InjectionScreening | InjectionScreeningMode | Warn | FR-014 (Off / Warn / Block) |
| InjectionPatterns | List\<string\> | built-in set | FR-014 operator-extensible patterns |
| SemanticCacheSimilarityThreshold | double | 0.97 | FR-030 conservative reuse threshold |

Validation rules (startup, FR-028): tenant column ⊕ placeholder both-or-neither; all limits > 0; threshold ∈ (0.5, 1.0]; MaxRetries ∈ [0,3].

### ColumnDef (extended)

| New field | Type | Default | Purpose |
|-----------|------|---------|---------|
| Sensitive | bool | false | FR-019 mask values in every output form |

New constructor overload; existing constructors unchanged (compat).

### QueryResult (extended)

| New field | Type | Default | Purpose |
|-----------|------|---------|---------|
| Truncated | bool | false | FR-016 marker when row cap applied |
| CorrelationId | string? | null | FR-010 links response ↔ server logs ↔ audit record |
| InjectionFlagged | bool | false | FR-014 warn-mode signal |

## New types

### AuditRecord (FR-018)

| Field | Type | Notes |
|-------|------|-------|
| Question | string | as received |
| Sql | string? | generated query (null if generation failed) |
| TenantId | string? | validated tenant ID |
| Outcome | string | `success · validation_rejected · injection_flagged · rate_limited · timeout · execution_error · llm_error` |
| DurationMs | long | end-to-end |
| TokensUsed | int | 0 when no LLM call |
| TimestampUtc | DateTime | record creation |
| CorrelationId | string | matches QueryResult.CorrelationId |
| Truncated | bool | result was capped |

Lifecycle: exactly one per processed question (success or failure); sink failure never fails the request (logged).

### QueryPreview (FR-032)

| Field | Type | Notes |
|-------|------|-------|
| Sql | string | generated, validated query |
| ChartType | string | recommended visualization |
| Title / Description | string | LLM metadata |
| EstimatedCost | QueryCostEstimate? | when estimator registered |
| CorrelationId | string | for approval flow tracing |

State transitions: `previewed → approved-executed` (re-validated at execution; stale approvals rejected) or `previewed → discarded`.

### Result page (FR-031)

Represented via `AskPagedAsync(page, pageSize)` slicing a captured (cached) result: stable ordering, `Truncated` semantics inherited, no AI call per page.

### Safety Verdict (FR-001…FR-007, internal)

Categories: `write_attempt · multi_statement · tenant_policy · tenant_filter_missing · forbidden_keyword · oversize · timeout`. Surface: existing string-error convention of `SqlValidator.Validate` (unchanged signature) + `NaturalQueryError.ErrorType`.

## New interfaces (extension points)

| Interface | Method(s) | Registered via | Failure semantics |
|-----------|-----------|----------------|-------------------|
| IAuditSink | `WriteAsync(AuditRecord, CancellationToken)` | `.UseAuditSink(...)` | swallow + log |
| IEmbeddingProvider | `EmbedAsync(string, CancellationToken) → float[]` | `.UseSemanticCache(...)` | fall through to exact cache/LLM |
| ISemanticQueryCache | `GetSimilarAsync / SetAsync` (tenant-scoped) | `.UseSemanticCache(...)` | miss on error |

Redis package: `RedisQueryCache : IQueryCache` (fail-open → miss), `RedisRateLimiter : IRateLimiter` (fail-closed → deny, FR-025).

## Relationships

```text
NaturalQueryOptions ──configures──▶ NaturalQueryEngine
TableSchema ─── has many ──▶ ColumnDef (Sensitive?) ──drives──▶ SensitiveDataMasker
NaturalQueryEngine ──emits──▶ AuditRecord ──▶ IAuditSink (opt-in)
NaturalQueryEngine ──produces──▶ QueryResult (Truncated, CorrelationId) / QueryPreview
SemanticQueryCache ──scoped by──▶ TenantId; uses IEmbeddingProvider
```
