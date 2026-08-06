# Research: Security Audit & Hardening with Value-Add Enhancements

**Feature**: `001-security-audit-enhancements` | **Date**: 2026-08-06

All decisions below resolve the technical unknowns for the plan. No NEEDS CLARIFICATION remain.

## R1. SQL safety validation approach

- **Decision**: Harden the existing string-based validator with a normalization pipeline: (1) strip `--` line comments and `/* */` block comments (non-nested, loop until stable), (2) remove string literals (single-quote aware, handling `''` escapes), (3) collapse all whitespace (spaces, tabs, newlines) to single spaces, (4) match forbidden operations with word-boundary regex (`\bDELETE\b`, `\bMERGE\b`, …) instead of space-suffixed `Contains`. Expand the forbidden set: `MERGE`, `EXEC`, `EXECUTE`, `CALL`, `ATTACH`, `DETACH`, `PRAGMA`, `COPY`, `VACUUM`, `REINDEX`, `LOAD`, `OUTFILE`, `DUMPFILE`, `INTO <ident>` after `SELECT` (select-into write), `xp_`/`sp_` procedure prefixes, `OPENROWSET`, `OPENQUERY`, `INFORMATION_SCHEMA` stays allowed (read-only).
- **Rationale**: Zero new dependencies, dialect-agnostic (Athena/Trino, Postgres, SQL Server, SQLite, MySQL all covered by one normalizer), fully testable, and preserves the existing public `SqlValidator.Validate` signature (FR-005). Comment stripping + whitespace collapse defeats the known bypasses (`DELETE/**/FROM`, `DELETE\nFROM`).
- **Alternatives considered**: Full SQL AST parser (`Microsoft.SqlServer.TransactSql.ScriptDom` — T-SQL only; `SqlParserCS` — extra dependency, incomplete dialect coverage for Athena/Trino). Rejected: a parser that fails to parse a legitimate dialect-specific query would break existing users (violates FR-020); a denylist-on-normalized-text is strictly additive.

## R2. Tenant isolation without injection

- **Decision**: Two-part fix. (a) Validate tenant IDs against default regex `^[A-Za-z0-9._-]{1,128}$` (configurable pattern + length via options) at engine entry, before cache/rate-limit/LLM. (b) After placeholder substitution, verify structurally-normalized SQL contains a filter of the form `<tenantColumn> = '<tenantId>'` (whitespace/case tolerant, also accepting the column qualified with a table alias) rather than a bare `Contains(tenantId)`.
- **Rationale**: (a) makes the replacement value inert by construction — no quotes, no comment markers, no whitespace can survive the character policy; (b) closes the "tenant value present in a literal/comment but not filtering" gap (FR-007).
- **Alternatives considered**: Parameterized queries for the tenant value — not possible generically because the SQL text is produced by the LLM with an inline placeholder; would require AST rewriting (rejected per R1). Escaping quotes in the tenant ID — weaker than rejecting; rejected.

## R3. Prompt-injection screening

- **Decision**: Regex/pattern list screener over the question and each conversation turn. Default patterns: instruction-override phrases ("ignore (all|your|previous) instructions", "disregard the system prompt", "you are now", "reveal your (system )?prompt", equivalents in Portuguese/Spanish), plus operator-extensible list in options. Default action: log warning + set flag on telemetry/audit record. Strict mode (`InjectionScreeningMode.Block`) refuses. Conversation turns supplied via the endpoint are additionally screened with the R1 SQL validator (turn SQL must itself pass validation before being included in context).
- **Rationale**: Matches clarification decision (warn default). SQL-validating caller-supplied turns closes FR-013 with existing machinery.
- **Alternatives considered**: LLM-based screening (second model call) — doubles cost/latency for marginal gain; rejected for default path, possible future opt-in.

## R4. Endpoint protection & error shaping

- **Decision**: New optional `NaturalQueryEndpointOptions` parameter on `MapNaturalQuery` (additive overload): `RequireAuthorization` (bool) / `AuthorizationPolicy` (string) delegating to ASP.NET's `RequireAuthorization()`; `MaxQuestionLength` (default 2000), `MaxContextTurns` (default 20). Error responses become `{ error, correlationId }` with the generic message; full exception logged server-side with the correlation ID. Correlation ID = `Activity.Current?.TraceId` fallback `Guid`. Playground: `MapNaturalQueryPlayground` checks `IHostEnvironment.IsDevelopment()`; refuses (404 + startup warning log) unless `allowInProduction: true` argument passed.
- **Rationale**: Host-auth delegation per clarification; additive overloads keep FR-020. Trace-ID correlation integrates with the existing OpenTelemetry support.
- **Alternatives considered**: Library-managed API keys — rejected in clarification. Middleware-based enforcement — endpoints extension is simpler and scoped.

## R5. Rate limiter memory bound

- **Decision**: On each `IsAllowedAsync` call, opportunistically evict entries idle beyond 2 windows (2 min) when the dictionary exceeds a soft cap (default 10,000 entries); eviction only removes windows whose newest timestamp is expired, so active tenants are never evicted mid-window. Sweep is amortized (at most once per second).
- **Rationale**: Bounded memory under tenant-ID floods (SC-004) with no timer thread and no behavior change for active tenants.
- **Alternatives considered**: Background timer — lifecycle complexity in a library; LRU cache dependency — unnecessary.

## R6. Result cap & execution timeout

- **Decision**: `MaxResultRows` (default 10,000) enforced in each executor's read loop; result marked `Truncated = true` (new additive property on `QueryResult`). `QueryTimeoutSeconds` (default 30) implemented in the engine as a linked `CancellationTokenSource` around executor calls; timeout surfaces as `error type = "timeout"`.
- **Rationale**: Read-loop capping works uniformly across ADO-based executors and Athena paging; linked CTS avoids touching every executor's signature (FR-020).
- **Alternatives considered**: Injecting `LIMIT` into SQL — dialect-dependent and can change query semantics (e.g., inside CTEs); rejected as primary mechanism (prompt already instructs LLM to use LIMIT).

## R7. Audit trail

- **Decision**: New `IAuditSink` interface (`WriteAsync(AuditRecord, CancellationToken)`), registered via `.UseAuditSink(...)` (instance, factory, or delegate). Engine emits exactly one record per `AskAsync` in a `finally`-style path (success and failure), wrapped in try/catch + log-on-failure. Record: question, generated SQL, tenant ID, outcome (`success|validation_rejected|injection_flagged|rate_limited|timeout|execution_error|llm_error`), duration ms, token usage, timestamp UTC, correlation ID, truncated flag, injection-flag.
- **Rationale**: Mirrors the existing `IErrorHandler` opt-in pattern (familiar to current users), zero overhead when unregistered (null check).
- **Alternatives considered**: Reusing `IErrorHandler` — it is failure-only; audit needs success records too.

## R8. Sensitive column masking

- **Decision**: Add `Sensitive` flag to `ColumnDef` via a new constructor overload + init-able property (keep existing constructors intact for binary/source compatibility). Masking applied post-execution in the engine over `TableData`/`ChartData` (and therefore inherited by CSV/JSON/Excel exports): value replaced with `"***"`. Column matching by result-column name equal (case-insensitive) to a sensitive column name in any configured table.
- **Rationale**: Engine-level masking covers every output path once (FR-019); flag on `ColumnDef` keeps schema config in one place. Also lets the system prompt annotate the column as "sensitive — do not select unless required".
- **Alternatives considered**: Executor-level masking — would need N implementations; rejected.

## R9. MySQL/MariaDB support

- **Decision**: `MySqlConnector` package (MIT, fully async, actively maintained) for `MySqlQueryExecutor` + `MySqlSchemaDiscovery` (via `information_schema`). Same shape as existing Postgres implementations, `wrapInTransaction` supported, read-only advisory checks connection string for a read-only intent hint.
- **Rationale**: MySqlConnector is the community standard over Oracle's `MySql.Data` (sync-over-async issues, licensing).
- **Alternatives considered**: `MySql.Data` — rejected (GPL/commercial dual license, poor async).

## R10. Direct Anthropic provider & OpenRouter

- **Decision**: `AnthropicProvider` via raw `HttpClient` against `https://api.anthropic.com/v1/messages` (`x-api-key` + `anthropic-version` headers), mapping `input_tokens + output_tokens` to the existing `LlmResponse.TokensUsed`, error classification consistent with `OpenAiProvider` (429 → rate-limit message). No SDK dependency. OpenRouter: `UseOpenRouterProvider(apiKey, model)` convenience that configures the existing `OpenAiProvider` with `BaseAddress = https://openrouter.ai/api/`, path `v1/chat/completions` unchanged, optional `HTTP-Referer`/`X-Title` headers.
- **Rationale**: Mirrors the proven no-SDK `OpenAiProvider` pattern; OpenRouter is OpenAI-compatible so a wrapper suffices (clarified with user).
- **Alternatives considered**: Official `Anthropic.SDK` — new dependency for one call; rejected.

## R11. Redis cache & rate limiter packaging

- **Decision**: New companion project/package `NaturalQuery.Redis` (same repo, same version) referencing `StackExchange.Redis`, providing `RedisQueryCache : IQueryCache` (key = existing SHA256 scheme, TTL from options) and `RedisRateLimiter : IRateLimiter` (fixed-window counter via `INCR` + `EXPIRE`, atomic via Lua or `ScriptEvaluate`; **fail closed** on store unavailability with error log, matching FR-025; cache fails open to miss).
- **Rationale**: Keeps `StackExchange.Redis` out of the core package (dependency-footprint promise in spec Assumptions). Fixed-window via INCR is the simplest cross-instance-correct algorithm.
- **Alternatives considered**: Sliding window via sorted sets — more precise but heavier; acceptable future refinement. Putting Redis in core — rejected (bloat).

## R12. Health checks, metrics, startup validation

- **Decision**:
  - Health: `NaturalQueryHealthCheck` implementing `IHealthCheck` (available via the existing `Microsoft.AspNetCore.App` framework reference — no new dependency): pings executor with `SELECT 1`-equivalent (`ValidateAsync` on `ConnectionValidator` where applicable) and reports LLM provider as configured/reachable (lightweight — no billable LLM call; reachability = configuration presence + optional TCP/HTTP HEAD opt-in). Registered via `.AddNaturalQueryHealthChecks()`.
  - Metrics: `System.Diagnostics.Metrics` `Meter` ("NaturalQuery") with counters `naturalquery.queries` (tags: outcome, tenant), `naturalquery.tokens`, `naturalquery.cache` (tag: hit/miss), histogram `naturalquery.duration`. Built-in, OTel-exportable.
  - Startup validation: `IValidateOptions<NaturalQueryOptions>` — errors on impossible configs only (tenant column set XOR placeholder set; negative limits; `MaxRetries` outside 0–3 clamps with warning not error). Registered automatically in `AddNaturalQuery` with `ValidateOnStart()`.
- **Rationale**: All built-in platform primitives, zero new packages, additive registration.
- **Alternatives considered**: Prometheus-specific metrics — OTel/`Meter` is vendor-neutral superset.

## R13. Excel export

- **Decision**: Hand-rolled minimal XLSX writer using `System.IO.Compression.ZipArchive` (built-in): one worksheet, inline strings, number detection. Exposed as `QueryResultExtensions.ToExcel()` returning `byte[]` (+ `ToExcelStream()`).
- **Rationale**: XLSX = zip of 4 small XML files; avoids ClosedXML/EPPlus (heavy, licensing). Scope is a flat result table — trivial subset of SpreadsheetML.
- **Alternatives considered**: ClosedXML — ~10 MB dependency chain for a flat table; rejected.

## R14. Semantic cache

- **Decision**: New opt-in `ISemanticQueryCache` + `.UseSemanticCache(...)` requiring an `IEmbeddingProvider` (new small interface; implementations: `OpenAiEmbeddingProvider` via existing HttpClient pattern, `BedrockEmbeddingProvider` via existing AWSSDK dependency — both no new packages). In-memory store of (embedding, tenantId, result, expiry); lookup = cosine similarity ≥ threshold (default 0.97, configurable) within same tenant only. Falls through to exact cache/LLM below threshold. Wrong-reuse treated as defect → conservative default threshold + tenant scoping tested explicitly.
- **Rationale**: Embedding via providers already in the dependency set keeps the no-new-core-deps rule; conservative threshold honors the spec edge case ("top products" vs "worst products" must not collide — verified in benchmark tests with antonym pairs).
- **Alternatives considered**: Vector DB integration — out of scope for v2.x; interface allows external implementations.

## R15. Pagination & preview

- **Decision**:
  - Pagination: `AskPagedAsync(question, page, pageSize, ...)` slices the (row-capped) executed result; result set retained via the existing `IQueryCache` entry (pages = slices of cached `QueryResult`), so paging never re-invokes the LLM and ordering is stable (captured-result semantics per spec edge case). Endpoint: `?page=&pageSize=` on GET / fields on POST.
  - Preview: `PreviewAsync(question, tenantId, context)` → `QueryPreview { Sql, ChartType, Title, EstimatedCost? , CorrelationId }` (no execution; cost via existing `IQueryCostEstimator` when registered). `ExecuteApprovedAsync(sql, tenantId)` re-runs full `SqlValidator` + tenant checks + masking + caps at execution time (stale-approval safety per spec edge case). Endpoints: `POST {prefix}/preview`, `POST {prefix}/execute`.
- **Rationale**: Reuses existing cache and cost-estimator machinery; captured-result pagination matches the spec's "never a silent mix" edge case.
- **Alternatives considered**: OFFSET/LIMIT re-execution per page — re-runs cost + unstable under writes; rejected as default.

## R16. Versioning & compatibility strategy

- **Decision**: Version 2.1.0 (minor, additive). All new options have defaults preserving current behavior; new APIs are new members/overloads only; `ColumnDef` gains overload not signature change; behavior deltas limited to rejecting dangerous inputs (documented in CHANGELOG under "Security fixes"). CI gate: existing test suite must pass unmodified (SC-003).
- **Rationale**: Direct mapping of FR-020/FR-021.
