# Tasks: Security Audit & Hardening with Value-Add Enhancements

**Input**: Design documents from `/specs/001-security-audit-enhancements/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/public-api.md, quickstart.md

**Tests**: REQUIRED — FR-021 mandates tests for every new mechanism; plan mandates test-first for security-critical validators. Test tasks precede implementation within each story.

**Organization**: Tasks grouped by user story (US1–US8, priorities P1–P5) so each story is independently implementable and testable. Release target: v2.1.0, strictly additive (FR-020, SC-003).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: US1–US8 mapping to spec.md user stories
- All paths relative to repository root

---

## Phase 1: Setup

**Purpose**: Baseline verification — existing solution/projects already exist; this feature adds onto them.

- [x] T001 Run `dotnet build` and `dotnet test` on tests/NaturalQuery.Tests to record the green baseline that must stay green unmodified throughout (SC-003)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Additive model/options surface used by multiple stories. MUST complete before any user story phase.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [x] T002 [P] Add InjectionScreeningMode enum (Off, Warn, Block) in src/NaturalQuery/Security/InjectionScreeningMode.cs
- [x] T003 Extend NaturalQueryOptions with additive properties MaxQuestionLength (2000), MaxContextTurns (20), MaxResultRows (10000), QueryTimeoutSeconds (30), TenantIdPattern (`^[A-Za-z0-9._-]{1,128}$`), InjectionScreening (Warn), InjectionPatterns (built-in list), SemanticCacheSimilarityThreshold (0.97) in src/NaturalQuery/NaturalQueryOptions.cs (depends on T002)
- [x] T004 [P] Add additive properties Truncated (bool, false), CorrelationId (string?, null), InjectionFlagged (bool, false) to QueryResult in src/NaturalQuery/Models/QueryResult.cs
- [x] T005 [P] Add Sensitive flag to ColumnDef via new constructor overload + init-able property (existing constructors untouched) in src/NaturalQuery/Models/TableSchema.cs
- [x] T006 Add correlation-ID generation in engine pipeline (Activity.Current?.TraceId fallback Guid) and stamp QueryResult.CorrelationId in src/NaturalQuery/NaturalQueryEngine.cs (depends on T004)

**Checkpoint**: Solution builds; existing suite still green; new surface available to all stories.

---

## Phase 3: User Story 1 — Trustworthy Query Safety Validation (Priority: P1) 🎯 MVP

**Goal**: No AI-generated query can modify/delete/exfiltrate data even when obfuscated (comments, newlines, casing, dialect operations). Public `SqlValidator.Validate` signature unchanged (FR-001…FR-005).

**Independent Test**: SC-001 corpus — 100% of dangerous/obfuscated queries rejected, 0% of legitimate read queries rejected; existing suite passes unmodified.

### Tests for User Story 1 (write FIRST, must FAIL before implementation)

- [x] T007 [P] [US1] Write SqlNormalizerTests: `--` line comments, `/* */` block comments stripped loop-until-stable, string literals removed with `''` escape handling, whitespace (spaces/tabs/newlines) collapsed to single spaces, in tests/NaturalQuery.Tests/SqlNormalizerTests.cs
- [x] T008 [P] [US1] Write SqlValidatorHardeningTests with SC-001 corpus: obfuscated bypasses (`DELETE/**/FROM`, newline-split keywords, casing tricks), expanded denylist (MERGE, EXEC, EXECUTE, CALL, ATTACH, DETACH, PRAGMA, COPY, VACUUM, REINDEX, LOAD, OUTFILE, DUMPFILE, SELECT…INTO, xp_/sp_ prefixes, OPENROWSET, OPENQUERY), multi-statement tricks, PLUS legitimate corpus (dangerous words inside quoted literals accepted, INFORMATION_SCHEMA reads accepted), in tests/NaturalQuery.Tests/SqlValidatorHardeningTests.cs

### Implementation for User Story 1

- [x] T009 [US1] Implement SqlNormalizer (comment stripping, single-quote-aware literal removal, whitespace collapse) in src/NaturalQuery/Validation/SqlNormalizer.cs
- [x] T010 [US1] Harden SqlValidator: run SqlNormalizer pipeline then word-boundary regex denylist (`\bDELETE\b` etc.) over normalized text with the expanded forbidden set; keep existing public signature and result conventions, in src/NaturalQuery/Validation/SqlValidator.cs (depends on T009)
- [x] T011 [US1] Verify/enforce identical validation on the retry/repair path in src/NaturalQuery/NaturalQueryEngine.cs — every route to execution passes the hardened validator (FR-004)

**Checkpoint**: SC-001 corpus green; existing suite green unmodified. US1 shippable as MVP.

---

## Phase 4: User Story 2 — Injection-Proof Tenant Isolation (Priority: P1)

**Goal**: Tenant ID treated as untrusted input (character policy) and tenant filtering structurally verified (FR-006…FR-008).

**Independent Test**: SC-002 — 100% of injection-syntax tenant IDs rejected before query construction; 100% of queries lacking a real tenant filter rejected when isolation configured; single-tenant configs unaffected.

### Tests for User Story 2 (write FIRST, must FAIL)

- [x] T012 [P] [US2] Write TenantIdValidatorTests: injection corpus (`abc' OR '1'='1`, comment markers, whitespace, quotes, >128 chars), valid corpus (letters/digits/hyphen/underscore/dot), custom-pattern override, empty string treated as not-provided, in tests/NaturalQuery.Tests/TenantIdValidatorTests.cs
- [x] T013 [P] [US2] Write TenantFilterVerifierTests: tenant value only in a literal/comment → rejected; real `tenant_col = 'id'` filter (whitespace/case tolerant, table-alias-qualified) → accepted, in tests/NaturalQuery.Tests/TenantFilterVerifierTests.cs

### Implementation for User Story 2

- [x] T014 [P] [US2] Implement TenantIdValidator applying NaturalQueryOptions.TenantIdPattern in src/NaturalQuery/Validation/TenantIdValidator.cs
- [x] T015 [P] [US2] Implement TenantFilterVerifier: structural check on normalized SQL for `<tenantColumn> = '<tenantId>'` (alias-qualified accepted) in src/NaturalQuery/Validation/TenantFilterVerifier.cs
- [x] T016 [US2] Wire tenant-ID validation at engine entry (before cache/rate-limit/LLM) and filter verification after generation and on retry path; no-op when tenant column unconfigured (FR-008), in src/NaturalQuery/NaturalQueryEngine.cs (depends on T014, T015)

**Checkpoint**: SC-002 green; single-tenant and well-formed multi-tenant behavior unchanged.

---

## Phase 5: User Story 3 — Protected Web Endpoints (Priority: P2)

**Goal**: Input limits, safe errors + correlation ID, host-delegated authorization opt-in, playground production guard, prompt/context screening (FR-009…FR-014).

**Independent Test**: SC-005 — oversized inputs rejected pre-LLM; no internal detail in any failure response; auth attachable in one config step; playground refuses in production without opt-in.

### Tests for User Story 3 (write FIRST, must FAIL)

- [x] T017 [P] [US3] Write EndpointProtectionTests: question > MaxQuestionLength rejected before AI call, history > MaxContextTurns rejected, backend failure returns `{ error, correlationId }` with zero internal detail, RequireAuthorization/AuthorizationPolicy refuse unauthenticated requests, default (no options) behavior unchanged, in tests/NaturalQuery.Tests/EndpointProtectionTests.cs
- [x] T018 [P] [US3] Write PlaygroundGuardTests: production without opt-in → 404 + logged warning; Development → serves; `allowInProduction: true` → serves, in tests/NaturalQuery.Tests/PlaygroundGuardTests.cs
- [x] T019 [P] [US3] Write PromptInjectionScreenerTests: instruction-override patterns (EN + PT/ES equivalents) detected, Warn flags without refusing, Block refuses, Off disables, legitimate questions resembling patterns not penalized in Warn, operator-extended patterns honored, dangerous SQL in conversation turns rejected (FR-013), in tests/NaturalQuery.Tests/PromptInjectionScreenerTests.cs

### Implementation for User Story 3

- [x] T020 [P] [US3] Create NaturalQueryEndpointOptions (RequireAuthorization, AuthorizationPolicy, MaxQuestionLength = 2000, MaxContextTurns = 20) in src/NaturalQuery/Extensions/NaturalQueryEndpointOptions.cs
- [x] T021 [US3] Add MapNaturalQuery additive overload accepting NaturalQueryEndpointOptions: enforce size limits pre-engine, shape all errors to `{ error, correlationId }` with full detail logged server-side keyed by correlation ID, delegate to ASP.NET `RequireAuthorization()`/named policy, in src/NaturalQuery/Extensions/EndpointRouteBuilderExtensions.cs (depends on T020)
- [x] T022 [P] [US3] Add playground production guard: refuse (404 + startup warning) unless IHostEnvironment.IsDevelopment() or `allowInProduction: true`, in src/NaturalQuery/Playground/PlaygroundExtensions.cs
- [x] T023 [P] [US3] Implement PromptInjectionScreener: built-in + operator-extensible regex patterns over question and each conversation turn, in src/NaturalQuery/Security/PromptInjectionScreener.cs
- [x] T024 [US3] Wire screening into engine: Warn → log + set QueryResult.InjectionFlagged; Block → refuse; screen caller-supplied history turns with hardened SqlValidator before inclusion in AI context, in src/NaturalQuery/NaturalQueryEngine.cs (depends on T023)

**Checkpoint**: SC-005 scenarios green; endpoints with no new options behave as before.

---

## Phase 6: User Story 4 — Abuse & Resource Governance (Priority: P2)

**Goal**: Bounded rate-limiter memory, result row cap with truncation marker, execution timeout (FR-015…FR-017).

**Independent Test**: SC-004 — 100k distinct tenant flood keeps memory bounded, latency within 10% of baseline; huge result truncated + marked; long query cancelled at timeout.

### Tests for User Story 4 (write FIRST, must FAIL)

- [x] T025 [P] [US4] Write RateLimiterEvictionTests: 100k distinct tenants stay under fixed memory bound (SC-004), idle entries evicted, active tenant never loses its window mid-minute, sweep amortized, in tests/NaturalQuery.Tests/RateLimiterEvictionTests.cs
- [x] T026 [P] [US4] Write ResultCapAndTimeoutTests: result > MaxResultRows truncated with Truncated = true, query's own smaller LIMIT binding → no truncation marker, execution past QueryTimeoutSeconds cancelled with timeout error type, in tests/NaturalQuery.Tests/ResultCapAndTimeoutTests.cs

### Implementation for User Story 4

- [x] T027 [US4] Add idle-entry eviction to InMemoryRateLimiter: on IsAllowedAsync, when entries > soft cap (10,000) evict windows idle ≥ 2 windows, sweep at most once/second, in src/NaturalQuery/RateLimiting/InMemoryRateLimiter.cs
- [x] T028 [US4] Enforce MaxResultRows in every executor read loop (Athena, PostgreSQL, SQL Server, SQLite, CSV/SQLite executors) setting Truncated when cap binds, in src/NaturalQuery/Providers/*.cs
- [x] T029 [US4] Implement QueryTimeoutSeconds via linked CancellationTokenSource around executor calls in engine; surface as `timeout` error type without changing executor signatures, in src/NaturalQuery/NaturalQueryEngine.cs

**Checkpoint**: SC-004 green; default caps generous — typical workloads unchanged.

---

## Phase 7: User Story 5 — Audit Trail & Sensitive Data Masking (Priority: P3)

**Goal**: Opt-in audit sink (one record per question, failure-isolated) and `***` redaction of sensitive columns in every output form (FR-018, FR-019).

**Independent Test**: SC-006 — exactly one audit record per processed question (success and failure); audit disabled = unchanged throughput; sensitive column masked in table/chart/export data.

### Tests for User Story 5 (write FIRST, must FAIL)

- [x] T030 [P] [US5] Write AuditSinkTests: exactly one record on success and on each failure class (validation_rejected, rate_limited, timeout, execution_error, llm_error), record fields complete (question, sql, tenant, outcome, duration, tokens, timestamp, correlationId, truncated), throwing sink never fails the request and is logged, no sink = zero-overhead path, in tests/NaturalQuery.Tests/AuditSinkTests.cs
- [x] T031 [P] [US5] Write SensitiveDataMaskerTests: sensitive column values → `***` in TableData and ChartData (grouping values included), unmasked columns untouched, CSV/JSON exports inherit masking, case-insensitive column matching, in tests/NaturalQuery.Tests/SensitiveDataMaskerTests.cs

### Implementation for User Story 5

- [x] T032 [P] [US5] Create IAuditSink (`WriteAsync(AuditRecord, CancellationToken)`) in src/NaturalQuery/Auditing/IAuditSink.cs and AuditRecord (fields per data-model.md) in src/NaturalQuery/Auditing/AuditRecord.cs
- [x] T033 [P] [US5] Implement SensitiveDataMasker replacing sensitive-column values with `***` across TableData/ChartData in src/NaturalQuery/Masking/SensitiveDataMasker.cs
- [x] T034 [US5] Add UseAuditSink registration overloads (instance, factory, delegate) in src/NaturalQuery/Extensions/ServiceCollectionExtensions.cs (depends on T032)
- [x] T035 [US5] Engine: emit exactly one AuditRecord per AskAsync in finally-style path wrapped in try/catch + log-on-failure; apply SensitiveDataMasker post-execution before caching/return, in src/NaturalQuery/NaturalQueryEngine.cs (depends on T032, T033)

**Checkpoint**: SC-006 green; audit/masking fully opt-in.

---

## Phase 8: User Story 6 — Expanded Integrations (Priority: P4)

**Goal**: MySQL/MariaDB executor + discovery, direct Anthropic provider, OpenRouter convenience, NaturalQuery.Redis companion package (FR-022…FR-025).

**Independent Test**: SC-011 — MySQL and Anthropic pass the same scenario suite as existing executors/providers; SC-008 — two instances sharing Redis enforce one combined limit and share cache.

### Tests for User Story 6 (write FIRST, must FAIL)

- [x] T036 [P] [US6] Write MySqlQueryExecutorTests (SQL-shape tests, no live server; transaction wrapping, row cap, timeout propagation) in tests/NaturalQuery.Tests/MySqlQueryExecutorTests.cs
- [x] T037 [P] [US6] Write AnthropicProviderTests (mocked HttpClient, same pattern as OpenAiProviderTests: response contract, input+output token mapping, 429 → rate-limit classification) in tests/NaturalQuery.Tests/AnthropicProviderTests.cs
- [x] T038 [P] [US6] Write OpenRouterConfigurationTests (BaseAddress `https://openrouter.ai/api/`, path unchanged, HTTP-Referer/X-Title headers when provided) in tests/NaturalQuery.Tests/OpenRouterConfigurationTests.cs

### Implementation for User Story 6

- [x] T039 [US6] Add MySqlConnector package reference to src/NaturalQuery/NaturalQuery.csproj (only new core dependency)
- [x] T040 [P] [US6] Implement MySqlQueryExecutor (wrapInTransaction support, read-only advisory, MaxResultRows cap, timeout token) in src/NaturalQuery/Providers/MySqlQueryExecutor.cs (depends on T039)
- [x] T041 [P] [US6] Implement MySqlSchemaDiscovery via information_schema in src/NaturalQuery/Discovery/MySqlSchemaDiscovery.cs (depends on T039)
- [x] T042 [P] [US6] Implement AnthropicProvider via raw HttpClient against `https://api.anthropic.com/v1/messages` (x-api-key + anthropic-version headers, token mapping, error classification) in src/NaturalQuery/Providers/AnthropicProvider.cs
- [x] T043 [US6] Add UseAnthropicProvider, UseOpenRouterProvider (OpenAiProvider with custom BaseAddress + attribution headers), UseMySqlExecutor, UseMySqlSchemaDiscovery registrations in src/NaturalQuery/Extensions/ServiceCollectionExtensions.cs (depends on T040, T041, T042)
- [x] T044 [US6] Create src/NaturalQuery.Redis/NaturalQuery.Redis.csproj (net8.0, StackExchange.Redis, version 2.1.0, version-locked to core) and add to solution
- [x] T045 [US6] Create tests/NaturalQuery.Redis.Tests project (xUnit + FluentAssertions + Moq) and write tests with mocked IConnectionMultiplexer: cache fail-open → miss, limiter fail-closed → deny + logged, INCR+EXPIRE window semantics, combined-limit behavior (SC-008), in tests/NaturalQuery.Redis.Tests/ (depends on T044)
- [x] T046 [P] [US6] Implement RedisQueryCache : IQueryCache (existing SHA256 key scheme, TTL from options, fail-open to miss) in src/NaturalQuery.Redis/RedisQueryCache.cs (depends on T044)
- [x] T047 [P] [US6] Implement RedisRateLimiter : IRateLimiter (atomic fixed-window INCR + EXPIRE via script, fail-closed with error log) in src/NaturalQuery.Redis/RedisRateLimiter.cs (depends on T044)
- [x] T048 [US6] Implement RedisExtensions UseRedisCache / UseRedisRateLimiter in src/NaturalQuery.Redis/RedisExtensions.cs (depends on T046, T047)

**Checkpoint**: SC-008 and SC-011 green; core dependency footprint = +MySqlConnector only.

---

## Phase 9: User Story 7 — Operational Readiness (Priority: P4)

**Goal**: Health checks, native metrics, fail-fast startup validation, Excel export (FR-026…FR-029).

**Independent Test**: SC-010 — invalid configs fail at startup with named problem, valid configs start unchanged; health reflects reachability; metrics observable; xlsx produced.

### Tests for User Story 7 (write FIRST, must FAIL)

- [x] T049 [P] [US7] Write HealthCheckTests (healthy when executor reachable + provider configured, degraded on unreachable database, recovery) in tests/NaturalQuery.Tests/HealthCheckTests.cs
- [x] T050 [P] [US7] Write MetricsTests via MeterListener (query count with outcome/tenant tags, duration histogram, token counter, cache hit/miss, error rate) in tests/NaturalQuery.Tests/MetricsTests.cs
- [x] T051 [P] [US7] Write OptionsValidationTests — SC-010 matrix: tenant column XOR placeholder → fail with named problem, negative limits → fail, threshold outside (0.5, 1.0] → fail, all currently-valid configs → start, in tests/NaturalQuery.Tests/OptionsValidationTests.cs
- [x] T052 [P] [US7] Write ExcelExportTests: xlsx round-trip via ZipArchive (worksheet present, inline strings, number cells, header row, masked values preserved as `***`) in tests/NaturalQuery.Tests/ExcelExportTests.cs

### Implementation for User Story 7

- [x] T053 [P] [US7] Implement NaturalQueryHealthCheck (IHealthCheck: executor ping via ConnectionValidator, provider configured/reachable — no billable LLM call) in src/NaturalQuery/Health/NaturalQueryHealthCheck.cs plus AddNaturalQueryHealthChecks extension
- [x] T054 [P] [US7] Implement NaturalQueryMetrics ("NaturalQuery" Meter: naturalquery.queries, naturalquery.tokens, naturalquery.cache, naturalquery.duration histogram) in src/NaturalQuery/Diagnostics/NaturalQueryMetrics.cs and instrument engine pipeline
- [x] T055 [P] [US7] Implement IValidateOptions&lt;NaturalQueryOptions&gt; (impossible-config errors only; MaxRetries clamp warns) and register automatically in AddNaturalQuery with ValidateOnStart semantics in src/NaturalQuery/Extensions/ServiceCollectionExtensions.cs
- [x] T056 [P] [US7] Implement MinimalXlsxWriter (System.IO.Compression.ZipArchive, one worksheet, inline strings, number detection) in src/NaturalQuery/Export/MinimalXlsxWriter.cs
- [x] T057 [US7] Add ToExcel() / ToExcelStream() extensions in src/NaturalQuery/Extensions/QueryResultExtensions.cs (depends on T056)

**Checkpoint**: SC-010 green; observability additive, zero new packages.

---

## Phase 10: User Story 8 — Smart Query Experience (Priority: P5)

**Goal**: Tenant-scoped semantic cache, stable pagination without extra AI calls, preview/approve with execution-time re-validation (FR-030…FR-032).

**Independent Test**: SC-009 — ≥30% AI-call reduction on paraphrase benchmark, zero wrong reuses, zero cross-tenant reuses; page 2 without AI call; preview executes only after approval with full re-validation.

### Tests for User Story 8 (write FIRST, must FAIL)

- [x] T058 [P] [US8] Write SemanticCacheTests: paraphrase pair hits without AI call, antonym pair ("top" vs "worst") below threshold → fresh call, different tenant never shares, expiry honored, SC-009 benchmark ≥30% reduction, in tests/NaturalQuery.Tests/SemanticCacheTests.cs
- [x] T059 [P] [US8] Write PaginationTests: stable ordering across pages, no AI call per page, captured-result semantics after underlying data changes, Truncated inheritance, in tests/NaturalQuery.Tests/PaginationTests.cs
- [x] T060 [P] [US8] Write PreviewExecuteTests: preview returns sql + cost estimate without execution, approved execution re-validated (hardened validator + tenant checks + masking + caps), stale approval after config change rejected, in tests/NaturalQuery.Tests/PreviewExecuteTests.cs

### Implementation for User Story 8

- [x] T061 [P] [US8] Create IEmbeddingProvider (`EmbedAsync(string, CancellationToken) → float[]`) in src/NaturalQuery/Embeddings/IEmbeddingProvider.cs
- [x] T062 [P] [US8] Implement OpenAiEmbeddingProvider (HttpClient pattern, no new dependency) in src/NaturalQuery/Embeddings/OpenAiEmbeddingProvider.cs (depends on T061)
- [x] T063 [P] [US8] Implement BedrockEmbeddingProvider (existing AWSSDK dependency) in src/NaturalQuery/Embeddings/BedrockEmbeddingProvider.cs (depends on T061)
- [x] T064 [US8] Create ISemanticQueryCache + SemanticQueryCache (in-memory embedding store, cosine similarity ≥ threshold, strict tenant scoping, exact-cache expiry rules, fall-through on miss/error) in src/NaturalQuery/Caching/ISemanticQueryCache.cs and src/NaturalQuery/Caching/SemanticQueryCache.cs (depends on T061)
- [x] T065 [US8] Add UseSemanticCache, UseOpenAiEmbeddings, UseBedrockEmbeddings registrations in src/NaturalQuery/Extensions/ServiceCollectionExtensions.cs and wire semantic lookup into engine ahead of LLM call (depends on T062, T063, T064)
- [x] T066 [P] [US8] Create QueryPreview model (Sql, ChartType, Title, Description, EstimatedCost?, CorrelationId) in src/NaturalQuery/Models/QueryPreview.cs
- [x] T067 [US8] Implement AskPagedAsync (slices cached captured result), PreviewAsync (no execution, cost via IQueryCostEstimator when registered), ExecuteApprovedAsync (full re-validation at execution time) in src/NaturalQuery/NaturalQueryEngine.cs, and add matching default interface members to src/NaturalQuery/INaturalQueryEngine.cs (depends on T066)
- [x] T068 [US8] Add `POST {prefix}/preview`, `POST {prefix}/execute` routes and `page`/`pageSize` parameters (GET query string / POST body) in src/NaturalQuery/Extensions/EndpointRouteBuilderExtensions.cs (depends on T067)

**Checkpoint**: SC-009 green; all opt-in — absence changes nothing for existing users.

---

## Phase 11: Polish & Release (v2.1.0)

**Purpose**: Cross-cutting release work — samples, docs, version, full verification.

- [x] T069 [P] Add hardened-configuration example (<10 lines enabling authorization, limits, audit, masking per quickstart.md — SC-007) to samples/WebApi without modifying existing sample code paths
- [x] T070 [P] Update README.md and CHANGELOG.md: new features, behavioral-delta table from contracts/public-api.md documented under "Security fixes"
- [x] T071 Bump version to 2.1.0 in src/NaturalQuery/NaturalQuery.csproj and src/NaturalQuery.Redis/NaturalQuery.Redis.csproj
- [x] T072 Run full suite (`dotnet test` on both test projects) and verify SC-001…SC-011 checklist; confirm existing 16 test files pass unmodified (SC-003)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: none
- **Foundational (Phase 2)**: after Setup — BLOCKS all user stories (options/model surface used everywhere)
- **US1 (Phase 3)** and **US2 (Phase 4)**: after Phase 2; independent of each other (US2's filter verifier reuses SqlNormalizer from T009 — schedule US1 first or share that one file)
- **US3 (Phase 5)**: after Phase 2; T024 history screening reuses hardened SqlValidator (T010)
- **US4 (Phase 6)**: after Phase 2; independent
- **US5 (Phase 7)**: after Phase 2; audit outcome taxonomy is richer once US1–US4 error types exist, but implementable independently
- **US6 (Phase 8)**: after Phase 2; new executors/providers should land after US1/US4 so they inherit hardened validation + caps (plan Wave 4)
- **US7 (Phase 9)**: after Phase 2; metrics instrumentation touches engine — sequence engine edits (T054) after T035
- **US8 (Phase 10)**: after Phase 2; preview/execute depends on hardened validation (US1, US2); pagination depends on existing cache
- **Polish (Phase 11)**: after all included stories

### Engine-file serialization

NaturalQueryEngine.cs is touched by T006, T011, T016, T024, T029, T035, T054, T065, T067 — these are never [P] against each other; execute in phase order.

### Parallel Opportunities

- Phase 2: T002, T004, T005 parallel; then T003, T006
- Every story: its test tasks all [P] together before implementation
- US1+US2 test corpora (T007, T008, T012, T013) can all be written in parallel
- US6: T040, T041, T042 parallel; T046, T047 parallel
- US7: T053–T056 all parallel (distinct files)
- US8: T061–T063, T066 parallel
- With Phase 2 done, different developers can own different stories (respecting engine-file serialization above)

---

## Parallel Example: User Story 1

```bash
# Write both failing test corpora together:
Task: "SqlNormalizerTests in tests/NaturalQuery.Tests/SqlNormalizerTests.cs"
Task: "SqlValidatorHardeningTests in tests/NaturalQuery.Tests/SqlValidatorHardeningTests.cs"

# Then sequentially: SqlNormalizer → SqlValidator hardening → retry-path wiring
```

---

## Implementation Strategy

### MVP First (US1 only)

1. Phase 1 → Phase 2 → Phase 3 (US1)
2. STOP and VALIDATE: SC-001 corpus green, existing suite green unmodified
3. This alone closes the highest-risk audit finding (bypassable validation)

### Incremental Delivery (matches plan.md waves)

1. Setup + Foundational → base ready
2. US1 + US2 (P1 security core) → validate → mergeable
3. US3 + US4 (P2 boundary + governance) → validate → mergeable
4. US5 (P3 trust) → validate → mergeable
5. US6 + US7 (P4 integrations + ops) → validate → mergeable
6. US8 (P5 smart experience) → validate → mergeable
7. Polish → release 2.1.0

Each increment leaves master-mergeable state: suite green, no partial public APIs exposed.

---

## Notes

- [P] = different files, no dependency on an incomplete task
- Test-first is mandatory for validators (US1, US2) and required for every new mechanism (FR-021)
- Zero breaking changes gate on every checkpoint: existing 16 test files must pass unmodified (SC-003)
- Only permitted behavior delta: rejecting dangerous inputs (documented in CHANGELOG as security fixes)
- Commit after each task or logical group
