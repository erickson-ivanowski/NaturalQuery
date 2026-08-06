# Implementation Plan: Security Audit & Hardening with Value-Add Enhancements

**Branch**: `001-security-audit-enhancements` | **Date**: 2026-08-06 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-security-audit-enhancements/spec.md`

## Summary

Harden NaturalQuery's NL2SQL pipeline against the six audited weaknesses (bypassable SQL validation, injectable tenant isolation, unprotected endpoints, prompt injection, unbounded resources, no audit trail) and ship the agreed value-add features (MySQL/MariaDB, Anthropic + OpenRouter providers, Redis cache/rate limiter, health checks, metrics, startup validation, Excel export, semantic cache, pagination, preview/dry-run) — as release 2.1.0, strictly additive, zero breaking changes. Technical approach per [research.md](./research.md): normalization-pipeline SQL validation (no parser dependency), character-policy tenant IDs + structural filter verification, host-delegated endpoint authorization, opt-in audit/masking/semantic layers, and a `NaturalQuery.Redis` companion package to keep the core dependency footprint unchanged.

## Technical Context

**Language/Version**: C# / .NET 8.0 (`net8.0`), nullable enabled, implicit usings

**Primary Dependencies**: Core: AWSSDK.BedrockRuntime, AWSSDK.Athena, Npgsql, Microsoft.Data.SqlClient, Microsoft.Data.Sqlite, Microsoft.Extensions.* (8.x), `Microsoft.AspNetCore.App` framework reference. New in core: **MySqlConnector** (only new core package). New companion package `NaturalQuery.Redis`: **StackExchange.Redis**.

**Storage**: N/A (library; targets user databases — Athena, PostgreSQL, SQL Server, SQLite, CSV→SQLite, +MySQL/MariaDB)

**Testing**: xUnit 2.5 + FluentAssertions 8 + Moq 4.20, `dotnet test` (existing suite in `tests/NaturalQuery.Tests` must pass unmodified — SC-003)

**Target Platform**: Cross-platform .NET 8 (NuGet library consumed by ASP.NET and console apps)

**Project Type**: Library (NuGet package `NaturalQuery` 2.0.0 → 2.1.0, + new `NaturalQuery.Redis`)

**Performance Goals**: Validation overhead negligible (<1 ms per query); rate-limiter memory bounded under 100k distinct tenants with latency within 10% of baseline (SC-004); audit disabled = zero overhead (SC-006); semantic cache ≥30% LLM-call reduction on paraphrase benchmark (SC-009)

**Constraints**: Zero breaking changes (FR-020): no public signature changes, additive members/overloads only, defaults preserve current behavior; only permitted behavior delta = rejecting dangerous input. No heavy new dependencies in core. All security mechanisms must also cover the retry/repair path (FR-004).

**Scale/Scope**: ~45 existing source files, 16 test files; feature adds ~25 new source files, 1 new project, ~20 new test files; 33 functional requirements

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` is an unfilled template — no ratified project constitution exists. No gates to evaluate. Applied general principles instead: additive-only API evolution, test-first for security-critical validators, no dependency bloat in core. **PASS** (pre-Phase-0 and post-Phase-1).

## Project Structure

### Documentation (this feature)

```text
specs/001-security-audit-enhancements/
├── plan.md              # This file
├── research.md          # Phase 0 output — 16 resolved decisions
├── data-model.md        # Phase 1 output — new/changed entities
├── quickstart.md        # Phase 1 output — adopter-facing usage of new capabilities
├── contracts/
│   └── public-api.md    # Phase 1 output — additive public API surface (contract)
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
src/NaturalQuery/                        # existing package (additions marked +)
├── NaturalQueryOptions.cs               # + limits, tenant policy, screening mode, masking, timeouts
├── NaturalQueryEngine.cs                # + tenant-ID validation, timeout, masking, audit emit, paged/preview APIs
├── INaturalQueryEngine.cs               # + AskPagedAsync, PreviewAsync, ExecuteApprovedAsync (default interface members)
├── Validation/
│   ├── SqlValidator.cs                  # hardened: SqlNormalizer pipeline + word-boundary denylist (signature unchanged)
│   ├── SqlNormalizer.cs                 # + comment stripping, literal removal, whitespace collapse
│   ├── TenantIdValidator.cs             # + character-policy validation
│   ├── TenantFilterVerifier.cs          # + structural WHERE-filter check
│   └── SchemaValidator.cs               # existing
├── Security/
│   ├── PromptInjectionScreener.cs       # + pattern screening (warn default / block strict)
│   └── InjectionScreeningMode.cs        # + enum: Off, Warn (default), Block
├── Auditing/
│   ├── IAuditSink.cs                    # + opt-in audit contract
│   └── AuditRecord.cs                   # + record type
├── Masking/
│   └── SensitiveDataMasker.cs           # + *** redaction over chart/table data
├── RateLimiting/
│   └── InMemoryRateLimiter.cs           # hardened: idle-entry eviction, soft cap
├── Caching/
│   ├── SemanticQueryCache.cs            # + opt-in similarity cache
│   └── ISemanticQueryCache.cs           # +
├── Embeddings/
│   ├── IEmbeddingProvider.cs            # +
│   ├── OpenAiEmbeddingProvider.cs       # + (HttpClient, no new dep)
│   └── BedrockEmbeddingProvider.cs      # + (existing AWSSDK dep)
├── Providers/
│   ├── AnthropicProvider.cs             # + direct Anthropic Messages API
│   ├── MySqlQueryExecutor.cs            # + (MySqlConnector)
│   └── (existing providers unchanged)
├── Discovery/
│   └── MySqlSchemaDiscovery.cs          # +
├── Health/
│   └── NaturalQueryHealthCheck.cs       # + IHealthCheck
├── Diagnostics/
│   └── NaturalQueryMetrics.cs           # + Meter/counters/histogram
├── Models/
│   ├── QueryResult.cs                   # + Truncated, CorrelationId (additive properties)
│   ├── QueryPreview.cs                  # +
│   ├── TableSchema.cs / ColumnDef       # + Sensitive flag via new overload
│   └── (others unchanged)
├── Extensions/
│   ├── ServiceCollectionExtensions.cs   # + UseAnthropicProvider, UseOpenRouterProvider, UseMySqlExecutor,
│   │                                    #   UseAuditSink, UseSemanticCache, AddNaturalQueryHealthChecks,
│   │                                    #   IValidateOptions registration (ValidateOnStart)
│   ├── EndpointRouteBuilderExtensions.cs# + NaturalQueryEndpointOptions overload, safe errors + correlation ID,
│   │                                    #   size limits, /preview + /execute routes, pagination params
│   └── QueryResultExtensions.cs         # + ToExcel()/ToExcelStream()
├── Export/
│   └── MinimalXlsxWriter.cs             # + zip-based SpreadsheetML writer
└── Playground/
    └── PlaygroundExtensions.cs          # + production guard (IsDevelopment / allowInProduction)

src/NaturalQuery.Redis/                  # + NEW companion package
├── NaturalQuery.Redis.csproj            # StackExchange.Redis; version 2.1.0
├── RedisQueryCache.cs                   # IQueryCache impl (fail-open to miss)
├── RedisRateLimiter.cs                  # IRateLimiter impl (INCR+EXPIRE, fail-closed)
└── RedisExtensions.cs                   # UseRedisCache / UseRedisRateLimiter

tests/NaturalQuery.Tests/                # existing 16 files unmodified + new test files per area:
├── SqlNormalizerTests.cs                # + bypass corpus (comments, newlines, dialect ops)
├── SqlValidatorHardeningTests.cs        # + SC-001 corpus (dangerous + legitimate)
├── TenantIdValidatorTests.cs            # + injection corpus (SC-002)
├── TenantFilterVerifierTests.cs         # +
├── PromptInjectionScreenerTests.cs      # +
├── AuditSinkTests.cs                    # + exactly-one-record, sink-failure isolation (SC-006)
├── SensitiveDataMaskerTests.cs          # +
├── RateLimiterEvictionTests.cs          # + 100k-tenant bound (SC-004)
├── ResultCapAndTimeoutTests.cs          # +
├── EndpointProtectionTests.cs           # + size limits, safe errors, auth opt-in (SC-005)
├── PlaygroundGuardTests.cs              # +
├── AnthropicProviderTests.cs            # + (mocked HttpClient, same pattern as OpenAiProviderTests)
├── OpenRouterConfigurationTests.cs      # +
├── MySqlQueryExecutorTests.cs           # + (SQL-shape tests; no live server)
├── HealthCheckTests.cs                  # +
├── MetricsTests.cs                      # +
├── OptionsValidationTests.cs            # + SC-010 invalid/valid config matrix
├── ExcelExportTests.cs                  # + xlsx structure round-trip
├── SemanticCacheTests.cs                # + threshold, antonym pairs, tenant scoping (SC-009)
├── PaginationTests.cs                   # +
└── PreviewExecuteTests.cs               # + stale-approval re-validation

tests/NaturalQuery.Redis.Tests/          # + NEW (mocked IConnectionMultiplexer; fail-open/fail-closed)
```

**Structure Decision**: Existing single-library layout retained; new capabilities slot into the established folder-per-concern convention (`Validation/`, `Providers/`, `Caching/`…). One new source project (`NaturalQuery.Redis`) isolates the only heavy new dependency, honoring the core-footprint promise. Samples (`samples/ConsoleApp`, `samples/WebApi`) gain a hardened-configuration example (SC-007) without modifying existing sample code paths.

## Delivery Phasing (informs /speckit-tasks ordering)

1. **Wave 1 — Security core (P1)**: SqlNormalizer, hardened SqlValidator (+ retry-path reuse), TenantIdValidator, TenantFilterVerifier. Pure additions + internal call-site changes; full test corpora first (test-first for validators).
2. **Wave 2 — Boundary protections (P2)**: endpoint options/limits/safe errors/auth, playground guard, prompt screener, rate-limiter eviction, result cap + timeout, options startup validation.
3. **Wave 3 — Trust features (P3)**: IAuditSink + engine emission, sensitive masking, metrics, health checks.
4. **Wave 4 — Integrations (P4)**: MySQL executor + discovery, AnthropicProvider, OpenRouter convenience, NaturalQuery.Redis package.
5. **Wave 5 — Smart experience (P5)**: Excel export, semantic cache + embedding providers, pagination, preview/execute endpoints.
6. **Wave 6 — Release**: samples update, README/CHANGELOG (security-fix notes), version bump 2.1.0, full-suite + SC verification.

Each wave leaves `master`-mergeable state: suite green, no partial public APIs exposed.

## Complexity Tracking

No constitution violations to justify (no constitution ratified). Notable accepted complexity:

| Item | Why Needed | Simpler Alternative Rejected Because |
|------|------------|--------------------------------------|
| Companion package `NaturalQuery.Redis` | Keep StackExchange.Redis out of core | Core dependency would inflate every consumer's footprint (spec assumption violated) |
| Hand-rolled minimal XLSX writer | Excel export without heavy deps | ClosedXML/EPPlus dependency chain disproportionate to a flat table |
| Normalization pipeline instead of SQL parser | Dialect-agnostic hardening, zero deps | AST parsers mis-parse legitimate dialect SQL → would break existing users (FR-020) |
