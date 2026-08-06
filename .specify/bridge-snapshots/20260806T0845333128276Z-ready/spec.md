# Feature Specification: Security Audit & Hardening with Value-Add Enhancements

**Feature Branch**: `001-security-audit-enhancements`

**Created**: 2026-08-06

**Status**: Draft

**Input**: User description: "quero que você analise todo o projeto e veja o que podemos incluir para agregar ainda mais este projeto. além disso faça uma auditoria de segurança e se possível inclua mecanismos de segurança mais acurados para que este projeto fique mais relevante. faça isso sem que o projeto perca sua estabilidade ou que ele tenha break changes."

## Overview

NaturalQuery is a library that converts natural language questions into database queries and executes them. Because it takes untrusted human input, sends it to an AI model, and runs the AI's output against real databases, its security posture is the product's core value proposition. A security audit of the current version identified several weaknesses in query safety validation, tenant isolation, web endpoint exposure, and abuse resistance. This feature hardens those areas, adds high-value protective capabilities (audit trail, data masking, resource governance), and expands the product's reach with new integrations (MySQL/MariaDB, direct Anthropic provider, shared-store cache and rate limiting), operational readiness (health checks, metrics, startup validation, spreadsheet export), and smart query features (semantic cache, pagination, preview/dry-run) — all without breaking changes: every existing integration must continue to work unchanged, and every new mechanism must be either strictly protective (blocking only genuinely dangerous behavior) or opt-in.

### Audit Findings Being Addressed

1. **Query safety validation is bypassable**: dangerous-operation detection relies on simple keyword matching that can be evaded with comments, line breaks, or alternate spacing, and several dangerous operation types (e.g., merge/upsert, stored procedure execution, file access, database attachment, system configuration commands, "select into" writes) are not covered at all.
2. **Tenant isolation is weak and injectable**: the tenant identifier — which can arrive from an HTTP request — is inserted into the query by plain text substitution with no sanitization, creating a query injection vector; and isolation is verified only by checking that the tenant value appears somewhere in the query text, not that it is actually used as a filter on the tenant column.
3. **Web endpoints lack abuse protection**: no input length limits (cost/abuse exposure), internal error details are returned to callers (information disclosure), and there is no built-in hook to require authentication or authorization before queries run.
4. **Prompt injection surface**: user questions and caller-supplied conversation history are passed to the AI model without screening, allowing a malicious caller to attempt to override the model's safety instructions.
5. **Unbounded resource usage**: per-tenant rate-limiter state grows without cleanup as new tenant identifiers arrive (memory exhaustion risk), no cap on result set size, and no query execution timeout independent of the caller's.
6. **No audit trail**: executed queries are not durably attributable — operators cannot answer "who asked what, what ran, and what was returned" after the fact.

## Clarifications

### Session 2026-08-06

- Q: Prompt-injection screening — default mode? → A: Warn by default: detection always on, logs + flags; refusal only when operator opts in (strict mode).
- Q: Endpoint authorization opt-in — mechanism? → A: Integrate with the host application's existing authorization (require authenticated user or an operator-named policy); no library-managed API keys.
- Q: Concrete default limits? → A: Question ≤ 2,000 chars; conversation history ≤ 20 turns; result cap 10,000 rows; execution timeout 30s; tenant ID ≤ 128 chars — all operator-configurable.
- Q: Sensitive column masking style? → A: Full redaction — masked values are replaced with `***` in every output form.
- Q: Additional value-add feature scope? → A: Include all proposed: MySQL/MariaDB support, direct Anthropic provider, shared-store (Redis) cache and rate limiter, health checks, native metrics, startup configuration validation, Excel export, semantic cache, result pagination, dry-run/preview.
- Q: OpenRouter support? → A: Yes, as a thin convenience over the existing OpenAI-compatible provider (custom endpoint + recommended headers); no new provider implementation.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Trustworthy Query Safety Validation (Priority: P1)

As a developer embedding NaturalQuery in my application, I need certainty that no AI-generated query can ever modify, delete, or exfiltrate data outside its read-only purpose — even when the query uses obfuscation tricks — so that I can safely expose natural language querying to end users.

**Why this priority**: Executing AI-generated queries against production databases is the product's riskiest operation. A validation bypass means data loss or corruption. This is the foundation every other protection builds on.

**Independent Test**: Can be fully tested by submitting a corpus of known-dangerous queries (including obfuscated variants using comments, line breaks, casing tricks, and dialect-specific write/execute operations) and verifying every one is rejected, while a corpus of legitimate read queries continues to pass.

**Acceptance Scenarios**:

1. **Given** a generated query containing a dangerous operation disguised with comments or unusual whitespace (e.g., a delete written across lines or wrapped in comment markers), **When** validation runs, **Then** the query is rejected with a clear safety error.
2. **Given** a generated query using a dangerous operation type not previously covered (merge/upsert, stored procedure execution, system commands, file access, database attachment, "select into" a new table), **When** validation runs, **Then** the query is rejected.
3. **Given** a legitimate read-only query that merely mentions a dangerous word inside a quoted text value (e.g., filtering on an event name called "INSERT"), **When** validation runs, **Then** the query is accepted (no false-positive regression).
4. **Given** an existing integration using the current public validation behavior, **When** the library is upgraded, **Then** the integration compiles and runs without any code changes.

---

### User Story 2 - Injection-Proof Tenant Isolation (Priority: P1)

As an operator of a multi-tenant product, I need the tenant identifier to be treated as untrusted input and tenant filtering to be structurally verified, so that one tenant can never read another tenant's data or inject query fragments through the tenant identifier.

**Why this priority**: Cross-tenant data leakage is the most severe breach class for a multi-tenant analytics feature; the tenant value currently flows from HTTP input into the query text unsanitized.

**Independent Test**: Can be fully tested by submitting tenant identifiers containing quote characters, comment markers, and boolean-logic fragments, verifying all are rejected or neutralized; and by submitting generated queries where the tenant value appears only in a text literal or comment (not as an actual filter) and verifying they are rejected.

**Acceptance Scenarios**:

1. **Given** a tenant identifier containing quote characters, comment markers, or query syntax (e.g., `abc' OR '1'='1`), **When** a question is asked with that tenant identifier, **Then** the request is rejected before any query is built or executed.
2. **Given** a generated query where the tenant value appears somewhere in the text but is not actually applied as a filter on the configured tenant column, **When** validation runs, **Then** the query is rejected.
3. **Given** an existing single-tenant integration (no tenant column configured), **When** the library is upgraded, **Then** behavior is unchanged.
4. **Given** an existing multi-tenant integration with well-formed tenant identifiers (letters, digits, hyphens, underscores), **When** the library is upgraded, **Then** all previously working requests continue to work.

---

### User Story 3 - Protected Web Endpoints (Priority: P2)

As a developer exposing the built-in web endpoints, I need input limits, safe error responses, and an easy way to require authorization, so that publishing the endpoint does not create an unauthenticated, unbounded-cost gateway to my database and AI budget.

**Why this priority**: The one-line endpoint mapping is the product's main convenience feature; today it ships with no guardrails, so every adopter inherits the same exposure.

**Independent Test**: Can be fully tested by calling the endpoints with oversized questions, oversized conversation histories, and failing backends, and verifying size limits are enforced, responses never contain internal error details, and an authorization requirement can be attached with one configuration step.

**Acceptance Scenarios**:

1. **Given** a question longer than the configured maximum length, **When** it is submitted to an endpoint, **Then** it is rejected with a clear "too long" error before any AI call is made.
2. **Given** a backend failure (AI provider error, database error), **When** the endpoint responds, **Then** the response contains a generic, safe error message plus a correlation identifier, and the full detail is available only in server-side logs.
3. **Given** an operator who wants the endpoint protected, **When** they enable the authorization option during endpoint mapping, **Then** unauthenticated or unauthorized requests are refused without reaching the engine.
4. **Given** an existing integration mapping endpoints with no new options, **When** the library is upgraded, **Then** the endpoints behave as before, except that generous default input limits now apply (large enough that no legitimate question is affected).
5. **Given** the interactive playground, **When** the application runs in a production environment without the operator explicitly opting in, **Then** the playground refuses to serve and logs a warning explaining why.

---

### User Story 4 - Abuse & Resource Governance (Priority: P2)

As an operator, I need bounded resource consumption — capped result sizes, query timeouts, bounded rate-limiter memory, and per-tenant cost visibility — so that a single abusive or runaway tenant cannot degrade the service or run up AI/database costs.

**Why this priority**: Each query costs real money (AI tokens, database scans). Without bounds, abuse is an economic attack even when no data is at risk.

**Independent Test**: Can be fully tested by simulating a flood of unique tenant identifiers (memory must stay bounded), a query returning a huge result set (result must be truncated at the cap with a truncation indicator), and a long-running query (must be cancelled at the timeout).

**Acceptance Scenarios**:

1. **Given** requests arriving under thousands of distinct tenant identifiers, **When** the rate limiter tracks them, **Then** memory usage stays bounded (stale tenant entries are evicted).
2. **Given** a query whose result exceeds the configured maximum row count, **When** results are returned, **Then** the result is truncated at the cap and clearly marked as truncated.
3. **Given** a query that runs longer than the configured execution timeout, **When** the timeout elapses, **Then** execution is cancelled and reported as a timeout error.
4. **Given** an existing integration, **When** the library is upgraded, **Then** default caps are generous enough that typical workloads see no behavior change.

---

### User Story 5 - Audit Trail & Sensitive Data Masking (Priority: P3)

As a compliance-conscious operator, I want an optional audit trail of every question, generated query, and outcome, and the ability to mark columns as sensitive so their values are masked in results, so that I can adopt NaturalQuery in regulated environments.

**Why this priority**: Not required for safety of the core flow, but a major adoption unlock ("agregar valor") for enterprise/regulated users and a differentiator versus competing tools.

**Independent Test**: Can be fully tested by enabling the audit hook and verifying every request produces exactly one audit record with question, query text, tenant, outcome, timing, and token usage; and by marking a column as sensitive and verifying its values are masked in returned results while unmasked columns are untouched.

**Acceptance Scenarios**:

1. **Given** an audit sink is registered, **When** any question is processed (success or failure), **Then** exactly one audit record is produced containing the question, generated query, tenant identifier, outcome, duration, and token usage.
2. **Given** no audit sink is registered, **When** questions are processed, **Then** behavior and performance are unchanged (zero overhead opt-in).
3. **Given** a column marked as sensitive in the schema configuration, **When** results containing that column are returned, **Then** its values are masked in the output.
4. **Given** an audit sink that itself fails, **When** a question is processed, **Then** the user's query still succeeds and the audit failure is logged server-side.

---

### User Story 6 - Expanded Integrations (Priority: P4)

As a developer whose stack is not yet covered, I want MySQL/MariaDB database support, a direct Anthropic AI provider, and shared-store (Redis-compatible) implementations of the cache and rate limiter, so that I can adopt NaturalQuery in more environments and run it across multiple application instances with consistent behavior.

**Why this priority**: Pure reach/adoption value. Depends on nothing above, but security hardening (P1–P2) must land first so new integrations inherit the hardened pipeline.

**Independent Test**: Can be fully tested by running the existing sample scenarios against a MySQL database and the Anthropic provider, and by verifying that two application instances sharing a store enforce one combined rate limit and share cached results.

**Acceptance Scenarios**:

1. **Given** a MySQL/MariaDB database, **When** it is configured as the query executor (with schema discovery), **Then** all engine features work with the same safety guarantees as the existing databases.
2. **Given** the direct Anthropic provider is configured, **When** questions are asked, **Then** behavior is equivalent to the existing AI providers (same response contract, token reporting, error classification).
3. **Given** two application instances sharing a store, **When** one tenant sends requests split across both, **Then** the combined rate limit is enforced and a result cached by one instance is served by the other.

---

### User Story 7 - Operational Readiness (Priority: P4)

As an operator running NaturalQuery in production, I want health checks, native usage metrics, fail-fast configuration validation, and spreadsheet export, so that the service is observable, misconfiguration is caught at deploy time, and business users get results in the format they use.

**Why this priority**: Production-operations value on top of the secured core; none of it changes query behavior.

**Independent Test**: Can be fully tested by registering the health checks and verifying they reflect provider/database reachability, scraping metrics during a query burst, starting the application with broken configuration (must fail at startup with an actionable message), and exporting a result to a spreadsheet file.

**Acceptance Scenarios**:

1. **Given** health checks are registered, **When** the AI provider or database becomes unreachable, **Then** the health status degrades accordingly and recovers when connectivity returns.
2. **Given** metrics are enabled, **When** queries run, **Then** counts, latency, token usage, cache hit rate, and error rate are observable through the host's standard monitoring pipeline.
3. **Given** a configuration that cannot possibly work (e.g., no AI provider registered, contradictory tenant settings), **When** the application starts, **Then** startup fails with a message naming the exact problem and fix.
4. **Given** a query result, **When** exported, **Then** a spreadsheet (Excel) file is produced alongside the existing CSV/JSON options.

---

### User Story 8 - Smart Query Experience (Priority: P5)

As a product owner controlling AI spend and serving analysts, I want semantically similar questions to reuse cached results, large results to be browsable page by page, and a preview mode where users see the generated query and its estimated cost before approving execution, so that costs drop and users gain trust and control.

**Why this priority**: Highest-sophistication layer; valuable but builds on everything above (cache, validation, cost estimation).

**Independent Test**: Can be fully tested by asking paraphrased question pairs (second one must hit the semantic cache without an AI call), paging through a large result set without additional AI calls, and running a preview that returns query + cost estimate without touching the database until approved.

**Acceptance Scenarios**:

1. **Given** semantic caching is enabled and "top products by sales" was answered, **When** the same tenant asks "best selling products", **Then** the cached result is reused without a new AI call.
2. **Given** semantic caching is enabled, **When** a *different* tenant asks a similar question, **Then** the cache is NOT shared across tenants.
3. **Given** a result larger than one page, **When** the caller requests subsequent pages, **Then** pages are returned with stable ordering and no additional AI calls.
4. **Given** preview mode, **When** a question is submitted, **Then** the generated query and estimated cost are returned without execution; **When** the caller approves, **Then** the query is re-validated with all safety rules and only then executed.

---

### Edge Cases

- Dangerous keyword appears inside a quoted text value AND the query also contains a genuinely dangerous operation → must still be rejected (literal-stripping must not mask real threats).
- Tenant identifier is empty string vs. not provided at all → both handled consistently; empty string treated as "not provided".
- Question consists entirely of prompt-injection content ("ignore your instructions and…") → AI may still generate something; validation remains the enforcement point. The screening layer flags obvious injection patterns (and refuses them only in opt-in strict mode), without penalizing legitimate questions that merely resemble them.
- Conversation history supplied by the caller contains fabricated dangerous queries → history is screened with the same query-safety rules before being included in AI context.
- Result truncation cap conflicts with a query's own smaller row limit → the smaller of the two applies; no truncation marker when the query's own limit was the binding one.
- Rate-limiter eviction races with an active tenant → an active tenant must never lose its window mid-minute; only idle entries are evicted.
- Upgraded integration that intentionally relied on previously-permitted dangerous patterns (e.g., testing harnesses submitting write queries expecting them to pass) → newly rejected; this is a documented security fix, not a supported break.
- Masked sensitive column used as a chart grouping value → masking applies to values in all output forms (chart data, table data, exports).
- Semantic cache matches a paraphrase whose intent actually differs ("top products" vs "worst products") → similarity threshold must be conservative; a wrong reuse is a correctness bug, so near-misses must fall through to a fresh AI call.
- Preview approved, then underlying schema or safety configuration changes before execution → approved query is re-validated at execution time; stale approvals are rejected.
- Pagination requested after underlying data changed → pages reflect the captured result set (or a documented re-execution semantic), never a silent mix of old and new rows.
- Shared-store (Redis) backend unavailable at runtime → cache degrades to miss and rate limiter fails closed conservatively (deny over allow) with clear operator-facing logging; the application itself keeps running.

## Requirements *(mandatory)*

### Functional Requirements

**Query Safety (P1)**

- **FR-001**: System MUST reject any generated query that performs data modification, schema modification, permission changes, stored procedure or system command execution, file or external resource access, database attachment, or writing results into new tables — regardless of formatting, casing, comments, or whitespace used to disguise the operation.
- **FR-002**: System MUST neutralize comments and normalize whitespace before safety evaluation so that obfuscation cannot hide dangerous operations.
- **FR-003**: System MUST continue to accept legitimate read-only queries that mention dangerous words only inside quoted text values (no new false positives on the existing documented cases).
- **FR-004**: System MUST apply identical safety validation at every point where a generated query can reach execution, including retry/repair flows.
- **FR-005**: Existing public validation entry points MUST retain their current signatures and result conventions (no compile-time or observable API breaks).

**Tenant Isolation (P1)**

- **FR-006**: System MUST validate tenant identifiers against a safe character policy (letters, digits, hyphen, underscore, dot; maximum 128 characters by default) before any use, rejecting requests with non-conforming identifiers. Operators MUST be able to customize the policy.
- **FR-007**: System MUST verify that the tenant value is applied as an actual filter condition on the configured tenant column, not merely present somewhere in the query text.
- **FR-008**: Single-tenant configurations (no tenant column configured) MUST be completely unaffected.

**Endpoint Protection (P2)**

- **FR-009**: Web endpoints MUST enforce configurable maximum lengths for questions (default 2,000 characters) and conversation history (default 20 turns), rejecting oversized input before any AI call.
- **FR-010**: Web endpoint error responses MUST NOT expose internal error details (AI provider messages, database errors, stack traces); they MUST return a safe message plus a correlation identifier, with full detail logged server-side.
- **FR-011**: Operators MUST be able to require authorization on the mapped endpoints via a single opt-in configuration that delegates to the host application's existing authorization (require an authenticated user, or an operator-named authorization policy). The library does not manage its own credentials or API keys. Without opt-in, current open behavior is preserved.
- **FR-012**: The interactive playground MUST refuse to serve outside development environments unless the operator explicitly opts in.

**Prompt & Context Screening (P2)**

- **FR-013**: Caller-supplied conversation history MUST be screened with the same query-safety rules before inclusion in AI context; turns containing dangerous queries are rejected.
- **FR-014**: System MUST detect questions matching known prompt-injection patterns (instruction-override attempts), with the pattern set extensible by the operator. Default action is to log a warning and flag the request (in telemetry and audit records); refusing flagged questions is an opt-in strict mode. AI-generated output remains subject to full validation regardless of screening outcome.

**Resource Governance (P2)**

- **FR-015**: Rate-limiter memory MUST remain bounded under unbounded distinct tenant identifiers (idle entries evicted; active windows never lost mid-window).
- **FR-016**: System MUST support a configurable maximum result row count (default 10,000 rows); results exceeding it are truncated and marked as truncated.
- **FR-017**: System MUST support a configurable query execution timeout (default 30 seconds) that cancels the database work when exceeded and reports a timeout error.

**Audit & Masking (P3)**

- **FR-018**: System MUST offer an opt-in audit sink receiving one record per processed question (success or failure) containing: question, generated query, tenant identifier, outcome classification, duration, and token usage. Audit sink failures MUST NOT fail the user's request.
- **FR-019**: Operators MUST be able to mark schema columns as sensitive; values of sensitive columns MUST be fully redacted (replaced with `***`) in all returned result forms (chart data, table data, exports).

**Expanded Integrations (P4)**

- **FR-022**: System MUST support MySQL/MariaDB as a query execution target, including schema discovery, with the same safety guarantees (validation, transaction wrapping, read-only advisory) as existing databases.
- **FR-023**: System MUST offer a direct Anthropic AI provider with the same response contract, token reporting, and error classification as existing providers.
- **FR-023a**: System MUST offer a one-line OpenRouter configuration that reuses the existing OpenAI-compatible provider (custom endpoint plus OpenRouter's recommended attribution headers), giving access to OpenRouter's model catalog without a new provider implementation.
- **FR-024**: System MUST offer a shared-store (Redis-compatible) implementation of the existing cache extension point so multiple application instances share cached results consistently.
- **FR-025**: System MUST offer a shared-store (Redis-compatible) implementation of the existing rate-limiter extension point enforcing per-tenant limits across all instances combined. If the shared store is unavailable, the limiter MUST fail closed (deny) and log the condition.

**Operational Readiness (P4)**

- **FR-026**: System MUST provide health checks reporting AI provider and database reachability, integrable with the host application's standard health system.
- **FR-027**: System MUST emit native metrics — query count, latency, token usage, cache hit rate, error rate, per tenant where applicable — consumable by standard monitoring pipelines.
- **FR-028**: System MUST validate configuration at startup and fail fast with an actionable message when the configuration cannot work (e.g., no AI provider registered, tenant column set without placeholder). Configurations that work today MUST continue to start successfully.
- **FR-029**: System MUST support exporting query results to spreadsheet (Excel) format alongside the existing CSV/JSON exports.

**Smart Query Experience (P5)**

- **FR-030**: System MUST offer an opt-in semantic cache that reuses results for questions with equivalent meaning, scoped strictly per tenant, honoring the same expiry rules as the exact-match cache, with a conservative similarity threshold (wrong reuse is treated as a defect, not a tradeoff).
- **FR-031**: System MUST support paging through large result sets with stable ordering and without additional AI calls per page.
- **FR-032**: System MUST offer a preview (dry-run) mode returning the generated query and its estimated cost (where the backend supports estimation) without executing; approved execution MUST re-apply all safety validation at execution time.

**Stability Guarantee (cross-cutting)**

- **FR-020**: All new mechanisms MUST be additive: existing public types, methods, signatures, and configuration defaults remain valid; existing integrations MUST compile and run unchanged. The only permitted behavior change is the rejection of genuinely dangerous inputs that previously slipped through (documented as security fixes).
- **FR-021**: The full existing automated test suite MUST continue to pass, and every new mechanism MUST ship with its own tests covering both the protective behavior and the no-regression path.

### Key Entities

- **Safety Verdict**: The outcome of validating a generated query — accepted, or rejected with a category (write attempt, tenant violation, injection, oversize, timeout) and a human-readable reason.
- **Tenant Identifier Policy**: The rule set defining what a well-formed tenant identifier looks like (allowed characters, maximum length); customizable per deployment.
- **Audit Record**: One entry per processed question: question text, generated query, tenant identifier, outcome, duration, token usage, timestamp, correlation identifier.
- **Sensitive Column Designation**: A per-column flag in the schema configuration indicating its values must be fully redacted (`***`) in output.
- **Query Preview**: The outcome of a dry-run — generated query text, estimated cost (when supported), and validity status — awaiting caller approval before execution.
- **Result Page**: A bounded slice of a larger result set with stable ordering and a position marker for retrieving the next slice.
- **Resource Limits**: The configured caps — question length (2,000 chars), conversation history (20 turns), result row count (10,000), execution timeout (30s), tenant ID length (128 chars), rate limit — all operator-configurable, with defaults that preserve current behavior for typical workloads.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of a documented corpus of dangerous-query bypass attempts (obfuscated writes, dialect-specific write/execute operations, multi-statement tricks) are rejected; 0 of the documented legitimate-query corpus is rejected.
- **SC-002**: 100% of tenant identifiers containing injection syntax are rejected before query construction; 100% of queries lacking a real tenant filter are rejected when tenant isolation is configured.
- **SC-003**: Zero breaking changes: all existing samples and the full pre-existing test suite build and pass without modification.
- **SC-004**: Under a simulated flood of 100,000 distinct tenant identifiers, rate-limiter memory stays below a fixed bound and request latency remains within 10% of baseline.
- **SC-005**: Endpoint error responses contain no internal detail in 100% of failure scenarios exercised by tests, while each failure remains fully diagnosable server-side via its correlation identifier.
- **SC-006**: With audit enabled, exactly one audit record is produced for 100% of processed questions (success and failure); with audit disabled, throughput is unchanged (within measurement noise).
- **SC-007**: A new adopter can enable all protections (authorization, limits, audit, masking) with fewer than 10 lines of configuration, verified in an updated sample.
- **SC-008**: Two application instances sharing a store enforce exactly one combined per-tenant rate limit and serve each other's cached results (verified by a two-instance test scenario).
- **SC-009**: On a benchmark set of paraphrased question pairs, the semantic cache eliminates at least 30% of AI calls with zero incorrect reuses and zero cross-tenant reuses.
- **SC-010**: 100% of tested invalid configurations fail at application startup with a message naming the problem; 100% of currently-valid configurations continue to start unchanged.
- **SC-011**: MySQL/MariaDB and the direct Anthropic provider pass the same end-to-end scenario suite as existing executors/providers.

## Assumptions

- "No breaking changes" means: public API surface, configuration defaults, and observable behavior for legitimate inputs are preserved. Rejecting previously-accepted *dangerous* inputs is considered a security fix, not a breaking change, and will be documented in the changelog as such.
- The library remains the enforcement point; it does not assume database accounts are read-only, but continues to recommend read-only accounts as defense in depth.
- Shared-store (Redis-compatible) cache and rate limiting are in scope (FR-024/FR-025); external audit stores remain extension points only. Integrations that would pull heavy new dependencies into the core package may ship as optional companion packages so existing users' dependency footprint is unchanged (no-break guarantee).
- Semantic caching requires a text-similarity capability; it is opt-in precisely because it may need an additional model/dependency, and its absence changes nothing for existing users.
- Prompt-injection screening is best-effort by nature; the hard guarantee remains output-side validation (FR-001–FR-007), which is why injection screening is P2 while validation is P1.
- The version bump for this release is a minor version (additive), consistent with the no-break guarantee.
- Default limits (question 2,000 chars; history 20 turns; 10,000 rows; 30s timeout; tenant ID 128 chars) are generous enough that no realistic legitimate workload changes behavior on upgrade; every limit is operator-configurable.
