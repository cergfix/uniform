# Test Coverage Report

**Date:** 2026-03-02
**Codebase:** 9,036 lines of Rust across 56 source files
**Tests:** 0
**Coverage:** 0%

---

## Current State

The codebase has **zero tests**. All 56 source files contain only production code with no `#[cfg(test)]` modules or `#[test]` functions.

```
running 0 tests  (lib)
running 0 tests  (bin)
running 0 tests  (doc-tests)
```

---

## Module Inventory

### Unit-Testable Modules (pure logic, no I/O or global state)

These modules are self-contained and can be tested with simple `#[cfg(test)]` modules -- no tokio runtime, no global state setup required.

| Module | Lines | Public Functions | Description | Priority |
|--------|-------|-----------------|-------------|----------|
| `types/value.rs` | 207 | 18 | Value enum: accessors, coercions, Ord, Hash, Display, From impls | **HIGH** |
| `types/row.rs` | 162 | 13 | Row/OwnedRow: filter_keys, patch, to_json, get accessors | **HIGH** |
| `types/query_response.rs` | 96 | 7 | QueryResponse constructors (ok_bool, ok_string, err, etc.) | MEDIUM |
| `store/table.rs` | 241 | 9 | Table: insert, pop_one, remove_claimed_rows, add_index, CAS claiming | **HIGH** |
| `store/index.rs` | 139 | 12 | SecondaryIndex: insert, remove, find_eq, find_range, find_gt/lt/gte/lte | **HIGH** |
| `store/proc.rs` | 170 | 3 | ProcType::from_str/as_str, Proc::new | LOW |
| `store/worker.rs` | 127 | 2 | WorkerClass::from_str/as_str | LOW |
| `query/command.rs` | 524 | 3 | parse_command (prefix dispatch), strip_metadata, strip_comments | **HIGH** |
| `query/condition.rs` | 191 | 1 | row_meets_conditions_simple (WHERE evaluator: =, !=, <, >, LIKE, AND, OR, IN, IS NULL) | **HIGH** |
| `query/planner.rs` | 188 | 2 | plan_query (index vs full scan), has_residual | MEDIUM |
| `crypto/aes_cbc.rs` | 62 | 2 | AES-256-CBC encrypt/decrypt with PKCS7 padding | **HIGH** |
| `compression/gzip.rs` | 56 | 4 | gzip_encode/decode, gzip_field/gunzip_field | **HIGH** |
| `util/escape.rs` | 50 | 2 | mysql_escape, mysql_unescape | MEDIUM |
| `util/template.rs` | 55 | 2 | build_string_template, build_url_encoded_template (${field} interpolation) | MEDIUM |
| `util/logging.rs` | 2 of 8 | 2 | get_log_level_from_string, is_valid_log_level_string (pure mappers) | LOW |
| `config/vars.rs` | 2 of 10 | 2 | version(), client() (compile-time constants) | LOW |
| `store/registry.rs` | 2 of 9 | 2 | ServerProtocol::from_str/as_str | LOW |

**Total unit-testable surface:** ~80 public functions across ~2,200 lines

### Integration-Testable Modules (require global state and/or tokio runtime)

These modules interact with global registries (`TABLES`, `SERVERS`, `WORKERS`, `PROCS`) or perform network I/O. They need `#[tokio::test]`, global state setup/teardown, or mock infrastructure.

| Module | Lines | Public Functions | Description | Priority |
|--------|-------|-----------------|-------------|----------|
| `query/engine.rs` | 341 | 1 | run_query dispatch (the central entry point) | **HIGH** |
| `query/select.rs` | 343 | 1 | sql_select (parse, plan, pop/peek, pipeline) | **HIGH** |
| `query/insert.rs` | 199 | 2 | sql_insert, insert_json_string | **HIGH** |
| `query/insert_pipeline.rs` | 844 | 5 | Full 13-stage insert pipeline + 11-stage select pipeline | **HIGH** |
| `query/create.rs` | 476 | 5 | CREATE TABLE/INDEX/SERVER/PROC, START WORKER | MEDIUM |
| `query/alter.rs` | 184 | 3 | ALTER TABLE/SERVER/PROC | MEDIUM |
| `query/drop.rs` | 130 | 6 | DROP TABLE/INDEX/SERVER/PROC/WORKER/CONNECTION | MEDIUM |
| `query/show.rs` | 297 | 1 | SHOW (tables, servers, workers, procs, variables, indexes) | MEDIUM |
| `query/set.rs` | 56 | 1 | SET (LOG_LEVEL, LOG_DEST, ROLE, SELECT_CACHE) | LOW |
| `query/cache.rs` | 72 | 4 | SELECT query cache (enable, disable, get, set) | LOW |
| `store/long_poll.rs` | 73 | 6 | LongPollClient: try_claim, is_active, try_deliver | MEDIUM |
| `store/registry.rs` | 7 of 9 | 4 | get_table, get_server, get_worker, get_proc_by_name | MEDIUM |
| `util/logging.rs` | 6 of 8 | 6 | set_log_level, get_log_level, log, set_log_dest | LOW |
| `util/cluster.rs` | 60 | 2 | set_cluster_role, get_cluster_role | LOW |
| `util/metrics.rs` | 45 | 2 | publish_server/worker_latency_probe | LOW |
| `config/vars.rs` | 8 of 10 | 8 | Global atomic get/set for feature flags | LOW |

**Total integration-testable surface:** ~50 public functions across ~3,200 lines

### Network/Protocol Modules (require TCP connections or external services)

| Module | Lines | Description | Priority |
|--------|-------|-------------|----------|
| `server/redis.rs` | 175 | Redis RESP protocol handler | MEDIUM |
| `server/mysql/protocol.rs` | 171 | MySQL wire protocol (handshake, packets) | MEDIUM |
| `server/mysql/handler.rs` | 134 | MySQL COM_QUERY dispatch | MEDIUM |
| `server/mysql/resultset.rs` | 94 | MySQL result set encoding | MEDIUM |
| `server/http.rs` | 402 | HTTP/1.1 request parsing + response building | MEDIUM |
| `server/fastcgi.rs` | 448 | FastCGI wire protocol (records, params, streams) | MEDIUM |
| `server/listener.rs` | 98 | TCP accept loop, connection cleanup | LOW |
| `server/connection.rs` | 84 | Connection struct | LOW |

**Total:** ~1,600 lines

### Worker Modules (require external services for full testing)

| Module | Lines | Description | Priority |
|--------|-------|-------------|----------|
| `worker/base.rs` | 85 | WorkerLoop trait, frequency/burst/terminator | MEDIUM |
| `worker/exec_query.rs` | 60 | EXEC_QUERY (template + run_query) | LOW |
| `worker/buffer_appl.rs` | 55 | BUFFER_APPL (async channel reader) | LOW |
| `worker/push_to_stdout.rs` | 33 | JSON to stdout | LOW |
| `worker/push_to_stderr.rs` | 33 | JSON to stderr | LOW |
| `worker/push_to_file.rs` | 131 | File write + gzip + AES | LOW |
| `worker/push_to_email.rs` | 167 | SMTP via lettre | LOW |
| `worker/push_to_slack.rs` | 124 | Slack API HTTP | LOW |
| `worker/push_to_pagerduty.rs` | 164 | PagerDuty Events API | LOW |
| `worker/push_to_elasticsearch.rs` | 119 | Elasticsearch PUT | LOW |
| `worker/push_to_cloud_storage.rs` | 180 | S3 upload | LOW |
| `worker/push_to_fastcgi.rs` | 75 | FastCGI client | LOW |
| `worker/mysql_data_transfer.rs` | 155 | MySQL bidirectional sync | LOW |

**Total:** ~1,381 lines

---

## Recommended Test Plan

### Phase 1: Unit Tests (highest ROI, no infrastructure needed)

Target: ~80 functions, ~2,200 lines. Estimated: **~150 test cases.**

| # | Module | Test Cases | What to Test |
|---|--------|-----------|--------------|
| 1 | `types/value.rs` | ~25 | Accessors (as_str, as_f64, as_bool, is_null), to_string_repr (integers vs floats), to_f64 coercions, Ord (Null < Bool < Number < String, within-type ordering), Hash consistency, Display, From conversions (str, String, f64, i64, i32, bool, serde_json::Value) |
| 2 | `types/row.rs` | ~15 | OwnedRow::new (auto Fb_id/Fb_created), filter_keys (keep/remove), patch (merge), to_json (roundtrip), get/get_str/get_f64, copy_row (deep clone independence), Row CAS claiming (try_claim race) |
| 3 | `store/table.rs` | ~20 | insert (basic, capacity enforcement, unique index violation), pop_one FIFO/LIFO ordering, CAS contention (two threads competing), remove_claimed_rows cleanup, len/is_empty, add_index (indexes existing rows), capacity_str |
| 4 | `store/index.rs` | ~20 | insert/remove, find_eq (exact match), find_range, find_gt/gte/lt/lte, has_live_entry (with mock predicate), empty index queries, multi-value same column, composite key ordering |
| 5 | `query/command.rs` | ~25 | parse_command: SELECT, INSERT, RPUSH, RPOP, CREATE TABLE/SERVER/PROC/INDEX, START WORKER, DROP, ALTER, SHOW, SET, SLEEP, NSLEEP, PING, QUIT, EXIT, SHUTDOWN, VERSION, LLEN, METADATA, unknown commands. strip_metadata extraction. strip_comments removal |
| 6 | `query/condition.rs` | ~20 | Equality (=, !=), comparison (<, <=, >, >=), string LIKE with %, AND/OR logic, IN lists, IS NULL/IS NOT NULL, nested parentheses, column not found (evaluates false), numeric vs string comparison, mixed types |
| 7 | `crypto/aes_cbc.rs` | ~5 | encrypt->decrypt roundtrip, different key lengths, empty plaintext, known test vector, invalid base64 input to decrypt |
| 8 | `compression/gzip.rs` | ~5 | gzip_field->gunzip_field roundtrip, empty string, large payload, invalid base64 to gunzip, binary safety |
| 9 | `util/escape.rs` | ~5 | Escape special chars (\0, \n, \r, \\, \', \", \Z), unescape roundtrip, empty string, no-special-chars passthrough |
| 10 | `util/template.rs` | ~5 | Single substitution, multiple substitutions, missing key (placeholder left), URL encoding, nested ${} |
| 11 | `query/planner.rs` | ~5 | FullScan (no WHERE), FullScan (WHERE on non-indexed column), IndexScan (WHERE on indexed column), prefer unique index, has_residual detection |
| 12 | `types/query_response.rs` | ~5 | ok_bool, ok_string, ok_number, ok_result, err constructors |

### Phase 2: Integration Tests (require global state setup)

Target: ~50 functions. Estimated: **~60 test cases.**

| # | Module | Test Cases | What to Test |
|---|--------|-----------|--------------|
| 1 | `query/engine.rs` | ~15 | End-to-end: CREATE TABLE -> INSERT -> SELECT -> verify rows. RPUSH/RPOP. SHOW TABLES. DROP TABLE. SET LOG_LEVEL. Error cases (table not found, parse error) |
| 2 | `query/select.rs` | ~10 | SELECT with WHERE (index scan vs full scan), LIMIT/OFFSET, column projection, READ_ONLY mode (non-destructive), empty table, DEFLECT_READ chain |
| 3 | `query/insert.rs` | ~5 | SQL INSERT parsing, JSON RPUSH, unique index enforcement, table full error |
| 4 | `query/insert_pipeline.rs` | ~10 | DEFLECT_WRITE redirect, MIRROR_WRITE copy, WRITE_OFFLINE block, ENCRYPT/DECRYPT roundtrip through pipeline, GZIP/GUNZIP roundtrip, AUTO_REPLY generation, DEFLECT_WRITE_ON_FULL_TABLE |
| 5 | `query/create.rs` | ~8 | CREATE TABLE (default options, custom size), CREATE INDEX, CREATE PROC (various types), error on duplicate name |
| 6 | `query/show.rs` | ~5 | SHOW TABLES (with content), SHOW PROCS, SHOW VARIABLES, LIKE filter, WHERE filter |
| 7 | `query/drop.rs` | ~5 | DROP TABLE, DROP INDEX, DROP PROC, drop non-existent (error) |
| 8 | `store/long_poll.rs` | ~5 | LongPollClient: try_claim CAS, is_expired, try_deliver sends via oneshot, double-claim fails |

### Phase 3: Protocol & Network Tests (require TCP or mock streams)

Estimated: **~30 test cases.**

| # | Module | Test Cases | What to Test |
|---|--------|-----------|--------------|
| 1 | `server/redis.rs` | ~8 | RESP parsing (inline, array), RPUSH/RPOP roundtrip, PING/PONG, multi-bulk commands, error responses |
| 2 | `server/mysql/protocol.rs` | ~8 | Packet encoding/decoding, length-encoded integers, handshake sequence, COM_QUERY routing |
| 3 | `server/http.rs` | ~6 | HTTP request parsing, CGI variable extraction, base64 body encoding, response building, gzip response |
| 4 | `server/fastcgi.rs` | ~6 | FCGI record parsing, name-value pair encoding, BEGIN_REQUEST/PARAMS/STDIN sequence, STDOUT response chunking |
| 5 | `server/mysql/resultset.rs` | ~4 | Result set encoding, field definitions, EOF markers |

### Phase 4: Worker Tests (mock external services or test against real ones)

Estimated: **~20 test cases.**

| # | Module | Test Cases | What to Test |
|---|--------|-----------|--------------|
| 1 | `worker/base.rs` | ~3 | WorkerLoop trait: frequency timing, burst count, terminator signal |
| 2 | `worker/exec_query.rs` | ~2 | Template substitution + query execution |
| 3 | `worker/push_to_file.rs` | ~3 | File write, gzip output, verify file contents |
| 4 | `worker/push_to_stdout.rs` | ~2 | JSON output capture |
| 5 | `worker/mysql_data_transfer.rs` | ~2 | build_insert_query (pure function), TransferMode::from_str |
| 6 | Other workers | ~8 | Fallback table insertion on error, row serialization |

---

## Coverage Targets

| Phase | Test Cases | Lines Covered | Estimated Coverage |
|-------|-----------|---------------|-------------------|
| Phase 1: Unit tests | ~150 | ~2,200 | ~24% |
| Phase 2: Integration tests | ~60 | ~3,200 | ~60% (cumulative) |
| Phase 3: Protocol tests | ~30 | ~1,600 | ~78% (cumulative) |
| Phase 4: Worker tests | ~20 | ~1,380 | ~90% (cumulative) |
| **Total** | **~260** | **~8,380 / 9,036** | **~90%** |

Remaining ~10% uncovered would be: main.rs entry point, mod.rs re-exports, error branches in network code difficult to trigger, and external service integrations (S3, Slack, PagerDuty, Elasticsearch, SMTP) that need live services or mocks.

---

## How to Run Tests

```bash
# Run all tests
cargo test

# Run tests for a specific module
cargo test --lib types::value
cargo test --lib store::table

# Run with output (see println! in tests)
cargo test -- --nocapture

# Run a single test by name
cargo test test_value_ordering
```

## How to Measure Coverage

Using [cargo-tarpaulin](https://github.com/xd009642/tarpaulin) (Linux only):

```bash
cargo install cargo-tarpaulin
cargo tarpaulin --out html
# Open tarpaulin-report.html
```

Using [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) (cross-platform):

```bash
cargo install cargo-llvm-cov
cargo llvm-cov --html
# Open target/llvm-cov/html/index.html
```
