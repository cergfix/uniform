# Uniform Architecture

Uniform is an async proxy/broker server and in-memory queue database written in Rust.
It wraps well-known protocols (MySQL, Redis, HTTP, FastCGI) around a custom in-memory
data structure optimized for queue semantics.

**Core principle:** There are no reads. Every SELECT is an atomic DELETE (pop).
Messages are consumed exactly once. Uniform is designed to hold a low amount of data
under normal operation -- it's not a persistent data store, but a high-throughput
message queue with SQL queryability.

**All configuration is runtime SQL-like commands** piped through STDIN or network
connections. Without configuration Uniform is an empty process that does nothing.

---

## Four Pillars

```
 SERVERS (protocol frontends)
    MySQL | Redis | HTTP | FastCGI
              |
              v
    QUERY ENGINE (SQL-like commands)
              |
              v
    TABLES (in-memory queues with indexes)
     /                  \
    v                    v
  PROCS                WORKERS
  (read/write          (background tasks
   triggers)            pushing to external systems)
```

### 1. Tables

In-memory queues with secondary indexes and per-row atomic operations.

Key behaviors:
- **SELECT = atomic pop** (delete + return). Messages consumed exactly once.
- Fixed capacity (default 1000). Writes fail when full unless handled by `DEFLECT_WRITE_ON_FULL_TABLE`.
- All fields are queryable via secondary indexes or full scan.
- `LIMIT offset,count` means: start scanning at element `offset`, return up to `count` matches.
- Optional front buffer for delayed/time-dilated writes (requires `BUFFER_APPL` worker).

Reserved columns:
- `u_id` -- Auto-generated UUID for each row
- `u_created_at` -- Auto-generated creation timestamp (RFC 3339, millisecond precision)
- `u_body` -- Reserved for HTTP/FastCGI body transfers (always base64 encoded)
- `u_reply_id` -- Used for request-response correlation

Table options:

| Option | Type | Default | Dynamic | Description |
|--------|------|---------|---------|-------------|
| Size | int | 1000 | no | Maximum table capacity (>0) |
| ForceLimit | int | -1 | yes | Force limit on all SELECT queries (-1 = disabled) |
| ForceOffset | int | -1 | yes | Force scan starting position (-1 = disabled) |
| MaxScanTimeMs | int | 0 | yes | Maximum scan time in milliseconds (0 = unlimited) |
| LongPollMaxClients | int | 200 | no | Maximum concurrent long-polling clients |
| BufferSize | int | 0 | no | Front buffer size (0 = disabled) |
| BufferLongPollMs | int | 2000 | yes | Long-poll timeout when reading buffer directly via `@tablename` |
| Silent | bool | false | yes | Suppress all table-level log messages |

#### Indexes

Secondary indexes for accelerated lookups:

```sql
CREATE INDEX idx1 ({"Table":"my_table", "Key":"status", "Unique":false});
CREATE INDEX idx2 ({"Table":"my_table", "Key":"order_id", "Unique":true});
```

When a `SELECT` query has a `WHERE` clause matching an indexed column, the query planner automatically uses the index for O(log n) lookup instead of a full table scan.

### 2. Servers

Protocol frontends that accept client connections and route commands to the query engine. All servers share the same SQL-like command parser.

Common server options:

| Option | Type | Default | Dynamic | Description |
|--------|------|---------|---------|-------------|
| Bind | string | "127.0.0.1:6400" | no | IP:port to listen on |
| TimeoutMs | int | 60000 | yes | Connection timeout (0 = no timeout) |
| BufferSize | int | 65535 | no | Network buffer size |
| ForceLimit | int | -1 | yes | Force SELECT limit |
| ForceOffset | int | -1 | yes | Force SELECT offset |
| MaxScanTimeMs | int | 0 | yes | Max scan time per query |
| ForceSelectSleepMs | int | 0 | yes | Sleep after each SELECT |
| LatencyTargetMs | float | -1 | yes | Latency warning threshold |

#### MySQL Server

Implements the MySQL binary wire protocol. Compatible with standard MySQL clients and drivers.

```sql
CREATE SERVER MYSQL m1 ({"Bind":"0.0.0.0:6400"});
```

Supports: `COM_QUERY`, `COM_PING`, `COM_QUIT`, `COM_INIT_DB`. Full result set encoding with column metadata.

#### Redis Server

Implements the Redis RESP protocol. Compatible with `redis-cli` and Redis client libraries.

```sql
CREATE SERVER REDIS r1 ({"Bind":"0.0.0.0:6379"});
```

Supports: `RPUSH`/`LPUSH`, `RPOP`/`LPOP`, `LLEN`, `PING`, plus all Uniform SQL commands.

#### HTTP Server

Raw HTTP/1.1 server that converts requests into table inserts. Request headers and CGI variables become row columns. Request body is base64-encoded as `u_body`. Response is built from the result row's columns (headers with underscores converted to dashes), `Status` (HTTP status code), and `u_body` (base64-decoded response body).

```sql
CREATE SERVER HTTP h1 ({"Bind":"0.0.0.0:8080", "Table":"requests"});
```

Additional options: `Table` (required), `HttpsRedirect`, `HttpsRedirectHeader`, `HttpsRedirectOn`, `Gzip`, `Robots`, `DocumentRoot`, `ScriptFilename`.

#### FastCGI Server

Implements the FastCGI wire protocol. Compatible with nginx, Apache, and other web servers.

```sql
CREATE SERVER FASTCGI f1 ({"Bind":"0.0.0.0:9000", "Table":"requests"});
```

Same `Table` option as HTTP. Request parameters and body handling work identically.

### 3. Procs

Procs (Programmable Random Occurrences) are triggers attached to table read/write operations. Tables + Procs form a directed graph where tables are nodes and procs are edges. Loops are allowed.

Common proc options:

| Option | Type | Description |
|--------|------|-------------|
| Src | string | Source table name |
| Dest | string | Destination table name |
| CaseQuery | string | Conditional query (e.g. `SELECT * FROM $record WHERE status = 'active'`) |
| Patch | string | JSON patch to apply to rows (e.g. `{"tag":"processed"}`) |
| Enabled | bool | Enable/disable the proc (default: true) |
| StartTime | string | Time window start (e.g. `09:00`) |
| EndTime | string | Time window end (e.g. `17:00`) |
| PostWaitMs | int | Milliseconds to wait after proc execution |

#### CaseQuery Virtual Tables

Proc conditions can query against:
- `$record` -- The current record being read/written
- `$metadata` -- The current session metadata
- `$connection` -- The current connection info
- Any real table name (cross-table queries)

#### Write-Side Procs

| Type | Description |
|------|-------------|
| DEFLECT_WRITE | Redirect a write to a different table (move, not copy) |
| BUFFER_CATCH | Catch writes into the table's front buffer |
| WRITE_OFFLINE | Block all writes to a table |
| MIRROR_WRITE | Copy a write to an additional table (original still happens) |
| GUNZIP_WRITE | Decompress (gunzip) specified fields on write |
| GZIP_WRITE | Compress (gzip) specified fields on write |
| DECRYPT_WRITE | Decrypt specified fields on write (AES-256-CBC) |
| ENCRYPT_WRITE | Encrypt specified fields on write (AES-256-CBC) |
| AUTO_REPLY | Generate a reply record in a destination table on write |
| READ_ONLY | Prevent records from being consumed (popped) by SELECT |
| DEFLECT_WRITE_ON_FULL_TABLE | Redirect writes to another table when source is full |

#### Read-Side Procs

| Type | Description |
|------|-------------|
| DEFLECT_READ | Redirect a read to a different table (follows chains) |
| READ_OFFLINE | Block all reads from a table |
| PING_OFFLINE | Block PING responses (for load balancer health checks) |
| LONG_POLL | If SELECT returns empty, block and wait for new data (`WaitMs` option) |
| DEFLECT_READ_ON_EMPTY_RESPONSE | If SELECT returns empty, try reading from a fallback table |
| MIRROR_ON_READ | Copy consumed (popped) records to another table |
| READ_REDUCE | Deduplicate result rows by key (`ReduceKey`, `ReduceToLatest`) |
| GUNZIP_ON_READ | Decompress specified fields on read |
| GZIP_ON_READ | Compress specified fields on read |
| DECRYPT_ON_READ | Decrypt specified fields on read (AES-256-CBC) |
| ENCRYPT_ON_READ | Encrypt specified fields on read (AES-256-CBC) |

#### FRONTEND_IO_MIX

Combines write and read in a single operation. Used with HTTP/FastCGI servers for request-response patterns: the incoming request is inserted, then the response is read from a different table (with long-polling).

```sql
CREATE PROC FRONTEND_IO_MIX fio1 ({"Src":"requests", "Dest":"responses"});
```

#### Proc Execution Order

**Write pipeline** (INSERT/RPUSH):
1. DEFLECT_WRITE
2. BUFFER_CATCH
3. WRITE_OFFLINE
4. MIRROR_WRITE
5. GUNZIP_WRITE
6. GZIP_WRITE
7. DECRYPT_WRITE
8. ENCRYPT_WRITE
9. AUTO_REPLY
10. READ_ONLY (check)
11. Long-poll delivery
12. Table insert
13. DEFLECT_WRITE_ON_FULL_TABLE (on failure)

**Read pipeline** (SELECT/RPOP):
1. DEFLECT_READ (chain, pre-select)
2. READ_OFFLINE (check)
3. READ_ONLY (non-destructive mode)
4. Table scan + pop
5. READ_REDUCE
6. DEFLECT_READ_ON_EMPTY_RESPONSE
7. MIRROR_ON_READ
8. GUNZIP_ON_READ
9. GZIP_ON_READ
10. DECRYPT_ON_READ
11. ENCRYPT_ON_READ

### 4. Workers

Background tasks that consume records from tables and push them to external destinations.

Common worker options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| Table | string | required | Source table to consume from |
| Query | string | "" | SELECT query (default: `SELECT * FROM <table> LIMIT 0,1`) |
| DestTable | string | "" | Fallback table on failure |
| FrequencyMs | int | 1000 | Poll interval in milliseconds |
| Burst | int | 1 | Records per run |
| MaxRuns | float | -1 | Maximum runs (-1 = unlimited) |
| StartTime | string | "" | Daily start time (e.g. `09:00`) |
| EndTime | string | "" | Daily end time (e.g. `17:00`) |
| LatencyTargetMs | float | -1 | Latency warning threshold |

#### Worker Classes

**EXEC_QUERY** -- Execute a SQL query for each consumed record. Supports `${field}` template substitution.
```sql
START WORKER EXEC_QUERY w1 ({
    "Table":"events",
    "ExecQuery":"INSERT INTO processed (id, status) VALUES('${u_id}', 'done')"
});
```

**BUFFER_APPL** -- Apply buffered writes from a table's front buffer. Required when using `BUFFER_CATCH` procs.

**PUSH_TO_STDOUT / PUSH_TO_STDERR** -- Print consumed records as JSON to stdout/stderr.

**PUSH_TO_FILE** -- Write consumed records to local files. Options: `FilePath`, `GzipEnabled`, `EncryptEnabled`, `EncryptKey`.

**PUSH_TO_EMAIL** -- Send consumed records as email via SMTP. Options: `SmtpHost`, `SmtpPort`, `SmtpUser`, `SmtpPass`, `From`, `To`, `Cc`, `Subject`.

**PUSH_TO_SLACK** -- Post consumed records to Slack via API. Options: `Token`, `Channel`, `Text` (template), `TextOnly`.

**PUSH_TO_PAGERDUTY** -- Send alerts to PagerDuty Events API v2. Options: `RoutingKey`, `Source`, `Severity`, `Link`, `LinkText`.

**PUSH_TO_ELASTICSEARCH** -- Index documents in Elasticsearch via HTTP PUT. Options: `Host`, `EsIndex`, `EsType`. Documents indexed with `u_id` as document ID.

**PUSH_TO_CLOUD_STORAGE** -- Upload to S3-compatible storage. Options: `Endpoint`, `Bucket`, `Region`, `AccessKey`, `SecretKey`, `GzipEnabled`, `EncryptEnabled`, `EncryptKey`. Path: `YYYY/MM/DD/<worker>-<u_id>.json`.

**PUSH_TO_FASTCGI** -- Send consumed records to a FastCGI backend. Options: `Host`, `Port`, `ScriptFilename`.

**MYSQL_DATA_TRANSFER** -- Bidirectional data transfer with external MySQL. 6 modes:

| Mode | Description |
|------|-------------|
| 1_WAY_PUSH | Push records to remote MySQL |
| 1_WAY_PULL | Pull and execute locally |
| 2_WAY_PUSH | Bidirectional with push priority |
| 2_WAY_PULL | Bidirectional with pull priority |
| IO_MIX_PUSH | Mixed I/O with push |
| IO_MIX_PULL | Mixed I/O with pull |

---

## Full Command Reference

### Table Commands

| Command | Description |
|---------|-------------|
| `CREATE TABLE <name> ({options})` | Create a new table |
| `DROP TABLE <name>` | Delete a table |
| `ALTER TABLE <name> ({"Key":"...", "Value":"..."})` | Modify table options |
| `SHOW TABLES` | List all tables |
| `CREATE INDEX <name> ({options})` | Create a secondary index |
| `DROP INDEX <name>` | Delete an index |
| `SHOW INDEXES` | List all indexes |

### Server Commands

| Command | Description |
|---------|-------------|
| `CREATE SERVER <protocol> <name> ({options})` | Create a server (REDIS/MYSQL/HTTP/FASTCGI) |
| `DROP SERVER <name>` | Stop and delete a server |
| `ALTER SERVER <name> ({"Key":"...", "Value":"..."})` | Modify server options |
| `SHOW SERVERS` | List all servers |
| `SHOW CONNECTIONS` | List active connections |
| `DROP CONNECTION <addr>` | Terminate a connection |

### Proc Commands

| Command | Description |
|---------|-------------|
| `CREATE PROC <type> <name> ({options})` | Create a proc |
| `DROP PROC <name>` | Delete a proc |
| `ALTER PROC <name> ({"Key":"...", "Value":"..."})` | Modify proc options |
| `SHOW PROCS` | List all procs |

### Worker Commands

| Command | Description |
|---------|-------------|
| `START WORKER <class> <name> ({options})` | Start a worker |
| `DROP WORKER <name>` | Stop a worker |
| `SHOW WORKERS` | List all workers |

### Data Commands

| Command | Description |
|---------|-------------|
| `INSERT INTO <table> (cols) VALUES(vals)` | SQL insert |
| `RPUSH <table> '<json>'` | JSON insert (Redis-style) |
| `SELECT cols FROM <table> [WHERE ...] [LIMIT o,n]` | Destructive pop with optional filter |
| `RPOP <table>` | Pop one record (Redis-style) |
| `LLEN <table>` | Get table row count |

### System Commands

| Command | Description |
|---------|-------------|
| `SET LOG_LEVEL '<level>'` | Set log level (debug/error/crit) |
| `SET LOG_DEST '<table>'` | Route log messages to a table |
| `SET SELECT_CACHE '<on/off>'` | Enable/disable SELECT query cache |
| `SET ROLE '<role>'` | Set cluster role identifier |
| `SHOW VARIABLES` | Show system variables |
| `SHOW METADATA` | Show current session metadata |
| `/* METADATA ({json}); */` | Set session metadata |
| `SLEEP <ms>` | Sleep for milliseconds |
| `NSLEEP <ns>` | Sleep for nanoseconds |
| `PING` | Health check |
| `VERSION` | Show version info |
| `SHUTDOWN` | Graceful shutdown |
| `QUIT` / `EXIT` | Close current connection |

---

## Custom SQL Functions

| Function | Operators | Description |
|----------|-----------|-------------|
| `RAND(start, end)` | `<`, `>`, `<=`, `>=`, `=`, `!=` | Random number generation and comparison |
| `SLEEP(ms)` | `>` | Sleep inside a query (always evaluates to true) |
| `RECORD_AGE_ABS()` | `<`, `>`, `<=`, `>=`, `=`, `!=` | Compare record creation time against absolute timestamp |
| `RECORD_AGE_REL_MS()` | `<`, `>`, `<=`, `>=`, `=`, `!=` | Compare record age in milliseconds since creation |

Examples:
```sql
SELECT * FROM events WHERE RECORD_AGE_REL_MS() > 5000;
SELECT * FROM events WHERE RAND(1, 100) <= 10;
SELECT * FROM events WHERE RECORD_AGE_ABS() > '2024-01-01 00:00:00';
```

---

## Session Metadata

Each connection/session can carry metadata used by Procs and Workers. Metadata is set as a SQL comment to allow combining with other commands:

```sql
/* METADATA ({"env":"production","team":"backend"}); */
/* METADATA ({"role":"admin"}); */ SELECT * FROM requests;
SHOW METADATA;
```

Workers save the creator session's metadata at creation time and use it during execution.

When using `mysql-client`, enable comment sending: `mysql -h 127.0.0.1 -P 6400 --comments`

---

## Query Cache

SELECT queries can be cached:

```sql
SET SELECT_CACHE "on";
SET SELECT_CACHE "off";
```

The cache has no size limit, as the variation of SELECT queries is typically finite.

---

## Binary Transfers

When using HTTP/FastCGI servers or FastCGI workers, the `u_body` field is reserved for body transfers. `u_body` should always be **base64 encoded** to handle binary data safely.

---

## HTTP Header Conversion

Internal message fields use underscores (`u_body`, `Content_Type`). When sent over HTTP or FastCGI, underscores are converted to dashes (`u-body`, `Content-Type`). Incoming HTTP headers are converted from dashes to underscores.

---

## Concurrency Model

| Component | Implementation |
|---|---|
| Primary storage | `crossbeam_skiplist::SkipMap<u64, Row>` -- lock-free, ordered by seq_id |
| Secondary indexes | `crossbeam_skiplist::SkipMap<(Value, u64), ()>` -- lock-free composite key |
| Row claiming (SELECT pop) | Per-row `AtomicBool` CAS (`false` -> `true`) -- exactly-once, no table lock |
| Global registries | `DashMap<String, Arc<T>>` -- shard-level locks |
| Proc list | `parking_lot::RwLock<Vec<Proc>>` -- concurrent reads |
| Connections | `tokio::spawn` per connection |
| Workers | `tokio::spawn` per worker |
| Long-poll | `tokio::sync::oneshot` per waiting client |
| Worker termination | `tokio::sync::watch` (multi-receiver) |
| Table buffers | `tokio::sync::mpsc` bounded channel |

---

## Monitoring and Logging

### Log Levels

| Level | Description |
|-------|-------------|
| `debug` | Maximum verbosity (not recommended for production) |
| `error` | Errors and notifications |
| `crit` | Only critical and important system messages |

```sql
SET LOG_LEVEL "debug";
SET LOG_DEST "my_log_table";
```

Only `error` and `crit` messages can be routed to a table. `debug` messages only go to STDOUT.

---

## Deployment

### Daemon Mode

```bash
sudo service uniform start
```

The recommendation is to put playbooks into `/etc/uniform/` with the `.uniform` extension:

```bash
cat /etc/uniform/*.uniform | uniform
```

### Graceful Shutdown

On SIGINT/SIGTERM: HTTP/FastCGI servers stop accepting connections first, then Redis/MySQL servers shut down. In-flight requests complete.

---

## Replication

No built-in replication. Replication-like patterns use:
- `MIRROR_WRITE` proc with `MYSQL_DATA_TRANSFER` worker
- `PUSH_TO_CLOUD_STORAGE` worker for cold backup

## Clustering

No built-in clustering. Use individual nodes behind a load balancer (e.g. HAProxy).

---

## Project Structure

```
uniform/
  Cargo.toml
  src/
    main.rs                       # Entry point, banner, signals, stdin REPL
    lib.rs                        # Re-exports

    config/
      vars.rs                     # VERSION, CLIENT, APP_NAME constants

    types/
      value.rs                    # Value enum (String, Number, Bool, Null) + Ord
      row.rs                      # Row (SkipMap entry), OwnedRow (cloned data)
      query_response.rs           # QueryResponse, QueryType

    store/
      table.rs                    # Table: SkipMap storage, indexes, push/pop, CAS claiming
      index.rs                    # SecondaryIndex: SkipMap<(Value, u64), ()>
      proc.rs                     # ProcType enum (23 variants), Proc struct
      worker.rs                   # Worker struct, lifecycle management
      long_poll.rs                # LongPollClient, CAS delivery
      registry.rs                 # Global statics: TABLES, WORKERS, SERVERS, PROCS

    query/
      engine.rs                   # run_query() dispatch
      command.rs                  # Command enum + parse_command() prefix dispatcher
      select.rs                   # SELECT/pop with query planning + index usage
      insert.rs                   # INSERT + RPUSH parsing
      insert_pipeline.rs          # Full proc pipeline (13 write stages + 11 read stages)
      condition.rs                # WHERE evaluator (operators, RAND, RECORD_AGE, SLEEP)
      planner.rs                  # Query plan: pick index vs full scan
      show.rs                     # SHOW commands
      create.rs                   # CREATE TABLE/SERVER/PROC/INDEX
      alter.rs                    # ALTER TABLE/SERVER/PROC
      drop.rs                     # DROP TABLE/SERVER/PROC/WORKER/INDEX
      set.rs                      # SET variable handling
      cache.rs                    # SELECT query parse cache (10s TTL)

    server/
      listener.rs                 # TCP accept loop, connection cleanup
      connection.rs               # Connection struct
      redis.rs                    # Redis RESP protocol
      mysql/
        constants.rs              # MySQL protocol constants
        protocol.rs               # Packet read/write, handshake
        handler.rs                # COM_QUERY dispatch
        resultset.rs              # Result set encoding
      http.rs                     # HTTP/1.1 handler
      fastcgi.rs                  # FastCGI wire protocol handler

    worker/
      base.rs                     # WorkerLoop trait
      exec_query.rs               # EXEC_QUERY
      buffer_appl.rs              # BUFFER_APPL
      push_to_stdout.rs           # PUSH_TO_STDOUT
      push_to_stderr.rs           # PUSH_TO_STDERR
      push_to_file.rs             # PUSH_TO_FILE (gzip + AES)
      push_to_email.rs            # PUSH_TO_EMAIL (SMTP via lettre)
      push_to_slack.rs            # PUSH_TO_SLACK (HTTP API)
      push_to_pagerduty.rs        # PUSH_TO_PAGERDUTY (Events API v2)
      push_to_elasticsearch.rs    # PUSH_TO_ELASTICSEARCH (HTTP PUT)
      push_to_cloud_storage.rs    # PUSH_TO_CLOUD_STORAGE (S3)
      push_to_fastcgi.rs          # PUSH_TO_FASTCGI
      mysql_data_transfer.rs      # MYSQL_DATA_TRANSFER (6 modes)

    crypto/
      aes_cbc.rs                  # AES-256-CBC encrypt/decrypt, PKCS7 padding

    compression/
      gzip.rs                     # Gzip/gunzip field-level operations

    util/
      escape.rs                   # MySQL string escape/unescape
      template.rs                 # ${field} string interpolation
      logging.rs                  # Log levels (CRIT/ERROR/DEBUG), log destination
      metrics.rs                  # Latency probes
      cluster.rs                  # Cluster role

  tests/
    integration.py                # 47 end-to-end tests (Redis, MySQL, HTTP, FastCGI)
    docker-compose.test.yml       # Test infrastructure (MySQL, php-fpm, nginx)
    docker/                       # Docker configs for test services
```
