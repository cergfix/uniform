# Uniform

A high-performance in-memory message queue and traffic processing framework, rewritten in Rust.

Uniform wraps well-known protocols (MySQL, Redis, HTTP, FastCGI) around a custom in-memory data structure optimized for queue semantics. It includes configurable processing pipelines (Procs), background workers for external integrations, and full SQL-like command support -- all dynamically configurable at runtime without restarts.

## Table of Contents

- [What is Uniform](#what-is-uniform)
- [Architecture](#architecture)
- [Building](#building)
- [Getting Started](#getting-started)
- [Data Types](#data-types)
- [Tables](#tables)
- [Servers](#servers)
- [Procs](#procs)
- [Workers](#workers)
- [Command Reference](#command-reference)
- [Custom SQL Functions](#custom-sql-functions)
- [Session Metadata](#session-metadata)
- [Query Cache](#query-cache)
- [Binary Transfers](#binary-transfers)
- [Monitoring and Logging](#monitoring-and-logging)
- [Deployment](#deployment)
- [Configuration Examples](#configuration-examples)
- [Replication](#replication)
- [Clustering](#clustering)
- [CI/CD](#cicd)
- [Project Structure](#project-structure)
- [Licensing](#licensing)

---

## What is Uniform

Uniform is an **async proxy / broker server** and framework designed for building data traffic pipelines.

**Core principle:** There are no reads. Every `SELECT` is an atomic `DELETE` (pop). Messages are consumed exactly once. Uniform is designed to hold a low amount of data under normal operation -- it's not designed as a persistent data store, but as a high-throughput message queue with SQL queryability.

All application configuration and programming is done using SQL-like statements passed either through process STDIN or through network sockets. Without configuration, Uniform is an empty process that does nothing.

### Use Cases

- **Infrastructure workerization** -- Convert "clients >> load-balancer >> reply-worker-servers" into "clients >> collector-load-balancer << pull-worker-servers" (near drop-in for PHP web applications)
- **Production-to-data-warehouse one-way pipelines** -- Especially for mixed Cloud / Bare metal with data buffering for unstable long-range links
- **Traffic emulation and simulation** -- Copy real-time traffic or prerecord for later offline replay
- **Disaster recovery** -- Circuit breakers and secondary backup paths for traffic serving
- **Security / Cyber security** -- Live traffic filtering and isolation of traffic segments
- **Cloud storage backup** -- Backup pipelines to Google Storage / Amazon S3

---

## Architecture

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

### Four Pillars

1. **Tables** -- In-memory queues with secondary indexes and per-row atomic operations. All fields are queryable. SELECT is a destructive pop (atomic delete + return). Supports configurable queue ordering (FIFO/LIFO), capacity limits, front buffers, and long-polling.

2. **Servers** -- Protocol frontends (MySQL, Redis, HTTP, FastCGI) that accept client connections and route commands to the query engine. All servers share the same SQL-like command parser.

3. **Procs** -- Triggers attached to table read/write operations for message routing, transformation, compression, encryption, and access control. Tables + Procs form a directed graph. Loops are allowed.

4. **Workers** -- Background tasks that consume from tables and push data to external destinations (Elasticsearch, Slack, PagerDuty, email, files, S3, external MySQL, FastCGI backends).

### Concurrency Model (Rust)

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

## Building

### Prerequisites

- Rust 1.75+ (with `cargo`)

### Build

```bash
cd uniform
cargo build --release
```

The release binary is produced at `target/release/uniform`.

### Debug Build

```bash
cargo build
```

---

## Getting Started

### Inline Mode

Pipe SQL-like commands through STDIN:

```bash
echo "
SET LOG_LEVEL 'debug';
CREATE TABLE test_table ({'Size':3000});
INSERT INTO test_table (a) VALUES(1);
SELECT * FROM test_table;
SLEEP 5000;
SHUTDOWN;
" | ./target/release/uniform
```

### With Network Server

```bash
echo "
CREATE TABLE requests ({'Size':5000});
CREATE SERVER REDIS r1 ({'Bind':'0.0.0.0:6379'});
CREATE SERVER MYSQL m1 ({'Bind':'0.0.0.0:6400'});
" | ./target/release/uniform
```

Then connect with standard clients:

```bash
# Redis
redis-cli -p 6379
> RPUSH requests '{"url":"https://example.com","method":"GET"}'
> RPOP requests

# MySQL
mysql -h 127.0.0.1 -P 6400
> INSERT INTO requests (url, method) VALUES('https://example.com', 'GET');
> SELECT * FROM requests;
```

### Uniform Playbooks

Configuration files use `.uniform` extension and contain SQL-like statements:

```bash
cat config.uniform | ./target/release/uniform
```

Playbooks can be fetched from remote locations:

```bash
curl -s https://example.com/config.uniform | ./target/release/uniform
```

Or combined from multiple sources:

```bash
echo "$(cat local.uniform ; curl -s https://example.com/remote.uniform ; php warmup.php)" | ./target/release/uniform
```

### Using PHP for Dynamic Configuration

```php
<?php
echo "SET LOG_LEVEL 'debug';\n";
echo "CREATE TABLE test_table ({'Size':3000});\n";
for ($i = 0; $i < 100; $i++) {
    echo "INSERT INTO test_table (a) VALUES($i);\n";
}
echo "SELECT * FROM test_table;\n";
```

```bash
php init.php | ./target/release/uniform
```

---

## Data Types

Uniform has 3 data types:

| Type | Description |
|------|-------------|
| **string** | Text values |
| **float** | Numeric values (64-bit floating point) |
| **boolean** | `true` or `false` |

All inputs are scanned for values -- each value is evaluated as float and boolean first before defaulting to string. Columns within the same table can contain different data types.

---

## Tables

Tables are in-memory queues with SQL querying support.

**Key behaviors:**
- `SELECT` is always an atomic pop (delete + return). Messages are consumed exactly once.
- All fields are always queryable via secondary indexes or full scan.
- `LIMIT offset,count` means: start scanning at element `offset`, return up to `count` matches.
- Tables can reach "Table full" state -- writes fail unless handled by `DEFLECT_WRITE_ON_FULL_TABLE` proc.
- Optional front buffer for delayed/time-dilated writes (requires `BUFFER_APPL` worker).

**Reserved columns:**
- `Fb_id` -- Auto-generated UUID for each row
- `Fb_created` -- Auto-generated creation timestamp (RFC 3339, millisecond precision)
- `Fb_body` -- Reserved for HTTP/FastCGI body transfers (always base64 encoded)
- `Fb_reply_id` -- Used for request-response correlation

### Table Commands

```sql
-- Create table with defaults
CREATE TABLE my_table ({});

-- Create table with full options
CREATE TABLE my_table ({
    "Size": 1000,
    "ForceLimit": -1,
    "ForceOffset": -1,
    "MaxScanTimeMs": 0,
    "LongPollMaxClients": 200,
    "BufferSize": 0,
    "BufferLongPollMs": 2000,
    "Silent": false
});

-- Write data
INSERT INTO my_table (a, b) VALUES('hello', 42);
RPUSH my_table '{"a":"hello","b":42}';

-- Read data (destructive pop)
SELECT * FROM my_table;
SELECT a FROM my_table WHERE b = 42 LIMIT 0,10;
RPOP my_table;

-- Management
SHOW TABLES;
ALTER TABLE my_table ({"Key":"ForceLimit", "Value":"100"});
DROP TABLE my_table;
```

### Table Options

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

### Indexes

Tables support secondary indexes for accelerated lookups:

```sql
-- Create index
CREATE INDEX idx1 ({"Table":"my_table", "Key":"status", "Unique":false});

-- Unique index (enforces uniqueness on insert)
CREATE INDEX idx2 ({"Table":"my_table", "Key":"order_id", "Unique":true});

-- View indexes
SHOW INDEXES;

-- Drop index
DROP INDEX idx1;
```

When a `SELECT` query has a `WHERE` clause matching an indexed column, the query planner automatically uses the index for O(log n) lookup instead of a full table scan.

---

## Servers

Protocol frontends that accept client connections and route commands to the query engine.

### Server Commands

```sql
-- Create server (protocols: REDIS, MYSQL, HTTP, FASTCGI)
CREATE SERVER <protocol> <name> ({<options>});

-- Management
SHOW SERVERS;
SHOW CONNECTIONS;
ALTER SERVER <name> ({"Key":"<option>", "Value":"<value>"});
DROP SERVER <name>;
DROP CONNECTION <addr>;
```

### Server Options

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

### MySQL Server

Implements the MySQL binary wire protocol. Compatible with standard MySQL clients and drivers.

```sql
CREATE SERVER MYSQL m1 ({"Bind":"0.0.0.0:6400"});
```

Connect with: `mysql -h 127.0.0.1 -P 6400`

Supports: `COM_QUERY`, `COM_PING`, `COM_QUIT`, `COM_INIT_DB`. Full result set encoding with column metadata.

### Redis Server

Implements the Redis RESP protocol. Compatible with `redis-cli` and Redis client libraries.

```sql
CREATE SERVER REDIS r1 ({"Bind":"0.0.0.0:6379"});
```

Connect with: `redis-cli -p 6379`

Supports: `RPUSH`/`LPUSH`, `RPOP`/`LPOP`, `LLEN`, `PING`, plus all Uniform SQL commands.

### HTTP Server

Raw HTTP/1.1 server that converts requests into table inserts.

```sql
CREATE SERVER HTTP h1 ({"Bind":"0.0.0.0:8080", "Table":"requests"});
```

Additional HTTP options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| Table | string | required | Target table for incoming requests |
| HttpsRedirect | bool | false | Redirect HTTP to HTTPS |
| HttpsRedirectHeader | string | "" | Header to check for HTTPS detection |
| HttpsRedirectOn | string | "" | Header value that triggers redirect |
| Gzip | bool | false | Enable gzip response compression |
| Robots | string | "" | Custom robots.txt content |
| DocumentRoot | string | "" | Document root for CGI vars |
| ScriptFilename | string | "" | Script filename for CGI vars |

Request headers and CGI variables are stored as row columns. Request body is base64-encoded as `Fb_body`. Response is built from the result row's columns (headers), `Status` (HTTP status code), and `Fb_body` (base64-decoded response body).

### FastCGI Server

Implements the FastCGI wire protocol. Compatible with nginx, Apache, and other web servers.

```sql
CREATE SERVER FASTCGI f1 ({"Bind":"0.0.0.0:9000", "Table":"requests"});
```

Same `Table` option as HTTP. Request parameters and body handling work identically to the HTTP server.

---

## Procs

Procs (Programmable Random Occurrences) are triggers attached to table read/write operations. Tables + Procs form a directed graph where tables are nodes and procs are edges. Loops are allowed.

### Proc Commands

```sql
-- Create proc
CREATE PROC <type> <name> ({<options>});

-- Management
SHOW PROCS;
ALTER PROC <name> ({"Key":"<option>", "Value":"<value>"});
DROP PROC <name>;
```

### Common Proc Options

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

### CaseQuery Virtual Tables

Proc conditions can query against:
- `$record` -- The current record being read/written
- `$metadata` -- The current session metadata
- `$connection` -- The current connection info
- Any real table name (cross-table queries)

### Write-Side Procs

These execute during INSERT operations, in order:

#### DEFLECT_WRITE
Redirects a write to a different table. The record is moved (not copied).

```sql
CREATE PROC DEFLECT_WRITE d1 ({"Src":"requests", "Dest":"overflow"});

-- With condition
CREATE PROC DEFLECT_WRITE d2 ({
    "Src":"requests", "Dest":"low_priority",
    "CaseQuery":"SELECT * FROM $record WHERE priority = 'low'"
});
```

#### BUFFER_CATCH
Catches writes and puts them into the table's front buffer instead of directly into the table.

```sql
CREATE PROC BUFFER_CATCH bc1 ({"Src":"requests"});
```

#### WRITE_OFFLINE
Blocks all writes to a table (returns error).

```sql
CREATE PROC WRITE_OFFLINE wo1 ({"Src":"requests"});
```

#### MIRROR_WRITE
Copies a write to an additional table (original write still happens).

```sql
CREATE PROC MIRROR_WRITE mw1 ({"Src":"requests", "Dest":"backup"});
```

#### GUNZIP_WRITE
Decompresses (gunzip) specified fields on write.

```sql
CREATE PROC GUNZIP_WRITE gz1 ({"Src":"requests", "GunzipFields":"Fb_body,payload"});
```

Use `*` to decompress all fields: `"GunzipFields":"*"`

#### GZIP_WRITE
Compresses (gzip) specified fields on write.

```sql
CREATE PROC GZIP_WRITE gz1 ({"Src":"requests", "GzipFields":"Fb_body"});
```

#### DECRYPT_WRITE
Decrypts specified fields on write (AES-256-CBC).

```sql
CREATE PROC DECRYPT_WRITE dw1 ({
    "Src":"requests",
    "DecryptKey":"your-secret-key",
    "DecryptFields":"sensitive_data"
});
```

#### ENCRYPT_WRITE
Encrypts specified fields on write (AES-256-CBC).

```sql
CREATE PROC ENCRYPT_WRITE ew1 ({
    "Src":"requests",
    "EncryptKey":"your-secret-key",
    "EncryptFields":"sensitive_data"
});
```

#### AUTO_REPLY
Automatically generates a reply record in a destination table when a write occurs.

```sql
CREATE PROC AUTO_REPLY ar1 ({
    "Src":"requests", "Dest":"responses",
    "ReplyStatus":"200 OK",
    "ReplyBody":"eyJzdGF0dXMiOiJvayJ9"
});
```

#### READ_ONLY (write-side check)
Prevents records from being consumed (popped) by SELECT. Records remain in the table after SELECT.

```sql
CREATE PROC READ_ONLY ro1 ({"Src":"requests"});
```

#### DEFLECT_WRITE_ON_FULL_TABLE
Redirects writes to another table when the source table is full.

```sql
CREATE PROC DEFLECT_WRITE_ON_FULL_TABLE df1 ({
    "Src":"requests", "Dest":"overflow"
});
```

### Read-Side Procs

These execute during SELECT operations:

#### DEFLECT_READ
Redirects a read to a different table. Follows chains (A -> B -> C).

```sql
CREATE PROC DEFLECT_READ dr1 ({"Src":"requests", "Dest":"processed"});
```

#### READ_OFFLINE
Blocks all reads from a table (returns error).

```sql
CREATE PROC READ_OFFLINE roff1 ({"Src":"requests"});
```

#### PING_OFFLINE
Blocks PING responses (causes load balancers to mark the server as dead).

```sql
CREATE PROC PING_OFFLINE po1 ({"Src":"requests"});
```

#### LONG_POLL
Enables long-polling: if SELECT returns empty, the client blocks and waits for new data.

```sql
CREATE PROC LONG_POLL lp1 ({"Src":"responses", "WaitMs":5000});
```

| Option | Type | Description |
|--------|------|-------------|
| WaitMs | int | Maximum wait time in milliseconds |

#### DEFLECT_READ_ON_EMPTY_RESPONSE
If SELECT returns empty, automatically tries reading from a fallback table.

```sql
CREATE PROC DEFLECT_READ_ON_EMPTY_RESPONSE dre1 ({
    "Src":"primary", "Dest":"fallback"
});
```

#### MIRROR_ON_READ
Copies consumed (popped) records to another table.

```sql
CREATE PROC MIRROR_ON_READ mor1 ({"Src":"requests", "Dest":"audit_log"});
```

#### READ_REDUCE
Deduplicates result rows by a key field, keeping either the latest or earliest record per key.

```sql
CREATE PROC READ_REDUCE rr1 ({
    "Src":"events",
    "ReduceKey":"user_id",
    "ReduceToLatest":true
});
```

| Option | Type | Description |
|--------|------|-------------|
| ReduceKey | string | Column to deduplicate by |
| ReduceToLatest | bool | Keep latest (true) or earliest (false) per key |

#### GUNZIP_ON_READ
Decompresses specified fields on read.

```sql
CREATE PROC GUNZIP_ON_READ gor1 ({"Src":"requests", "GunzipFields":"Fb_body"});
```

#### GZIP_ON_READ
Compresses specified fields on read.

```sql
CREATE PROC GZIP_ON_READ gzr1 ({"Src":"requests", "GzipFields":"payload"});
```

#### DECRYPT_ON_READ
Decrypts specified fields on read (AES-256-CBC).

```sql
CREATE PROC DECRYPT_ON_READ dor1 ({
    "Src":"requests",
    "DecryptKey":"your-secret-key",
    "DecryptFields":"sensitive_data"
});
```

#### ENCRYPT_ON_READ
Encrypts specified fields on read (AES-256-CBC).

```sql
CREATE PROC ENCRYPT_ON_READ eor1 ({
    "Src":"requests",
    "EncryptKey":"your-secret-key",
    "EncryptFields":"payload"
});
```

### Proc Execution Order

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

#### FRONTEND_IO_MIX
Combines write and read in a single operation. Used with HTTP/FastCGI servers for request-response patterns: the incoming request is inserted, then the response is read from a different table (with long-polling).

```sql
CREATE PROC FRONTEND_IO_MIX fio1 ({"Src":"requests", "Dest":"responses"});
```

---

## Workers

Background tasks that consume records from tables and push them to external destinations.

### Worker Commands

```sql
-- Start worker
START WORKER <class> <name> ({<options>});

-- Management
SHOW WORKERS;
DROP WORKER <name>;
```

### Common Worker Options

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

### EXEC_QUERY

Executes a configured SQL query for each consumed record. Supports `${field}` template substitution.

```sql
START WORKER EXEC_QUERY w1 ({
    "Table":"events",
    "Query":"SELECT * FROM events LIMIT 0,1",
    "ExecQuery":"INSERT INTO processed (id, status) VALUES('${Fb_id}', 'done')"
});
```

### BUFFER_APPL

Applies buffered writes from a table's front buffer to the actual table. Required when using `BUFFER_CATCH` procs.

```sql
START WORKER BUFFER_APPL w1 ({"Table":"requests"});
```

### PUSH_TO_STDOUT

Prints consumed records as JSON to process stdout.

```sql
START WORKER PUSH_TO_STDOUT w1 ({"Table":"events"});
```

### PUSH_TO_STDERR

Prints consumed records as JSON to process stderr.

```sql
START WORKER PUSH_TO_STDERR w1 ({"Table":"errors"});
```

### PUSH_TO_FILE

Writes consumed records to local files. Supports optional gzip compression and AES encryption.

```sql
START WORKER PUSH_TO_FILE w1 ({
    "Table":"events",
    "FilePath":"/var/log/uniform/",
    "GzipEnabled":true,
    "EncryptEnabled":false
});
```

| Option | Type | Description |
|--------|------|-------------|
| FilePath | string | Directory path for output files |
| GzipEnabled | bool | Enable gzip compression |
| EncryptEnabled | bool | Enable AES encryption |
| EncryptKey | string | Encryption key |

### PUSH_TO_EMAIL

Sends consumed records as email via SMTP.

```sql
START WORKER PUSH_TO_EMAIL w1 ({
    "Table":"alerts",
    "SmtpHost":"smtp.gmail.com",
    "SmtpPort":587,
    "SmtpUser":"user@gmail.com",
    "SmtpPass":"password",
    "From":"uniform@example.com",
    "To":"admin@example.com",
    "Cc":"",
    "Subject":"Uniform Alert"
});
```

### PUSH_TO_SLACK

Posts consumed records to Slack channels via the Slack API.

```sql
START WORKER PUSH_TO_SLACK w1 ({
    "Table":"notifications",
    "Token":"xoxb-...",
    "Channel":"#alerts",
    "Text":"Alert: ${message}",
    "TextOnly":true
});
```

| Option | Type | Description |
|--------|------|-------------|
| Token | string | Slack bot token |
| Channel | string | Target channel |
| Text | string | Message template (supports `${field}` substitution) |
| TextOnly | bool | true = chat.postMessage, false = files.upload (JSON) |

### PUSH_TO_PAGERDUTY

Sends incident alerts to PagerDuty via the Events API v2.

```sql
START WORKER PUSH_TO_PAGERDUTY w1 ({
    "Table":"alerts",
    "RoutingKey":"your-routing-key",
    "Source":"uniform-prod",
    "Severity":"critical",
    "Link":"https://dashboard.example.com/alert/${Fb_id}",
    "LinkText":"View Alert"
});
```

| Option | Type | Description |
|--------|------|-------------|
| RoutingKey | string | PagerDuty integration routing key |
| Source | string | Alert source identifier |
| Severity | string | Alert severity (critical, error, warning, info) |
| Link | string | Link URL template (supports `${field}`) |
| LinkText | string | Link display text template |

The `message` column is used as the alert summary. SHA256 of the full message is used as the dedup key.

### PUSH_TO_ELASTICSEARCH

Indexes consumed records as documents in Elasticsearch.

```sql
START WORKER PUSH_TO_ELASTICSEARCH w1 ({
    "Table":"events",
    "Host":"http://localhost:9200",
    "EsIndex":"uniform-events",
    "EsType":"_doc"
});
```

| Option | Type | Description |
|--------|------|-------------|
| Host | string | Elasticsearch URL |
| EsIndex | string | Target index name |
| EsType | string | Document type (use `_doc` for ES 7+) |

Documents are indexed with `Fb_id` as the document ID (PUT request).

### PUSH_TO_CLOUD_STORAGE

Uploads consumed records to S3-compatible cloud storage (AWS S3, MinIO, Google Cloud Storage).

```sql
START WORKER PUSH_TO_CLOUD_STORAGE w1 ({
    "Table":"backups",
    "Endpoint":"s3.amazonaws.com",
    "Bucket":"my-bucket",
    "Region":"us-east-1",
    "AccessKey":"AKIA...",
    "SecretKey":"...",
    "GzipEnabled":true,
    "EncryptEnabled":false
});
```

| Option | Type | Description |
|--------|------|-------------|
| Endpoint | string | S3 endpoint URL |
| Bucket | string | Target bucket name |
| Region | string | AWS region |
| AccessKey | string | AWS access key |
| SecretKey | string | AWS secret key |
| GzipEnabled | bool | Gzip compress before upload |
| EncryptEnabled | bool | AES encrypt before upload |
| EncryptKey | string | Encryption key |

Files are uploaded with date-based path: `YYYY/MM/DD/<worker>-<Fb_id>.json`.

### PUSH_TO_FASTCGI

Sends consumed records to a FastCGI backend server.

```sql
START WORKER PUSH_TO_FASTCGI w1 ({
    "Table":"requests",
    "Host":"127.0.0.1",
    "Port":9000,
    "ScriptFilename":"/var/www/app.php"
});
```

### MYSQL_DATA_TRANSFER

Bidirectional data transfer with external MySQL databases. Supports 6 modes:

| Mode | Description |
|------|-------------|
| 1_WAY_PUSH | Push records to remote MySQL |
| 1_WAY_PULL | Pull and execute locally |
| 2_WAY_PUSH | Bidirectional with push priority |
| 2_WAY_PULL | Bidirectional with pull priority |
| IO_MIX_PUSH | Mixed I/O with push |
| IO_MIX_PULL | Mixed I/O with pull |

```sql
START WORKER MYSQL_DATA_TRANSFER w1 ({
    "Table":"outbound",
    "Mode":"1_WAY_PUSH",
    "RemoteDsn":"user:pass@tcp(remote:3306)/db",
    "RemoteTable":"events",
    "Query":"SELECT * FROM outbound LIMIT 0,10"
});
```

---

## Command Reference

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

Uniform implements custom SQL functions for queue-specific operations:

| Function | Operators | Description |
|----------|-----------|-------------|
| `RAND(start, end)` | `<`, `>`, `<=`, `>=`, `=`, `!=` | Random number generation and comparison |
| `SLEEP(ms)` | `>` | Sleep inside a query (always evaluates to true) |
| `RECORD_AGE_ABS()` | `<`, `>`, `<=`, `>=`, `=`, `!=` | Compare record creation time against absolute timestamp |
| `RECORD_AGE_REL_MS()` | `<`, `>`, `<=`, `>=`, `=`, `!=` | Compare record age in milliseconds since creation |

Examples:

```sql
-- Select records older than 5 seconds
SELECT * FROM events WHERE RECORD_AGE_REL_MS() > 5000;

-- Random sampling (10% of records)
SELECT * FROM events WHERE RAND(1, 100) <= 10;

-- Select records created after a specific time
SELECT * FROM events WHERE RECORD_AGE_ABS() > '2024-01-01 00:00:00';
```

---

## Session Metadata

Each connection/session can carry metadata used by Procs and Workers. Metadata is set as a SQL comment to allow combining with other commands:

```sql
-- Set metadata
/* METADATA ({"env":"production","team":"backend"}); */

-- Set metadata + execute command in one statement
/* METADATA ({"role":"admin"}); */ SELECT * FROM requests;

-- View metadata
SHOW METADATA;
```

Workers save the creator session's metadata at creation time and use it during execution.

When using `mysql-client`, enable comment sending: `mysql -h 127.0.0.1 -P 6400 --comments`

---

## Query Cache

SELECT queries can be cached for improved performance:

```sql
-- Enable cache
SET SELECT_CACHE "on";

-- Disable cache (also empties it)
SET SELECT_CACHE "off";
```

The cache has no size limit, as the variation of SELECT queries is typically finite (limited consumer types).

---

## Binary Transfers

When using HTTP/FastCGI servers or FastCGI workers, the `Fb_body` field is reserved for body transfers. `Fb_body` should always be **base64 encoded** to handle binary and other non-text data safely.

---

## Monitoring and Logging

### Log Levels

| Level | Description |
|-------|-------------|
| `debug` | Maximum verbosity (not recommended for production) |
| `error` | Errors and notifications |
| `crit` | Only critical and important system messages |

```sql
-- Set log level
SET LOG_LEVEL "debug";

-- Route log messages to a table
SET LOG_DEST "my_log_table";

-- Disable log output
SET LOG_DEST "";
```

Only `error` and `crit` messages can be routed to a table. `debug` messages only go to STDOUT (to avoid message loops and link overload).

Use `PUSH_TO_STDOUT` or `PUSH_TO_STDERR` workers for flexible log routing to external destinations.

---

## Deployment

### Daemon Mode

Using the uniform-bootstrap system package:

```bash
sudo service uniform start
```

Configuration files are placed in `/etc/uniform/` as `.uniform` playbooks.

### Command Line Mode

```bash
cat config.uniform | uniform
```

### Graceful Shutdown

On SIGINT/SIGTERM: HTTP/FastCGI servers stop accepting connections first, then Redis/MySQL servers shut down. In-flight requests are allowed to complete.

---

## Configuration Examples

### Basic Queue with MySQL Interface

```sql
SET LOG_LEVEL 'error';
CREATE TABLE requests ({"Size":10000});
CREATE SERVER MYSQL m1 ({"Bind":"0.0.0.0:6400"});
```

### HTTP Request Queue with Worker Processing

```sql
CREATE TABLE http_requests ({"Size":5000});
CREATE TABLE processed ({"Size":5000});
CREATE SERVER HTTP h1 ({"Bind":"0.0.0.0:8080", "Table":"http_requests"});
START WORKER EXEC_QUERY w1 ({
    "Table":"http_requests",
    "ExecQuery":"INSERT INTO processed (url, method) VALUES('${REQUEST_URI}', '${REQUEST_METHOD}')"
});
```

### Traffic Mirroring with Backup

```sql
CREATE TABLE production ({"Size":10000});
CREATE TABLE mirror ({"Size":10000});
CREATE TABLE backup ({"Size":10000});

CREATE PROC MIRROR_WRITE m1 ({"Src":"production", "Dest":"mirror"});
CREATE PROC MIRROR_WRITE m2 ({"Src":"production", "Dest":"backup"});

START WORKER PUSH_TO_ELASTICSEARCH w1 ({
    "Table":"mirror",
    "Host":"http://es:9200",
    "EsIndex":"production-events",
    "EsType":"_doc"
});

START WORKER PUSH_TO_CLOUD_STORAGE w2 ({
    "Table":"backup",
    "Endpoint":"s3.amazonaws.com",
    "Bucket":"production-backup",
    "Region":"us-east-1",
    "GzipEnabled":true
});
```

### Request-Response with Long Polling (FastCGI)

```sql
CREATE TABLE requests ({"Size":5000, "LongPollMaxClients":1000});
CREATE TABLE responses ({"Size":5000, "LongPollMaxClients":1000});

CREATE SERVER FASTCGI f1 ({"Bind":"0.0.0.0:9000", "Table":"requests"});

CREATE PROC FRONTEND_IO_MIX fio1 ({"Src":"requests", "Dest":"responses"});
CREATE PROC LONG_POLL lp1 ({"Src":"responses", "WaitMs":5000});
```

### Encrypted Pipeline

```sql
CREATE TABLE raw_data ({"Size":5000});
CREATE TABLE encrypted_data ({"Size":5000});

CREATE PROC ENCRYPT_WRITE ew1 ({
    "Src":"raw_data",
    "EncryptKey":"my-secret-key-32chars-long!!!!!",
    "EncryptFields":"payload,sensitive_field"
});

CREATE PROC DECRYPT_ON_READ dor1 ({
    "Src":"encrypted_data",
    "DecryptKey":"my-secret-key-32chars-long!!!!!",
    "DecryptFields":"payload,sensitive_field"
});
```

---

## Replication

No built-in traditional replication is available, as it does not fit the queue context. Replication-like patterns can be implemented using:

- `MIRROR_WRITE` proc with `MYSQL_DATA_TRANSFER` worker to copy data to another system in real time
- `PUSH_TO_CLOUD_STORAGE` worker for cold backup

---

## Clustering

No built-in clustering is currently available. Use individual nodes behind a load balancer (e.g. HAProxy).

---

## CI/CD

GitHub Actions workflows are provided in `.github/workflows/`.

### CI (`ci.yml`)

Runs on every push and pull request to `main`, `master`, and `develop` branches.

| Job | Description |
|-----|-------------|
| **Check** | `cargo check` -- fast compilation validation |
| **Format** | `cargo fmt --check` -- enforces consistent code formatting |
| **Clippy** | `cargo clippy -D warnings` -- lint for common mistakes and non-idiomatic code |
| **Test** | `cargo test` -- runs all unit and integration tests |
| **Build** | Release builds for Linux (x86_64) and macOS (aarch64), uploaded as artifacts |

All jobs use [rust-cache](https://github.com/Swatinem/rust-cache) for dependency caching across runs.

### Release (`release.yml`)

Triggered by pushing a version tag (e.g. `git tag v2.0.0 && git push --tags`).

Builds release binaries for 4 targets:

| Target | Artifact |
|--------|----------|
| `x86_64-unknown-linux-gnu` | `uniform-linux-amd64.tar.gz` |
| `aarch64-unknown-linux-gnu` | `uniform-linux-arm64.tar.gz` |
| `x86_64-apple-darwin` | `uniform-darwin-amd64.tar.gz` |
| `aarch64-apple-darwin` | `uniform-darwin-arm64.tar.gz` |

Each artifact includes a `.sha256` checksum file. A GitHub Release is created automatically with generated release notes and all binary artifacts attached.

### Creating a Release

```bash
# Tag the release
git tag v2.0.0
git push origin v2.0.0
```

The release workflow will build all binaries and create a GitHub Release automatically.

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
```

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
