#!/usr/bin/env python3
"""
Load test: Uniform vs Redis and MySQL.

Redis: RPUSH/RPOP benchmark via redis-cli stdin batches (10k commands).
MySQL: INSERT / SELECT+DELETE benchmark via mysql CLI stdin batches.
Compares Uniform against real Redis 7 and MySQL 8.4 MEMORY engine.
Fails CI if Uniform throughput drops below expected baselines.
"""

import os
import socket
import subprocess
import sys
import time

BINARY = os.environ.get("BINARY", "./uniform")
COMPOSE_FILE = os.path.join(os.path.dirname(__file__), "docker-compose.load.yml")
UNIFORM_PORT = 6393
REDIS_PORT = 6394
UNIFORM_MYSQL_PORT = 6395
MYSQL_PORT = 3307
MYSQL_USER = "load_user"
MYSQL_PASS = "load_pass"
MYSQL_DB = "load_test_db"
NUM_ROWS = 100_000
BATCH_SIZE = 10_000
MYSQL_NUM_ROWS = 20_000
MYSQL_BATCH_SIZE = 100
# Soft baselines — ~75% of CI-observed values, rounded down to nearest 500.
# Gives headroom for run-to-run variance while catching real regressions.
REDIS_WRITE_BASELINE = 5_000
REDIS_READ_BASELINE = 5_000
MYSQL_LIMIT1_WRITE_BASELINE = 245_000
MYSQL_LIMIT1_READ_BASELINE = 3_000
MYSQL_WHERE_WRITE_BASELINE = 215_000
MYSQL_WHERE_READ_BASELINE = 2_500


def wait_for_port(port: int, timeout: float = 30.0):
    """Block until 127.0.0.1:port is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.3)
    raise TimeoutError(f"port {port} not open after {timeout}s")


def redis_cli_batch(port: int, commands: str, timeout: int = 120) -> subprocess.CompletedProcess:
    """Pipe a batch of commands to redis-cli via stdin."""
    return subprocess.run(
        ["redis-cli", "-p", str(port)],
        input=commands, capture_output=True, text=True, timeout=timeout,
    )


def run_benchmark(port: int, key: str, label: str) -> dict:
    """Run the RPUSH/RPOP benchmark on the given port and return stats."""
    print(f"\n--- {label} (port {port}) ---\n")

    # Write phase
    print(f"  Write: {NUM_ROWS:,} RPUSH in batches of {BATCH_SIZE:,} ...")
    t0 = time.monotonic()
    for start in range(0, NUM_ROWS, BATCH_SIZE):
        end = min(start + BATCH_SIZE, NUM_ROWS)
        cmds = "".join(
            f"RPUSH {key} '{{"
            f'"i":"{i}","data":"payload_{i}"'
            f"}}'\n"
            for i in range(start, end)
        )
        redis_cli_batch(port, cmds)
    write_elapsed = time.monotonic() - t0
    write_ops = NUM_ROWS / write_elapsed if write_elapsed > 0 else 0
    print(f"    {NUM_ROWS:,} rows in {write_elapsed:.2f}s ({write_ops:,.0f} ops/sec)")

    # Verify
    r = subprocess.run(
        ["redis-cli", "-p", str(port), "LLEN", key],
        capture_output=True, text=True, timeout=10,
    )
    raw = r.stdout.strip()
    count = int("".join(c for c in raw if c.isdigit()) or "0")
    print(f"    LLEN {key} = {count}")
    if count != NUM_ROWS:
        print(f"    WARNING: expected {NUM_ROWS}, got {count}")

    # Read phase
    print(f"  Read: {count:,} RPOP in batches of {BATCH_SIZE:,} ...")
    t0 = time.monotonic()
    remaining = count
    while remaining > 0:
        n = min(BATCH_SIZE, remaining)
        cmds = f"RPOP {key}\n" * n
        redis_cli_batch(port, cmds)
        remaining -= n
    read_elapsed = time.monotonic() - t0
    read_ops = count / read_elapsed if read_elapsed > 0 else 0
    print(f"    {count:,} rows in {read_elapsed:.2f}s ({read_ops:,.0f} ops/sec)")

    return {
        "label": label,
        "write_ops": write_ops,
        "read_ops": read_ops,
        "count": count,
    }


def mysql_batch(port: int, user: str, password: str, db: str, sql: str,
                 timeout: int = 120,
                 get_server_public_key: bool = False) -> subprocess.CompletedProcess:
    """Pipe SQL commands to mysql CLI via stdin (batch mode)."""
    cmd = ["mysql", "-h", "127.0.0.1", "-P", str(port),
           "-u", user, f"-p{password}"]
    if get_server_public_key:
        cmd.append("--get-server-public-key")
    cmd.extend([db, "-N", "--batch"])
    return subprocess.run(
        cmd, input=sql, capture_output=True, text=True, timeout=timeout,
    )


def wait_for_mysql_table(port: int, user: str, password: str, db: str,
                         table: str, timeout: float = 90.0):
    """Poll until a MySQL table is queryable (init SQL has finished)."""
    deadline = time.monotonic() + timeout
    last_err = ""
    while time.monotonic() < deadline:
        try:
            r = mysql_batch(port, user, password, db,
                            f"SELECT 1 FROM {table} LIMIT 0;", timeout=5,
                            get_server_public_key=True)
            if r.returncode == 0:
                return
            last_err = (r.stderr or r.stdout or "").strip()
        except subprocess.TimeoutExpired:
            last_err = "mysql CLI timed out"
        time.sleep(1)
    raise TimeoutError(
        f"MySQL table {db}.{table} not ready after {timeout}s — last error: {last_err}"
    )


def mysql_insert_rows(port: int, user: str, password: str, db: str,
                      table: str, num_rows: int,
                      get_server_public_key: bool = False):
    """Insert num_rows into table, 1000 rows per INSERT, 10k per batch."""
    rows_per_insert = 1000
    for start in range(0, num_rows, BATCH_SIZE):
        end = min(start + BATCH_SIZE, num_rows)
        sql_parts = []
        for ins_start in range(start, end, rows_per_insert):
            ins_end = min(ins_start + rows_per_insert, end)
            values = ",".join(
                f"('{i}','payload_{i}')"
                for i in range(ins_start, ins_end)
            )
            sql_parts.append(f"INSERT INTO {table} (i, data) VALUES {values};")
        r = mysql_batch(port, user, password, db, "\n".join(sql_parts),
                        get_server_public_key=get_server_public_key)
        if r.returncode != 0 and r.stderr.strip():
            print(f"    INSERT stderr: {r.stderr.strip()[:200]}")


def run_mysql_benchmark(port: int, user: str, password: str, db: str,
                        table: str, label: str,
                        destructive_select: bool) -> dict:
    """Run INSERT / simple SELECT LIMIT 1 benchmark against a MySQL-protocol server."""
    print(f"\n--- {label} (port {port}) ---\n")

    # Write phase
    print(f"  Write: {MYSQL_NUM_ROWS:,} INSERT in batches of {BATCH_SIZE:,} ...")
    t0 = time.monotonic()
    mysql_insert_rows(port, user, password, db, table, MYSQL_NUM_ROWS,
                      get_server_public_key=destructive_select)
    write_elapsed = time.monotonic() - t0
    write_ops = MYSQL_NUM_ROWS / write_elapsed if write_elapsed > 0 else 0
    print(f"    {MYSQL_NUM_ROWS:,} rows in {write_elapsed:.2f}s ({write_ops:,.0f} ops/sec)")

    # Verify — skip COUNT(*) on Uniform (SELECT is destructive/pop)
    if destructive_select:
        r = mysql_batch(port, user, password, db,
                        f"SELECT COUNT(*) FROM {table};", timeout=10,
                        get_server_public_key=True)
        raw = r.stdout.strip()
        count = int(raw) if raw.isdigit() else 0
        print(f"    COUNT(*) = {count}")
        if count != MYSQL_NUM_ROWS:
            print(f"    WARNING: expected {MYSQL_NUM_ROWS}, got {count}")
    else:
        count = MYSQL_NUM_ROWS
        print(f"    (skipping COUNT(*) — Uniform SELECT is destructive, assuming {count:,})")

    # Read phase — simple SELECT LIMIT 1
    print(f"  Read (simple SELECT LIMIT 1): {count:,} in batches of {MYSQL_BATCH_SIZE:,} ...")
    t0 = time.monotonic()
    remaining = count
    verified = 0
    while remaining > 0:
        n = min(MYSQL_BATCH_SIZE, remaining)
        if destructive_select:
            # Real MySQL: transactional SELECT FOR UPDATE + DELETE
            stmts = []
            for _ in range(n):
                stmts.append(
                    f"START TRANSACTION;"
                    f"SELECT @uid := id, i, data FROM {table} LIMIT 1 FOR UPDATE;"
                    f"DELETE FROM {table} WHERE id = @uid;"
                    f"COMMIT;"
                )
            sql = "\n".join(stmts)
        else:
            # Uniform MySQL: SELECT is already destructive (pop)
            sql = "\n".join(
                f"SELECT * FROM {table} LIMIT 1;"
                for _ in range(n)
            )
        r = mysql_batch(port, user, password, db, sql,
                        get_server_public_key=destructive_select)
        # Verify each returned row contains valid payload data
        for line in r.stdout.split('\n'):
            if 'payload_' in line:
                verified += 1
        remaining -= n
    read_elapsed = time.monotonic() - t0
    read_ops = count / read_elapsed if read_elapsed > 0 else 0
    print(f"    {count:,} rows in {read_elapsed:.2f}s ({read_ops:,.0f} ops/sec)")
    print(f"    Data verified: {verified:,}/{count:,} rows contain expected payload")

    return {
        "label": label,
        "write_ops": write_ops,
        "read_ops": read_ops,
        "count": count,
        "verified": verified,
    }


def run_mysql_where_benchmark(port: int, user: str, password: str, db: str,
                              table: str, label: str,
                              destructive_select: bool) -> dict:
    """Run INSERT / SELECT WHERE benchmark against a MySQL-protocol server.

    Uses indexed column `i` for point lookups.
    """
    print(f"\n--- {label} (port {port}) ---\n")

    # Write phase
    print(f"  Write: {MYSQL_NUM_ROWS:,} INSERT in batches of {BATCH_SIZE:,} ...")
    t0 = time.monotonic()
    mysql_insert_rows(port, user, password, db, table, MYSQL_NUM_ROWS,
                      get_server_public_key=destructive_select)
    write_elapsed = time.monotonic() - t0
    write_ops = MYSQL_NUM_ROWS / write_elapsed if write_elapsed > 0 else 0
    print(f"    {MYSQL_NUM_ROWS:,} rows in {write_elapsed:.2f}s ({write_ops:,.0f} ops/sec)")

    # Verify
    if destructive_select:
        r = mysql_batch(port, user, password, db,
                        f"SELECT COUNT(*) FROM {table};", timeout=10,
                        get_server_public_key=True)
        raw = r.stdout.strip()
        count = int(raw) if raw.isdigit() else 0
        print(f"    COUNT(*) = {count}")
        if count != MYSQL_NUM_ROWS:
            print(f"    WARNING: expected {MYSQL_NUM_ROWS}, got {count}")
    else:
        count = MYSQL_NUM_ROWS
        print(f"    (skipping COUNT(*) — Uniform SELECT is destructive, assuming {count:,})")

    # Read phase — SELECT WHERE i = '...' LIMIT 1
    print(f"  Read (SELECT WHERE): {count:,} in batches of {MYSQL_BATCH_SIZE:,} ...")
    t0 = time.monotonic()
    row_idx = 0
    remaining = count
    verified = 0
    while remaining > 0:
        n = min(MYSQL_BATCH_SIZE, remaining)
        if destructive_select:
            # Real MySQL: SELECT WHERE + DELETE by indexed column
            stmts = []
            for j in range(n):
                val = row_idx + j
                stmts.append(
                    f"START TRANSACTION;"
                    f"SELECT @uid := id, i, data FROM {table} WHERE i = '{val}' LIMIT 1 FOR UPDATE;"
                    f"DELETE FROM {table} WHERE id = @uid;"
                    f"COMMIT;"
                )
            sql = "\n".join(stmts)
        else:
            # Uniform MySQL: SELECT WHERE is already destructive (indexed pop)
            sql = "\n".join(
                f"SELECT * FROM {table} WHERE i = '{row_idx + j}' LIMIT 1;"
                for j in range(n)
            )
        r = mysql_batch(port, user, password, db, sql,
                        get_server_public_key=destructive_select)
        # Verify each row: check that payload_N matches the queried i value
        output = r.stdout
        for j in range(n):
            if f"payload_{row_idx + j}" in output:
                verified += 1
        row_idx += n
        remaining -= n
    read_elapsed = time.monotonic() - t0
    read_ops = count / read_elapsed if read_elapsed > 0 else 0
    print(f"    {count:,} rows in {read_elapsed:.2f}s ({read_ops:,.0f} ops/sec)")
    print(f"    Data verified: {verified:,}/{count:,} rows match expected payload")

    return {
        "label": label,
        "write_ops": write_ops,
        "read_ops": read_ops,
        "count": count,
        "verified": verified,
    }


def print_mysql_comparison(title: str, read_label: str,
                           uniform_stats: dict, mysql_stats: dict,
                           write_baseline: int, read_baseline: int) -> list:
    """Print a comparison table for MySQL benchmarks. Returns list of failures."""
    failures = []

    print("\n" + "=" * 60)
    print(f"  {title}")
    print(f"  {'':20s} {'Uniform':>12s} {'MySQL 8.4':>12s} {'Ratio':>8s}")
    print(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*8}")

    umw = uniform_stats["write_ops"]
    mw = mysql_stats["write_ops"]
    mwr = umw / mw if mw > 0 else 0
    print(f"  {'Write (ops/sec)':20s} {umw:>12,.0f} {mw:>12,.0f} {mwr:>7.2f}x")

    umr = uniform_stats["read_ops"]
    mr = mysql_stats["read_ops"]
    mrd = umr / mr if mr > 0 else 0
    print(f"  {read_label:20s} {umr:>12,.0f} {mr:>12,.0f} {mrd:>7.2f}x")

    uc = uniform_stats["count"]
    mc = mysql_stats["count"]
    print(f"  {'Rows read':20s} {uc:>12,} {mc:>12,}")

    uv = uniform_stats.get("verified", 0)
    mv = mysql_stats.get("verified", 0)
    print(f"  {'Data verified':20s} {uv:>12,} {mv:>12,}")
    print("=" * 60)

    if umw < write_baseline:
        msg = f"{title}: Uniform write ({umw:,.0f} ops/sec) below baseline ({write_baseline:,} ops/sec)"
        print(f"\n  FAIL: {msg}")
        failures.append(msg)
    if umr < read_baseline:
        msg = f"{title}: Uniform read ({umr:,.0f} ops/sec) below baseline ({read_baseline:,} ops/sec)"
        print(f"\n  FAIL: {msg}")
        failures.append(msg)
    if umw >= write_baseline and umr >= read_baseline:
        print("\n  Uniform MySQL throughput above baseline.")
    if uv != uc:
        msg = f"{title}: Uniform data verification failed ({uv:,}/{uc:,})"
        print(f"\n  FAIL: {msg}")
        failures.append(msg)
    if mv != mc:
        msg = f"{title}: MySQL data verification failed ({mv:,}/{mc:,})"
        print(f"\n  FAIL: {msg}")
        failures.append(msg)

    return failures


def main():
    if not os.path.isfile(BINARY):
        print(f"ERROR: binary not found at {BINARY}")
        sys.exit(1)

    print(f"=== Load Test: Uniform vs Redis ({NUM_ROWS:,} rows) ===")

    # Start containers
    print("\nStarting containers...")
    env = os.environ.copy()
    env["BINARY"] = BINARY
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "up", "-d"],
        env=env, capture_output=True, text=True, timeout=60,
    )

    failures = []

    try:
        print(f"Waiting for Uniform Redis (port {UNIFORM_PORT})...")
        wait_for_port(UNIFORM_PORT)
        print(f"Waiting for Redis (port {REDIS_PORT})...")
        wait_for_port(REDIS_PORT)
        time.sleep(1)
        print("Redis ports ready.")

        # Benchmark Redis first (as baseline)
        redis_stats = run_benchmark(REDIS_PORT, "load_test", "Redis 7")

        # Benchmark Uniform
        uniform_stats = run_benchmark(UNIFORM_PORT, "load_test", "Uniform")

        # Comparison
        print("\n" + "=" * 60)
        print(f"  {'':20s} {'Uniform':>12s} {'Redis 7':>12s} {'Ratio':>8s}")
        print(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*8}")

        uw = uniform_stats["write_ops"]
        rw = redis_stats["write_ops"]
        wr = uw / rw if rw > 0 else 0
        print(f"  {'Write (ops/sec)':20s} {uw:>12,.0f} {rw:>12,.0f} {wr:>7.2f}x")

        ur = uniform_stats["read_ops"]
        rr = redis_stats["read_ops"]
        rd = ur / rr if rr > 0 else 0
        print(f"  {'Read (ops/sec)':20s} {ur:>12,.0f} {rr:>12,.0f} {rd:>7.2f}x")

        print(f"  {'Rows verified':20s} {uniform_stats['count']:>12,} {redis_stats['count']:>12,}")
        print("=" * 60)

        if uw < REDIS_WRITE_BASELINE:
            msg = f"Redis: Uniform write ({uw:,.0f} ops/sec) below baseline ({REDIS_WRITE_BASELINE:,} ops/sec)"
            print(f"\n  FAIL: {msg}")
            failures.append(msg)
        if ur < REDIS_READ_BASELINE:
            msg = f"Redis: Uniform read ({ur:,.0f} ops/sec) below baseline ({REDIS_READ_BASELINE:,} ops/sec)"
            print(f"\n  FAIL: {msg}")
            failures.append(msg)
        if uw >= REDIS_WRITE_BASELINE and ur >= REDIS_READ_BASELINE:
            print("\n  Uniform throughput above baseline.")

        # --- MySQL Benchmark: simple SELECT LIMIT 1 ---
        print(f"\n\n=== MySQL: simple SELECT LIMIT 1 ({MYSQL_NUM_ROWS:,} rows) ===")

        # Check container status before waiting
        subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "ps", "-a"],
            env=env, timeout=10,
        )

        print(f"Waiting for MySQL (port {MYSQL_PORT})...")
        try:
            wait_for_port(MYSQL_PORT, timeout=60)
        except TimeoutError:
            print("MySQL port not open — dumping container logs:")
            subprocess.run(
                ["docker", "compose", "-f", COMPOSE_FILE, "logs", "mysql"],
                env=env, timeout=10,
            )
            raise
        print("Waiting for MySQL init SQL to finish...")
        wait_for_mysql_table(MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_DB, "load_test")
        print(f"Waiting for Uniform MySQL (port {UNIFORM_MYSQL_PORT})...")
        wait_for_port(UNIFORM_MYSQL_PORT)
        time.sleep(1)
        print("Both MySQL ports ready.")

        # Benchmark real MySQL first (as baseline)
        mysql_stats = run_mysql_benchmark(
            MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_DB,
            "load_test", "MySQL 8.4 MEMORY", destructive_select=True,
        )

        # Benchmark Uniform MySQL
        uniform_mysql_stats = run_mysql_benchmark(
            UNIFORM_MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_DB,
            "mysql_load_test", "Uniform MySQL", destructive_select=False,
        )

        failures += print_mysql_comparison(
            "simple SELECT LIMIT 1", "Read LIMIT 1",
            uniform_mysql_stats, mysql_stats,
            MYSQL_LIMIT1_WRITE_BASELINE, MYSQL_LIMIT1_READ_BASELINE,
        )

        # --- MySQL Benchmark: SELECT WHERE (indexed) ---
        print(f"\n\n=== MySQL: SELECT WHERE (indexed) ({MYSQL_NUM_ROWS:,} rows) ===")

        # Wait for the WHERE tables to be ready
        wait_for_mysql_table(MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_DB, "load_test_where")

        mysql_where_stats = run_mysql_where_benchmark(
            MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_DB,
            "load_test_where", "MySQL 8.4 MEMORY", destructive_select=True,
        )

        uniform_where_stats = run_mysql_where_benchmark(
            UNIFORM_MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_DB,
            "mysql_load_test_where", "Uniform MySQL", destructive_select=False,
        )

        failures += print_mysql_comparison(
            "SELECT WHERE (indexed)", "Read WHERE",
            uniform_where_stats, mysql_where_stats,
            MYSQL_WHERE_WRITE_BASELINE, MYSQL_WHERE_READ_BASELINE,
        )

    finally:
        print("\nStopping containers...")
        env = os.environ.copy()
        env["BINARY"] = BINARY
        subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "down", "-v"],
            env=env, capture_output=True, text=True, timeout=60,
        )
        print("Done.")

    if failures:
        print(f"\n{'='*60}")
        print(f"  FAILED — {len(failures)} baseline(s) not met:\n")
        for f in failures:
            print(f"    - {f}")
        print(f"{'='*60}")
        sys.exit(1)


if __name__ == "__main__":
    main()
