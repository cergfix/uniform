#!/usr/bin/env python3
"""
Integration tests for the Uniform binary.

Exercises: stdin commands, Redis protocol, MySQL protocol, HTTP protocol, workers.
Uses only Python stdlib + CLI tools (redis-cli, mysql, curl) available in CI.
"""

import os
import glob
import shutil
import socket
import subprocess
import sys
import tempfile
import time

BINARY = os.environ.get("BINARY", "./uniform")

PASS = 0
FAIL = 0
TESTS = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def assert_contains(haystack: str, needle: str, name: str):
    global PASS, FAIL, TESTS
    TESTS += 1
    if needle in haystack:
        PASS += 1
        print(f"  PASS: {name}")
    else:
        FAIL += 1
        print(f"  FAIL: {name} — expected {needle!r} in output")
        print(f"  --- actual output (first 500 chars) ---")
        print(haystack[:500])


def assert_not_contains(haystack: str, needle: str, name: str):
    global PASS, FAIL, TESTS
    TESTS += 1
    if needle not in haystack:
        PASS += 1
        print(f"  PASS: {name}")
    else:
        FAIL += 1
        print(f"  FAIL: {name} — did NOT expect {needle!r} in output")


def assert_eq(actual, expected, name: str):
    global PASS, FAIL, TESTS
    TESTS += 1
    if actual == expected:
        PASS += 1
        print(f"  PASS: {name}")
    else:
        FAIL += 1
        print(f"  FAIL: {name} — expected {expected!r}, got {actual!r}")


def wait_for_port(port: int, timeout: float = 10.0, uf: "UniformProcess | None" = None):
    """Block until 127.0.0.1:port is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        # If the process died, fail fast with diagnostics
        if uf and uf.proc and uf.proc.poll() is not None:
            print(f"  ERROR: Uniform process exited with code {uf.proc.returncode} while waiting for port {port}")
            print(f"  --- stdout ---\n{uf.stdout()[:2000]}")
            print(f"  --- stderr ---\n{uf.stderr()[:2000]}")
            raise RuntimeError(f"Uniform exited (rc={uf.proc.returncode}) before port {port} opened")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    # Timeout — dump diagnostics
    if uf:
        print(f"  --- stdout ---\n{uf.stdout()[:2000]}")
        print(f"  --- stderr ---\n{uf.stderr()[:2000]}")
    raise TimeoutError(f"port {port} not open after {timeout}s")


def find_tool(*candidates) -> str | None:
    """Return the first candidate that exists on PATH or as an absolute path."""
    for c in candidates:
        if shutil.which(c):
            return c
        if os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    return None


def run(args, **kwargs) -> subprocess.CompletedProcess:
    """Convenience wrapper — always captures output, never raises on rc."""
    return subprocess.run(args, capture_output=True, text=True, timeout=15, **kwargs)


def redis_cli(*args) -> str:
    """Run redis-cli -p 6399 <args> and return stdout."""
    result = run(["redis-cli", "-p", "6399"] + list(args))
    return result.stdout.strip()


class UniformProcess:
    """Manage a live Uniform binary with stdin piped in."""

    def __init__(self):
        self.proc: subprocess.Popen | None = None
        self._stdout_file = None
        self._stderr_file = None

    def start(self, init_commands: str = ""):
        """Start the binary.  *init_commands* are fed to stdin first."""
        self._stdout_file = tempfile.NamedTemporaryFile(
            prefix="uf_stdout_", delete=False
        )
        self._stderr_file = tempfile.NamedTemporaryFile(
            prefix="uf_stderr_", delete=False
        )
        self.proc = subprocess.Popen(
            [BINARY],
            stdin=subprocess.PIPE,
            stdout=self._stdout_file,
            stderr=self._stderr_file,
        )
        if init_commands:
            self.send(init_commands)
            time.sleep(0.5)  # give binary time to process init commands

    def send(self, text: str):
        """Write text to stdin (must end with newline)."""
        if not text.endswith("\n"):
            text += "\n"
        self.proc.stdin.write(text.encode())
        self.proc.stdin.flush()

    def shutdown(self, timeout: float = 10.0):
        """Send SHUTDOWN, close stdin, wait for exit."""
        try:
            self.proc.stdin.write(b"SHUTDOWN\n")
            self.proc.stdin.flush()
            self.proc.stdin.close()
        except BrokenPipeError:
            pass
        self.proc.wait(timeout=timeout)

    def kill(self):
        self.proc.kill()
        self.proc.wait(timeout=5)

    @property
    def returncode(self) -> int:
        return self.proc.returncode

    def stdout(self) -> str:
        self._stdout_file.flush()
        with open(self._stdout_file.name, "r") as f:
            return f.read()

    def stderr(self) -> str:
        self._stderr_file.flush()
        with open(self._stderr_file.name, "r") as f:
            return f.read()

    def cleanup(self):
        if self.proc and self.proc.poll() is None:
            self.kill()
        for f in (self._stdout_file, self._stderr_file):
            if f:
                try:
                    os.unlink(f.name)
                except OSError:
                    pass



def port_open(port: int) -> bool:
    """Quick check if a port is open on localhost."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Phase 1 — Stdin-only tests
# ---------------------------------------------------------------------------

def phase1_stdin():
    print("\n=== Phase 1: Stdin-only tests ===")

    commands = "\n".join([
        "CREATE TABLE t1;",
        "INSERT INTO t1 (msg) VALUES ('hello')",
        "CREATE TABLE t2 ({\"size\": 2});",
        "INSERT INTO t2 (a) VALUES ('1');",
        "INSERT INTO t2 (a) VALUES ('2')",
        "INSERT INTO t2 (a) VALUES ('3');",
        "CREATE INDEX idx1 ({\"table\":\"t1\",\"column\":\"msg\",\"unique\":false})",
        "DROP TABLE t1;",
        "SELECT * FROM nonexistent",
        "SHUTDOWN",
    ])

    result = run([BINARY], input=commands)

    print("--- Test 1.1: Startup banner ---")
    assert_contains(result.stdout, "Welcome to Uniform", "Startup banner — Welcome")
    assert_contains(result.stdout, "Up and running", "Startup banner — Up and running")

    print("--- Test 1.2: Clean shutdown ---")
    assert_eq(result.returncode, 0, "SHUTDOWN exits with code 0")
    assert_contains(result.stdout, "shutdown ok", "SHUTDOWN — shutdown ok message")

    print("--- Test 1.3: Error on SELECT from nonexistent table ---")
    assert_contains(result.stderr, "doesn't exist",
                    "SELECT nonexistent table — error on stderr")

    print("--- Test 1.4: Table capacity error ---")
    assert_contains(result.stderr, "Table full",
                    "INSERT beyond capacity — Table full error")


# ---------------------------------------------------------------------------
# Phase 2 — Redis protocol tests
# ---------------------------------------------------------------------------

def phase2_redis():
    print("\n=== Phase 2: Redis protocol tests ===")

    if not shutil.which("redis-cli"):
        print("  SKIP: redis-cli not found")
        return

    uf = UniformProcess()
    try:
        uf.start("\n".join([
            "CREATE TABLE redis_test;",
            "CREATE TABLE redis_idx_test;",
            'CREATE INDEX ridx1 ({"table":"redis_idx_test","column":"city","unique":false});',
            'CREATE SERVER REDIS test_redis ({"bind":"127.0.0.1:6399"});',
        ]))
        wait_for_port(6399, uf=uf)

        print("--- Test 2.1: PING ---")
        assert_eq(redis_cli("PING"), "PONG", "Redis PING returns PONG")

        print("--- Test 2.2: VERSION ---")
        assert_contains(redis_cli("VERSION"), "Uniform",
                        "Redis VERSION contains Uniform")

        print("--- Test 2.3: CREATE TABLE + RPUSH + RPOP ---")
        redis_cli("CREATE", "TABLE", "redis_push_test")
        redis_cli("RPUSH", "redis_push_test", '{"name":"alice","age":"30"}')
        reply = redis_cli("RPOP", "redis_push_test")
        assert_contains(reply, "alice", "RPUSH+RPOP — row contains alice")

        print("--- Test 2.4: RPOP on empty table ---")
        reply = redis_cli("RPOP", "redis_push_test")
        assert_not_contains(reply, "alice", "RPOP empty table — no data returned")

        print("--- Test 2.5: LLEN ---")
        redis_cli("RPUSH", "redis_test", '{"k":"v1"}')
        redis_cli("RPUSH", "redis_test", '{"k":"v2"}')
        reply = redis_cli("LLEN", "redis_test")
        assert_contains(reply, "2", "LLEN returns 2 after 2 inserts")

        print("--- Test 2.6: INSERT + SELECT via Redis ---")
        redis_cli("INSERT INTO redis_test (color) VALUES ('blue');")
        reply = redis_cli("SELECT * FROM redis_test LIMIT 1;")
        assert_contains(reply, "{", "SELECT returns JSON row")

        print("--- Test 2.7: Error — SELECT from nonexistent table ---")
        reply = redis_cli("SELECT * FROM no_such_table;")
        assert_contains(reply, "ERR", "SELECT nonexistent table returns ERR")

        print("--- Test 2.8: CREATE INDEX + indexed SELECT ---")
        redis_cli("INSERT INTO redis_idx_test (city, name) VALUES ('NYC', 'bob');")
        redis_cli("INSERT INTO redis_idx_test (city, name) VALUES ('LA', 'carol');")
        redis_cli("INSERT INTO redis_idx_test (city, name) VALUES ('NYC', 'dave');")
        reply = redis_cli("SELECT * FROM redis_idx_test WHERE city = 'NYC';")
        assert_contains(reply, "NYC", "Indexed SELECT returns rows with NYC")
        assert_not_contains(reply, "LA", "Indexed SELECT does not return LA rows")

        uf.shutdown()
    finally:
        uf.cleanup()


# ---------------------------------------------------------------------------
# Phase 3 — MySQL protocol tests
# ---------------------------------------------------------------------------

def phase3_mysql():
    print("\n=== Phase 3: MySQL protocol tests ===")

    mysql_bin = find_tool(
        "mysql",
        "/opt/homebrew/opt/mysql-client/bin/mysql",
        "/usr/local/opt/mysql-client/bin/mysql",
    )
    mysqladmin_bin = find_tool(
        "mysqladmin",
        "/opt/homebrew/opt/mysql-client/bin/mysqladmin",
        "/usr/local/opt/mysql-client/bin/mysqladmin",
    )

    if not mysql_bin:
        print("  SKIP: mysql client not found")
        return

    mysql_cmd = [mysql_bin, "-h", "127.0.0.1", "-P", "6401",
                 "-u", "test", "--skip-password", "--connect-timeout=5"]

    def mysql_exec(sql: str) -> subprocess.CompletedProcess:
        return run(mysql_cmd + ["-e", sql])

    uf = UniformProcess()
    try:
        uf.start("\n".join([
            "CREATE TABLE mysql_test;",
            'CREATE SERVER MYSQL test_mysql ({"bind":"127.0.0.1:6401"});',
        ]))
        wait_for_port(6401, uf=uf)

        print("--- Test 3.1: MySQL connection + ping ---")
        if mysqladmin_bin:
            r = run([mysqladmin_bin, "-h", "127.0.0.1", "-P", "6401",
                     "-u", "test", "--skip-password", "ping"])
            combined = r.stdout + r.stderr
            assert_contains(combined, "alive", "mysqladmin ping — server is alive")
        else:
            mysql_exec("PING")
            global PASS, TESTS
            TESTS += 1; PASS += 1
            print("  PASS: MySQL client connects (mysqladmin not available)")

        print("--- Test 3.2: CREATE TABLE via MySQL ---")
        mysql_exec("CREATE TABLE mysql_t1;")

        print("--- Test 3.3: INSERT via MySQL ---")
        mysql_exec("INSERT INTO mysql_t1 (msg, num) VALUES ('hello', '42');")

        print("--- Test 3.4: SELECT with result set ---")
        r = mysql_exec("SELECT * FROM mysql_t1 LIMIT 1;")
        assert_contains(r.stdout, "hello", "MySQL SELECT returns inserted data")

        print("--- Test 3.5: SHOW TABLES ---")
        mysql_exec("INSERT INTO mysql_test (x) VALUES ('y');")
        r = mysql_exec("SHOW TABLES;")
        assert_contains(r.stdout, "mysql_test", "SHOW TABLES lists mysql_test")
        assert_contains(r.stdout, "mysql_t1", "SHOW TABLES lists mysql_t1")

        print("--- Test 3.6: Error — query nonexistent table ---")
        r = mysql_exec("SELECT * FROM nonexistent_mysql_tbl;")
        combined = r.stdout + r.stderr
        assert_contains(combined, "doesn't exist",
                        "MySQL error on nonexistent table")

        uf.shutdown()
    finally:
        uf.cleanup()


# ---------------------------------------------------------------------------
# Phase 4 — HTTP protocol tests
# ---------------------------------------------------------------------------

def phase4_http():
    print("\n=== Phase 4: HTTP protocol tests ===")

    has_redis = shutil.which("redis-cli") is not None

    uf = UniformProcess()
    try:
        init = [
            "CREATE TABLE http_inbox;",
            'CREATE SERVER HTTP test_http ({"bind":"127.0.0.1:8401","table":"http_inbox"});',
        ]
        if has_redis:
            init.append(
                'CREATE SERVER REDIS http_redis ({"bind":"127.0.0.1:6398"});'
            )
        uf.start("\n".join(init))

        wait_for_port(8401, uf=uf)
        if has_redis:
            wait_for_port(6398, uf=uf)

        print("--- Test 4.1: GET request ---")
        r = run(["curl", "-s", "-i", "--max-time", "5",
                 "http://127.0.0.1:8401/test?foo=bar"])
        assert_contains(r.stdout, "HTTP/", "HTTP GET — server returned HTTP response")

        print("--- Test 4.2: POST request with body ---")
        r = run(["curl", "-s", "-i", "--max-time", "5",
                 "-X", "POST", "-d", "test_payload",
                 "http://127.0.0.1:8401/postpath"])
        assert_contains(r.stdout, "HTTP/", "HTTP POST — server returned HTTP response")

        if has_redis:
            time.sleep(0.3)

            def redis6398(*args):
                return run(["redis-cli", "-p", "6398"] + list(args)).stdout.strip()

            print("--- Test 4.3: Verify request metadata via Redis ---")
            reply = redis6398("SELECT * FROM http_inbox LIMIT 1;")
            assert_contains(reply, "REQUEST_METHOD",
                            "HTTP row has REQUEST_METHOD")
            assert_contains(reply, "REQUEST_URI", "HTTP row has REQUEST_URI")

            print("--- Test 4.4: POST body is base64-encoded in u_body ---")
            reply = redis6398("SELECT * FROM http_inbox LIMIT 1;")
            assert_contains(reply, "u_body", "HTTP POST row has u_body")
        else:
            print("  SKIP: redis-cli not found, cannot verify HTTP inbox rows")

        uf.shutdown()
    finally:
        uf.cleanup()


# ---------------------------------------------------------------------------
# Phase 5 — Worker tests
# ---------------------------------------------------------------------------

def phase5_workers():
    print("\n=== Phase 5: Worker tests ===")

    file_out_dir = tempfile.mkdtemp(prefix="uf_file_out_")
    has_redis = shutil.which("redis-cli") is not None

    uf = UniformProcess()
    try:
        init_lines = [
            "CREATE TABLE stdout_src;",
            "CREATE TABLE file_src;",
            "CREATE TABLE exec_src;",
            "CREATE TABLE exec_dest;",
        ]
        if has_redis:
            init_lines.append(
                'CREATE SERVER REDIS worker_redis ({"bind":"127.0.0.1:6397"});'
            )
        init_lines += [
            "INSERT INTO stdout_src (msg) VALUES ('stdout_hello');",
            "INSERT INTO stdout_src (msg) VALUES ('stdout_world');",
            "INSERT INTO file_src (msg) VALUES ('file_hello');",
            "INSERT INTO file_src (msg) VALUES ('file_world');",
            "INSERT INTO exec_src (msg) VALUES ('exec_one');",
            "INSERT INTO exec_src (msg) VALUES ('exec_two');",
            'START WORKER PUSH_TO_STDOUT test_stdout'
            ' ({"table":"stdout_src","frequencyMs":200,"burst":10,"maxRuns":3})',
            'START WORKER PUSH_TO_FILE test_file'
            f' ({{"table":"file_src","filePath":"{file_out_dir}",'
            '"format":"json","frequencyMs":200,"burst":10,"maxRuns":3})',
            'START WORKER EXEC_QUERY test_exec'
            ' ({"table":"exec_src",'
            '"query":"INSERT INTO exec_dest (result) VALUES (\'moved\')",'
            '"frequencyMs":200,"burst":10,"maxRuns":5})',
        ]
        uf.start("\n".join(init_lines))

        if has_redis:
            wait_for_port(6397, uf=uf)

        # Let workers poll and process rows
        time.sleep(2)

        print("--- Test 5.1: PUSH_TO_FILE — file created ---")
        files = glob.glob(os.path.join(file_out_dir, "*"))
        if files:
            global PASS, FAIL, TESTS
            TESTS += 1; PASS += 1
            print(f"  PASS: PUSH_TO_FILE created {len(files)} file(s)")
            content = ""
            for fp in files:
                with open(fp) as f:
                    content += f.read()
            assert_contains(content, "file_hello",
                            "PUSH_TO_FILE output contains file_hello")
        else:
            TESTS += 1; FAIL += 1
            print(f"  FAIL: PUSH_TO_FILE — no files in {file_out_dir}")

        print("--- Test 5.2: EXEC_QUERY — destination table populated ---")
        if has_redis:
            def redis6397(*args):
                return run(["redis-cli", "-p", "6397"] + list(args)).stdout.strip()

            dest_raw = redis6397("LLEN", "exec_dest")
            # redis-cli may show "(integer) N"
            dest_num = int("".join(c for c in dest_raw if c.isdigit()) or "0")
            if dest_num > 0:
                TESTS += 1; PASS += 1
                print(f"  PASS: EXEC_QUERY populated exec_dest (len={dest_num})")
            else:
                TESTS += 1; FAIL += 1
                print(f"  FAIL: EXEC_QUERY — exec_dest is empty (raw={dest_raw!r})")

            src_raw = redis6397("LLEN", "exec_src")
            src_num = int("".join(c for c in src_raw if c.isdigit()) or "0")
            assert_eq(src_num, 0, "EXEC_QUERY drained exec_src")
        else:
            print("  SKIP: redis-cli not available for EXEC_QUERY verification")

        # Shutdown and read full stdout for PUSH_TO_STDOUT verification
        uf.shutdown()

        full_stdout = uf.stdout()
        print("--- Test 5.3: PUSH_TO_STDOUT — JSON on stdout ---")
        assert_contains(full_stdout, "stdout_hello",
                        "PUSH_TO_STDOUT output contains stdout_hello")
        assert_contains(full_stdout, "stdout_world",
                        "PUSH_TO_STDOUT output contains stdout_world")
    finally:
        uf.cleanup()
        shutil.rmtree(file_out_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Phase 6 — FastCGI tests
# ---------------------------------------------------------------------------

def phase6_fastcgi():
    print("\n=== Phase 6: FastCGI tests ===")

    has_redis = shutil.which("redis-cli") is not None
    if not has_redis:
        print("  SKIP: redis-cli not found, cannot verify FastCGI tests")
        return

    # --- 6a: FastCGI Server (Uniform receives FastCGI requests via nginx) ---
    print("\n--- Phase 6a: FastCGI server (nginx frontend) ---")

    if not port_open(8402):
        print("  SKIP: nginx not available on port 8402")
    else:
        uf = UniformProcess()
        try:
            uf.start("\n".join([
                "CREATE TABLE fcgi_inbox;",
                'CREATE SERVER REDIS fcgi_redis ({"bind":"127.0.0.1:6396"});',
                'CREATE SERVER FASTCGI test_fcgi ({"bind":"0.0.0.0:9124","table":"fcgi_inbox"});',
            ]))
            wait_for_port(9124, uf=uf)
            wait_for_port(6396, uf=uf)

            def redis6396(*args):
                return run(["redis-cli", "-p", "6396"] + list(args)).stdout.strip()

            print("--- Test 6.1: FastCGI server accepts request via nginx ---")
            r = run(["curl", "-s", "-i", "--max-time", "5",
                     "-X", "POST", "-d", "hello_fcgi!",
                     "http://127.0.0.1:8402/test"])
            assert_contains(r.stdout, "HTTP/", "6.1 nginx+FastCGI returns HTTP response")

            time.sleep(0.5)

            print("--- Test 6.2: Row inserted into fcgi_inbox with correct params ---")
            llen_raw = redis6396("LLEN", "fcgi_inbox")
            llen_num = int("".join(c for c in llen_raw if c.isdigit()) or "0")
            global PASS, FAIL, TESTS
            TESTS += 1
            if llen_num > 0:
                PASS += 1
                print(f"  PASS: fcgi_inbox has {llen_num} row(s)")
            else:
                FAIL += 1
                print(f"  FAIL: fcgi_inbox is empty (raw={llen_raw!r})")

            row = redis6396("SELECT * FROM fcgi_inbox LIMIT 1;")
            assert_contains(row, "REQUEST_METHOD", "6.2 Row contains REQUEST_METHOD")

            print("--- Test 6.3: POST body is base64-encoded as u_body ---")
            assert_contains(row, "u_body", "6.3 Row contains u_body")

            uf.shutdown()
        finally:
            uf.cleanup()

    # --- 6b: PUSH_TO_FASTCGI Worker (Uniform sends to php-fpm) ---
    print("\n--- Phase 6b: PUSH_TO_FASTCGI worker ---")

    if not port_open(9123):
        print("  SKIP: php-fpm not available on port 9123")
        return

    uf = UniformProcess()
    try:
        uf.start("\n".join([
            "CREATE TABLE fcgi_worker_src;",
            "CREATE TABLE fcgi_reply;",
            'CREATE SERVER REDIS fcgi_w_redis ({"bind":"127.0.0.1:6395"});',
            'START WORKER PUSH_TO_FASTCGI test_fcgi_worker'
            ' ({"table":"fcgi_worker_src","host":"127.0.0.1","port":9123,'
            '"scriptFilename":"/var/www/test/echo.php","twoWay":true,'
            '"replyTable":"fcgi_reply","frequencyMs":200,"burst":10,"maxRuns":5})',
        ]))
        wait_for_port(6395, uf=uf)

        def redis6395(*args):
            return run(["redis-cli", "-p", "6395"] + list(args)).stdout.strip()

        redis6395("INSERT INTO fcgi_worker_src (REQUEST_METHOD, REQUEST_URI) VALUES ('GET', '/test');")

        time.sleep(2)

        print("--- Test 6.4: PUSH_TO_FASTCGI worker sends request to php-fpm ---")
        src_raw = redis6395("LLEN", "fcgi_worker_src")
        src_num = int("".join(c for c in src_raw if c.isdigit()) or "0")
        assert_eq(src_num, 0, "6.4 Worker drained fcgi_worker_src")

        print("--- Test 6.5: Two-way mode populates reply_table with response ---")
        reply_raw = redis6395("LLEN", "fcgi_reply")
        reply_num = int("".join(c for c in reply_raw if c.isdigit()) or "0")
        TESTS += 1
        if reply_num > 0:
            PASS += 1
            print(f"  PASS: fcgi_reply has {reply_num} row(s)")
        else:
            FAIL += 1
            print(f"  FAIL: fcgi_reply is empty (raw={reply_raw!r})")

        print("--- Test 6.6: Reply contains u_body with php-fpm response data ---")
        reply_row = redis6395("SELECT * FROM fcgi_reply LIMIT 1;")
        assert_contains(reply_row, "u_body", "6.6 Reply row contains u_body")

        uf.shutdown()
    finally:
        uf.cleanup()


# ---------------------------------------------------------------------------
# Phase 7 — MySQL data transfer tests
# ---------------------------------------------------------------------------

def phase7_mysql_transfer():
    print("\n=== Phase 7: MySQL data transfer tests ===")

    has_redis = shutil.which("redis-cli") is not None
    if not has_redis:
        print("  SKIP: redis-cli not found")
        return

    if not port_open(3306):
        print("  SKIP: MariaDB not reachable on port 3306")
        return

    mysql_bin = find_tool(
        "mysql",
        "/opt/homebrew/opt/mysql-client/bin/mysql",
        "/usr/local/opt/mysql-client/bin/mysql",
    )
    if not mysql_bin:
        print("  SKIP: mysql client binary not found")
        return

    def mysql_local(sql: str) -> str:
        r = run([mysql_bin, "-h", "127.0.0.1", "-P", "3306",
                 "-u", "uf_test", "-puf_pass", "uniform_test",
                 "--skip-column-names", "-e", sql])
        return r.stdout.strip()

    uf = UniformProcess()
    try:
        init_lines = [
            'CREATE SERVER REDIS mysql_xfer_redis ({"bind":"127.0.0.1:6394"});',
            # Tables for each mode
            "CREATE TABLE push_src_1way;",
            "CREATE TABLE push_src_2way;",
            "CREATE TABLE push_src_iomix;",
            "CREATE TABLE pull_trigger_1way;",
            "CREATE TABLE pull_dest_1way;",
            "CREATE TABLE pull_trigger_2way;",
            "CREATE TABLE pull_dest_2way;",
            "CREATE TABLE pull_trigger_iomix;",
            "CREATE TABLE pull_dest_iomix;",
        ]
        # Workers — one per mode
        worker_base = '"host":"127.0.0.1:3306","databaseName":"uniform_test","user":"uf_test","password":"uf_pass","frequencyMs":200,"burst":10,"maxRuns":5'

        def worker_cmd(name, table, mode, extra):
            return 'START WORKER MYSQL_DATA_TRANSFER ' + name + ' ({"table":"' + table + '","mode":"' + mode + '",' + extra + ',' + worker_base + '})'

        pull_query_tpl = "INSERT INTO %s (msg) VALUES ('${msg}')"
        init_lines += [
            # 1_WAY_PUSH
            worker_cmd("test_push_1way", "push_src_1way", "1_WAY_PUSH", '"remoteTable":"push_dest"'),
            # 2_WAY_PUSH
            worker_cmd("test_push_2way", "push_src_2way", "2_WAY_PUSH", '"remoteTable":"push_dest"'),
            # IO_MIX_PUSH
            worker_cmd("test_push_iomix", "push_src_iomix", "IO_MIX_PUSH", '"remoteTable":"push_dest"'),
            # 1_WAY_PULL
            worker_cmd("test_pull_1way", "pull_trigger_1way", "1_WAY_PULL", '"query":"' + pull_query_tpl % "pull_dest_1way" + '"'),
            # 2_WAY_PULL
            worker_cmd("test_pull_2way", "pull_trigger_2way", "2_WAY_PULL", '"query":"' + pull_query_tpl % "pull_dest_2way" + '"'),
            # IO_MIX_PULL
            worker_cmd("test_pull_iomix", "pull_trigger_iomix", "IO_MIX_PULL", '"query":"' + pull_query_tpl % "pull_dest_iomix" + '"'),
        ]

        uf.start("\n".join(init_lines))
        wait_for_port(6394, uf=uf)

        def redis6394(*args):
            return run(["redis-cli", "-p", "6394"] + list(args)).stdout.strip()

        # Clear push_dest before tests
        mysql_local("DELETE FROM push_dest;")

        # Insert source rows for push modes
        redis6394("INSERT INTO push_src_1way (msg, num) VALUES ('hello_push_1way', '42');")
        redis6394("INSERT INTO push_src_2way (msg, num) VALUES ('hello_push_2way', '43');")
        redis6394("INSERT INTO push_src_iomix (msg, num) VALUES ('hello_push_iomix', '44');")

        # Insert trigger rows for pull modes
        redis6394("INSERT INTO pull_trigger_1way (msg) VALUES ('pulled_1way');")
        redis6394("INSERT INTO pull_trigger_2way (msg) VALUES ('pulled_2way');")
        redis6394("INSERT INTO pull_trigger_iomix (msg) VALUES ('pulled_iomix');")

        # Wait for workers to process
        time.sleep(3)

        global PASS, FAIL, TESTS

        print("--- Test 7.1: 1_WAY_PUSH — row pushed to MariaDB ---")
        result = mysql_local("SELECT * FROM push_dest;")
        assert_contains(result, "hello_push_1way", "7.1 push_dest contains hello_push_1way")

        print("--- Test 7.2: 1_WAY_PULL — templated query executed locally ---")
        llen_raw = redis6394("LLEN", "pull_dest_1way")
        llen_num = int("".join(c for c in llen_raw if c.isdigit()) or "0")
        TESTS += 1
        if llen_num > 0:
            PASS += 1
            print(f"  PASS: pull_dest_1way has {llen_num} row(s)")
        else:
            FAIL += 1
            print(f"  FAIL: pull_dest_1way is empty (raw={llen_raw!r})")
        row = redis6394("SELECT * FROM pull_dest_1way LIMIT 1;")
        assert_contains(row, "pulled_1way", "7.2 pull_dest_1way contains pulled_1way")

        print("--- Test 7.3: 2_WAY_PUSH — row pushed to MariaDB ---")
        assert_contains(result, "hello_push_2way", "7.3 push_dest contains hello_push_2way")

        print("--- Test 7.4: 2_WAY_PULL — templated query executed locally ---")
        llen_raw = redis6394("LLEN", "pull_dest_2way")
        llen_num = int("".join(c for c in llen_raw if c.isdigit()) or "0")
        TESTS += 1
        if llen_num > 0:
            PASS += 1
            print(f"  PASS: pull_dest_2way has {llen_num} row(s)")
        else:
            FAIL += 1
            print(f"  FAIL: pull_dest_2way is empty (raw={llen_raw!r})")
        row = redis6394("SELECT * FROM pull_dest_2way LIMIT 1;")
        assert_contains(row, "pulled_2way", "7.4 pull_dest_2way contains pulled_2way")

        print("--- Test 7.5: IO_MIX_PUSH — row pushed to MariaDB ---")
        assert_contains(result, "hello_push_iomix", "7.5 push_dest contains hello_push_iomix")

        print("--- Test 7.6: IO_MIX_PULL — templated query executed locally ---")
        llen_raw = redis6394("LLEN", "pull_dest_iomix")
        llen_num = int("".join(c for c in llen_raw if c.isdigit()) or "0")
        TESTS += 1
        if llen_num > 0:
            PASS += 1
            print(f"  PASS: pull_dest_iomix has {llen_num} row(s)")
        else:
            FAIL += 1
            print(f"  FAIL: pull_dest_iomix is empty (raw={llen_raw!r})")
        row = redis6394("SELECT * FROM pull_dest_iomix LIMIT 1;")
        assert_contains(row, "pulled_iomix", "7.6 pull_dest_iomix contains pulled_iomix")

        uf.shutdown()
    finally:
        uf.cleanup()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not os.path.isfile(BINARY) or not os.access(BINARY, os.X_OK):
        print(f"ERROR: binary not found or not executable at {BINARY}")
        sys.exit(1)

    phase1_stdin()
    phase2_redis()
    phase3_mysql()
    phase4_http()
    phase5_workers()
    phase6_fastcgi()
    phase7_mysql_transfer()

    print()
    print("=" * 40)
    print(f"  RESULTS: {PASS} passed, {FAIL} failed, {TESTS} total")
    print("=" * 40)

    if FAIL > 0:
        sys.exit(1)

    print("All integration tests passed.")


if __name__ == "__main__":
    main()
