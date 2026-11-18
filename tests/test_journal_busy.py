import contextlib
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from almanak.state import Journal

LOCKER = """
import sqlite3, sys
connection = sqlite3.connect(sys.argv[1])
connection.execute("PRAGMA locking_mode=EXCLUSIVE")
connection.execute("BEGIN IMMEDIATE")
connection.execute("UPDATE counter SET value=value+1")
connection.commit()
print("locked", flush=True)
sys.stdin.readline()
connection.close()
"""


@contextlib.contextmanager
def locked_journal(tmp_path):
    path = tmp_path / "journal.db"
    with contextlib.closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE counter(value INTEGER)")
        connection.execute("INSERT INTO counter VALUES(0)")
        connection.commit()
    process = subprocess.Popen(
        [sys.executable, "-c", LOCKER, str(path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "locked"
        yield Journal(path), process
    finally:
        process.communicate("\n", timeout=10)
        assert process.returncode == 0


def test_busy_setup_waits_for_other_process_without_losing_committed_state(tmp_path):
    with locked_journal(tmp_path) as (journal, process):
        with contextlib.closing(sqlite3.connect(journal.path, timeout=0)) as raw:
            with pytest.raises(sqlite3.OperationalError) as busy:
                raw.execute("PRAGMA synchronous=FULL")
            assert busy.value.sqlite_errorcode & 255 == sqlite3.SQLITE_BUSY
        release = threading.Timer(0.2, lambda: (process.stdin.write("\n"), process.stdin.flush()))
        release.start()
        try:
            with journal.connect() as connection:
                assert connection.execute("PRAGMA synchronous").fetchone() == (2,)
                assert connection.execute("PRAGMA busy_timeout").fetchone() == (5000,)
                assert connection.execute("SELECT value FROM counter").fetchone() == (1,)
                connection.execute("UPDATE counter SET value=value+1")
            with journal.connect() as connection:
                assert connection.execute("SELECT value FROM counter").fetchone() == (2,)
        finally:
            release.join()


def test_busy_setup_has_bounded_deadline_and_never_enters_transaction(tmp_path):
    with locked_journal(tmp_path) as (journal, _):
        started = time.monotonic()
        with pytest.raises(sqlite3.OperationalError) as busy:
            with journal.connect():
                pytest.fail("locked setup must not admit caller work")
        assert busy.value.sqlite_errorcode & 255 == sqlite3.SQLITE_BUSY
        assert 4.9 <= time.monotonic() - started < 7


def test_transaction_busy_is_not_retried_and_uncommitted_change_rolls_back(tmp_path):
    path = tmp_path / "journal.db"
    with contextlib.closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE counter(value INTEGER)")
        connection.execute("INSERT INTO counter VALUES(0)")
        connection.commit()
    journal = Journal(path)
    calls = []
    with pytest.raises(sqlite3.OperationalError, match="caller busy"):
        with journal.connect() as connection:
            calls.append("called")
            connection.execute("UPDATE counter SET value=value+1")
            raise sqlite3.OperationalError("caller busy")
    assert calls == ["called"]
    with journal.connect() as connection:
        assert connection.execute("SELECT value FROM counter").fetchone() == (0,)


def test_non_busy_setup_error_remains_visible(tmp_path):
    path = tmp_path / "journal.db"
    path.write_bytes(b"not a SQLite database" * 100)
    with pytest.raises(sqlite3.DatabaseError, match="not a database"):
        with Journal(path).connect():
            pytest.fail("invalid database must not admit caller work")


def test_setup_retries_extended_busy_but_propagates_other_operational_errors():
    from almanak.state import _configure_connection

    class Connection:
        def __init__(self, code):
            self.code = code
            self.statements = []

        def execute(self, sql):
            self.statements.append(sql)
            if len(self.statements) == 1:
                error = sqlite3.OperationalError("setup failure")
                error.sqlite_errorcode = self.code
                raise error

    recovering = Connection(sqlite3.SQLITE_BUSY_RECOVERY)
    _configure_connection(recovering)
    assert recovering.statements == [
        "PRAGMA synchronous=FULL",
        "PRAGMA synchronous=FULL",
        "PRAGMA busy_timeout=5000",
    ]
    denied = Connection(sqlite3.SQLITE_READONLY)
    with pytest.raises(sqlite3.OperationalError, match="setup failure"):
        _configure_connection(denied)
    assert denied.statements == ["PRAGMA synchronous=FULL"]
