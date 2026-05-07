"""Unit coverage for the defensive SQLite row helpers extracted on PR #2162.

``SQLiteStore._safe_row_str`` and ``SQLiteStore._safe_row_json`` collapse
six near-identical defensive read blocks in
``_row_to_portfolio_snapshot``. The helpers are the load-bearing
legacy-DB tolerance contract: they decide what happens when a SQLite row
predates a column added by a later migration, holds NULL, or stores
invalid JSON. Locking that truth table here so a future "tighten the
helper" change cannot silently break legacy DB reads.
"""

from __future__ import annotations

import logging
import sqlite3

from almanak.framework.state.backends.sqlite import SQLiteStore


def _row(schema: dict[str, object]) -> sqlite3.Row:
    """Build a real ``sqlite3.Row`` from a column→value dict.

    Using a real Row (not a dict) is deliberate — Row's ``__getitem__``
    raises ``IndexError`` on missing columns, which is exactly the
    legacy-DB shape the helpers must tolerate.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cols = ", ".join(f'"{k}"' for k in schema)
    placeholders = ", ".join("?" for _ in schema)
    conn.execute(f"CREATE TABLE t ({cols})")
    conn.execute(f"INSERT INTO t ({cols}) VALUES ({placeholders})", tuple(schema.values()))
    row = conn.execute(f"SELECT {cols} FROM t").fetchone()
    conn.close()
    return row


# --------------------------------------------------------------------------
# _safe_row_str
# --------------------------------------------------------------------------


class TestSafeRowStr:
    def test_present_string_returns_value(self):
        row = _row({"deployment_id": "Strat:abc"})
        assert SQLiteStore._safe_row_str(row, "deployment_id", "") == "Strat:abc"

    def test_present_null_returns_default(self):
        row = _row({"deployment_id": None})
        assert SQLiteStore._safe_row_str(row, "deployment_id", "fallback") == "fallback"

    def test_present_empty_returns_default(self):
        # ``"" or default`` → default. Documents the "empty == unstamped"
        # contract the snapshot-identity preserve clause depends on.
        row = _row({"deployment_id": ""})
        assert SQLiteStore._safe_row_str(row, "deployment_id", "fallback") == "fallback"

    def test_missing_column_returns_default(self):
        # Legacy DB shape: column simply doesn't exist on this row.
        row = _row({"some_other_column": "x"})
        assert SQLiteStore._safe_row_str(row, "deployment_id", "") == ""

    def test_default_is_zero_string_for_decimal_columns(self):
        # VIB-3614 cash split columns default to ``"0"`` so the model's
        # ``Decimal("0")`` parser succeeds when reading legacy rows.
        row = _row({"some_other_column": "x"})
        assert SQLiteStore._safe_row_str(row, "deployed_capital_usd", "0") == "0"

    def test_int_value_is_coerced_to_str(self):
        row = _row({"iteration_number": 42})
        assert SQLiteStore._safe_row_str(row, "iteration_number", "0") == "42"


# --------------------------------------------------------------------------
# _safe_row_json
# --------------------------------------------------------------------------


class TestSafeRowJson:
    def test_present_valid_json_decodes(self):
        row = _row({"token_prices_json": '{"USDC": {"usd": 1.0}}'})
        out = SQLiteStore._safe_row_json(row, "token_prices_json", {})
        assert out == {"USDC": {"usd": 1.0}}

    def test_present_null_returns_default(self):
        row = _row({"token_prices_json": None})
        assert SQLiteStore._safe_row_json(row, "token_prices_json", {}) == {}

    def test_present_empty_returns_default(self):
        row = _row({"token_prices_json": ""})
        assert SQLiteStore._safe_row_json(row, "token_prices_json", []) == []

    def test_missing_column_returns_default_silently(self, caplog):
        # Missing column = legacy DB shape; expected, MUST NOT log.
        row = _row({"some_other_column": "x"})
        with caplog.at_level(logging.WARNING):
            out = SQLiteStore._safe_row_json(row, "token_prices_json", {"sentinel": True})
        assert out == {"sentinel": True}
        assert caplog.records == []

    def test_invalid_json_returns_default_and_warns(self, caplog):
        # Corrupt payload = NOT expected; operator must see it.
        row = _row({"token_prices_json": "not-json{"})
        with caplog.at_level(logging.WARNING, logger="almanak.framework.state.backends.sqlite"):
            out = SQLiteStore._safe_row_json(row, "token_prices_json", {})
        assert out == {}
        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warns) == 1
        msg = warns[0].getMessage()
        assert "token_prices_json" in msg
        assert "Corrupt JSON" in msg

    def test_default_object_is_returned_by_identity_when_missing(self):
        # Caller passes mutable defaults (``[]`` / ``{}``) — the helper
        # must hand back a value the caller can use, not None.
        sentinel: list[dict] = []
        row = _row({"some_other_column": "x"})
        out = SQLiteStore._safe_row_json(row, "wallet_balances_json", sentinel)
        assert out is sentinel
