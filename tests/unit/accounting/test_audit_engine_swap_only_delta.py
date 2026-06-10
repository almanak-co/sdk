"""Unit tests for ``_section_swap_only_delta`` (VIB-5014).

The old first-row/last-row heuristic computed ``last.amount_out −
first.amount_in`` whenever ``first.token_in == last.token_out``. On a real
mainnet run the LAST ledger row was a teardown-lane dust sweep
(amount_out=0.001 USDC), so the audit printed ``swap-only delta=$-2.999`` for a
round trip whose true net was +$0.0028. The fix sums token flows for the
round-trip asset X across ALL ledger rows, and flags the round trip as
incomplete only when a non-X token retains a residual above dust relative to
its gross flow.

Uses an in-memory SQLite DB with the minimal ``transaction_ledger`` columns the
section reads (``_ledger_query`` is a ``SELECT *`` filtered by
``deployment_id`` and ordered by ``timestamp``; the section reads ``token_in``
/ ``amount_in`` / ``token_out`` / ``amount_out``).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

import pytest

from strategies.accounting._audit_engine import AuditProse, _section_swap_only_delta

_PROSE = AuditProse(title="test", sections=("swap_only_delta",))

_DEPLOYMENT_ID = "deployment:abc123def456"


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    """In-memory DB with the minimal transaction_ledger schema the query needs."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE transaction_ledger ("
        "  deployment_id TEXT,"
        "  timestamp TEXT,"
        "  intent_type TEXT,"
        "  token_in TEXT,"
        "  amount_in TEXT,"
        "  token_out TEXT,"
        "  amount_out TEXT"
        ")"
    )
    yield conn
    conn.close()


def _insert(
    conn: sqlite3.Connection,
    rows: list[tuple[str, str, str, str, str]],
    *,
    deployment_id: str = _DEPLOYMENT_ID,
) -> None:
    """Insert (timestamp, token_in, amount_in, token_out, amount_out) rows."""
    conn.executemany(
        "INSERT INTO transaction_ledger "
        "(deployment_id, timestamp, intent_type, token_in, amount_in, token_out, amount_out) "
        "VALUES (?, ?, 'SWAP', ?, ?, ?, ?)",
        [(deployment_id, ts, ti, ai, to, ao) for ts, ti, ai, to, ao in rows],
    )
    conn.commit()


def _render(conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]) -> str:
    _section_swap_only_delta(conn, _PROSE, _DEPLOYMENT_ID)
    return capsys.readouterr().out


def test_teardown_dust_sweep_regression(conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]) -> None:
    """The exact 3-row mainnet case: the teardown WETH dust sweep as the LAST
    row must contribute its +0.001 USDC flow, not replace the whole round trip.

    Old heuristic printed delta = 0.001 − 3 = −2.999. True net for X=USDC is
    (3.001801 + 0.001) − 3 = +0.002801. The 6.1e-7 WETH residual (≈3.3 bps of
    the 0.00182861 WETH gross flow) is dust → round trip counts as complete.
    """
    _insert(
        conn,
        [
            ("2026-06-08T00:00:00", "USDC", "3", "WETH", "0.001828"),
            ("2026-06-08T00:01:00", "WETH", "0.001828", "USDC", "3.001801"),
            ("2026-06-08T00:02:00", "WETH", "0.00000061", "USDC", "0.001"),  # teardown sweep
        ],
    )
    out = _render(conn, capsys)
    assert out == "swap-only delta=$+0.00280100\n"
    assert "-2.999" not in out


def test_clean_two_row_round_trip(conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]) -> None:
    """Buy then fully sell back: delta = amount_out − amount_in on X."""
    _insert(
        conn,
        [
            ("2026-06-08T00:00:00", "USDC", "200.0", "WETH", "0.08"),
            ("2026-06-08T00:01:00", "WETH", "0.08", "USDC", "200.5"),
        ],
    )
    out = _render(conn, capsys)
    assert out == "swap-only delta=$+0.50000000\n"


def test_incomplete_round_trip_buy_only(conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]) -> None:
    """Buys never converted back: material non-X residual → incomplete branch."""
    _insert(
        conn,
        [
            ("2026-06-08T00:00:00", "USDC", "100.0", "WETH", "0.04"),
            ("2026-06-08T00:01:00", "USDC", "100.0", "WETH", "0.04"),
        ],
    )
    out = _render(conn, capsys)
    assert out == "swap-only delta=— (round trip incomplete: USDC→…→WETH)\n"


def test_incomplete_partial_sell_back(conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]) -> None:
    """Half the WETH inventory still held (50% of gross flow ≫ dust) → incomplete."""
    _insert(
        conn,
        [
            ("2026-06-08T00:00:00", "USDC", "200.0", "WETH", "0.08"),
            ("2026-06-08T00:01:00", "WETH", "0.04", "USDC", "100.1"),
        ],
    )
    out = _render(conn, capsys)
    assert out == "swap-only delta=— (round trip incomplete: USDC→…→WETH)\n"


def test_empty_ledger_early_return(conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]) -> None:
    out = _render(conn, capsys)
    assert out == ""


def test_single_row_early_return(conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]) -> None:
    _insert(conn, [("2026-06-08T00:00:00", "USDC", "100.0", "WETH", "0.04")])
    out = _render(conn, capsys)
    assert out == ""


def test_deployment_id_filter_excludes_foreign_rows(
    conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]
) -> None:
    """Rows from another deployment must not leak into the flow sums."""
    _insert(
        conn,
        [
            ("2026-06-08T00:00:00", "USDC", "3", "WETH", "0.001828"),
            ("2026-06-08T00:01:00", "WETH", "0.001828", "USDC", "3.002"),
        ],
    )
    _insert(
        conn,
        [("2026-06-08T00:02:00", "USDC", "999", "WETH", "0.4")],
        deployment_id="deployment:other000000",
    )
    out = _render(conn, capsys)
    assert out == "swap-only delta=$+0.00200000\n"


def test_first_row_missing_token_in(conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]) -> None:
    """No round-trip asset to anchor on → incomplete branch (never a phantom $ delta), no crash."""
    _insert(
        conn,
        [
            ("2026-06-08T00:00:00", "", "", "WETH", "0.04"),
            ("2026-06-08T00:01:00", "WETH", "0.04", "USDC", "100.0"),
        ],
    )
    out = _render(conn, capsys)
    assert out == "swap-only delta=— (round trip incomplete: →…→USDC)\n"
