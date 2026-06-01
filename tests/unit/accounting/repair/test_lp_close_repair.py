"""Unit tests for the LP_CLOSE teardown-bug repair engine (VIB-4896).

Three concerns:

1. ``select_open_for_lp_close`` parity with the runner's OPEN-selection rule
   (newest-OPEN-after-last-CLOSE wins; intervening CLOSE invalidates;
   SNAPSHOT/COLLECT ignored; timestamp upper bound).
2. ``compute_lp_close_value_usd`` Empty != Zero (fail-closed, never 0).
3. Each skip-reason path of the engine over an in-memory SQLite DB.
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal

import pytest

from almanak.framework.accounting.repair.lp_close_repair import (
    PRICE_PROVENANCE_LEDGER,
    PRICE_PROVENANCE_OVERRIDE,
    SKIP_MISSING_AMOUNTS,
    SKIP_NO_MATCHING_OPEN,
    SKIP_PRICE_UNAVAILABLE,
    repair_teardown_lp_close,
)
from almanak.framework.observability.pnl_attributor import (
    CURRENT_VERSION,
    select_open_for_lp_close,
)
from almanak.framework.observability.position_events import compute_lp_close_value_usd


# ---------------------------------------------------------------------------
# select_open_for_lp_close — parity with the runner rule
# ---------------------------------------------------------------------------


def _ev(event_type: str, ts: str, **kw):
    base = {"event_type": event_type, "timestamp": ts}
    base.update(kw)
    return base


def test_select_open_single_open_then_close_is_none():
    history = [_ev("OPEN", "t1", token0="WETH"), _ev("CLOSE", "t2")]
    # CLOSE at t2 invalidates the OPEN; with no later OPEN, none matches.
    assert select_open_for_lp_close(history) is None


def test_select_open_newest_open_after_last_close_wins():
    history = [
        _ev("OPEN", "t1", liquidity="111"),
        _ev("CLOSE", "t2"),
        _ev("OPEN", "t3", liquidity="222"),
    ]
    chosen = select_open_for_lp_close(history)
    assert chosen is not None and chosen["liquidity"] == "222"


def test_select_open_intervening_close_invalidates():
    history = [_ev("OPEN", "t1"), _ev("CLOSE", "t2")]
    assert select_open_for_lp_close(history) is None


def test_select_open_ignores_snapshot_and_collect():
    history = [
        _ev("OPEN", "t1", liquidity="999"),
        _ev("SNAPSHOT", "t2"),
        _ev("COLLECT_FEES", "t3"),
    ]
    chosen = select_open_for_lp_close(history)
    assert chosen is not None and chosen["liquidity"] == "999"


def test_select_open_respects_close_timestamp_upper_bound():
    history = [
        _ev("OPEN", "2026-05-22T12:00:00", liquidity="aaa"),
        # A LATER re-open after our CLOSE must not be picked.
        _ev("OPEN", "2026-05-22T13:00:00", liquidity="bbb"),
    ]
    chosen = select_open_for_lp_close(history, close_timestamp="2026-05-22T12:30:00")
    assert chosen is not None and chosen["liquidity"] == "aaa"


def test_select_open_no_upper_bound_picks_latest():
    history = [
        _ev("OPEN", "2026-05-22T12:00:00", liquidity="aaa"),
        _ev("OPEN", "2026-05-22T13:00:00", liquidity="bbb"),
    ]
    chosen = select_open_for_lp_close(history, close_timestamp=None)
    assert chosen is not None and chosen["liquidity"] == "bbb"


def test_select_open_empty_history():
    assert select_open_for_lp_close([]) is None


# ---------------------------------------------------------------------------
# compute_lp_close_value_usd — Empty != Zero
# ---------------------------------------------------------------------------

# WETH 18dp @ $2000, USDC 6dp @ $1.
_PRICES = {"WETH": {"price_usd": "2000"}, "USDC": {"price_usd": "1"}}
_A0 = str(5 * 10**17)  # 0.5 WETH
_A1 = str(1000 * 10**6)  # 1000 USDC


def test_compute_value_usd_happy_path():
    res = compute_lp_close_value_usd("WETH", "USDC", _A0, _A1, _PRICES, chain="arbitrum")
    assert Decimal(res.value_usd) == Decimal("2000")
    assert res.skip_reason is None
    assert res.decimals0 == 18 and res.decimals1 == 6
    assert res.price0 == Decimal("2000") and res.price1 == Decimal("1")


def test_compute_value_usd_missing_amount_is_empty_not_zero():
    res = compute_lp_close_value_usd("WETH", "USDC", "", _A1, _PRICES, chain="arbitrum")
    assert res.value_usd == ""  # Empty != Zero — NEVER "0"
    assert res.skip_reason == "missing_tokens_or_amounts"


def test_compute_value_usd_missing_token_is_empty_not_zero():
    res = compute_lp_close_value_usd("", "USDC", _A0, _A1, _PRICES, chain="arbitrum")
    assert res.value_usd == ""
    assert res.skip_reason == "missing_tokens_or_amounts"


def test_compute_value_usd_price_miss_is_empty_not_zero():
    res = compute_lp_close_value_usd("WETH", "USDC", _A0, _A1, {"WETH": {"price_usd": "2000"}}, chain="arbitrum")
    assert res.value_usd == ""
    assert res.skip_reason == "price_unavailable"


def test_compute_value_usd_accepts_bare_scalar_price():
    res = compute_lp_close_value_usd("WETH", "USDC", _A0, _A1, {"WETH": "2000", "USDC": "1"}, chain="arbitrum")
    assert Decimal(res.value_usd) == Decimal("2000")


# ---------------------------------------------------------------------------
# Engine skip-reason paths over an in-memory SQLite DB
# ---------------------------------------------------------------------------


def _make_db(tmp_path, rows, ledger=None):
    """Build a minimal state DB with the position_events + transaction_ledger
    columns the engine reads. ``rows`` is a list of dicts; ``ledger`` is a
    dict id -> price_inputs_json string.
    """
    db = tmp_path / "state.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE position_events (
            id TEXT PRIMARY KEY, deployment_id TEXT, position_id TEXT,
            position_type TEXT, event_type TEXT, timestamp TEXT, protocol TEXT,
            chain TEXT, token0 TEXT DEFAULT '', token1 TEXT DEFAULT '',
            amount0 TEXT DEFAULT '', amount1 TEXT DEFAULT '',
            value_usd TEXT DEFAULT '', tick_lower INTEGER, tick_upper INTEGER,
            liquidity TEXT DEFAULT '', ledger_entry_id TEXT,
            attribution_json TEXT DEFAULT '{}', attribution_version INTEGER DEFAULT 0
        )
        """
    )
    conn.execute("CREATE TABLE transaction_ledger (id TEXT PRIMARY KEY, price_inputs_json TEXT DEFAULT '{}')")
    for r in rows:
        conn.execute(
            """
            INSERT INTO position_events
            (id, deployment_id, position_id, position_type, event_type, timestamp,
             protocol, chain, token0, token1, amount0, amount1, value_usd,
             tick_lower, tick_upper, liquidity, ledger_entry_id, attribution_json,
             attribution_version)
            VALUES (:id, :deployment_id, :position_id, :position_type, :event_type,
                    :timestamp, :protocol, :chain, :token0, :token1, :amount0,
                    :amount1, :value_usd, :tick_lower, :tick_upper, :liquidity,
                    :ledger_entry_id, :attribution_json, :attribution_version)
            """,
            {
                "id": r["id"],
                "deployment_id": r.get("deployment_id", "D"),
                "position_id": r["position_id"],
                "position_type": r.get("position_type", "LP"),
                "event_type": r["event_type"],
                "timestamp": r["timestamp"],
                "protocol": r.get("protocol", "uniswap_v3"),
                "chain": r.get("chain", "arbitrum"),
                "token0": r.get("token0", ""),
                "token1": r.get("token1", ""),
                "amount0": r.get("amount0", ""),
                "amount1": r.get("amount1", ""),
                "value_usd": r.get("value_usd", ""),
                "tick_lower": r.get("tick_lower"),
                "tick_upper": r.get("tick_upper"),
                "liquidity": r.get("liquidity", ""),
                "ledger_entry_id": r.get("ledger_entry_id"),
                "attribution_json": r.get("attribution_json", "{}"),
                "attribution_version": r.get("attribution_version", 0),
            },
        )
    for lid, pij in (ledger or {}).items():
        conn.execute(
            "INSERT INTO transaction_ledger (id, price_inputs_json) VALUES (?, ?)",
            (lid, pij),
        )
    conn.commit()
    conn.close()
    return str(db)


def _open(**kw):
    base = {
        "event_type": "OPEN",
        "timestamp": "t1",
        "token0": "WETH",
        "token1": "USDC",
        "tick_lower": -201000,
        "tick_upper": -199000,
        "liquidity": "123",
    }
    base.update(kw)
    return base


def _broken_close(**kw):
    base = {"event_type": "CLOSE", "timestamp": "t2", "amount0": _A0, "amount1": _A1, "ledger_entry_id": "L1"}
    base.update(kw)
    return base


def test_engine_no_matching_open(tmp_path):
    db = _make_db(tmp_path, [dict(_broken_close(id="c1", position_id="P1"))])
    res = repair_teardown_lp_close(db, dry_run=True)
    assert res.detected == 1
    assert res.rows[0].skip_reason == SKIP_NO_MATCHING_OPEN
    assert res.rows[0].value_usd == ""


def test_engine_missing_amounts_carries_bracket(tmp_path):
    db = _make_db(
        tmp_path,
        [
            dict(_open(id="o1", position_id="P1")),
            dict(_broken_close(id="c1", position_id="P1", amount0="", amount1="")),
        ],
        ledger={"L1": json.dumps(_PRICES)},
    )
    res = repair_teardown_lp_close(db, dry_run=True)
    row = res.rows[0]
    assert row.skip_reason == SKIP_MISSING_AMOUNTS
    assert row.value_usd == ""  # Empty != Zero
    # Bracket still carried where knowable.
    assert row.token0 == "WETH" and row.token1 == "USDC"
    assert row.tick_lower == -201000 and row.liquidity == "123"


def test_engine_price_unavailable(tmp_path):
    db = _make_db(
        tmp_path,
        [
            dict(_open(id="o1", position_id="P1")),
            dict(_broken_close(id="c1", position_id="P1", ledger_entry_id="")),
        ],
    )
    res = repair_teardown_lp_close(db, dry_run=True)
    row = res.rows[0]
    assert row.skip_reason == SKIP_PRICE_UNAVAILABLE
    assert row.value_usd == ""


def test_engine_price_override_used_when_ledger_empty(tmp_path):
    db = _make_db(
        tmp_path,
        [
            dict(_open(id="o1", position_id="P1")),
            dict(_broken_close(id="c1", position_id="P1", ledger_entry_id="")),
        ],
    )
    override = tmp_path / "prices.json"
    override.write_text(json.dumps(_PRICES))
    res = repair_teardown_lp_close(db, dry_run=True, prices_source=str(override))
    row = res.rows[0]
    assert row.skip_reason is None
    assert Decimal(row.value_usd) == Decimal("2000")
    assert row.price_provenance == PRICE_PROVENANCE_OVERRIDE


def test_engine_ledger_price_preferred_over_override(tmp_path):
    db = _make_db(
        tmp_path,
        [
            dict(_open(id="o1", position_id="P1")),
            dict(_broken_close(id="c1", position_id="P1")),
        ],
        ledger={"L1": json.dumps(_PRICES)},
    )
    override = tmp_path / "prices.json"
    override.write_text(json.dumps({"WETH": "9999", "USDC": "9999"}))
    res = repair_teardown_lp_close(db, dry_run=True, prices_source=str(override))
    row = res.rows[0]
    assert row.price_provenance == PRICE_PROVENANCE_LEDGER
    assert Decimal(row.value_usd) == Decimal("2000")  # ledger prices, not override


def test_engine_full_repair_writes_and_backfills_principal(tmp_path):
    db = _make_db(
        tmp_path,
        [
            dict(_open(id="o1", position_id="P1")),
            dict(
                _broken_close(
                    id="c1",
                    position_id="P1",
                    attribution_json='{"current_prices": {"WETH": "2000"}}',
                )
            ),
        ],
        ledger={"L1": json.dumps(_PRICES)},
    )
    res = repair_teardown_lp_close(db, dry_run=False)
    assert res.repaired == 1 and res.written == 1
    assert res.backup_path is not None

    conn = sqlite3.connect(db)
    r = conn.execute(
        "SELECT token0, token1, tick_lower, liquidity, value_usd, attribution_json, attribution_version "
        "FROM position_events WHERE id='c1'"
    ).fetchone()
    conn.close()
    assert r[0] == "WETH" and r[1] == "USDC"
    assert r[2] == -201000 and r[3] == "123"
    assert Decimal(r[4]) == Decimal("2000")
    attr = json.loads(r[5])
    # Existing keys preserved; principal backfilled = value_usd.
    assert attr["current_prices"] == {"WETH": "2000"}
    assert Decimal(attr["principal_recovered_usd"]) == Decimal("2000")
    assert r[6] == CURRENT_VERSION


def test_engine_dry_run_writes_nothing_and_no_backup(tmp_path):
    db = _make_db(
        tmp_path,
        [dict(_open(id="o1", position_id="P1")), dict(_broken_close(id="c1", position_id="P1"))],
        ledger={"L1": json.dumps(_PRICES)},
    )
    res = repair_teardown_lp_close(db, dry_run=True)
    assert res.backup_path is None
    conn = sqlite3.connect(db)
    r = conn.execute("SELECT token0, value_usd FROM position_events WHERE id='c1'").fetchone()
    conn.close()
    assert r == ("", "")  # untouched


def test_engine_idempotent(tmp_path):
    db = _make_db(
        tmp_path,
        [dict(_open(id="o1", position_id="P1")), dict(_broken_close(id="c1", position_id="P1"))],
        ledger={"L1": json.dumps(_PRICES)},
    )
    first = repair_teardown_lp_close(db, dry_run=False)
    assert first.repaired == 1
    second = repair_teardown_lp_close(db, dry_run=False)
    assert second.detected == 0 and second.repaired == 0


def test_engine_never_matches_measured_zero_or_null(tmp_path):
    # value_usd='0' (measured zero) must NOT be detected.
    db = _make_db(
        tmp_path,
        [dict(_broken_close(id="zero", position_id="PZ", value_usd="0"))],
    )
    res = repair_teardown_lp_close(db, dry_run=True)
    assert res.detected == 0  # '0' is measured zero, not the bug shape


def test_engine_deployment_id_scopes(tmp_path):
    db = _make_db(
        tmp_path,
        [
            dict(_open(id="oA", position_id="PA", deployment_id="DA")),
            dict(_broken_close(id="cA", position_id="PA", deployment_id="DA")),
            dict(_open(id="oB", position_id="PB", deployment_id="DB")),
            dict(_broken_close(id="cB", position_id="PB", deployment_id="DB")),
        ],
        ledger={"L1": json.dumps(_PRICES)},
    )
    res = repair_teardown_lp_close(db, dry_run=True, deployment_id="DA")
    assert res.detected == 1 and res.rows[0].event_id == "cA"


def test_engine_missing_db_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        repair_teardown_lp_close(str(tmp_path / "nope.db"), dry_run=True)


def test_engine_malformed_prices_source_raises(tmp_path):
    db = _make_db(tmp_path, [dict(_broken_close(id="c1", position_id="P1"))])
    bad = tmp_path / "bad.json"
    bad.write_text("[1, 2, 3]")  # not an object
    with pytest.raises(ValueError):
        repair_teardown_lp_close(db, dry_run=True, prices_source=str(bad))
