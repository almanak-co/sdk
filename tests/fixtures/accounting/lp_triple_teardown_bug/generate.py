"""Generator for the frozen ``lp_triple_teardown_bug`` fixture DB (VIB-4896).

Builds a SQLite state DB that reproduces the pre-VIB-4839 silent-cache bug:
teardown LP_CLOSE rows that landed with ``token0=''``, ``token1=''`` and
``value_usd=''`` (and consequently ``principal_recovered_usd=0``). The repair
engine + CLI are exercised against this committed DB by
``tests/e2e/accounting/test_repair_teardown_lp_close_e2e.py``.

It is deliberately a *generator*, not a hand-edited binary (the binary is the
committed artifact ``state.db``). Re-run with:

    uv run python tests/fixtures/accounting/lp_triple_teardown_bug/generate.py

Shape (single deployment, ``LpTriple:fixture4896``):

* ``LP1`` — broken teardown CLOSE (WETH/USDC). Has a matching OPEN with a full
  bracket + a ledger row whose price_inputs_json carries WETH+USDC prices →
  the engine repairs it fully.
* ``LP2`` — broken teardown CLOSE (WETH/USDC), second position → also fully
  repaired (proves multi-position handling).
* ``LP3`` — healthy *rebalance* CLOSE (already has token0/token1/value_usd).
  NOT broken → the negative control: must be byte-identical after repair.

All amounts/prices are deterministic so the recomputed value_usd is exact.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.observability.position_events import PositionEvent
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

DEPLOYMENT_ID = "LpTriple:fixture4896"
CHAIN = "arbitrum"
PROTOCOL = "uniswap_v3"
# Committed fixtures are deliberately NOT named ``almanak_state.db``: that name
# is gitignored (``**/almanak_state.db``) and blocked by the public-repo sync
# safety check, since a real one holds live runtime/wallet state. This is a
# synthetic fixture, so it follows the committed-fixture convention used by
# ``tests/fixtures/accounting/baseline/`` (``lp.db``, ``looping.db``, ...).
DB_NAME = "state.db"

# Deterministic WETH/USDC pricing (close-time, execution-time).
WETH_PRICE = "2000"
USDC_PRICE = "1"
# Received amounts at CLOSE (raw on-chain integer, smallest unit).
#   WETH 18dp: 0.5 WETH = 5e17  -> 0.5 * 2000 = $1000
#   USDC  6dp: 1000 USDC = 1e9   -> 1000 * 1   = $1000
#   value_usd = $2000.000...
AMOUNT0_WETH_RAW = str(5 * 10**17)
AMOUNT1_USDC_RAW = str(1000 * 10**6)


def _ts(base: datetime, minutes: int) -> datetime:
    return base + timedelta(minutes=minutes)


async def _build(db_path: str) -> None:
    # wal_mode=False so the committed artifact is a single self-contained file
    # (no -wal / -shm sidecar dependency for the frozen fixture).
    store = SQLiteStore(SQLiteConfig(db_path=db_path, wal_mode=False))
    await store.initialize()

    base = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)

    # Ledger rows carrying execution-time prices for the CLOSE legs.
    for lid, pos in (("ledger-lp1-close", "LP1"), ("ledger-lp2-close", "LP2")):
        await store.save_ledger_entry(
            LedgerEntry(
                id=lid,
                deployment_id=DEPLOYMENT_ID,
                cycle_id=f"teardown-{pos}",
                intent_type="LP_CLOSE",
                timestamp=_ts(base, 30),
                chain=CHAIN,
                tx_hash=f"0x{pos.lower()}close",
                token_in="",
                token_out="",
                amount_in="",
                amount_out="",
                gas_usd="0.50",
                price_inputs_json=json.dumps(
                    {
                        "WETH": {"price_usd": WETH_PRICE, "source": "fixture"},
                        "USDC": {"price_usd": USDC_PRICE, "source": "fixture"},
                    }
                ),
            )
        )

    # --- LP1: OPEN (full bracket) + broken teardown CLOSE -----------------
    await store.save_position_event(
        PositionEvent(
            id="lp1-open",
            deployment_id=DEPLOYMENT_ID,
            position_id="LP1",
            position_type="LP",
            event_type="OPEN",
            timestamp=_ts(base, 0),
            protocol=PROTOCOL,
            chain=CHAIN,
            token0="WETH",
            token1="USDC",
            amount0=AMOUNT0_WETH_RAW,
            amount1=AMOUNT1_USDC_RAW,
            value_usd="2000",
            tick_lower=-201000,
            tick_upper=-199000,
            liquidity="123456789",
        )
    )
    await store.save_position_event(
        PositionEvent(
            id="lp1-close",
            deployment_id=DEPLOYMENT_ID,
            position_id="LP1",
            position_type="LP",
            event_type="CLOSE",
            timestamp=_ts(base, 30),
            protocol=PROTOCOL,
            chain=CHAIN,
            # Amounts ARE present (received at CLOSE) — only token/value blanked.
            amount0=AMOUNT0_WETH_RAW,
            amount1=AMOUNT1_USDC_RAW,
            ledger_entry_id="ledger-lp1-close",
            attribution_json='{"current_prices": {"WETH": "2000"}, "principal_recovered_usd": "0"}',
        )
    )

    # --- LP2: OPEN (full bracket) + broken teardown CLOSE -----------------
    await store.save_position_event(
        PositionEvent(
            id="lp2-open",
            deployment_id=DEPLOYMENT_ID,
            position_id="LP2",
            position_type="LP",
            event_type="OPEN",
            timestamp=_ts(base, 1),
            protocol=PROTOCOL,
            chain=CHAIN,
            token0="WETH",
            token1="USDC",
            amount0=AMOUNT0_WETH_RAW,
            amount1=AMOUNT1_USDC_RAW,
            value_usd="2000",
            tick_lower=-202000,
            tick_upper=-198000,
            liquidity="987654321",
        )
    )
    await store.save_position_event(
        PositionEvent(
            id="lp2-close",
            deployment_id=DEPLOYMENT_ID,
            position_id="LP2",
            position_type="LP",
            event_type="CLOSE",
            timestamp=_ts(base, 31),
            protocol=PROTOCOL,
            chain=CHAIN,
            amount0=AMOUNT0_WETH_RAW,
            amount1=AMOUNT1_USDC_RAW,
            ledger_entry_id="ledger-lp2-close",
            attribution_json='{"principal_recovered_usd": "0"}',
        )
    )

    # --- LP3: HEALTHY rebalance CLOSE (negative control) ------------------
    await store.save_position_event(
        PositionEvent(
            id="lp3-open",
            deployment_id=DEPLOYMENT_ID,
            position_id="LP3",
            position_type="LP",
            event_type="OPEN",
            timestamp=_ts(base, 2),
            protocol=PROTOCOL,
            chain=CHAIN,
            token0="WETH",
            token1="USDC",
            amount0=AMOUNT0_WETH_RAW,
            amount1=AMOUNT1_USDC_RAW,
            value_usd="2000",
            tick_lower=-201500,
            tick_upper=-199500,
            liquidity="555555555",
        )
    )
    await store.save_position_event(
        PositionEvent(
            id="lp3-close",
            deployment_id=DEPLOYMENT_ID,
            position_id="LP3",
            position_type="LP",
            event_type="CLOSE",
            timestamp=_ts(base, 32),
            protocol=PROTOCOL,
            chain=CHAIN,
            token0="WETH",
            token1="USDC",
            amount0=AMOUNT0_WETH_RAW,
            amount1=AMOUNT1_USDC_RAW,
            value_usd="1950",  # healthy CLOSE — measured value present
            tick_lower=-201500,
            tick_upper=-199500,
            liquidity="555555555",
            attribution_json='{"principal_recovered_usd": "1950"}',
        )
    )

    await store.close()

    # Blank the two broken CLOSE rows' columns via a direct UPDATE so the
    # committed DB reproduces the exact on-disk shape the bug produced.
    # (save_position_event is INSERT OR IGNORE and cannot reach back into the
    # row; a direct UPDATE is the generator-side analogue of the bug.)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE position_events SET token0='', token1='', value_usd='', "
            "tick_lower=NULL, tick_upper=NULL, liquidity='' "
            "WHERE id IN ('lp1-close', 'lp2-close')"
        )
        conn.commit()
        # Fold any WAL back into the main DB file and switch to a rollback
        # journal so the committed artifact is a single self-contained file
        # (no -wal / -shm sidecar dependency).
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    out = Path(__file__).resolve().parent / DB_NAME
    if out.exists():
        out.unlink()
    # Remove any WAL/SHM sidecars from a prior run.
    for suffix in ("-wal", "-shm"):
        side = out.with_name(out.name + suffix)
        if side.exists():
            side.unlink()
    asyncio.run(_build(str(out)))
    # Drop any residual sidecars so only the single .db file is committed.
    for suffix in ("-wal", "-shm"):
        side = out.with_name(out.name + suffix)
        if side.exists():
            side.unlink()
    print(f"wrote fixture DB: {out}")


if __name__ == "__main__":
    main()
