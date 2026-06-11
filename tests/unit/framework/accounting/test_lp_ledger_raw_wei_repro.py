"""Regression contract (VIB-5036): LP_OPEN ledger amounts persist HUMAN units.

Colleague field report (deployment a9e54a85-…, strategy ``base-clp-full``,
uniswap_v3 on base, SDK 2.16.1rc9):

    decimal_unit_guard emits warnings for LP_OPEN/LP_CLOSE payload fields
    (amount_in, fees_token0) with wei-scale magnitudes, even though strategy
    intent amounts are human-scale decimals and transactions execute
    successfully. This indicates inconsistent unit normalization in
    accounting payload persistence.

Investigation split the report into two contracts:

* ``transaction_ledger.amount_in / amount_out`` is a HUMAN-units column (SWAP
  and lending write human; the blueprint L2 golden fixture asserts
  ``amount_in_human``). LP_OPEN alone violated it — ``_extract_from_lp_open``
  wrote ``LPOpenData.amount0 / amount1`` (raw on-chain integers) verbatim. The
  fix scales them to human at the writer (mirroring ``_extract_from_lending``),
  which both resolves the colleague's ``amount_in`` warning and silences the
  guard legitimately. THIS FILE pins that contract.

* ``position_events.fees_token0`` (the colleague's other field) is instead
  RAW-by-contract (NAV valuation / hydration / attribution all read it raw and
  scale at use), so the guard there was a false positive — fixed separately by
  removing the guard wiring on position_events (see
  ``test_decimal_unit_soft_fail.py`` /
  ``test_build_position_event_raw_wei_fees_does_not_warn``).

If these assertions ever revert to the raw integers, the write-side bug has
regressed.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.execution.extracted_data import LPOpenData
from almanak.framework.observability.ledger import build_ledger_entry

# Realistic base-clp-full magnitudes: a small WETH/USDC concentrated-LP open.
# token0 = WETH (18 dp): 0.0015 WETH deposited -> 1_500_000_000_000_000 (1.5e15)
# token1 = USDC ( 6 dp): 3.00 USDC deposited   -> 3_000_000           (3.0e6)
_WETH_RAW = 1_500_000_000_000_000
_USDC_RAW = 3_000_000


class _FakeLPOpenResult:
    """Minimal ``result`` stub carrying an LP_OPEN's on-chain actuals.

    ``swap_amounts`` MUST be falsy so the dispatcher routes through
    ``_extract_from_lp_open`` rather than the SWAP path.
    """

    swap_amounts = None
    success = True
    tx_hash = "0xlpopen"
    total_gas_cost_wei = 0

    def __init__(self) -> None:
        self.extracted_data: dict[str, Any] = {
            "lp_open_data": LPOpenData(
                position_id=12345,
                tick_lower=-887220,
                tick_upper=887220,
                liquidity=10**18,
                amount0=_WETH_RAW,
                amount1=_USDC_RAW,
                pool_address="0xpool",
            )
        }

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - stub default
        return None


class _FakeLPOpenIntent:
    """LP_OPEN intent whose *human-scale* amounts are available but discarded.

    The strategy author requested human-form decimals (0.0015 WETH / 3 USDC).
    The ledger writer prefers the raw ``LPOpenData`` ints over these — the very
    asymmetry the colleague reported.
    """

    intent_type = "LP_OPEN"
    token0 = "WETH"
    token1 = "USDC"
    pool = "WETH/USDC"
    # Human-scale amounts the strategy actually expressed:
    amount0 = Decimal("0.0015")
    amount1 = Decimal("3.0")


def test_lp_open_ledger_persists_human_not_raw_wei() -> None:
    """REGRESSION CONTRACT (VIB-5036): LP_OPEN ledger row stores HUMAN amounts.

    Pre-fix, ``amount_in`` / ``amount_out`` were the raw integer strings
    (``1500000000000000`` / ``3000000``). The writer now scales the on-chain
    ``LPOpenData`` raw integers to human units via the token decimals
    (WETH 18-dp, USDC 6-dp), matching the SWAP / lending contract and the L2
    golden-fixture ``amount_in_human``. If these assertions ever revert to the
    raw integers, the write-side bug has regressed.
    """
    entry = build_ledger_entry(
        deployment_id="deployment:a9e54a85d12d",
        cycle_id="cycle-1",
        intent=_FakeLPOpenIntent(),
        result=_FakeLPOpenResult(),
        chain="base",
        success=True,
    )

    assert entry is not None
    # HUMAN units persisted — the fix. 1.5e15 raw WETH / 1e18 = 0.0015;
    # 3e6 raw USDC / 1e6 = 3.
    assert entry.amount_in == "0.0015", f"expected human WETH amount, got {entry.amount_in!r}"
    assert entry.amount_out == "3", f"expected human USDC amount, got {entry.amount_out!r}"
    # The raw-wei integers must NOT appear.
    assert entry.amount_in != str(_WETH_RAW)
    assert entry.amount_out != str(_USDC_RAW)


def test_lp_open_ledger_write_does_not_fire_decimal_unit_guard(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """REGRESSION CONTRACT (VIB-5036): the guard is SILENT on the fixed LP_OPEN row.

    The colleague's reported ``decimal_unit_guard`` warning on ``amount_in`` was
    the symptom of the raw-wei write. Now that the writer emits human units, the
    guard must not fire on a legitimate, successful LP_OPEN.
    """
    with caplog.at_level(logging.WARNING, logger="almanak.framework.observability.ledger"):
        entry = build_ledger_entry(
            deployment_id="deployment:a9e54a85d12d",
            cycle_id="cycle-1",
            intent=_FakeLPOpenIntent(),
            result=_FakeLPOpenResult(),
            chain="base",
            success=True,
        )

    assert entry is not None
    assert "decimal_unit_guard" not in caplog.text
