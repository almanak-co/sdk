"""VIB-3883 — LP_OPEN PositionEvent stamps value_usd from price_oracle.

Closes the May 2 cascade: with ``position_events.value_usd`` empty,
``portfolio_valuer._enrich_lp_pnl`` produces ``cost_basis_usd=0``, which
makes ``deployed_capital_usd=$0`` on every snapshot — even after a
successful LP_OPEN. The "Open exposure" tile then renders $0 with an
open position, an obvious nonsense reading.

The fix populates ``value_usd`` at write time using the same
``price_oracle`` that already flows into the ledger writer. Failure
to price either leg leaves ``value_usd=""`` — the fail-closed contract
``compute_lp_cost_basis`` already enforces.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.observability.position_events import (
    PositionEvent,
    _apply_lp_open_value_usd,
    build_position_event_from_intent,
)


# ──────────────────────────────────────────────────────────────────────────
# _apply_lp_open_value_usd — direct unit coverage
# ──────────────────────────────────────────────────────────────────────────


def _make_lp_open_event(
    amount0="891556839636852",  # raw 18-dec WETH (≈ 0.000891 WETH)
    amount1="2294332",  # raw 6-dec USDC (≈ 2.294 USDC)
):
    return PositionEvent(
        deployment_id="d",
        position_id="5463956",
        position_type="LP",
        event_type="OPEN",
        protocol="uniswap_v3",
        chain="arbitrum",
        token0="WETH",
        token1="USDC",
        amount0=amount0,
        amount1=amount1,
    )


def test_value_usd_computed_from_flat_oracle():
    """Flat ``{symbol: price}`` shape → leg amounts × prices summed.

    The PositionEvent carries RAW on-chain integer amounts. The helper
    must scale by token decimals before multiplying, otherwise WETH at
    891556839636852 raw × $2301 = $2e18 of nonsense (the original H2 bug).
    """
    event = _make_lp_open_event()
    _apply_lp_open_value_usd(event, {"WETH": "2301.69", "USDC": "1.0001"}, chain="arbitrum")
    assert event.value_usd != ""
    # 0.000891557 × 2301.69 + 2.294332 × 1.0001 ≈ 2.052 + 2.295 ≈ $4.35
    assert Decimal("4.30") < Decimal(event.value_usd) < Decimal("4.40"), (
        f"expected ~$4.35; got {event.value_usd!r}. "
        "Pre-fix this asserted only != '' and silently let $2e18 through."
    )


def test_value_usd_computed_from_nested_oracle():
    """Canonical AttemptNo17 §1.2 G12 nested shape — same result."""
    event = _make_lp_open_event()
    nested = {
        "WETH": {"price_usd": "2301.69", "oracle_source": "coingecko"},
        "USDC": {"price_usd": "1.0001", "oracle_source": "chainlink"},
    }
    _apply_lp_open_value_usd(event, nested, chain="arbitrum")
    assert Decimal("4.30") < Decimal(event.value_usd) < Decimal("4.40")


def test_value_usd_accepts_human_decimal_amounts_too():
    """Backwards-compat: an upstream caller that already wrote
    ``amount0`` in human-decimal form (fractional) is detected by the
    integer test and used verbatim — the helper does not double-scale."""
    event = _make_lp_open_event(
        amount0="0.000891556839636852",  # human-readable
        amount1="2.294332",  # human-readable
    )
    _apply_lp_open_value_usd(event, {"WETH": "2301.69", "USDC": "1.0001"}, chain="arbitrum")
    assert Decimal("4.30") < Decimal(event.value_usd) < Decimal("4.40")


def test_value_usd_unset_when_one_token_unpriceable():
    """Fail-closed: missing price for one leg → ``value_usd`` stays empty."""
    event = _make_lp_open_event()
    _apply_lp_open_value_usd(event, {"WETH": "2301.69"}, chain="arbitrum")
    assert event.value_usd == ""


def test_value_usd_unset_when_oracle_empty():
    event = _make_lp_open_event()
    _apply_lp_open_value_usd(event, {}, chain="arbitrum")
    assert event.value_usd == ""


def test_value_usd_unset_when_amount_missing():
    """Missing amount field → no value_usd; the upstream ``_apply_lp_open``
    couldn't populate them, so we have nothing to multiply."""
    event = _make_lp_open_event(amount0="", amount1="")
    _apply_lp_open_value_usd(event, {"WETH": "2301.69", "USDC": "1.0001"}, chain="arbitrum")
    assert event.value_usd == ""


def test_value_usd_unset_when_decimals_unknown():
    """Fail-closed: unknown chain → token resolver can't find decimals
    → helper refuses to emit a wildly mis-scaled value. Better silent
    NULL than 1e12-off."""
    event = _make_lp_open_event()
    _apply_lp_open_value_usd(event, {"WETH": "2301.69", "USDC": "1.0001"}, chain="madeup-chain")
    assert event.value_usd == ""


def test_value_usd_skipped_for_close_events():
    """The helper only fires for LP OPEN — close events have their own
    ``value_usd`` semantic (proceeds returned)."""
    event = _make_lp_open_event()
    event.event_type = "CLOSE"
    _apply_lp_open_value_usd(event, {"WETH": "2301.69", "USDC": "1.0001"}, chain="arbitrum")
    assert event.value_usd == ""


def test_value_usd_does_not_overwrite_pre_set_field():
    event = _make_lp_open_event()
    event.value_usd = "100"  # set by some upstream path
    _apply_lp_open_value_usd(event, {"WETH": "2301.69", "USDC": "1.0001"}, chain="arbitrum")
    assert event.value_usd == "100"


def test_value_usd_handles_decimal_oracle_values():
    """Decimal-typed oracle values (the new VIB-3885 helper output) work."""
    event = _make_lp_open_event()
    _apply_lp_open_value_usd(
        event, {"WETH": Decimal("2301.69"), "USDC": Decimal("1.0001")}, chain="arbitrum"
    )
    assert Decimal("4.30") < Decimal(event.value_usd) < Decimal("4.40")


def test_value_usd_handles_lowercase_symbol_lookup():
    event = _make_lp_open_event()
    event.token0 = "weth"  # mixed case
    _apply_lp_open_value_usd(event, {"WETH": "2301.69", "USDC": "1.0001"}, chain="arbitrum")
    # Helper upper-cases symbols before lookup; this should still hit.
    assert event.value_usd != ""
    assert Decimal("4.30") < Decimal(event.value_usd) < Decimal("4.40")


def test_value_usd_rejected_on_non_finite_price():
    event = _make_lp_open_event()
    _apply_lp_open_value_usd(event, {"WETH": "NaN", "USDC": "1.0001"}, chain="arbitrum")
    assert event.value_usd == ""


# ──────────────────────────────────────────────────────────────────────────
# Builder integration — price_oracle flows through to value_usd
# ──────────────────────────────────────────────────────────────────────────


def _make_intent_and_result_for_lp_open() -> tuple[Any, Any]:
    """Mock LPOpenIntent + result that mirrors the May 2 successful run."""
    lp_open_data = SimpleNamespace(
        position_id=5463956,
        liquidity="928906698473",
        tick_lower=-199960,
        tick_upper=-197960,
        amount0=891556839636852,  # raw int
        amount1=2294332,
    )
    intent = SimpleNamespace(
        intent_type=SimpleNamespace(value="LP_OPEN"),
        protocol="uniswap_v3",
        chain="arbitrum",
        token0="WETH",
        token1="USDC",
        pool="WETH/USDC/500",
    )
    result = SimpleNamespace(
        extracted_data={"lp_open_data": lp_open_data},
        position_id=5463956,
        transaction_results=[SimpleNamespace(tx_hash="0xabc")],
        gas_cost_usd="0.012",
    )
    return intent, result


def test_builder_populates_value_usd_when_oracle_provided():
    """Builder integration: prices flow through to value_usd at the
    correct magnitude. End-to-end mirror of the May 2 LP_OPEN scenario.
    """
    intent, result = _make_intent_and_result_for_lp_open()

    event = build_position_event_from_intent(
        deployment_id="d",
        intent=intent,
        result=result,
        ledger_entry_id="led-1",
        chain="arbitrum",
        price_oracle={"WETH": "2301.69", "USDC": "1.0001"},
    )
    assert event is not None
    assert event.event_type == "OPEN"
    assert event.position_type == "LP"
    # Raw amounts from the receipt parser are 891556839636852 (WETH 18-dec,
    # ≈ 0.000891 WETH) and 2294332 (USDC 6-dec, ≈ 2.294 USDC).
    # Scaled and priced: 0.000891 * 2301.69 + 2.294 * 1.0001 ≈ $4.35.
    assert event.value_usd != "", (
        "VIB-3883: price_oracle MUST flow through to PositionEvent.value_usd "
        "so portfolio_snapshots.deployed_capital_usd reflects open positions"
    )
    assert Decimal("4.30") < Decimal(event.value_usd) < Decimal("4.40"), (
        f"VIB-3883: expected ~$4.35 wallet NAV from May 2 reproducer; "
        f"got {event.value_usd!r}. Magnitude bug: helper isn't scaling "
        f"raw on-chain integers by token decimals before pricing."
    )


def test_builder_leaves_value_usd_empty_without_oracle():
    """Backwards compat: callers that don't pass an oracle keep the
    pre-VIB-3883 behaviour (value_usd empty)."""
    intent, result = _make_intent_and_result_for_lp_open()
    event = build_position_event_from_intent(
        deployment_id="d",
        intent=intent,
        result=result,
        ledger_entry_id="led-1",
        chain="arbitrum",
        # price_oracle omitted
    )
    assert event is not None
    assert event.value_usd == ""
