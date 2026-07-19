"""VIB-5896 — trade tape renders every coin of an N-coin fungible LP leg.

Pre-fix, ``_format_lp_direction`` read the 2-slot accounting payload / ledger
columns, so a Curve 3pool LP_OPEN of 100 DAI + 100 USDC + 100 USDT rendered as
"100 DAI + 100 USDC" — the blind dashboard-auditor read a $300 deposit as $200
(2 of 3 legs). The N-coin branch reads the receipt-parsed ``lp_open_data`` /
``lp_close_data`` (coin_symbols + amount0/amount1 + additional_amounts) off
``extracted_data_json`` and renders all N pool-coin-ordered legs.
"""

from __future__ import annotations

import json
from unittest.mock import patch

from almanak.framework.dashboard.gateway_client import TradeTapeRow
from almanak.framework.dashboard.pages.trade_tape import _format_ncoin_lp_direction


def _row(*, intent_type: str, extracted: dict) -> TradeTapeRow:
    return TradeTapeRow(
        id="led-1",
        cycle_id="cyc-1",
        timestamp=None,
        intent_type=intent_type,
        token_in="DAI",
        amount_in="100",
        token_out="USDC",
        amount_out="100",
        effective_price="",
        slippage_bps=0.0,
        gas_used=0,
        gas_usd="",
        tx_hash="0xabc",
        chain="ethereum",
        protocol="curve",
        success=True,
        error="",
        amount_in_usd="",
        amount_out_usd="",
        extracted_data_json=json.dumps(extracted),
        price_inputs_json="",
        pre_state_json="",
        post_state_json="",
        accounting_payload_json="",
        accounting_event_type="",
        position_key="",
        confidence="HIGH",
        unavailable_reason="",
        schema_version=1,
        formula_version=1,
        matching_policy_version=1,
        position_event_json="",
        position_id="",
        position_event_type="",
    )


def _patch_decimals(decimals_by_symbol: dict[str, int]):
    """Patch the tape's token-decimals lookup used by ``_scale_lp_amount``."""
    return patch(
        "almanak.framework.dashboard.pages.trade_tape._try_token_decimals",
        side_effect=lambda symbol, chain: decimals_by_symbol.get(symbol),
    )


def test_three_coin_open_renders_all_three_legs():
    extracted = {
        "lp_open_data": {
            "_type": "LPOpenData",
            "amount0": "100000000000000000000",  # 100 DAI (18 dec)
            "amount1": "100000000",  # 100 USDC (6 dec)
            "additional_amounts": {"2": "100000000"},  # 100 USDT (6 dec)
            "coin_symbols": ["DAI", "USDC", "USDT"],
        }
    }
    with _patch_decimals({"DAI": 18, "USDC": 6, "USDT": 6}):
        html = _format_ncoin_lp_direction(_row(intent_type="LP_OPEN", extracted=extracted), is_close=False)
    assert html is not None
    for sym in ("DAI", "USDC", "USDT"):
        assert sym in html, f"missing {sym} leg — the 2-of-3 tape bug (VIB-5896)"
    assert html.count("100.00") == 3


def test_three_coin_close_renders_all_three_legs():
    extracted = {
        "lp_close_data": {
            "_type": "LPCloseData",
            "amount0_collected": "100000000000000000000",
            "amount1_collected": "100000000",
            "additional_amounts": {"2": "100046000"},
            "coin_symbols": ["DAI", "USDC", "USDT"],
        }
    }
    with _patch_decimals({"DAI": 18, "USDC": 6, "USDT": 6}):
        html = _format_ncoin_lp_direction(_row(intent_type="LP_CLOSE", extracted=extracted), is_close=True)
    assert html is not None
    assert "USDT" in html
    assert "100.05" in html  # 100.046 rounds to 2dp in the human formatter


def test_unmeasured_third_leg_renders_dash_not_zero():
    """Empty ≠ Zero — a null (unmeasured) leg renders as an em-dash, never 0."""
    extracted = {
        "lp_open_data": {
            "_type": "LPOpenData",
            "amount0": "100000000000000000000",
            "amount1": "100000000",
            "additional_amounts": {"2": None},
            "coin_symbols": ["DAI", "USDC", "USDT"],
        }
    }
    with _patch_decimals({"DAI": 18, "USDC": 6, "USDT": 6}):
        html = _format_ncoin_lp_direction(_row(intent_type="LP_OPEN", extracted=extracted), is_close=False)
    assert html is not None
    assert "—" in html


def test_two_coin_venue_falls_through_to_canonical_path():
    extracted = {
        "lp_open_data": {
            "_type": "LPOpenData",
            "amount0": "100000000",
            "amount1": "50000000000000000",
            "coin_symbols": ["USDC", "WETH"],
        }
    }
    assert _format_ncoin_lp_direction(_row(intent_type="LP_OPEN", extracted=extracted), is_close=False) is None


def test_no_extracted_data_falls_through():
    assert _format_ncoin_lp_direction(_row(intent_type="LP_OPEN", extracted={}), is_close=False) is None


def test_misaligned_additional_amounts_falls_through_not_dash():
    """A malformed payload (3 coin_symbols but no index-2 entry) must fall back
    to the canonical path — NOT render a '—' that dresses incomplete receipt
    data up as intentionally unmeasured (CodeRabbit on PR #3329). Contrast with
    an index PRESENT with a null value, which IS the honest unmeasured case."""
    for additional in (None, {}, {"3": "100000000"}, {"2": "1", "4": "2"}):
        extracted = {
            "lp_open_data": {
                "_type": "LPOpenData",
                "amount0": "100000000000000000000",
                "amount1": "100000000",
                "coin_symbols": ["DAI", "USDC", "USDT"],
            }
        }
        if additional is not None:
            extracted["lp_open_data"]["additional_amounts"] = additional
        assert _format_ncoin_lp_direction(_row(intent_type="LP_OPEN", extracted=extracted), is_close=False) is None


def test_unresolvable_decimals_renders_raw_never_misscaled():
    extracted = {
        "lp_open_data": {
            "_type": "LPOpenData",
            "amount0": "100000000000000000000",
            "amount1": "100000000",
            "additional_amounts": {"2": "100000000"},
            "coin_symbols": ["DAI", "USDC", "XYZ"],
        }
    }
    with _patch_decimals({"DAI": 18, "USDC": 6}):  # XYZ unresolvable
        html = _format_ncoin_lp_direction(_row(intent_type="LP_OPEN", extracted=extracted), is_close=False)
    assert html is not None
    assert "100000000</code> XYZ" in html  # raw, honest — never a guessed scale
