"""Tests for the decimals-aware decimal-unit soft-fail guard (VIB-4780 / W1-5).

Complements ``test_decimal_unit_soft_fail.py`` which covers the original
magnitude-only rule.  The cases here pin the decimals-aware rule to the
canonical Appendix B bug fixtures and exercise multi-connector / cross-chain
shape coverage that the prompt mandates.

These tests must demonstrate:

1. The exact production-bug values from §B.4 LP-2 trigger the warning.
2. Legitimate human-form Decimal values do NOT trigger.
3. The Prometheus counter ``accounting_raw_wei_suspected_total`` increments
   once per suspect field.
4. The guard is soft-fail end-to-end: writes complete, no exception raised.
5. Multi-connector / cross-chain coverage (Uniswap V3 LP, Aerodrome LP, Enso
   SWAP across Arbitrum / Base / Ethereum).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.accounting.decimal_guards import (
    _check_decimal_unit_soft_fail,
)
from almanak.framework.observability.metrics import (
    ACCOUNTING_RAW_WEI_SUSPECTED_TOTAL,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _counter_value(*, chain: str, field: str, event_type: str, token_symbol: str) -> float:
    """Read the current value of the raw-wei counter for a label tuple."""
    return ACCOUNTING_RAW_WEI_SUSPECTED_TOTAL.labels(
        chain=chain.lower(),
        field=field,
        event_type=event_type,
        token_symbol=token_symbol,
    )._value.get()


# ---------------------------------------------------------------------------
# 1. Canonical bug-value fixtures (§B.4 LP-2, §B.4 LP-3)
# ---------------------------------------------------------------------------


def test_canonical_weth_fees_token0_raw_wei_triggers(caplog: pytest.LogCaptureFixture) -> None:
    """WETH ``fees_token0 = "75817134186"`` (the canonical production bug) → flag."""
    payload = {"fees_token0": "75817134186"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="lp-close-001",
            event_type="LP_CLOSE",
            chain="arbitrum",
            token_decimals_map={"token0": 18},
            token_symbols_map={"token0": "WETH"},
        )
    assert count == 1
    assert "decimal_unit_guard" in caplog.text
    assert "WETH" in caplog.text
    assert "fees_token0" in caplog.text
    # Human-form interpretation in warning so the on-call sees the magnitude.
    assert "human_form_if_raw_wei" in caplog.text
    # 75817134186 / 10**18 = 7.58e-8
    assert "rule=decimals_aware_" in caplog.text


def test_canonical_usdc_fees_token1_raw_wei_triggers(caplog: pytest.LogCaptureFixture) -> None:
    """USDC ``fees_token1 = "148"`` (canonical bug; raw 6-dp) → flag.

    Critically, this value is 148 — below any magnitude threshold.  The
    decimals-aware rule is the ONLY rule that can catch it.
    """
    payload = {"fees_token1": "148"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="lp-close-002",
            event_type="LP_CLOSE",
            chain="arbitrum",
            token_decimals_map={"token1": 6},
            token_symbols_map={"token1": "USDC"},
        )
    assert count == 1
    assert "decimal_unit_guard" in caplog.text
    assert "USDC" in caplog.text
    assert "rule=decimals_aware_" in caplog.text


def test_canonical_weth_ledger_amount_in_raw_wei_triggers(caplog: pytest.LogCaptureFixture) -> None:
    """Ledger ``amount_in = "701279299182337"`` WETH raw-wei (canonical §B.4 LP-3) → flag."""
    payload = {"amount_in": "701279299182337"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="ledger-001",
            event_type="LP_OPEN",
            chain="arbitrum",
            token_decimals_map={"in": 18},
            token_symbols_map={"in": "WETH"},
        )
    assert count == 1
    assert "decimal_unit_guard" in caplog.text


def test_canonical_usdc_ledger_amount_out_raw_wei_triggers(caplog: pytest.LogCaptureFixture) -> None:
    """Ledger ``amount_out = "1585552"`` USDC raw-wei (= $1.59) → flag.

    Below the 10^12 magnitude threshold; only the decimals-aware rule catches it.
    """
    payload = {"amount_out": "1585552"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="ledger-002",
            event_type="LP_OPEN",
            chain="arbitrum",
            token_decimals_map={"out": 6},
            token_symbols_map={"out": "USDC"},
        )
    assert count == 1


# ---------------------------------------------------------------------------
# 2. Negative cases — legitimate human Decimals must NOT trip
# ---------------------------------------------------------------------------


def test_legit_human_decimal_usdc_fee_no_trip(caplog: pytest.LogCaptureFixture) -> None:
    """``Decimal("0.000148")`` for USDC fees (the real-world value) → no warning."""
    payload = {"fees_token1": str(Decimal("0.000148"))}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="evt-legit-1",
            event_type="LP_CLOSE",
            chain="arbitrum",
            token_decimals_map={"token1": 6},
            token_symbols_map={"token1": "USDC"},
        )
    assert count == 0
    assert "decimal_unit_guard" not in caplog.text


def test_legit_human_decimal_swap_usd_amount_no_trip(caplog: pytest.LogCaptureFixture) -> None:
    """``Decimal("4.50")`` for a $4.50 SWAP → no warning."""
    payload = {"amount_out": str(Decimal("4.50"))}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="evt-legit-2",
            event_type="SWAP",
            chain="ethereum",
            token_decimals_map={"out": 6},
            token_symbols_map={"out": "USDC"},
        )
    assert count == 0


def test_measured_zero_no_trip(caplog: pytest.LogCaptureFixture) -> None:
    """``Decimal("0")`` is a measured zero, not raw-wei → no warning (Empty ≠ Zero)."""
    payload = {"fees_token0": str(Decimal("0")), "fees_token1": str(Decimal("0"))}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="evt-legit-3",
            event_type="LP_CLOSE",
            chain="base",
            token_decimals_map={"token0": 18, "token1": 6},
            token_symbols_map={"token0": "WETH", "token1": "USDC"},
        )
    assert count == 0


def test_unmeasured_none_no_trip(caplog: pytest.LogCaptureFixture) -> None:
    """``None`` is unmeasured → no warning."""
    payload: dict[str, Any] = {"fees_token0": None, "amount_in": None}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="evt-legit-4",
            event_type="LP_CLOSE",
            chain="arbitrum",
            token_decimals_map={"token0": 18, "in": 18},
            token_symbols_map={"token0": "WETH", "in": "WETH"},
        )
    assert count == 0


def test_legit_large_usdc_integer_amount_below_threshold_no_trip(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Legit human-integer ``"10000"`` USDC amount_in → no warning.

    A $10k USDC swap is a normal production amount.  Under the post-VIB-4885
    threshold (``10 ** (decimals - 1) = 100000`` for USDC 6dp), an integer
    of 10000 falls *below* the amount-class decimals-aware threshold and
    must NOT be flagged.  Pins the gemini-code-assist VIB-4885 review fix.
    """
    payload = {"amount_in": "10000"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="evt-legit-10k-usdc",
            event_type="SWAP",
            chain="ethereum",
            token_decimals_map={"in": 6},
            token_symbols_map={"in": "USDC"},
        )
    assert count == 0
    assert "decimal_unit_guard" not in caplog.text


def test_canonical_usdc_just_above_threshold_still_caught(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """USDC ``amount_out = "100000"`` (== threshold) and any value above it
    still flags as decimals-aware suspect.  Below threshold (e.g. 99999)
    must NOT flag — see boundary test above.
    """
    payload = {"amount_out": "100000"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="evt-boundary-usdc",
            event_type="SWAP",
            chain="ethereum",
            token_decimals_map={"out": 6},
            token_symbols_map={"out": "USDC"},
        )
    assert count == 1


def test_legit_large_human_weth_amount_no_trip(caplog: pytest.LogCaptureFixture) -> None:
    """A real big-but-decimal WETH amount (``Decimal("12.5")``) → no warning.

    Has a decimal point → not integer-shaped → decimals-aware rule skips it,
    and magnitude is far below the fallback threshold.
    """
    payload = {"amount_in": str(Decimal("12.5"))}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="evt-legit-5",
            event_type="SWAP",
            chain="arbitrum",
            token_decimals_map={"in": 18},
            token_symbols_map={"in": "WETH"},
        )
    assert count == 0


# ---------------------------------------------------------------------------
# 3. Metric emission — counter must increment per suspect field
# ---------------------------------------------------------------------------


def test_metric_counter_increments_on_detection() -> None:
    """``accounting_raw_wei_suspected_total`` increments once per suspect field."""
    label_kwargs = dict(
        chain="arbitrum",
        field="fees_token0",
        event_type="LP_CLOSE",
        token_symbol="WETH",
    )
    before = _counter_value(**label_kwargs)

    _check_decimal_unit_soft_fail(
        {"fees_token0": "75817134186"},
        event_id="metric-evt-1",
        event_type="LP_CLOSE",
        chain="arbitrum",
        token_decimals_map={"token0": 18},
        token_symbols_map={"token0": "WETH"},
    )

    after = _counter_value(**label_kwargs)
    assert after - before == pytest.approx(1.0)


def test_metric_counter_increments_per_field() -> None:
    """Two suspect fields → counter increments twice (one per (field, token))."""
    before0 = _counter_value(
        chain="arbitrum",
        field="fees_token0",
        event_type="LP_CLOSE",
        token_symbol="WETH",
    )
    before1 = _counter_value(
        chain="arbitrum",
        field="fees_token1",
        event_type="LP_CLOSE",
        token_symbol="USDC",
    )

    count = _check_decimal_unit_soft_fail(
        {"fees_token0": "75817134186", "fees_token1": "148"},
        event_id="metric-evt-2",
        event_type="LP_CLOSE",
        chain="arbitrum",
        token_decimals_map={"token0": 18, "token1": 6},
        token_symbols_map={"token0": "WETH", "token1": "USDC"},
    )
    assert count == 2

    after0 = _counter_value(
        chain="arbitrum",
        field="fees_token0",
        event_type="LP_CLOSE",
        token_symbol="WETH",
    )
    after1 = _counter_value(
        chain="arbitrum",
        field="fees_token1",
        event_type="LP_CLOSE",
        token_symbol="USDC",
    )
    assert after0 - before0 == pytest.approx(1.0)
    assert after1 - before1 == pytest.approx(1.0)


def test_metric_counter_does_not_increment_on_legit_value() -> None:
    """Legit human-form Decimal → counter unchanged."""
    label_kwargs = dict(
        chain="base",
        field="fees_token1",
        event_type="LP_CLOSE",
        token_symbol="USDC",
    )
    before = _counter_value(**label_kwargs)
    _check_decimal_unit_soft_fail(
        {"fees_token1": "0.000148"},
        event_id="metric-evt-3",
        event_type="LP_CLOSE",
        chain="base",
        token_decimals_map={"token1": 6},
        token_symbols_map={"token1": "USDC"},
    )
    after = _counter_value(**label_kwargs)
    assert after == before


# ---------------------------------------------------------------------------
# 4. Soft-fail discipline: no raise; write completes
# ---------------------------------------------------------------------------


def test_soft_fail_returns_count_no_raise(caplog: pytest.LogCaptureFixture) -> None:
    """Guard returns suspicious count; never raises, even on hostile input.

    The payload mixes:
      * ``object()`` — un-stringifiable in any sane Decimal sense → SKIPPED.
      * ``float("inf")`` — parses to ``Decimal("Infinity")`` → SKIPPED (non-finite).
      * ``"not-a-number"`` — InvalidOperation → SKIPPED.
      * ``"148"`` USDC fees_token1 (raw 6-dp) — canonical bug, must FLAG.
    """
    weird_payload: dict[str, Any] = {
        "fees_token0": object(),
        "fees_token1": "148",  # canonical raw-wei USDC fees bug
        "amount_in": float("inf"),
        "amount_out": "not-a-number",
    }
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            weird_payload,
            event_id="soft-fail-1",
            event_type="LP_CLOSE",
            chain="arbitrum",
            token_decimals_map={"token0": 18, "token1": 6, "in": 18, "out": 18},
            token_symbols_map={"token0": "WETH", "token1": "USDC", "in": "WETH", "out": "WETH"},
        )
    # Behavior-specific: exactly the one parseable + suspect field (fees_token1)
    # is flagged.  Hostile inputs are silently skipped without raising.
    assert count == 1, (
        f"Expected exactly 1 suspect field (fees_token1 raw-wei USDC); got {count}."
    )
    assert "fees_token1" in caplog.text
    assert "USDC" in caplog.text
    assert "soft-fail-1" in caplog.text


def test_soft_fail_nan_decimal_no_raise() -> None:
    """``Decimal("NaN")`` is non-finite; guard must skip silently, not raise."""
    payload: dict[str, Any] = {
        "amount_in": Decimal("NaN"),
        "amount_out": Decimal("Infinity"),
        "fees_token0": Decimal("-Infinity"),
    }
    # Must complete without raising despite hostile non-finite Decimals.
    count = _check_decimal_unit_soft_fail(
        payload,
        event_id="nan-1",
        event_type="LP_CLOSE",
        chain="arbitrum",
        token_decimals_map={"token0": 18, "in": 18, "out": 18},
    )
    assert count == 0


def test_soft_fail_non_int_decimals_no_raise() -> None:
    """Bogus ``decimals`` types (str, float, bool, negative) must NOT raise.

    The guard validates the decimals operand as ``int and >= 0`` and falls
    through to the magnitude rule on any other shape.
    """
    payload = {"amount_in": "701279299182337"}  # WETH raw-wei, magnitude-rule catches it
    # str decimals — should be ignored, magnitude rule still fires.
    assert (
        _check_decimal_unit_soft_fail(
            payload,
            event_id="bogus-1",
            event_type="SWAP",
            chain="arbitrum",
            token_decimals_map={"in": "18"},  # type: ignore[dict-item]
        )
        == 1
    )
    # Negative decimals — ignored, fall through to magnitude rule.
    assert (
        _check_decimal_unit_soft_fail(
            payload,
            event_id="bogus-2",
            event_type="SWAP",
            chain="arbitrum",
            token_decimals_map={"in": -5},
        )
        == 1
    )
    # Boolean — Python `True` is `isinstance(_, int)` but semantically wrong.
    assert (
        _check_decimal_unit_soft_fail(
            payload,
            event_id="bogus-3",
            event_type="SWAP",
            chain="arbitrum",
            token_decimals_map={"in": True},  # type: ignore[dict-item]
        )
        == 1
    )


# ---------------------------------------------------------------------------
# 5. Multi-connector / cross-chain coverage
#
# These exercise the SAME heuristic against representative payload shapes
# from different connectors and chains.  Per the prompt's "scalable designs
# proven across multiple protocols/chains" emphasis, the guard must catch
# the raw-wei shape regardless of the connector that produced it.
# ---------------------------------------------------------------------------


_RAW_WEI_FIXTURES = [
    # (connector_label, chain, event_type, payload, decimals_map, symbols_map, expected_suspect_count)
    pytest.param(
        "uniswap_v3",
        "arbitrum",
        "LP_OPEN",
        {"amount_in": "701279299182337", "amount_out": "1585552"},
        {"in": 18, "out": 6},
        {"in": "WETH", "out": "USDC"},
        2,
        id="uniswap_v3_lp_open_arbitrum",
    ),
    pytest.param(
        "uniswap_v3",
        "arbitrum",
        "LP_CLOSE",
        {"fees_token0": "75817134186", "fees_token1": "148"},
        {"token0": 18, "token1": 6},
        {"token0": "WETH", "token1": "USDC"},
        2,
        id="uniswap_v3_lp_close_arbitrum",
    ),
    pytest.param(
        "aerodrome",
        "base",
        "LP_OPEN",
        {"amount0_in": "23456789012345", "amount1_in": "789456"},
        {"token0": 18, "token1": 6},
        {"token0": "WETH", "token1": "USDC"},
        2,
        id="aerodrome_lp_open_base",
    ),
    pytest.param(
        "aerodrome",
        "base",
        "LP_CLOSE",
        {"fees_token0": "918273645", "fees_token1": "12345"},
        {"token0": 18, "token1": 6},
        {"token0": "WETH", "token1": "USDC"},
        2,
        id="aerodrome_lp_close_base",
    ),
    pytest.param(
        "enso",
        "ethereum",
        "SWAP",
        {"amount_in": "1234567", "amount_out": "765432109876543"},
        {"in": 6, "out": 18},
        {"in": "USDC", "out": "WETH"},
        2,
        id="enso_swap_ethereum",
    ),
    pytest.param(
        "uniswap_v3",
        "ethereum",
        "SWAP",
        # amount_in 1.23e13 wei WETH (~1.23e-5 WETH human, magnitude-caught);
        # amount_out 1.5e6 raw-wei USDC (= $1.50 human, decimals-aware caught
        # because >= 10^5 USDC threshold).
        {"amount_in": "12345678901234", "amount_out": "1585552"},
        {"in": 18, "out": 6},
        {"in": "WETH", "out": "USDC"},
        2,
        id="uniswap_v3_swap_ethereum",
    ),
]


@pytest.mark.parametrize(
    "connector,chain,event_type,payload,decimals_map,symbols_map,expected",
    _RAW_WEI_FIXTURES,
)
def test_multi_connector_raw_wei_caught(
    connector: str,
    chain: str,
    event_type: str,
    payload: dict[str, Any],
    decimals_map: dict[str, int],
    symbols_map: dict[str, str],
    expected: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Same heuristic must catch raw-wei across Uniswap V3, Aerodrome, Enso
    on Arbitrum, Base, and Ethereum."""
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id=f"{connector}-{chain}-{event_type}",
            event_type=event_type,
            chain=chain,
            token_decimals_map=decimals_map,
            token_symbols_map=symbols_map,
        )
    assert count == expected, (
        f"{connector}/{chain}/{event_type}: expected {expected} suspect fields, got {count}"
    )
    assert "decimal_unit_guard" in caplog.text


# ---------------------------------------------------------------------------
# 6. Cross-chain legitimate-value coverage — must NOT trip
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "chain,token_symbol,decimals,human_value",
    [
        ("arbitrum", "USDC", 6, "0.000148"),
        ("base", "USDC", 6, "4.50"),
        ("ethereum", "WETH", 18, "0.001130"),
        ("arbitrum", "WETH", 18, "12.5"),
        ("ethereum", "USDC", 6, "0"),  # measured zero
    ],
)
def test_cross_chain_legit_values_no_warning(
    chain: str,
    token_symbol: str,
    decimals: int,
    human_value: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Legitimate human-form Decimal values across chains must not trip the guard."""
    payload = {"amount_in": human_value}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id=f"legit-{chain}-{token_symbol}",
            event_type="SWAP",
            chain=chain,
            token_decimals_map={"in": decimals},
            token_symbols_map={"in": token_symbol},
        )
    assert count == 0, f"{chain}/{token_symbol}/{human_value}: expected 0 suspect, got {count}"


# ---------------------------------------------------------------------------
# 7. Decimals-unknown fallback — magnitude rule still runs
# ---------------------------------------------------------------------------


def test_decimals_unknown_falls_back_to_magnitude_rule(caplog: pytest.LogCaptureFixture) -> None:
    """When decimals are not supplied, the 10^12 magnitude rule still catches huge values."""
    payload = {"amount_in": "1000000000000"}  # 10^12 exactly → at-threshold
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="fallback-1",
            event_type="SWAP",
        )
    assert count >= 1
    assert "rule=magnitude" in caplog.text


def test_decimals_unknown_small_raw_wei_NOT_caught(caplog: pytest.LogCaptureFixture) -> None:
    """Without decimals supplied, the small ``148`` raw-wei is invisible (acceptable false-negative).

    This documents the trade-off in the heuristic: the decimals-aware rule
    is the only one that catches small raw-wei.  Call sites SHOULD plumb
    decimals when available.  W3-1 will tighten this.
    """
    payload = {"fees_token1": "148"}
    with caplog.at_level(logging.WARNING):
        count = _check_decimal_unit_soft_fail(
            payload,
            event_id="fallback-2",
            event_type="LP_CLOSE",
        )
    # Documented limitation: without decimals, 148 is indistinguishable
    # from a legit small integer count.  No false-positive at the cost
    # of this false-negative; W3-1 protocol-aware shape check addresses it.
    assert count == 0


# ---------------------------------------------------------------------------
# 8. Wiring tests — exercise the guard through the actual AccountingWriter
#    chokepoints (build_ledger_entry + position_events._decimal_unit_soft_fail).
#
# These verify:
#  (a) build_ledger_entry's lazy resolver gate fires only on integer-shaped
#      amounts (happy path stays free of resolver init cost).
#  (b) Resolver miss falls back to magnitude rule rather than blocking write.
#  (c) position_events._decimal_unit_soft_fail threads token0/token1 decimals
#      through and never raises even when the resolver is degraded.
# ---------------------------------------------------------------------------


def test_build_ledger_entry_skips_resolver_for_decimal_amounts(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: legitimate Decimal-string amount_in/out → no resolver hit, no warning.

    The lazy gate ``needs_decimals_lookup = any(_is_integer_shaped(v) ...)``
    in ``build_ledger_entry`` must skip the resolver entirely for human-form
    Decimal strings.  Spy the resolver and assert it was NEVER invoked — that
    is the actual invariant this test exists to pin (CodeRabbit review on the
    initial test asked for proof, not just absence-of-warning).
    """
    from types import SimpleNamespace

    import almanak.framework.data.tokens.resolver as resolver_mod
    from almanak.framework.execution.extracted_data import SwapAmounts
    from almanak.framework.observability.ledger import build_ledger_entry

    def _unexpected_resolver() -> object:
        # Raising AssertionError immediately surfaces a regression with the
        # exact "happy path touched the resolver" diagnostic.  build_ledger_entry
        # wraps the get_token_resolver() call in a try/except so the assertion
        # would be swallowed at runtime — instead track invocations via a
        # mutable container and assert the count is zero after the write.
        nonlocal_calls.append("called")
        raise AssertionError(
            "get_token_resolver should NOT be called for decimal-form amounts"
        )

    nonlocal_calls: list[str] = []
    monkeypatch.setattr(resolver_mod, "get_token_resolver", _unexpected_resolver)

    intent = SimpleNamespace(intent_type=SimpleNamespace(value="SWAP"), protocol="")
    swap = SwapAmounts(
        amount_in=1130000000000000,  # raw wei (1.13e15 = 0.00113 WETH)
        amount_out=4500000,  # raw 6dp USDC = 4.50
        amount_in_decimal=Decimal("0.001130"),  # human-form (what ledger reads)
        amount_out_decimal=Decimal("4.50"),
        effective_price=Decimal("3982.30"),
        token_in="WETH",
        token_out="USDC",
    )
    result = SimpleNamespace(
        tx_hash="0xhappy",
        gas_used=0,
        success=True,
        swap_amounts=swap,
        extracted_data={},
    )
    with caplog.at_level(logging.WARNING):
        entry = build_ledger_entry(
            deployment_id="depl-1",
            cycle_id="cyc-1",
            intent=intent,
            result=result,
            chain="ethereum",
        )
    assert entry.intent_type == "SWAP"
    # No raw-wei warning should fire on a legitimate Decimal-string write.
    assert "decimal_unit_guard" not in caplog.text
    # The lazy gate actually skipped the resolver.
    assert nonlocal_calls == [], (
        f"Resolver was invoked unexpectedly; lazy gate broken: {nonlocal_calls}"
    )


def test_build_ledger_entry_flags_raw_wei_swap(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end via build_ledger_entry: raw-wei amount_out triggers warning + counter.

    Threads the token resolver via monkeypatch so the test does not depend on
    the global token registry's real entries.  Verifies the chokepoint actually
    plumbs symbols + decimals + chain through to the guard.
    """
    from types import SimpleNamespace

    from almanak.framework.execution.extracted_data import SwapAmounts
    from almanak.framework.observability import ledger as ledger_mod

    # Stub token resolver to return decimals deterministically.
    class _FakeInfo:
        def __init__(self, decimals: int) -> None:
            self.decimals = decimals

    class _FakeResolver:
        def resolve(self, symbol: str, chain: str | None = None):  # noqa: ARG002
            return _FakeInfo(18) if symbol == "WETH" else _FakeInfo(6)

    import almanak.framework.data.tokens.resolver as resolver_mod

    monkeypatch.setattr(resolver_mod, "get_token_resolver", lambda: _FakeResolver())

    intent = SimpleNamespace(intent_type=SimpleNamespace(value="SWAP"), protocol="")
    swap = SwapAmounts(
        amount_in=1130000000000000,
        amount_out=1585552,
        amount_in_decimal=Decimal("0.001130"),
        amount_out_decimal=Decimal("1585552"),  # raw-wei in human field — the bug
        effective_price=None,
        token_in="WETH",
        token_out="USDC",
    )
    result = SimpleNamespace(
        tx_hash="0xraw_wei",
        gas_used=0,
        success=True,
        swap_amounts=swap,
        extracted_data={},
    )
    # Snapshot metric counter before write.
    before = _counter_value(
        chain="ethereum", field="amount_out", event_type="SWAP", token_symbol="USDC"
    )
    with caplog.at_level(logging.WARNING):
        entry = ledger_mod.build_ledger_entry(
            deployment_id="depl-rw",
            cycle_id="cyc-rw",
            intent=intent,
            result=result,
            chain="ethereum",
        )
    after = _counter_value(
        chain="ethereum", field="amount_out", event_type="SWAP", token_symbol="USDC"
    )
    # Entry built; guard did not break the write.
    assert entry.intent_type == "SWAP"
    # Warning fired with the right token + field labels.
    assert "decimal_unit_guard" in caplog.text
    assert "USDC" in caplog.text
    assert "amount_out" in caplog.text
    # Prometheus counter incremented exactly once for this field/token tuple.
    assert after - before == 1


def test_build_ledger_entry_resolver_failure_falls_back_to_magnitude(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolver miss / raise must NOT block the write; magnitude rule still runs.

    Pins the soft-fail contract on the resolver path: a broken resolver
    downgrades to magnitude-only detection, not a halt.
    """
    from types import SimpleNamespace

    from almanak.framework.execution.extracted_data import SwapAmounts
    from almanak.framework.observability import ledger as ledger_mod

    class _BrokenResolver:
        def resolve(self, symbol: str, chain: str | None = None):  # noqa: ARG002
            raise RuntimeError("resolver unavailable")

    import almanak.framework.data.tokens.resolver as resolver_mod

    monkeypatch.setattr(resolver_mod, "get_token_resolver", lambda: _BrokenResolver())

    intent = SimpleNamespace(intent_type=SimpleNamespace(value="SWAP"), protocol="")
    # WETH-scale raw-wei (7e14) — caught by magnitude rule even without decimals.
    swap = SwapAmounts(
        amount_in=701279299182337,
        amount_out=0,
        amount_in_decimal=Decimal("701279299182337"),  # raw-wei in human field — the bug
        amount_out_decimal=Decimal("0"),
        effective_price=None,
        token_in="WETH",
        token_out="USDC",
    )
    result = SimpleNamespace(
        tx_hash="0xfallback",
        gas_used=0,
        success=True,
        swap_amounts=swap,
        extracted_data={},
    )
    with caplog.at_level(logging.WARNING):
        entry = ledger_mod.build_ledger_entry(
            deployment_id="depl-fb",
            cycle_id="cyc-fb",
            intent=intent,
            result=result,
            chain="ethereum",
        )
    # Write succeeded (no raise) and magnitude rule still warned.
    assert entry.intent_type == "SWAP"
    assert "decimal_unit_guard" in caplog.text
    assert "rule=magnitude" in caplog.text


def test_position_events_guard_wiring_removed() -> None:
    """VIB-5036: the decimal-unit guard is no longer wired over position_events.

    position_events ``amount0`` / ``amount1`` / ``fees_token0`` / ``fees_token1``
    are RAW-by-contract (NAV valuation, hydration, and the attribution lane read
    them as raw and scale at point-of-use), so the human-form guard only ever
    produced false warnings there. The ``_decimal_unit_soft_fail`` wiring helper
    was removed; the guard stays active on the genuinely-human
    ``transaction_ledger`` via ``build_ledger_entry`` (covered above).
    """
    from almanak.framework.observability import position_events as pe_mod

    assert not hasattr(pe_mod, "_decimal_unit_soft_fail")
