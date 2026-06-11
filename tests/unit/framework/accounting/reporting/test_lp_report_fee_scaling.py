"""VIB-5036: build_lp_report scales raw-by-contract fee columns to human units.

position_events ``fees_token0`` / ``fees_token1`` are persisted RAW (smallest
unit). The LP report must scale them via the token decimals before summing /
displaying, else it renders e.g. ``75817134186 WETH`` instead of
``7.58e-8 WETH`` (the VIB-4780 symptom).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.accounting.reporting.loader import AccountingData
from almanak.framework.accounting.reporting.lp_report import _scale_fee, build_lp_report


def _data(position_events: list[dict]) -> AccountingData:
    return AccountingData(
        deployment_id="deployment:test",
        metrics=None,
        ledger_entries=[],
        position_events=position_events,
        snapshot=None,
    )


def test_scale_fee_scales_raw_to_human() -> None:
    # 75817134186 raw WETH (18 dp) -> 7.5817134186e-8
    assert _scale_fee("75817134186", "WETH", "arbitrum") == Decimal("75817134186") / Decimal(10**18)
    # 148 raw USDC (6 dp) -> 0.000148
    assert _scale_fee("148", "USDC", "arbitrum") == Decimal("0.000148")


def test_scale_fee_empty_is_zero_not_raw() -> None:
    # Empty != Zero: unmeasured contributes nothing.
    assert _scale_fee("", "WETH", "arbitrum") == Decimal("0")
    assert _scale_fee(None, "WETH", "arbitrum") == Decimal("0")


def test_scale_fee_unresolvable_token_drops_leg_not_poison() -> None:
    # No chain -> can't resolve decimals -> DROP the leg (Decimal 0), never
    # return the raw ~1e18 value which would poison the summed fee total.
    assert _scale_fee("148", "USDC", "") == Decimal("0")
    assert _scale_fee("75817134186", "NOTATOKEN", "arbitrum") == Decimal("0")


def test_scale_fee_malformed_resolver_decimals_drops_leg(monkeypatch) -> None:
    """A resolver result with decimals=None or decimals<0 drops the leg (0),
    never a mis-scaled / raw value (CodeRabbit regression pin)."""
    import almanak.framework.data.tokens.resolver as resolver_mod

    class _Info:
        def __init__(self, decimals):
            self.decimals = decimals

    class _Resolver:
        def __init__(self, decimals):
            self._d = decimals

        def resolve(self, *_a, **_k):
            return _Info(self._d)

    for bad in (None, -1):
        monkeypatch.setattr(resolver_mod, "get_token_resolver", lambda d=bad: _Resolver(d))
        assert _scale_fee("75817134186", "WETH", "arbitrum") == Decimal("0")


def test_build_lp_report_sums_human_fees() -> None:
    """A CLOSE event with raw fees is summarised in human units, not raw."""
    events = [
        {
            "position_id": "1",
            "position_type": "LP",
            "protocol": "uniswap_v3",
            "chain": "arbitrum",
            "token0": "WETH",
            "token1": "USDC",
            "event_type": "CLOSE",
            "value_usd": "100",
            "fees_token0": "75817134186",  # raw WETH (18 dp)
            "fees_token1": "148",  # raw USDC (6 dp)
        }
    ]
    section = build_lp_report(_data(events))
    assert len(section.positions) == 1
    summary = section.positions[0]
    # Scaled to human, NOT the raw integers.
    assert summary.fees_token0 == Decimal("75817134186") / Decimal(10**18)
    assert summary.fees_token1 == Decimal("0.000148")
    assert summary.fees_token0 < Decimal("1")


def test_build_lp_report_aggregates_fees_across_events() -> None:
    """Fees from multiple events (COLLECT_FEES + CLOSE) are summed in human units.

    The report accumulates fees across every COLLECT_FEES/CLOSE row for a
    position; this pins that the summation happens on SCALED (human) values,
    not raw — so two raw legs don't sum into a ~1e18 artefact.
    """
    base = {
        "position_id": "1",
        "position_type": "LP",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "WETH",
        "token1": "USDC",
    }
    events = [
        {**base, "event_type": "COLLECT_FEES", "value_usd": "50", "fees_token0": "50000000000", "fees_token1": "100"},
        {**base, "event_type": "CLOSE", "value_usd": "100", "fees_token0": "25817134186", "fees_token1": "48"},
    ]
    summary = build_lp_report(_data(events)).positions[0]
    expected0 = (Decimal("50000000000") + Decimal("25817134186")) / Decimal(10**18)
    expected1 = (Decimal("100") + Decimal("48")) / Decimal(10**6)
    assert summary.fees_token0 == expected0
    assert summary.fees_token1 == expected1
