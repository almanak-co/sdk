"""Tests for strategy profile detection on dashboard detail page."""

from decimal import Decimal

from almanak.framework.dashboard.models import LPPosition, PositionSummary, Strategy, StrategyStatus
from almanak.framework.dashboard.pages.detail import _detect_strategy_profile


def _strategy(protocol: str, position: PositionSummary | None = None) -> Strategy:
    return Strategy(
        id="s1",
        name="Test",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("0"),
        chain="arbitrum",
        protocol=protocol,
        position=position,
    )


def test_detect_strategy_profile_lp_from_position():
    position = PositionSummary(
        lp_positions=[
            LPPosition(
                pool="WETH/USDC",
                token0="WETH",
                token1="USDC",
                liquidity_usd=Decimal("1000"),
                range_lower=Decimal("1800"),
                range_upper=Decimal("2200"),
                current_price=Decimal("2000"),
                in_range=True,
            )
        ]
    )
    assert _detect_strategy_profile(_strategy("Uniswap V3", position)) == "LP"


def test_detect_strategy_profile_lending_from_protocol():
    assert _detect_strategy_profile(_strategy("Aave V3")) == "LENDING"


def test_detect_strategy_profile_perps_from_leverage():
    position = PositionSummary(leverage=Decimal("3.0"))
    assert _detect_strategy_profile(_strategy("GMX", position)) == "PERPS"


def test_detect_strategy_profile_defaults_to_ta():
    assert _detect_strategy_profile(_strategy("Uniswap V3")) == "TA"


def test_detect_strategy_profile_substring_lookalike_is_ta():
    """The kind-derived sets use exact matching: a protocol that merely
    CONTAINS "perp"/"aave" as a substring must not classify (plan 027 —
    the old substring sniff classified "spare_finance" as PERPS)."""
    assert _detect_strategy_profile(_strategy("spare_finance")) == "TA"
    assert _detect_strategy_profile(_strategy("megaave_yield")) == "TA"


def test_detect_strategy_profile_hyperliquid_is_perps():
    """hyperliquid declares no perps_read; coverage must come from its
    manifest kind=PERP (plan 027 — a read-registry-derived set would
    silently reclassify hyperliquid PERPS -> TA)."""
    assert _detect_strategy_profile(_strategy("hyperliquid")) == "PERPS"
    assert _detect_strategy_profile(_strategy("Hyperliquid")) == "PERPS"


def test_detect_strategy_profile_comma_separated_protocols():
    """Comma-separated multi-protocol strings classify on any segment."""
    assert _detect_strategy_profile(_strategy("uniswap_v3, aave_v3")) == "LENDING"
