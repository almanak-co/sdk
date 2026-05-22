"""Hyperliquid connector compiler surface tests."""

from decimal import Decimal

from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import PerpCloseIntent, PerpOpenIntent


def _compiler() -> IntentCompiler:
    return IntentCompiler(
        chain="hyperliquid",
        wallet_address="0x0000000000000000000000000000000000000001",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


def test_perp_open_fails_explicitly_for_offchain_execution_lane() -> None:
    intent = PerpOpenIntent(
        market="ETH-USD",
        collateral_token="USDC",
        collateral_amount=Decimal("100"),
        size_usd=Decimal("500"),
        is_long=True,
        leverage=Decimal("5"),
        protocol="hyperliquid",
    )

    result = _compiler().compile(intent)

    assert result.status == CompilationStatus.FAILED
    assert "off-chain signed orders" in (result.error or "")


def test_perp_close_fails_explicitly_for_offchain_execution_lane() -> None:
    intent = PerpCloseIntent(
        market="ETH-USD",
        collateral_token="USDC",
        is_long=True,
        protocol="hyperliquid",
    )

    result = _compiler().compile(intent)

    assert result.status == CompilationStatus.FAILED
    assert "off-chain signed orders" in (result.error or "")
