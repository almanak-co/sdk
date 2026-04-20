"""Tests for LST token registry entries and silent-zero balance guards.

Covers VIB-2324, VIB-2325, VIB-2326:
- stETH and rETH are resolvable by symbol and by address (VIB-2326)
- market.balance() raises BalanceUnavailableError for unregistered address with 0 balance (VIB-2324)
- market.balance() returns 0 for registered address with 0 balance (no false positive)
- fund_tokens logs actionable error for address-based keys not in registry (VIB-2325)
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.market_snapshot import BalanceUnavailableError, MarketSnapshot
from almanak.framework.data.tokens import get_token_resolver
from almanak.framework.data.tokens.exceptions import TokenNotFoundError


# =============================================================================
# VIB-2326: stETH and rETH in registry
# =============================================================================


class TestLSTTokenRegistry:
    """stETH and rETH should be resolvable from the default token registry."""

    def test_steth_resolves_by_symbol_on_ethereum(self):
        """stETH resolves by symbol on Ethereum. ResolvedToken.symbol is uppercased by design."""
        resolver = get_token_resolver()
        token = resolver.resolve("stETH", "ethereum")
        assert token.symbol == "STETH"  # ResolvedToken normalizes symbols to uppercase
        assert token.address == "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"
        assert token.decimals == 18

    def test_reth_resolves_by_symbol_on_ethereum(self):
        """rETH resolves by symbol on Ethereum. ResolvedToken.symbol is uppercased by design."""
        resolver = get_token_resolver()
        token = resolver.resolve("rETH", "ethereum")
        assert token.symbol == "RETH"  # ResolvedToken normalizes symbols to uppercase
        assert token.address == "0xae78736cd615f374d3085123a210448e74fc6393"
        assert token.decimals == 18

    def test_steth_resolves_by_address_on_ethereum(self):
        """stETH resolves by its Ethereum address."""
        resolver = get_token_resolver()
        token = resolver.resolve("0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", "ethereum")
        assert token.symbol == "STETH"
        assert token.decimals == 18

    def test_reth_resolves_by_address_on_ethereum(self):
        """rETH resolves by its Ethereum address."""
        resolver = get_token_resolver()
        token = resolver.resolve("0xae78736Cd615f374D3085123A210448E74Fc6393", "ethereum")
        assert token.symbol == "RETH"
        assert token.decimals == 18

    def test_steth_not_on_arbitrum(self):
        """stETH has no entry on Arbitrum — should raise TokenNotFoundError."""
        resolver = get_token_resolver()
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("stETH", "arbitrum")

    def test_steth_decimals_are_18(self):
        """get_decimals() should return 18 for stETH on Ethereum."""
        resolver = get_token_resolver()
        assert resolver.get_decimals("ethereum", "stETH") == 18

    def test_reth_decimals_are_18(self):
        """get_decimals() should return 18 for rETH on Ethereum."""
        resolver = get_token_resolver()
        assert resolver.get_decimals("ethereum", "rETH") == 18

    def test_existing_lsts_still_resolve(self):
        """Previously-added LSTs (swETH, ankrETH, wstETH) should still resolve."""
        resolver = get_token_resolver()
        sweth = resolver.resolve("swETH", "ethereum")
        assert sweth.decimals == 18
        ankr = resolver.resolve("ankrETH", "ethereum")
        assert ankr.decimals == 18
        wsteth = resolver.resolve("wstETH", "ethereum")
        assert wsteth.decimals == 18


# =============================================================================
# VIB-2324: market.balance() silent-zero guard
# =============================================================================


def _make_market_snapshot(chain: str, balance_result: Decimal) -> MarketSnapshot:
    """Build a MarketSnapshot backed by a mock BalanceProvider returning balance_result."""
    from almanak.framework.data.interfaces import BalanceResult
    from datetime import UTC, datetime

    mock_provider = MagicMock()
    mock_provider.get_balance = AsyncMock(
        return_value=BalanceResult(
            balance=balance_result,
            token="dummy",
            address="0xwallet",
            decimals=18,
            raw_balance=0,
            timestamp=datetime.now(UTC),
            stale=False,
        )
    )
    return MarketSnapshot(
        chain=chain,
        wallet_address="0xdeadbeef1234567890abcdef1234567890abcdef",
        balance_provider=mock_provider,
    )


class TestBalanceSilentZeroGuard:
    """market.balance() must not silently return 0 for unregistered address-based tokens."""

    def test_unregistered_address_zero_balance_returns_zero_with_warning(self, caplog):
        """If balance is 0 and the address is not in the registry, return 0 (no exception).

        VIB-2364: Changed from raising BalanceUnavailableError to a logger.warning.
        Strategies that hold zero of an exotic unregistered token should keep
        running, not crash.  The warning provides visibility without breaking execution.
        """
        import logging

        # Use a clearly fake address that is definitely not in the registry
        fake_address = "0x000000000000000000000000000000000000dead"
        snapshot = _make_market_snapshot("ethereum", Decimal("0"))

        # Should NOT raise -- just emit a logger.warning and return 0
        with caplog.at_level(logging.WARNING):
            result = snapshot.balance(fake_address)

        assert result == Decimal("0")
        # Verify that a warning was emitted (VIB-2364 logging contract)
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any(
            fake_address.lower() in msg.lower() or "balance_zero_unregistered" in msg.lower()
            for msg in warning_messages
        ), (
            "Expected a logger.warning about the unregistered address, but none was found. "
            f"warnings: {warning_messages}"
        )

    def test_registered_address_zero_balance_returns_zero(self):
        """If balance is 0 but the address IS in the registry, return 0 (no false positive)."""
        # wstETH Ethereum address is in the registry
        wsteth_address = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
        snapshot = _make_market_snapshot("ethereum", Decimal("0"))
        result = snapshot.balance(wsteth_address)
        assert result == Decimal("0")

    def test_steth_address_zero_balance_returns_zero_after_registry_add(self):
        """stETH address is now in registry, so zero balance returns 0 without raising."""
        steth_address = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
        snapshot = _make_market_snapshot("ethereum", Decimal("0"))
        result = snapshot.balance(steth_address)
        assert result == Decimal("0")

    def test_reth_address_zero_balance_returns_zero_after_registry_add(self):
        """rETH address is now in registry, so zero balance returns 0 without raising."""
        reth_address = "0xae78736Cd615f374D3085123A210448E74Fc6393"
        snapshot = _make_market_snapshot("ethereum", Decimal("0"))
        result = snapshot.balance(reth_address)
        assert result == Decimal("0")

    def test_nonzero_balance_for_unregistered_address_returns_balance(self):
        """If balance is non-zero, return it even for unregistered addresses."""
        fake_address = "0x000000000000000000000000000000000000dead"
        snapshot = _make_market_snapshot("ethereum", Decimal("5.5"))
        result = snapshot.balance(fake_address)
        assert result == Decimal("5.5")

    def test_symbol_based_lookup_unaffected(self):
        """Symbol-based balance lookups are not affected by the address guard."""
        snapshot = _make_market_snapshot("ethereum", Decimal("0"))
        # USDC is not an address, so the guard doesn't apply
        result = snapshot.balance("USDC")
        assert result == Decimal("0")


# =============================================================================
# VIB-2325: TOKEN_DECIMALS entries for LST tokens
# =============================================================================


class TestTokenDecimalsLST:
    """TOKEN_DECIMALS in fork_manager should include common LST tokens."""

    def test_steth_in_token_decimals(self):
        from almanak.framework.anvil.fork_manager import TOKEN_DECIMALS

        assert "stETH" in TOKEN_DECIMALS
        assert TOKEN_DECIMALS["stETH"] == 18

    def test_reth_in_token_decimals(self):
        from almanak.framework.anvil.fork_manager import TOKEN_DECIMALS

        assert "rETH" in TOKEN_DECIMALS
        assert TOKEN_DECIMALS["rETH"] == 18

    def test_sweth_in_token_decimals(self):
        from almanak.framework.anvil.fork_manager import TOKEN_DECIMALS

        assert "swETH" in TOKEN_DECIMALS
        assert TOKEN_DECIMALS["swETH"] == 18

    def test_ankreth_in_token_decimals(self):
        from almanak.framework.anvil.fork_manager import TOKEN_DECIMALS

        assert "ankrETH" in TOKEN_DECIMALS
        assert TOKEN_DECIMALS["ankrETH"] == 18
