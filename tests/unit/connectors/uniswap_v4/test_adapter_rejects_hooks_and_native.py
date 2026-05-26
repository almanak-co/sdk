"""VIB-4475: V4 adapter compile-time guards for hooks≠0 and native-ETH currency.

V0 (VIB-4426) supports only hookless ERC20-ERC20 pools. The adapter must fail
loud BEFORE any transaction is built — soft-error empty bundles would let the
strategy proceed past compilation and surface the error in a less obvious place.

Salt is intentionally NOT validated (VIB-4426 §Q7: salt = bytes32(tokenId) is
the canonical PositionManager._mint path and is always non-zero for a minted
position). A regression that rejects non-zero salt would break every real LP
open — there is no test that asserts rejection of salt.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
    UniswapV4UnsupportedPoolError,
)


# =============================================================================
# Fixtures
# =============================================================================


def _make_resolver():
    """Mock token resolver that also exposes the native-ETH zero address."""
    resolver = MagicMock()

    def resolve_for_swap(symbol, chain):
        tokens = {
            "WETH": MagicMock(address="0x82af49447d8a07e3bd95bd0d56f35241523fbab1", decimals=18, is_native=False),
            "USDC": MagicMock(address="0xaf88d065e77c8cc2239327c5edb3a432268e5831", decimals=6, is_native=False),
            "ETH": MagicMock(address="0x0000000000000000000000000000000000000000", decimals=18, is_native=True),
        }
        return tokens[symbol.upper()]

    def resolve(symbol_or_addr, chain):
        return resolve_for_swap(symbol_or_addr, chain)

    resolver.resolve_for_swap = resolve_for_swap
    resolver.resolve = resolve
    return resolver


@pytest.fixture()
def adapter():
    resolver = _make_resolver()
    config = UniswapV4Config(
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
    )
    return UniswapV4Adapter(config=config, token_resolver=resolver)


# =============================================================================
# LP_OPEN guards
# =============================================================================


class TestLPOpenRejectsHooks:
    """compile_lp_open_intent must reject non-zero hooks address."""

    def test_rejects_non_zero_hooks(self, adapter):
        from almanak.framework.intents.vocabulary import LPOpenIntent

        intent = LPOpenIntent(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.1"),
            amount1=Decimal("200"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            protocol="uniswap_v4",
            protocol_params={
                # Any non-zero hooks address triggers V0 scope rejection.
                "hooks": "0x0000000000000000000000000000000000000800",
            },
        )
        price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}

        with pytest.raises(UniswapV4UnsupportedPoolError) as exc_info:
            adapter.compile_lp_open_intent(intent, price_oracle)

        msg = str(exc_info.value)
        assert "hook" in msg.lower(), "Error must explain it's a hooks problem"
        assert "V0" in msg, "Error must cite V0 scope"
        assert "VIB-4485" in msg, "Error must cite the V1 lifting ticket VIB-4485"
        assert "P-V1-D" in msg, "Error must cite the P-V1-D placeholder code"

    def test_rejects_uppercase_hooks_address(self, adapter):
        """Case-insensitive: an EIP-55 checksummed non-zero hooks still rejects."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        intent = LPOpenIntent(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.1"),
            amount1=Decimal("200"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            protocol="uniswap_v4",
            protocol_params={
                "hooks": "0x000000000000000000000000000000000000ABCD",
            },
        )
        with pytest.raises(UniswapV4UnsupportedPoolError):
            adapter.compile_lp_open_intent(intent)


class TestLPOpenRejectsNativeETH:
    """compile_lp_open_intent must reject native-ETH currency pools."""

    def test_rejects_eth_pool(self, adapter):
        from almanak.framework.intents.vocabulary import LPOpenIntent

        # ETH/USDC: ETH resolves to address(0) for V4 → currency0 == 0x0 after sort.
        intent = LPOpenIntent(
            pool="ETH/USDC/3000",
            amount0=Decimal("0.1"),
            amount1=Decimal("200"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            protocol="uniswap_v4",
        )
        price_oracle = {"ETH": Decimal("2000"), "USDC": Decimal("1")}

        with pytest.raises(UniswapV4UnsupportedPoolError) as exc_info:
            adapter.compile_lp_open_intent(intent, price_oracle)

        msg = str(exc_info.value)
        assert "native" in msg.lower() or "eth" in msg.lower(), "Error must explain it's a native-ETH problem"
        assert "V0" in msg, "Error must cite V0 scope"
        assert "VIB-4483" in msg, "Error must cite the V1 lifting ticket VIB-4483"
        assert "P-V1-B" in msg, "Error must cite the P-V1-B placeholder code"


# =============================================================================
# LP_CLOSE guards
# =============================================================================


class TestLPCloseRejectsNativeETH:
    """compile_lp_close_intent must reject native-ETH currency0 leg."""

    def test_rejects_native_currency0(self, adapter):
        from almanak.framework.intents.vocabulary import LPCloseIntent

        intent = LPCloseIntent(
            position_id="42",
            protocol="uniswap_v4",
        )
        with pytest.raises(UniswapV4UnsupportedPoolError) as exc_info:
            adapter.compile_lp_close_intent(
                intent,
                liquidity=1_000_000,
                currency0="0x0000000000000000000000000000000000000000",
                currency1="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            )

        msg = str(exc_info.value)
        assert "native" in msg.lower() or "eth" in msg.lower(), "Error must explain it's a native-ETH problem"
        assert "VIB-4483" in msg
        assert "P-V1-B" in msg

    def test_close_with_erc20_currencies_does_not_raise_guard(self, adapter):
        """Regression: ERC20-ERC20 close must NOT raise UniswapV4UnsupportedPoolError."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        intent = LPCloseIntent(
            position_id="42",
            protocol="uniswap_v4",
        )
        # Should complete without raising the V0 guard. Other paths are
        # exercised by existing tests; here we only assert no guard fires.
        bundle = adapter.compile_lp_close_intent(
            intent,
            liquidity=1_000_000,
            currency0="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            currency1="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        )
        assert bundle.intent_type == "LP_CLOSE"
        assert len(bundle.transactions) == 1


# =============================================================================
# Salt non-rejection (VIB-4426 §Q7) — guard against a future regression
# =============================================================================


class TestSaltNotRejected:
    """Locked-in regression: the V0 guard must never look at salt.

    Per VIB-4426 §Q7, salt = bytes32(tokenId) is the canonical
    PositionManager._mint path. The guard helper's docstring is the
    architectural contract here; this test catches any drift that would
    accidentally reject non-zero salt.
    """

    def test_guard_helper_does_not_inspect_salt(self):
        """If a 'salt' field ever appears on PoolKey, the guard must not read it."""
        from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter

        # A duck-typed PoolKey stand-in WITHOUT a salt attribute. If the guard
        # tries to read salt, it would AttributeError. Hookless ERC20-ERC20
        # input must pass cleanly.
        class _FakePoolKey:
            hooks = "0x0000000000000000000000000000000000000000"
            currency0 = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
            currency1 = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"

        # Should not raise — and crucially should not touch salt.
        UniswapV4Adapter._reject_unsupported_v0_pool(_FakePoolKey())
