"""Unit tests for Curve metapool routing in the compiler (VIB-5419).

Covers the two pure module-level resolver helpers extracted from
``compile_swap`` / ``compile_lp_open`` during the CRAP-gate refactor:

- ``_resolve_swap_pool_and_route`` — native vs metapool-underlying SWAP routing.
- ``_resolve_lp_open_amounts`` — native vs metapool-underlying LP_OPEN deposit.

Plus a couple of end-to-end ``compile_swap`` / ``compile_lp_open`` guards using
the ``_StubContext`` pattern (mirrors ``test_compiler_lp_open_coin_amounts.py``)
to prove the parent wiring dispatches to the right adapter path.

No chain: the adapter falls back to deterministic estimates when no rpc/gateway
is configured.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors.curve.adapter import CURVE_POOLS
from almanak.connectors.curve.compiler import (
    CurveCompiler,
    _resolve_lp_open_amounts,
    _resolve_swap_pool_and_route,
)
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import LPOpenIntent, SwapIntent

CHAIN = "ethereum"
ETH_POOLS = CURVE_POOLS["ethereum"]
META = ETH_POOLS["frax_3crv"]
META_ADDR = META["address"]
THREEPOOL_ADDR = ETH_POOLS["3pool"]["address"]
WALLET = "0x1234567890123456789012345678901234567890"

# Token addresses for the stub resolver / price service.
_ADDR = {
    "FRAX": "0x853d955aCEf822Db058eb8505911ED77F175b99e",
    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    "3CRV": "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
}


# =============================================================================
# _resolve_swap_pool_and_route (pure helper)
# =============================================================================


class TestResolveSwapPoolAndRoute:
    def test_native_pair_3pool(self) -> None:
        addr, name, underlying = _resolve_swap_pool_and_route("USDC", "DAI", {}, ETH_POOLS)
        assert addr == ETH_POOLS["3pool"]["address"]
        assert name == "3pool"
        assert underlying is False

    def test_metapool_underlying_pair(self) -> None:
        # No NATIVE pool carries FRAX/USDT -> metapool combined space does.
        addr, name, underlying = _resolve_swap_pool_and_route("FRAX", "USDT", {}, ETH_POOLS)
        assert addr == META_ADDR
        assert name == "frax_3crv"
        assert underlying is True

    def test_explicit_pool_underlying(self) -> None:
        # Explicit metapool + a pair on the combined space (FRAX/USDC) -> underlying.
        addr, name, underlying = _resolve_swap_pool_and_route("FRAX", "USDC", {"pool": META_ADDR}, ETH_POOLS)
        assert addr == META_ADDR
        assert underlying is True

    def test_explicit_pool_native_pair_stays_native(self) -> None:
        # Explicit metapool + the NATIVE pair (FRAX/3CRV) -> NOT underlying.
        addr, _name, underlying = _resolve_swap_pool_and_route("FRAX", "3CRV", {"pool": META_ADDR}, ETH_POOLS)
        assert addr == META_ADDR
        assert underlying is False

    def test_no_pool_found(self) -> None:
        # WBTC + FRAX never co-occur in any ethereum pool (native or combined).
        addr, name, underlying = _resolve_swap_pool_and_route("WBTC", "FRAX", {}, ETH_POOLS)
        assert addr is None
        assert name == ""
        assert underlying is False

    def test_ambiguous_metapool_raises(self) -> None:
        # Two metapools whose combined spaces both carry FRAX/USDC -> ambiguous.
        pools = {
            "meta_a": dict(META, address="0x" + "a" * 40),
            "meta_b": dict(META, address="0x" + "b" * 40),
        }
        with pytest.raises(ValueError, match="[Aa]mbiguous"):
            _resolve_swap_pool_and_route("FRAX", "USDC", {}, pools)


# =============================================================================
# _resolve_lp_open_amounts (pure helper)
# =============================================================================


def _lp_intent(pool: str, *, coin_amounts: list[Decimal] | None = None, a0: str = "0", a1: str = "0") -> LPOpenIntent:
    return LPOpenIntent(
        pool=pool,
        amount0=Decimal(a0),
        amount1=Decimal(a1),
        range_lower=Decimal("1"),
        range_upper=Decimal("2"),
        protocol="curve",
        chain=CHAIN,
        coin_amounts=coin_amounts,
    )


class TestResolveLpOpenAmounts:
    def test_underlying_deposit_combined_length(self) -> None:
        intent = _lp_intent("frax_3crv", coin_amounts=[Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0")])
        result = _resolve_lp_open_amounts(intent, META, "frax_3crv", META_ADDR, CHAIN)
        assert isinstance(result, tuple)
        amounts, is_underlying = result
        assert is_underlying is True
        assert amounts == [Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0")]

    def test_native_deposit_n_coins_length(self) -> None:
        intent = _lp_intent("frax_3crv", coin_amounts=[Decimal("100"), Decimal("50")])
        result = _resolve_lp_open_amounts(intent, META, "frax_3crv", META_ADDR, CHAIN)
        assert isinstance(result, tuple)
        amounts, is_underlying = result
        assert is_underlying is False
        assert amounts == [Decimal("100"), Decimal("50")]

    def test_wrong_length_returns_error_string_with_hint(self) -> None:
        # length 3 != native 2 and != combined 4 -> error string with the zap hint.
        intent = _lp_intent("frax_3crv", coin_amounts=[Decimal("1"), Decimal("1"), Decimal("1")])
        result = _resolve_lp_open_amounts(intent, META, "frax_3crv", META_ADDR, CHAIN)
        assert isinstance(result, str)
        assert "(or 4 for an underlying deposit via the zap)" in result

    def test_legacy_two_slot_fallback(self) -> None:
        # Non-metapool 3pool, no coin_amounts, amount0/amount1 set -> zero-tail-filled.
        pool_data = ETH_POOLS["3pool"]
        intent = _lp_intent("3pool", a0="100", a1="200")
        result = _resolve_lp_open_amounts(intent, pool_data, "3pool", THREEPOOL_ADDR, CHAIN)
        assert isinstance(result, tuple)
        amounts, is_underlying = result
        assert is_underlying is False
        assert amounts == [Decimal("100"), Decimal("200"), Decimal("0")]


# =============================================================================
# End-to-end parent wiring (compile_swap / compile_lp_open)
# =============================================================================


@dataclass
class _StubToken:
    symbol: str
    address: str
    decimals: int = 18

    def to_dict(self) -> dict[str, Any]:
        return {"symbol": self.symbol, "address": self.address, "decimals": self.decimals}


class _StubServices:
    """Minimal services satisfying compile_swap's resolve_token / require_token_price."""

    def resolve_token(self, token: str) -> _StubToken:
        sym = token.upper()
        decimals = 6 if sym in ("USDC", "USDT") else 18
        return _StubToken(symbol=sym, address=_ADDR.get(sym, token), decimals=decimals)

    def require_token_price(self, symbol: str) -> Decimal:
        # All combined-space coins are USD stables.
        return Decimal("1")


@dataclass
class _StubContext:
    chain: str = CHAIN
    wallet_address: str = WALLET
    rpc_url: str | None = None
    gateway_client: Any = None
    services: Any = None


class TestCompileMetapoolE2E:
    def test_compile_swap_routes_underlying(self) -> None:
        """FRAX -> USDT compiles to an exchange_underlying bundle on the metapool."""
        intent = SwapIntent(
            from_token="FRAX",
            to_token="USDT",
            amount=Decimal("50"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN,
        )
        ctx = _StubContext(services=_StubServices())
        result = CurveCompiler().compile_swap(ctx, intent)
        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["pool_address"].lower() == META_ADDR.lower()
        # exchange_underlying selector present on a tx targeting the metapool.
        datas = [tx["data"] for tx in result.action_bundle.transactions if tx["to"].lower() == META_ADDR.lower()]
        assert any(d.startswith("0xa6417ed6") for d in datas), datas

    def test_compile_lp_open_routes_underlying(self) -> None:
        """coin_amounts of combined length 4 compiles to a zap add_liquidity bundle."""
        intent = LPOpenIntent(
            pool="frax_3crv",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            coin_amounts=[Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0")],
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="curve",
            chain=CHAIN,
        )
        ctx = _StubContext(services=_StubServices())
        result = CurveCompiler().compile_lp_open(ctx, intent)
        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle is not None
        zap_addr = META["zap_address"].lower()
        datas = [tx["data"] for tx in result.action_bundle.transactions if tx["to"].lower() == zap_addr]
        assert any(d.startswith("0x384e03db") for d in datas), datas

    def test_compile_lp_open_native_two_coin(self) -> None:
        """coin_amounts of native length 2 compiles to a direct metapool add_liquidity."""
        intent = LPOpenIntent(
            pool="frax_3crv",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            coin_amounts=[Decimal("100"), Decimal("100")],
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="curve",
            chain=CHAIN,
        )
        ctx = _StubContext(services=_StubServices())
        result = CurveCompiler().compile_lp_open(ctx, intent)
        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle is not None
        # Native add_liquidity(uint256[2]) targets the metapool itself, NOT the zap.
        zap_addr = META["zap_address"].lower()
        assert all(tx["to"].lower() != zap_addr for tx in result.action_bundle.transactions)
