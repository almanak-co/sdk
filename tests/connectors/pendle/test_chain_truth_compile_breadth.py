"""Pendle chain-truth compile-breadth proof (VIB-5324).

The companion guard ``test_chain_truth_matrix_alignment.py`` asserts the
*manifest* never advertises a chain the compiler cannot build on. This module
goes one step further and exercises the compile-time *resolution helpers* on
each advertised chain to prove the breadth claim end-to-end at the data layer:

1. Every advertised ``strategy_chains`` chain resolves a PT market address and
   PT/YT token-info for a buy-PT SWAP (the helpers the compiler dereferences).
2. The Ethereum long-dated ``PT-stETH-30DEC2027`` market — the VIB-5324 demo
   roll target — resolves through those same helpers (the durable, Anvil-
   fundable wstETH/stETH market that replaces the maturing Arbitrum
   PT-wstETH-25JUN2026 demo market).
3. ``plasma`` clears the compiler's chain allowlist + has market data, i.e. it
   is compile-true today even though it is intentionally NOT advertised yet
   (pending intent-test coverage — VIB-5328). This documents the real
   compile-truth set {arbitrum, ethereum, plasma} so the breadth claim is
   honest about what compiles vs. what is advertised.

These are pure, offline resolution checks (no RPC / no gateway): they assert the
static market registry the compiler reads is internally consistent for the
advertised universe. On-chain execution proof for the Ethereum roll lives in the
managed-Anvil run captured in ``tests/reports/pendle_breadth_*``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.pendle import compiler as cp
from almanak.connectors.pendle.compiler import _check_pendle_chain_supported, _resolve_pendle_market
from almanak.connectors.pendle.connector import CONNECTOR
from almanak.connectors.pendle.sdk import (
    MARKET_BY_PT_TOKEN,
    MARKET_BY_YT_TOKEN,
    MARKET_TOKEN_MINT_SY,
    PT_TOKEN_INFO,
    YT_TOKEN_INFO,
)
from almanak.framework.intents.vocabulary import SwapIntent

# Long-dated Ethereum wstETH/stETH market — the VIB-5324 demo roll target.
# On-chain-verified via market.readTokens() / expiry() (expiry 2027-12-30).
ETH_STETH_MARKET = "0x34280882267ffa6383B363E278B027Be083bBe3b"
ETH_STETH_PT = "0xb253Eff1104802b97aC7E3aC9FdD73AecE295a2c"
ETH_STETH_YT = "0x04B7Fa1e727d7290D6E24fA9b426d0c940283a95"
ETH_WSTETH = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"

# A representative buy-PT token per advertised chain. Each must resolve a market
# and PT/YT token-info, or a real strategy on that chain fails at compile time.
_ADVERTISED_PROBE_PT = {
    "arbitrum": "PT-wstETH",
    "ethereum": "PT-stETH-30DEC2027",
}


def _mock_compiler(chain: str) -> MagicMock:
    compiler = MagicMock(name="MockPendleCompiler")
    compiler.chain = chain
    # Force the PT/YT static-info fallback (the registry path under test) rather
    # than a generic token resolution.
    compiler._resolve_token.return_value = None
    return compiler


def _buy_pt_intent(pt_token: str) -> SwapIntent:
    return SwapIntent(from_token="WSTETH", to_token=pt_token, amount_usd=Decimal("100"))


@pytest.mark.parametrize("chain", sorted(CONNECTOR.strategy_chains or ()))
def test_every_advertised_chain_resolves_a_pt_market(chain: str) -> None:
    """Each advertised chain must resolve a PT market + PT info for a buy-PT swap."""
    probe = _ADVERTISED_PROBE_PT.get(chain)
    assert probe is not None, (
        f"Advertised chain {chain!r} has no probe PT token in this test — add a "
        f"representative buy-PT token so the breadth claim stays proven."
    )

    # 1. Compiler chain allowlist accepts the chain.
    assert _check_pendle_chain_supported(MagicMock(chain=chain), "probe", "probe") is None

    # 2. Market resolves for the buy-PT side.
    market = _resolve_pendle_market(_buy_pt_intent(probe), _mock_compiler(chain), "buying_pt")
    assert isinstance(market, str) and market.startswith("0x"), (
        f"{probe} did not resolve a market on {chain}: {market!r}"
    )

    # 3. The PT must be resolvable as a token through the same fallback the
    # compiler uses (exercised on the sell side, where the PT is the from_token).
    sell_pt = cp._resolve_pendle_from_token(
        _mock_compiler(chain),
        SwapIntent(from_token=probe, to_token="WSTETH", amount_usd=Decimal("100")),
    )
    assert sell_pt is not None, f"PT {probe} not resolvable as a token on {chain}"
    assert sell_pt.address.startswith("0x")


def test_ethereum_steth_market_resolves_end_to_end() -> None:
    """The VIB-5324 Ethereum stETH roll target resolves across every registry map."""
    chain = "ethereum"
    for name in ("PT-stETH-30DEC2027", "PT-wstETH-30DEC2027"):
        assert MARKET_BY_PT_TOKEN[chain][name].lower() == ETH_STETH_MARKET.lower()
        assert PT_TOKEN_INFO[chain][name][0].lower() == ETH_STETH_PT.lower()
        assert PT_TOKEN_INFO[chain][name][1] == 18
    for name in ("YT-stETH-30DEC2027", "YT-wstETH-30DEC2027"):
        assert MARKET_BY_YT_TOKEN[chain][name].lower() == ETH_STETH_MARKET.lower()
        assert YT_TOKEN_INFO[chain][name][0].lower() == ETH_STETH_YT.lower()
    # SY mint token is wstETH (Anvil-fundable, an accepted getTokensIn input) so
    # from_token=WSTETH == tokenMintSy and no V3 pre-swap is inserted.
    assert MARKET_TOKEN_MINT_SY[chain][ETH_STETH_MARKET.lower()].lower() == ETH_WSTETH.lower()

    # And it resolves through the live compile helper for a buy-PT swap.
    market = _resolve_pendle_market(_buy_pt_intent("PT-stETH-30DEC2027"), _mock_compiler(chain), "buying_pt")
    assert market.lower() == ETH_STETH_MARKET.lower()


def test_plasma_is_compile_true_but_unadvertised() -> None:
    """plasma compiles today (allowlist + market data) but is not advertised.

    Documents the real compile-truth set {arbitrum, ethereum, plasma} so the
    breadth claim is honest: plasma advertising is gated on intent-test coverage
    (VIB-5328), not on a missing compile path.
    """
    assert _check_pendle_chain_supported(MagicMock(chain="plasma"), "probe", "probe") is None
    assert bool(PT_TOKEN_INFO.get("plasma")) and bool(MARKET_BY_PT_TOKEN.get("plasma"))
    assert bool(YT_TOKEN_INFO.get("plasma")) and bool(MARKET_BY_YT_TOKEN.get("plasma"))
    # ...yet it is deliberately absent from the advertised manifest.
    assert "plasma" not in (CONNECTOR.strategy_chains or ())
