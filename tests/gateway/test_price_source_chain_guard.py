"""Chain-correctness guard on the per-chain-bound price sources (VIB-5651 §2, test 4).

This is the **safety crux** of the per-chain price-aggregator change. The primary
guarantee is routing (``MarketServiceServicer._aggregator_for`` picks a chain-correct
sub-aggregator — covered in ``test_market_service_multichain_provisioning.py``). This
file locks in the *defense-in-depth* second guarantee: even if a mis-route ever handed a
cross-chain ``ResolvedToken`` to a chain-bound source, the source must MISS
(``DataSourceUnavailable``) rather than silently answer with the wrong chain's price.

The failure this prevents is silent cross-chain corruption: an ``arbitrum`` Chainlink
instance answering a ``hyperevm``-tagged request with arbitrum's number — a wrong price
fed into teardown / slippage / accounting. ``DataSourceUnavailable`` is a non-error skip
in ``PriceAggregator`` (``aggregator.py``), so a guarded miss degrades cleanly to the
other, chain-valid sources instead of poisoning the median.
"""

from decimal import Decimal

import pytest

from almanak.core.chains import ChainRegistry
from almanak.framework.data.interfaces import DataSourceUnavailable, PriceResult
from almanak.framework.data.tokens import ResolvedToken
from almanak.framework.data.tokens.models import CHAIN_ID_MAP
from almanak.gateway.data.price.hyperevm import HypercoreOraclePriceSource
from almanak.gateway.data.price.onchain import OnChainPriceSource


def _resolved(chain: str, *, symbol: str = "ETH", is_stablecoin: bool = False) -> ResolvedToken:
    """A minimal ResolvedToken tagged to ``chain`` (only ``.chain`` matters to the guard).

    ``ResolvedToken.__post_init__`` canonicalizes ``chain`` (e.g. ``bnb`` -> ``bsc``) and
    then cross-checks ``chain_id`` against ``CHAIN_ID_MAP`` for the canonical name, so we
    look the id up by canonical name rather than hard-coding one.
    """
    desc = ChainRegistry.try_resolve(chain.lower())
    canonical = desc.name if desc is not None else chain.lower()
    return ResolvedToken(
        symbol=symbol,
        address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        decimals=18,
        chain=chain,
        chain_id=CHAIN_ID_MAP.get(canonical, 0),
        is_stablecoin=is_stablecoin,
    )


class TestOnChainSourceChainGuard:
    """``OnChainPriceSource`` (bound to one chain's Chainlink feeds) must reject a
    request tagged with a different chain — the load-bearing guard of §2."""

    @pytest.mark.asyncio
    async def test_cross_chain_resolved_token_raises_and_never_returns_bound_chain_price(self):
        """THE CRUX: an arbitrum on-chain source handed a hyperevm-tagged token
        MUST raise ``DataSourceUnavailable(chain_mismatch)`` and MUST NOT return
        arbitrum's price. The raise is the proof it never fell through to a read."""
        source = OnChainPriceSource(chain="arbitrum")
        rt = _resolved("hyperevm")

        with pytest.raises(DataSourceUnavailable) as exc_info:
            await source.get_price("ETH", "USD", resolved_token=rt)

        # Reason must identify a chain mismatch and name the offending chain.
        assert "chain_mismatch" in exc_info.value.reason
        assert "hyperevm" in exc_info.value.reason
        assert "arbitrum" in exc_info.value.reason
        # It was the *onchain* source that refused (not some downstream miss).
        assert exc_info.value.source == "onchain"
        await source.close()

    @pytest.mark.asyncio
    async def test_matching_chain_resolved_token_passes_guard(self):
        """Positive control: a same-chain ResolvedToken must NOT trip the guard.

        We use a stablecoin so the source returns the $1.00 fast-path without any
        RPC — the point is only that the guard let the request through.
        """
        source = OnChainPriceSource(chain="arbitrum")
        rt = _resolved("arbitrum", symbol="USDC", is_stablecoin=True)

        result = await source.get_price("USDC", "USD", resolved_token=rt)

        assert isinstance(result, PriceResult)
        assert result.price == Decimal("1.00")
        await source.close()

    @pytest.mark.asyncio
    async def test_none_resolved_token_skips_guard(self):
        """A bare-symbol request (``resolved_token=None``) must skip the guard
        entirely — that is today's lenient symbol behaviour (lead decision 1)."""
        source = OnChainPriceSource(chain="arbitrum")

        # USDC symbol → $1.00 stablecoin fast-path, no RPC, no guard.
        result = await source.get_price("USDC", "USD", resolved_token=None)

        assert result.price == Decimal("1.00")
        await source.close()

    @pytest.mark.asyncio
    async def test_chain_alias_is_accepted_by_guard(self):
        """Alias canonicalization: a ``bsc`` source handed a ``bnb``-tagged token
        must NOT trip the guard — both canonicalize to ``bsc`` via ChainRegistry."""
        source = OnChainPriceSource(chain="bsc")
        rt = _resolved("bnb", symbol="USDC", is_stablecoin=True)

        result = await source.get_price("USDC", "USD", resolved_token=rt)

        assert result.price == Decimal("1.00")
        await source.close()


class TestHypercoreOracleChainGuard:
    """Symmetric guard on the HyperEVM venue oracle: it must only answer
    hyperevm-tagged requests, never a foreign chain's."""

    @pytest.mark.asyncio
    async def test_non_hyperevm_resolved_token_raises(self):
        """A HyperCore oracle source handed an arbitrum-tagged token MUST raise
        ``DataSourceUnavailable(chain_mismatch)`` — the venue must never answer a
        non-hyperevm request, even though its majors are globally priced."""
        source = HypercoreOraclePriceSource()
        rt = _resolved("arbitrum")

        with pytest.raises(DataSourceUnavailable) as exc_info:
            await source.get_price("ETH", "USD", resolved_token=rt)

        assert "chain_mismatch" in exc_info.value.reason
        assert "arbitrum" in exc_info.value.reason
        assert "hyperevm" in exc_info.value.reason
        assert exc_info.value.source == "hypercore_oracle"
        await source.close()

    @pytest.mark.asyncio
    async def test_hyperevm_resolved_token_passes_guard(self):
        """Positive control: a hyperevm-tagged stablecoin returns the $1.00 peg
        (no RPC) — the guard let the same-chain request through."""
        source = HypercoreOraclePriceSource()
        rt = _resolved("hyperevm", symbol="USDC", is_stablecoin=True)

        result = await source.get_price("USDC", "USD", resolved_token=rt)

        assert result.price == Decimal("1.00")
        await source.close()

    @pytest.mark.asyncio
    async def test_none_resolved_token_skips_guard(self):
        """Bare-symbol (``resolved_token=None``) skips the venue guard."""
        source = HypercoreOraclePriceSource()

        result = await source.get_price("USDC", "USD", resolved_token=None)

        assert result.price == Decimal("1.00")
        await source.close()
