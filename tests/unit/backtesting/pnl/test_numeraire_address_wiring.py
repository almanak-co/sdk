"""End-to-end wiring: the PnL engine registers contract-address identities.

The unpriceable-numeraire bug was a non-native ERC20 numeraire (e.g. cbBTC on
Base) declared via ``@almanak_strategy(quote_asset=...)`` that the strategy never
trades. The CLI's ``build_token_address_map`` only covers *traded* tokens, so the
numeraire was auto-added to the data-fetch set with no address entry, hit an
honest miss in the CoinGecko leg, and failed loud at metrics time.

The fix: the engine registers every authoritative ``(chain, address)`` mapping
with the data provider before the iteration loop. CLI / service callers provide
the traded-token map, and the strategy's ``QuoteAsset`` contributes any
numeraire mapping. These tests drive the REAL :class:`PnLBacktester` over
network-free synthetic data and assert the registration actually reaches the
provider (the per-provider resolution behavior is pinned in
``test_coingecko_resolution.py::TestRegisterTokenAddresses``).
"""

from __future__ import annotations
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.core.models.quote_asset import QuoteAsset
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataConfig,
    MarketState,
    TokenRef,
    token_ref_provider_symbol,
)
from almanak.framework.backtesting.pnl.engine import (


    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)

_START = datetime(2024, 1, 1, tzinfo=UTC)
_TICK_SECONDS = 3600
# Canonical WETH on Arbitrum (chain_id 42161, the config default chain).
_WETH_ARBITRUM = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
_USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


class _RecordingProvider:
    """Network-free ``HistoricalDataProvider`` that records address registrations.

    Mirrors the trust-matrix synthetic provider's surface (the same protocol
    production providers implement) and additionally captures every
    ``register_token_addresses`` call so the test can assert the engine pushes
    address identity through.
    """

    provider_name = "recording-numeraire-test"

    def __init__(self, price_series: dict[str, list[Decimal]]) -> None:
        self._series = {token.upper(): list(series) for token, series in price_series.items()}
        self.registered: list[dict[str, tuple[str, str]]] = []

    @staticmethod
    def _series_key(token: TokenRef, chain: str) -> str:
        return token_ref_provider_symbol(token, chain).upper()

    def register_token_addresses(self, token_addresses: dict[str, tuple[str, str]]) -> None:
        self.registered.append(dict(token_addresses))

    @property
    def supported_tokens(self) -> list[str]:
        return list(self._series)

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum"]

    async def get_price(self, token: TokenRef, timestamp: datetime) -> Decimal:
        series = self._series.get(self._series_key(token, "arbitrum"))
        return series[0] if series else Decimal("1")

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        current = config.start_time
        index = 0
        while current <= config.end_time:
            chain = config.chains[0] if config.chains else "arbitrum"
            prices: dict[str, Decimal] = {}
            for token in config.tokens:
                key = self._series_key(token, chain)
                series = self._series.get(key)
                prices[key] = series[min(index, len(series) - 1)] if series else Decimal("1")
            yield (
                current,
                MarketState(
                    timestamp=current,
                    prices=prices,
                    chain=chain,
                    block_number=1_000_000 + index,
                    gas_price_gwei=Decimal("30"),
                ),
            )
            index += 1
            current += timedelta(seconds=config.interval_seconds)


class _HoldStrategy:
    """Cash-only strategy (never trades) -- the numeraire-only case the bug hit."""

    deployment_id = "numeraire-wiring-test"

    def __init__(self, quote_asset: Any | None = None) -> None:
        if quote_asset is not None:
            self.quote_asset = quote_asset

    def decide(self, market: Any) -> Any:
        return None


def _run(
    strategy: Any,
    provider: _RecordingProvider,
    *,
    token_addresses: dict[str, tuple[str, str]] | None = None,
) -> Any:
    config = PnLBacktestConfig(
        start_time=_START,
        end_time=_START + timedelta(hours=2),
        interval_seconds=_TICK_SECONDS,
        token_funding=_pnl_token_funding(Decimal("10000"), chain="arbitrum"),
        tokens=["WETH", "USDC"],
        chain="arbitrum",
        include_gas_costs=False,
        inclusion_delay_blocks=0,
    )
    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        token_addresses=token_addresses,
    )
    return asyncio.run(backtester.backtest(strategy, config))


def test_engine_registers_numeraire_address_on_provider() -> None:
    """A token numeraire's (chain, address) is pushed to the provider before iterate.

    Even though WETH is natively resolvable, the engine still registers its
    address: the registration is keyed off the QuoteAsset, not on whether the
    symbol happens to be native -- which is exactly what makes a non-native
    numeraire (cbBTC) priceable.
    """
    provider = _RecordingProvider({"WETH": [Decimal("2000")] * 3, "USDC": [Decimal("1")] * 3})
    result = _run(_HoldStrategy(QuoteAsset.token(42161, _WETH_ARBITRUM)), provider)

    assert {"WETH": ("arbitrum", _WETH_ARBITRUM.lower())} in provider.registered
    assert result.numeraire == "WETH"


def test_engine_registers_all_known_token_addresses_on_provider() -> None:
    """Configured token addresses reach providers that need post-construction registration."""
    provider = _RecordingProvider({"WETH": [Decimal("2000")] * 3, "USDC": [Decimal("1")] * 3})
    result = _run(
        _HoldStrategy(QuoteAsset.usd()),
        provider,
        token_addresses={
            "weth": ("arbitrum", _WETH_ARBITRUM),
            "USDC": ("arbitrum", _USDC_ARBITRUM),
        },
    )

    assert provider.registered == [
        {
            "WETH": ("arbitrum", _WETH_ARBITRUM.lower()),
            "USDC": ("arbitrum", _USDC_ARBITRUM.lower()),
        }
    ]
    assert result.numeraire is None


def test_usd_numeraire_registers_nothing() -> None:
    """A USD (default) strategy resolves to no numeraire -- nothing is registered."""
    provider = _RecordingProvider({"WETH": [Decimal("2000")] * 3, "USDC": [Decimal("1")] * 3})
    result = _run(_HoldStrategy(QuoteAsset.usd()), provider)

    assert provider.registered == []
    assert result.numeraire is None
