from __future__ import annotations

from typing import Any

from almanak.core.chains import ChainRegistry
from almanak.integrations._base import PriceSourceScope
from almanak.integrations.chainlink.catalog import CATALOG


class ChainlinkPriceSourceFactory:
    name = "chainlink"
    scope = PriceSourceScope.CHAIN
    order = 10

    def supports(self, chain: str | None) -> bool:
        return chain is not None and CATALOG.supports_chain(chain)

    def build(self, *, chain: str | None, settings: Any) -> Any:
        if chain is None:
            raise ValueError("Chainlink price source requires a chain")
        from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource

        return ChainlinkPriceSource(chain=chain, network=settings.network)


class ChainlinkOracleReaderFactory:
    """Construct gateway-owned, provider-exact Chainlink readers."""

    name = "chainlink"

    def supports(self, chain: str, token: str) -> bool:
        return CATALOG.feed_for_token(chain, token) is not None

    def build(self, *, chain: str, settings: Any) -> Any:
        from almanak.config.backtest import backtest_config_from_env
        from almanak.gateway.utils import get_rpc_url
        from almanak.integrations.chainlink.gateway.history import ChainlinkHistoryReader

        descriptor = ChainRegistry.try_resolve(chain)
        canonical_chain = descriptor.name if descriptor is not None else chain.strip().lower()
        archive_rpc = backtest_config_from_env(archive_rpc_chains=(canonical_chain,)).archive_rpc_urls.get(
            canonical_chain, ""
        )
        return ChainlinkHistoryReader(
            chain=canonical_chain,
            rpc_url=archive_rpc or get_rpc_url(canonical_chain, network=settings.network),
        )
