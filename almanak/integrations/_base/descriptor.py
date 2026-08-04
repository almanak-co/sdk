"""Immutable integration manifests.

Descriptors contain no provider implementation imports.  Gateway-only code is
referenced lazily so importing provider metadata cannot open sockets or pull
gateway internals into the strategy container.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any


def _canonical_asset_ids(name: str, asset_ids: Mapping[str, str]) -> Mapping[str, str]:
    normalized = dict(asset_ids)
    if any(not symbol or not isinstance(value, str) or not value for symbol, value in normalized.items()):
        raise ValueError(f"Integration {name!r} asset_ids must map symbols to non-empty strings")
    return MappingProxyType(normalized)


def _canonical_market_symbols(
    name: str,
    market_symbols: Mapping[tuple[str, str], str],
) -> Mapping[tuple[str, str], str]:
    normalized = {
        (base.strip().upper(), quote.strip().upper()): value for (base, quote), value in market_symbols.items()
    }
    if any(
        not base or not quote or not isinstance(value, str) or not value for (base, quote), value in normalized.items()
    ):
        raise ValueError(f"Integration {name!r} market_symbols must map (base, quote) to non-empty strings")
    return MappingProxyType(normalized)


@dataclass(frozen=True, order=True)
class ImportRef:
    """A lazy import reference used by an integration manifest."""

    module: str
    attribute: str
    order: int = 100

    def load(self) -> Any:
        """Load the referenced object."""
        return getattr(importlib.import_module(self.module), self.attribute)

    def instantiate(self) -> Any:
        """Instantiate the referenced factory class."""
        return self.load()()


@dataclass(frozen=True)
class Integration:
    """Capabilities and provider-global metadata for one data provider.

    Provider-specific *chain* identifiers are implemented by
    :class:`almanak.core.chains.ExternalChainIds`, not duplicated here.
    """

    name: str
    asset_ids: Mapping[str, str] | None = None
    market_symbols: Mapping[tuple[str, str], str] | None = None
    gateway_price_source: ImportRef | None = None
    gateway_oracle_reader: ImportRef | None = None
    gateway_api_client: ImportRef | None = None
    gateway_portfolio_provider: ImportRef | None = None
    price_source_exclusive_group: str | None = None
    price_source_blocked_by_groups: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        normalized = self.name.strip().lower()
        if not normalized or normalized != self.name:
            raise ValueError("Integration.name must be a non-empty canonical lowercase slug")
        if self.asset_ids is not None:
            object.__setattr__(self, "asset_ids", _canonical_asset_ids(self.name, self.asset_ids))
        if self.market_symbols is not None:
            object.__setattr__(self, "market_symbols", _canonical_market_symbols(self.name, self.market_symbols))
        for capability, ref in (
            ("gateway price source", self.gateway_price_source),
            ("gateway oracle reader", self.gateway_oracle_reader),
            ("gateway API client", self.gateway_api_client),
            ("gateway portfolio provider", self.gateway_portfolio_provider),
        ):
            owned_gateway = f"almanak.integrations.{self.name}.gateway"
            if ref is not None and ref.module != owned_gateway and not ref.module.startswith(owned_gateway + "."):
                raise ValueError(
                    f"Integration {self.name!r} must own its {capability} under "
                    f"almanak.integrations.{self.name}.gateway, got {ref.module!r}"
                )
        if self.price_source_exclusive_group is not None:
            group = self.price_source_exclusive_group.strip().lower()
            if not group or group != self.price_source_exclusive_group:
                raise ValueError("Integration.price_source_exclusive_group must be a canonical lowercase slug")
            if self.gateway_price_source is None:
                raise ValueError("price_source_exclusive_group requires gateway_price_source")
        blocked = frozenset(group.strip().lower() for group in self.price_source_blocked_by_groups)
        if any(not group for group in blocked) or blocked != self.price_source_blocked_by_groups:
            raise ValueError("Integration.price_source_blocked_by_groups must contain canonical lowercase slugs")
        if blocked and self.gateway_price_source is None:
            raise ValueError("price_source_blocked_by_groups requires gateway_price_source")
