"""Manifest-derived registry for venue-native perp price history (ALM-3149).

The registry contains provider factories, never market symbols.  A connector's
runtime catalogue is the source of truth for which markets exist.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Protocol, cast, runtime_checkable

from almanak.framework.backtesting.pnl.data_provider import HistoricalDataProvider

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

__all__ = [
    "PerpPriceHistoryProvider",
    "PerpPriceHistoryProviderFactory",
    "PerpPriceHistoryRegistry",
]

_invalidation_hook_registered = False


@runtime_checkable
class PerpPriceHistoryProvider(HistoricalDataProvider, Protocol):
    """Prepared connector-native provider installed into a PnL backtester."""

    async def prepare_backtest(self, config: PnLBacktestConfig) -> str: ...

    @property
    def price_history_target(self) -> tuple[str, str, str]: ...


class PerpPriceHistoryProviderFactory(Protocol):
    """Class-level construction seam published by a connector manifest."""

    def for_backtest(
        self,
        *,
        fallback: HistoricalDataProvider,
        chain: str,
        market: str,
        venue: str,
    ) -> PerpPriceHistoryProvider: ...


class PerpPriceHistoryRegistry:
    """Resolve connector-declared historical price providers by protocol."""

    _venue_map: ClassVar[dict[str, str] | None] = None
    _alias_map: ClassVar[dict[str, str] | None] = None
    _chains_map: ClassVar[dict[str, tuple[str, ...]] | None] = None
    _provider_ref_map: ClassVar[dict[str, Any] | None] = None
    _provider_class_cache: ClassVar[dict[str, PerpPriceHistoryProviderFactory | None]] = {}

    @classmethod
    def _build_dispatch(cls) -> None:
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        _ensure_registry_invalidation_hook(CONNECTOR_REGISTRY)
        venues: dict[str, str] = {}
        aliases: dict[str, str] = {}
        chains: dict[str, tuple[str, ...]] = {}
        providers: dict[str, Any] = {}
        for connector_manifest in CONNECTOR_REGISTRY.with_perp_price_history():
            decl = connector_manifest.perp_price_history
            assert decl is not None
            venues[connector_manifest.name] = decl.venue
            chains[connector_manifest.name] = decl.chains
            if decl.backtest_provider is not None:
                providers[connector_manifest.name] = decl.backtest_provider
            for alias in decl.aliases:
                aliases[alias] = connector_manifest.name
        cls._venue_map = venues
        cls._alias_map = aliases
        cls._chains_map = chains
        cls._provider_ref_map = providers

    @classmethod
    def _ensure(cls) -> None:
        if cls._venue_map is None:
            cls._build_dispatch()

    @classmethod
    def canonical(cls, protocol: str | None) -> str | None:
        cls._ensure()
        if not isinstance(protocol, str):
            return None
        key = protocol.strip().lower().replace("-", "_")
        assert cls._venue_map is not None and cls._alias_map is not None
        canonical = cls._alias_map.get(key, key)
        return canonical if canonical in cls._venue_map else None

    @classmethod
    def has(cls, protocol: str | None) -> bool:
        return cls.canonical(protocol) is not None

    @classmethod
    def venue_for(cls, protocol: str) -> str:
        canonical = cls.canonical(protocol)
        if canonical is None:
            raise KeyError(f"No perp price-history declaration for {protocol!r}")
        assert cls._venue_map is not None
        return cls._venue_map[canonical]

    @classmethod
    def declared_chains(cls, protocol: str) -> tuple[str, ...]:
        canonical = cls.canonical(protocol)
        if canonical is None:
            return ()
        assert cls._chains_map is not None
        return cls._chains_map.get(canonical, ())

    @classmethod
    def _load_backtest_provider(cls, canonical: str) -> PerpPriceHistoryProviderFactory | None:
        """Load and validate one manifest provider class."""
        assert cls._provider_ref_map is not None
        ref = cls._provider_ref_map.get(canonical)
        if ref is None:
            return None
        provider_cls = ref.load()
        if not isinstance(provider_cls, type):
            raise TypeError(
                f"Perp price-history provider for {canonical!r} must resolve to a class, got {provider_cls!r}"
            )
        return cast(PerpPriceHistoryProviderFactory, provider_cls)

    @classmethod
    def backtest_provider(cls, protocol: str) -> PerpPriceHistoryProviderFactory | None:
        canonical = cls.canonical(protocol)
        if canonical is None:
            return None
        if canonical not in cls._provider_class_cache:
            cls._provider_class_cache[canonical] = cls._load_backtest_provider(canonical)
        return cls._provider_class_cache[canonical]

    @classmethod
    def reset_cache(cls) -> None:
        """Reset derived state for connector-registry tests."""
        cls._venue_map = None
        cls._alias_map = None
        cls._chains_map = None
        cls._provider_ref_map = None
        cls._provider_class_cache.clear()

    clear_cache = reset_cache


def _ensure_registry_invalidation_hook(registry: Any) -> None:
    """Register the connector-derived cache reset exactly once, on first use."""
    global _invalidation_hook_registered
    if not _invalidation_hook_registered:
        registry.on_clear(PerpPriceHistoryRegistry.reset_cache)
        _invalidation_hook_registered = True
