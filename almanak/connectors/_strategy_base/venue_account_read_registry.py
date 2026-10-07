"""Manifest-driven dispatch for connector-owned venue-account reads.

A connector opts in with ``venue_account_read=ImportRef(...)`` naming a module-level
:class:`~almanak.connectors._strategy_base.venue_account_read_base.VenueAccountReadSpec`.
Only the owning connector's module is imported on lookup, so a broken sibling
cannot break an unrelated read.
"""

from __future__ import annotations

import importlib
import logging
from typing import Any, ClassVar

from almanak.connectors._strategy_base.venue_account_read_base import VenueAccountRead, VenueAccountReadSpec

logger = logging.getLogger(__name__)

__all__ = ["VenueAccountReadRegistry"]


class VenueAccountReadRegistry:
    """Protocol identifier → connector venue-account-read spec."""

    _spec_loader_map: ClassVar[dict[str, tuple[str, str]] | None] = None
    _spec_cache: ClassVar[dict[str, VenueAccountReadSpec]] = {}

    @classmethod
    def _spec_loaders(cls) -> dict[str, tuple[str, str]]:
        if cls._spec_loader_map is None:
            from almanak.connectors._connector import CONNECTOR_REGISTRY

            cls._spec_loader_map = {
                manifest.name: (manifest.venue_account_read.module, manifest.venue_account_read.attribute)
                for manifest in CONNECTOR_REGISTRY.with_venue_account_read()
                if manifest.venue_account_read is not None
            }
        return cls._spec_loader_map

    @classmethod
    def _normalize(cls, protocol: object) -> str:
        return protocol.lower().replace("-", "_") if isinstance(protocol, str) else ""

    @classmethod
    def _load_spec(cls, protocol: str) -> VenueAccountReadSpec | None:
        cached = cls._spec_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._spec_loaders().get(protocol)
        if entry is None:
            return None
        module_path, attribute = entry
        spec = getattr(importlib.import_module(module_path), attribute, None)
        if not isinstance(spec, VenueAccountReadSpec):
            raise TypeError(f"{module_path}.{attribute} is {type(spec).__name__}, not a VenueAccountReadSpec.")
        cls._spec_cache[protocol] = spec
        return spec

    @classmethod
    def has(cls, protocol: object) -> bool:
        return cls._normalize(protocol) in cls._spec_loaders()

    @classmethod
    def protocols_to_read(cls, protocols: list[str], chain: str) -> list[str]:
        """The declared protocols that own a venue account on ``chain``."""
        out: list[str] = []
        for protocol in dict.fromkeys(cls._normalize(p) for p in protocols):
            try:
                spec = cls._load_spec(protocol)
            except Exception:  # noqa: BLE001 — a broken connector must not break valuation of the others
                logger.warning("venue-account-read spec for %r failed to load", protocol, exc_info=True)
                continue
            if spec is not None and chain.lower() in spec.chains:
                out.append(protocol)
        return out

    @classmethod
    def read(cls, protocol: str, *, gateway_client: Any, chain: str, wallet_address: str) -> VenueAccountRead:
        """Read ``protocol``'s account for ``wallet_address``; never raises."""
        try:
            spec = cls._load_spec(cls._normalize(protocol))
            if spec is None:
                return VenueAccountRead(ok=False, error=f"no venue-account read for {protocol!r}")
            return spec.read_account(gateway_client=gateway_client, chain=chain, wallet_address=wallet_address)
        except Exception as exc:  # noqa: BLE001 — unmeasured, never a fabricated empty account
            logger.warning("venue-account read for %s failed", protocol, exc_info=True)
            return VenueAccountRead(ok=False, error=f"{type(exc).__name__}: {exc}")

    @classmethod
    def reset_cache(cls) -> None:
        cls._spec_cache.clear()
        cls._spec_loader_map = None
