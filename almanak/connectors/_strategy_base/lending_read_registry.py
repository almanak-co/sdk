"""Strategy-side dispatch registry for connector-owned lending-position reads.

Sibling of the other ``_strategy_base`` registries (:class:`AddressRegistry`,
:class:`PoolValidationRegistry`, …). It owns the single protocol-identifier →
owning-connector ``lending_read`` mapping and lazily imports *only* the
connector that owns a requested protocol, so a broken sibling connector cannot
poison an unrelated lookup, and the framework lending reader never hardcodes a
protocol name, a per-DEX contract kind, or an ABI selector.

Each lending connector that supports a single-reserve on-chain read publishes a
module-level :data:`LENDING_READ_SPEC` (a
:class:`~almanak.connectors._strategy_base.lending_read_base.LendingReadSpec`)
in its ``lending_read`` module. The registry resolves the spec, then resolves
the per-chain read-target address through :class:`AddressRegistry`
(``spec.contract_kinds``) — so the address table stays owned by each
connector's ``addresses.py``.

The framework reader asks :meth:`LendingReadRegistry.resolve` for a fully
materialised :class:`LendingReadPlan` (target address + calldata + decoder) and
executes the gateway-routed ``eth_call`` itself. When a caller does not know
which protocol a position belongs to, it uses :meth:`default_protocol` — the
registry owns the default-family choice so the framework names no protocol.

Gateway-boundary note: this module is strategy-side and performs no network
egress. The owning connector ``lending_read`` modules it imports are pure data
+ pure functions; the gateway-routed ``eth_call`` lives in the framework reader.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import ClassVar

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.lending_read_base import (
    LendingPositionOnChain,
    LendingReadSpec,
)

logger = logging.getLogger(__name__)

# ``LendingPositionOnChain`` is re-exported so callers can name the result type
# without reaching into ``lending_read_base``.
__all__ = ["LendingPositionOnChain", "LendingReadPlan", "LendingReadRegistry"]


@dataclass(frozen=True)
class LendingReadPlan:
    """A fully materialised single-reserve read for one ``(protocol, chain)``.

    Produced by :meth:`LendingReadRegistry.resolve`. The framework reader needs
    only the gateway client to execute it — it carries the resolved read-target
    address, the calldata, and the connector's return decoder.

    Attributes:
        target_address: Contract to ``eth_call`` (the resolved data provider).
        calldata: Hex calldata for the read.
        parse_result: ``(result_hex, asset_address) -> LendingPositionOnChain |
            None`` decoder for the return data.
    """

    target_address: str
    calldata: str
    parse_result: Callable[[str, str], LendingPositionOnChain | None]


class LendingReadRegistry:
    """Protocol-identifier → connector lending-read-spec dispatch registry."""

    # Protocol identifier -> (module path, attribute) naming the connector's
    # published LendingReadSpec. The Aave V2 / V3 forks (Aave V3, Spark, Radiant
    # V2) each publish their own spec attribute; the specs happen to be the
    # shared AAVE_FORK_RESERVE_READ instance, but the *opt-in* lives in each
    # connector so adding a fork needs no edit here beyond one row.
    _SPEC_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {
        "aave_v3": ("almanak.connectors.aave_v3.lending_read", "LENDING_READ_SPEC"),
        "spark": ("almanak.connectors.spark.lending_read", "LENDING_READ_SPEC"),
        "radiant_v2": ("almanak.connectors.radiant_v2.lending_read", "LENDING_READ_SPEC"),
    }

    # Protocol aliases that map onto a canonical key in ``_SPEC_LOADERS``.
    _ALIASES: ClassVar[dict[str, str]] = {
        "aave": "aave_v3",
    }

    # Default protocol used when a caller does not know which lending protocol a
    # position belongs to (legacy single-reserve read path). The framework reader
    # consumes this instead of naming a protocol itself.
    _DEFAULT_PROTOCOL: ClassVar[str] = "aave_v3"

    _spec_cache: ClassVar[dict[str, LendingReadSpec]] = {}

    @classmethod
    def _normalize(cls, protocol: str) -> str:
        key = protocol.lower().replace("-", "_")
        return cls._ALIASES.get(key, key)

    @classmethod
    def default_protocol(cls) -> str:
        """Return the protocol the framework reader uses when none is specified.

        The default-family choice is connector/registry knowledge — the
        framework reader calls this rather than naming a protocol, so the reader
        stays protocol-agnostic.
        """
        return cls._DEFAULT_PROTOCOL

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned lending read."""
        return cls._normalize(protocol) in cls._SPEC_LOADERS

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return every protocol identifier with a connector-owned lending read."""
        return tuple(sorted(cls._SPEC_LOADERS))

    @classmethod
    def _load_spec(cls, protocol: str) -> LendingReadSpec | None:
        """Resolve and cache one protocol's read spec.

        Imports ONLY the connector module that owns ``protocol`` (per
        ``_SPEC_LOADERS``) — a broken sibling connector cannot block this
        lookup. Returns ``None`` when the protocol is unknown.
        """
        cached = cls._spec_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._SPEC_LOADERS.get(protocol)
        if entry is None:
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        spec = getattr(module, attribute, None)
        if not isinstance(spec, LendingReadSpec):
            raise TypeError(
                f"Registry maps {protocol!r} to {module_path}.{attribute}, "
                f"but that attribute is {type(spec).__name__}, not a LendingReadSpec."
            )
        cls._spec_cache[protocol] = spec
        return spec

    @classmethod
    def resolve(
        cls,
        protocol: str,
        chain: str,
        asset_address: str,
        wallet_address: str,
    ) -> LendingReadPlan | None:
        """Materialise a single-reserve read for ``(protocol, chain)``.

        Resolves the connector's read spec, then the per-chain read-target
        address through :class:`AddressRegistry` (the spec's ``contract_kinds``),
        and builds the calldata. Returns ``None`` when the protocol is unknown
        or the chain has no read-target address — the framework reader fails
        closed on ``None``.

        Args:
            protocol: Protocol identifier (e.g. ``"aave_v3"``, ``"spark"``,
                ``"radiant_v2"``, or the ``"aave"`` alias).
            chain: Chain identifier (e.g. ``"arbitrum"``).
            asset_address: Underlying reserve asset address.
            wallet_address: User wallet address.

        Returns:
            A :class:`LendingReadPlan`, or ``None`` if unresolvable.
        """
        key = cls._normalize(protocol)
        spec = cls._load_spec(key)
        if spec is None:
            logger.debug("No lending-read spec for protocol %s", protocol)
            return None

        target = AddressRegistry.resolve_contract_address(key, chain, spec.contract_kinds)
        if not target:
            logger.debug(
                "No %s read-target address for protocol %s on chain %s",
                spec.contract_kinds,
                key,
                chain,
            )
            return None

        calldata = spec.build_calldata(asset_address, wallet_address)
        return LendingReadPlan(
            target_address=target,
            calldata=calldata,
            parse_result=spec.parse_result,
        )

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-spec cache so the next call re-imports.

        Production code should never call this — it exists for narrow test
        setups that intentionally re-trigger a connector import.
        """
        cls._spec_cache.clear()
