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

VIB-4929 adds a parallel **aggregate account-state** dispatch alongside the
single-reserve one. A connector that supports an account-state read publishes a
module-level :data:`ACCOUNT_STATE_READ_SPEC` (an
:class:`~almanak.connectors._strategy_base.lending_read_base.AccountStateReadSpec`)
in the same ``lending_read`` module; the registry maps it through the parallel
``_ACCOUNT_STATE_LOADERS`` table and exposes
:meth:`LendingReadRegistry.supports_account_state`,
:meth:`LendingReadRegistry.position_manager_address`, and
:meth:`LendingReadRegistry.resolve_account_state_plan`. (Naming note: this
capability extends ``LendingReadRegistry`` rather than introducing a
``LendingProtocolAdapter`` — that name is already taken by the compile-side
``class LendingProtocolAdapter(Protocol)`` in
``almanak/framework/intents/compiler_adapters.py``.)

Gateway-boundary note: this module is strategy-side and performs no network
egress. The owning connector ``lending_read`` modules it imports are pure data
+ pure functions; the gateway-routed ``eth_call`` lives in the framework reader.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import ClassVar

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.lending_read_base import (
    AccountStateQuery,
    AccountStateReadSpec,
    EthCall,
    LendingAccountState,
    LendingPositionOnChain,
    LendingReadSpec,
)

logger = logging.getLogger(__name__)

# ``LendingPositionOnChain`` / ``LendingAccountState`` are re-exported so callers
# can name the result types without reaching into ``lending_read_base``.
__all__ = [
    "AccountStatePlan",
    "LendingAccountState",
    "LendingPositionOnChain",
    "LendingReadPlan",
    "LendingReadRegistry",
]


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


@dataclass(frozen=True)
class AccountStatePlan:
    """A fully materialised aggregate account-state read for one ``(protocol, chain)``.

    Produced by :meth:`LendingReadRegistry.resolve_account_state_plan`. The
    framework reader needs only the gateway client to execute it: it carries the
    ordered :class:`EthCall` reads and the connector's pure reducer. The reducer
    is invoked with the same :class:`AccountStateQuery` and the return blobs in
    ``calls`` order (``None`` for any read that failed).

    Attributes:
        query: The resolved query (carries the per-chain target the calls
            reference); passed back into ``reduce`` so the reducer can use
            ``market_id`` / ``block`` / etc.
        calls: Ordered reads the framework reader executes via the gateway.
        reduce: ``(AccountStateQuery, list[str | None]) -> LendingAccountState |
            None`` decoder for the read results.
    """

    query: AccountStateQuery
    calls: tuple[EthCall, ...]
    reduce: Callable[[AccountStateQuery, list[str | None]], LendingAccountState | None]


class LendingReadRegistry:
    """Protocol-identifier → connector lending-read-spec dispatch registry.

    Owns two parallel dispatch tables: ``_SPEC_LOADERS`` for single-reserve reads
    (:class:`LendingReadSpec`) and ``_ACCOUNT_STATE_LOADERS`` for aggregate
    account-state reads (:class:`AccountStateReadSpec`, VIB-4929). A protocol may
    appear in either or both.
    """

    # Protocol identifier -> (module path, attribute) naming the connector's
    # published LendingReadSpec. The Aave V3 forks (Aave V3, Spark) each
    # publish their own spec attribute; the specs happen to be the shared
    # AAVE_FORK_RESERVE_READ instance, but the *opt-in* lives in each connector
    # so adding a fork needs no edit here beyond one row.
    _SPEC_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {
        "aave_v3": ("almanak.connectors.aave_v3.lending_read", "LENDING_READ_SPEC"),
        "spark": ("almanak.connectors.spark.lending_read", "LENDING_READ_SPEC"),
    }

    # Parallel table for aggregate account-state reads (VIB-4929). Maps a
    # protocol identifier to the connector's published ``ACCOUNT_STATE_READ_SPEC``.
    # The Aave V3 forks (Aave V3, Spark) each publish their own attribute; the
    # specs happen to be the shared ``AAVE_FORK_ACCOUNT_STATE_READ`` instance, but
    # the opt-in lives in each connector so adding a fork needs one row here.
    # Morpho/Compound are intentionally absent in PR-1 — they need the
    # price-oracle injection seam, deferred to a later PR.
    _ACCOUNT_STATE_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {
        "aave_v3": ("almanak.connectors.aave_v3.lending_read", "ACCOUNT_STATE_READ_SPEC"),
        "spark": ("almanak.connectors.spark.lending_read", "ACCOUNT_STATE_READ_SPEC"),
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
    _account_state_cache: ClassVar[dict[str, AccountStateReadSpec]] = {}

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
    def canonical(cls, protocol: str | None) -> str | None:
        """Return the canonical key for ``protocol`` if it has a lending read.

        Resolves case and aliases (e.g. ``"aave"`` -> ``"aave_v3"``) and returns
        the canonical ``_SPEC_LOADERS`` key, or ``None`` when the protocol has no
        connector-owned lending read. Lets a strategy-side caller map a declared
        / loosely-spelled protocol identifier onto the registry's canonical key
        without reaching into ``_normalize`` or duplicating the alias table — so
        protocol-identity knowledge stays owned here, in the registry.

        Total by design: ``None`` / non-``str`` input (loosely typed strategy
        metadata) returns ``None`` rather than raising, so callers can use it
        directly in a ``canonical(p) or fallback`` normalisation.
        """
        if not isinstance(protocol, str) or not protocol:
            return None
        key = cls._normalize(protocol)
        return key if key in cls._SPEC_LOADERS else None

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
                or the ``"aave"`` alias).
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

    # -- Aggregate account-state dispatch (VIB-4929) -----------------------

    @classmethod
    def supports_account_state(cls, protocol: str) -> bool:
        """Return True when ``protocol`` publishes an aggregate account-state read."""
        return cls._normalize(protocol) in cls._ACCOUNT_STATE_LOADERS

    @classmethod
    def _load_account_state_spec(cls, protocol: str) -> AccountStateReadSpec | None:
        """Resolve and cache one protocol's account-state spec.

        Imports ONLY the connector module that owns ``protocol`` (per
        ``_ACCOUNT_STATE_LOADERS``) — a broken sibling connector cannot block
        this lookup. Returns ``None`` when the protocol is unknown.
        """
        cached = cls._account_state_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._ACCOUNT_STATE_LOADERS.get(protocol)
        if entry is None:
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        spec = getattr(module, attribute, None)
        if not isinstance(spec, AccountStateReadSpec):
            raise TypeError(
                f"Registry maps {protocol!r} to {module_path}.{attribute}, "
                f"but that attribute is {type(spec).__name__}, not an AccountStateReadSpec."
            )
        cls._account_state_cache[protocol] = spec
        return spec

    @classmethod
    def position_manager_address(cls, protocol: str, chain: str) -> str | None:
        """Resolve the per-chain account-state read target for ``(protocol, chain)``.

        Resolves the contract the account-state reads target (the Aave-family
        ``pool``; the Compound ``comet``; …) through :class:`AddressRegistry`,
        using the connector's published ``AccountStateReadSpec.contract_kinds``.
        Returns ``None`` when the protocol has no account-state read or the chain
        has no such address — so callers fail closed.
        """
        key = cls._normalize(protocol)
        spec = cls._load_account_state_spec(key)
        if spec is None:
            return None
        return AddressRegistry.resolve_contract_address(key, chain, spec.contract_kinds)

    @classmethod
    def resolve_account_state_plan(
        cls,
        protocol: str,
        query: AccountStateQuery,
    ) -> AccountStatePlan | None:
        """Materialise an aggregate account-state read for ``(protocol, query.chain)``.

        Resolves the connector's account-state spec, resolves the per-chain
        target address through :class:`AddressRegistry` (the spec's
        ``contract_kinds``), rebinds it onto the query, and invokes the
        connector's pure ``build_calls`` planner. Returns ``None`` when the
        protocol is unknown or the chain has no target address — the framework
        reader fails closed on ``None``.

        The caller may pass a ``query`` with a placeholder
        ``position_manager_address`` (it is overwritten with the registry-resolved
        address), so callers need not pre-resolve the target themselves.

        Args:
            protocol: Protocol identifier (e.g. ``"aave_v3"``, ``"spark"``, or
                the ``"aave"`` alias).
            query: The account-state request (chain, wallet, optional market id /
                block). Its ``position_manager_address`` is resolved by the
                registry.

        Returns:
            An :class:`AccountStatePlan` (ordered calls + reducer + resolved
            query), or ``None`` if unresolvable.
        """
        key = cls._normalize(protocol)
        spec = cls._load_account_state_spec(key)
        if spec is None:
            logger.debug("No account-state spec for protocol %s", protocol)
            return None

        target = AddressRegistry.resolve_contract_address(key, query.chain, spec.contract_kinds)
        if not target:
            logger.debug(
                "No %s account-state target for protocol %s on chain %s",
                spec.contract_kinds,
                key,
                query.chain,
            )
            return None

        # Rebind the resolved target onto the (frozen) query so the planner emits
        # fully-formed EthCall targets and the reducer sees the same resolved query.
        resolved_query = replace(query, position_manager_address=target)
        calls = tuple(spec.build_calls(resolved_query))
        return AccountStatePlan(
            query=resolved_query,
            calls=calls,
            reduce=spec.reduce_calls,
        )

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-spec caches so the next call re-imports.

        Production code should never call this — it exists for narrow test
        setups that intentionally re-trigger a connector import.
        """
        cls._spec_cache.clear()
        cls._account_state_cache.clear()
