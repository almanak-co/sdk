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
manifest-derived account-state dispatch and exposes
:meth:`LendingReadRegistry.supports_account_state`,
:meth:`LendingReadRegistry.position_manager_address`,
:meth:`LendingReadRegistry.resolve_account_state_plan`, and — for per-market
protocols (Morpho Blue, VIB-4929 PR-3a) — :meth:`LendingReadRegistry.market_params`,
which lazily resolves the connector's ``market_id -> params`` catalogue so the
framework consumer can inject ``lltv`` (and other market params) into the query
without importing the connector. (Naming note: this
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
from typing import Any, ClassVar

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.lending_read_base import (
    AccountStateQuery,
    AccountStateReadSpec,
    EthCall,
    LendingAccountState,
    LendingPositionOnChain,
    LendingPositionRef,
    LendingReadSpec,
    MarketOraclePriceSpec,
)

logger = logging.getLogger(__name__)

# A connector's per-chain market catalogue: chain -> market_id -> params.
# (e.g. Morpho's ``MORPHO_MARKETS``.) Params values are heterogeneous (str
# addresses, the int ``lltv``, bool flags), hence ``object``.
_MarketTable = dict[str, dict[str, dict[str, object]]]

# ``LendingPositionOnChain`` / ``LendingAccountState`` are re-exported so callers
# can name the result types without reaching into ``lending_read_base``.
__all__ = [
    "AccountStatePlan",
    "LendingAccountState",
    "LendingPositionOnChain",
    "LendingPositionRef",
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


@dataclass(frozen=True)
class _LendingDispatchMaps:
    """Manifest-derived dispatch tables, built once per process.

    Each map mirrors one of the registry's historical hardcoded loader tables;
    values stay ``(module path, attribute)`` so per-protocol imports remain
    lazy (importlib on first lookup, never at derivation time — the VIB-4928
    PR-1 xdist member-drop hazard).
    """

    spec_loaders: dict[str, tuple[str, str]]
    account_state_loaders: dict[str, tuple[str, str]]
    market_health_loaders: dict[str, tuple[str, str]]
    market_table_loaders: dict[str, tuple[str, str]]
    backtest_provider_loaders: dict[str, tuple[str, str]]
    aliases: dict[str, str]
    # Plan 027 Step 5: set of canonical protocol keys that declare
    # accepts_is_collateral=True on their LendingReadDecl.
    collateral_flag_protocols: frozenset[str]
    # VIB-5493: set of canonical protocol keys that declare token_keyed=True
    # (supply-only, one-position-per-underlying-token, carry no market_id).
    token_keyed_protocols: frozenset[str]
    # VIB-5418: set of canonical protocol keys that declare market_isolated=True
    # (one collateral + one loan token per market — Morpho Blue).
    market_isolated_protocols: frozenset[str]
    # VIB-5729: canonical protocol keys that declare collateral_earns_no_yield=True
    # (posted collateral is held, not lent — Morpho Blue's supplyCollateral).
    collateral_no_yield_protocols: frozenset[str]


class LendingReadRegistry:
    """Protocol-identifier → connector lending-read-spec dispatch registry.

    Dispatch is derived from connector manifests: each lending connector
    declares ``lending_read=LendingReadDecl(...)`` on its ``CONNECTOR``,
    bundling its single-reserve spec (:class:`LendingReadSpec`), aggregate
    account-state spec (:class:`AccountStateReadSpec`, VIB-4929), per-market
    catalogue, multi-collateral health reader, and lending-scoped aliases. A
    protocol may publish either or both specs. Adding a lending connector (or
    an Aave fork) requires no edit here — the manifest declaration in the
    connector's own folder is the registration. Per-connector reader design
    notes live on each connector's manifest / ``lending_read`` module.
    """

    # Sentinel ``position_manager_address`` returns for a market-scoped protocol
    # (empty ``contract_kinds``): a truthy "this chain has a deployment" signal for
    # the framework reader's existence gate. It is NEVER used as an EthCall target —
    # the real per-market target is bound in ``resolve_account_state_plan`` from the
    # market table's ``comet_address``.
    _MARKET_SCOPED_TARGET: ClassVar[str] = "<market-scoped>"

    # Manifest-derived dispatch maps, built lazily on first use. ``None`` means
    # "not built yet".
    _dispatch_maps: ClassVar[_LendingDispatchMaps | None] = None

    @classmethod
    def _dispatch(cls) -> _LendingDispatchMaps:
        """Return the manifest-derived dispatch maps."""
        if cls._dispatch_maps is None:
            # Deferred import: avoids a module-level cycle through the
            # connector descriptor.
            from almanak.connectors._connector import CONNECTOR_REGISTRY

            spec_loaders: dict[str, tuple[str, str]] = {}
            account_state_loaders: dict[str, tuple[str, str]] = {}
            market_health_loaders: dict[str, tuple[str, str]] = {}
            market_table_loaders: dict[str, tuple[str, str]] = {}
            backtest_provider_loaders: dict[str, tuple[str, str]] = {}
            aliases: dict[str, str] = {}
            collateral_flag_protocols: set[str] = set()
            token_keyed_protocols: set[str] = set()
            market_isolated_protocols: set[str] = set()
            collateral_no_yield_protocols: set[str] = set()
            for connector_manifest in CONNECTOR_REGISTRY.with_lending_read():
                decl = connector_manifest.lending_read
                assert decl is not None
                key = connector_manifest.name
                if decl.spec is not None:
                    spec_loaders[key] = (decl.spec.module, decl.spec.attribute)
                if decl.account_state is not None:
                    account_state_loaders[key] = (decl.account_state.module, decl.account_state.attribute)
                if decl.market_health is not None:
                    market_health_loaders[key] = (decl.market_health.module, decl.market_health.attribute)
                if decl.market_table is not None:
                    market_table_loaders[key] = (decl.market_table.module, decl.market_table.attribute)
                if decl.backtest_provider is not None:
                    backtest_provider_loaders[key] = (
                        decl.backtest_provider.module,
                        decl.backtest_provider.attribute,
                    )
                for alias in decl.aliases:
                    aliases[alias] = key
                if decl.accepts_is_collateral:
                    collateral_flag_protocols.add(key)
                if decl.token_keyed:
                    token_keyed_protocols.add(key)
                if decl.market_isolated:
                    market_isolated_protocols.add(key)
                if decl.collateral_earns_no_yield:
                    collateral_no_yield_protocols.add(key)
            cls._dispatch_maps = _LendingDispatchMaps(
                spec_loaders=spec_loaders,
                account_state_loaders=account_state_loaders,
                market_health_loaders=market_health_loaders,
                market_table_loaders=market_table_loaders,
                backtest_provider_loaders=backtest_provider_loaders,
                aliases=aliases,
                collateral_flag_protocols=frozenset(collateral_flag_protocols),
                token_keyed_protocols=frozenset(token_keyed_protocols),
                market_isolated_protocols=frozenset(market_isolated_protocols),
                collateral_no_yield_protocols=frozenset(collateral_no_yield_protocols),
            )
        return cls._dispatch_maps

    # Default protocol used when a caller does not know which lending protocol a
    # position belongs to (legacy single-reserve read path). The framework reader
    # consumes this instead of naming a protocol itself.
    _DEFAULT_PROTOCOL: ClassVar[str] = "aave_v3"

    _spec_cache: ClassVar[dict[str, LendingReadSpec]] = {}
    _account_state_cache: ClassVar[dict[str, AccountStateReadSpec]] = {}
    # Per-protocol resolved market-health reader callable (VIB-4851 PR-2). Lazily
    # populated by ``market_health_reader`` on first access.
    _market_health_cache: ClassVar[dict[str, Callable[..., Any]]] = {}
    _backtest_provider_cache: ClassVar[dict[str, type | None]] = {}
    # Per-protocol resolved market table (VIB-4929 PR-3a). Lazily populated by
    # ``market_params`` on first access so the connector ``addresses`` module is
    # imported once, on demand, never eagerly at registry import.
    _market_cache: ClassVar[dict[str, _MarketTable]] = {}

    @classmethod
    def _normalize(cls, protocol: str) -> str:
        key = protocol.strip().lower().replace("-", "_")
        return cls._dispatch().aliases.get(key, key)

    @classmethod
    def normalize_protocol(cls, protocol: str | None) -> str:
        """Resolve a loosely-spelled lending protocol onto its canonical key.

        Folds whitespace, case, and hyphens, then applies the manifest-declared
        lending aliases (e.g. ``"comet"`` -> ``compound_v3``,
        ``"morpho"`` -> ``morpho_blue``). Unknown spellings pass through in
        folded form — no silent swallowing of typos; downstream capability
        checks fail closed on them. Consumers (``position_health``, …) call
        this instead of rolling their own alias tables, so protocol-identity
        knowledge stays declared on the owning connector's manifest.

        Total by design: ``None`` / non-``str`` input (loosely typed strategy
        metadata) normalises to the empty string rather than raising, so every
        capability lookup then fails closed.
        """
        if not isinstance(protocol, str):
            return ""
        return cls._normalize(protocol)

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
        return cls._normalize(protocol) in cls._dispatch().spec_loaders

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return every protocol identifier with a connector-owned lending read."""
        return tuple(sorted(cls._dispatch().spec_loaders))

    @classmethod
    def canonical(cls, protocol: str | None) -> str | None:
        """Return the canonical key for ``protocol`` if it has a lending read.

        Resolves case and aliases (e.g. ``"aave"`` -> ``"aave_v3"``) and returns
        the canonical dispatch key, or ``None`` when the protocol has no
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
        return key if key in cls._dispatch().spec_loaders else None

    @classmethod
    def _load_spec(cls, protocol: str) -> LendingReadSpec | None:
        """Resolve and cache one protocol's read spec.

        Imports ONLY the connector module that owns ``protocol`` (per the
        manifest-derived dispatch) — a broken sibling connector cannot block this
        lookup. Returns ``None`` when the protocol is unknown.
        """
        cached = cls._spec_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._dispatch().spec_loaders.get(protocol)
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
        return cls._normalize(protocol) in cls._dispatch().account_state_loaders

    @classmethod
    def publishes_market_table(cls, protocol: str) -> bool:
        """Return True when ``protocol`` publishes a per-market parameter table.

        Per-market protocols (Morpho Blue, Compound V3, Silo V2, Euler V2,
        BENQI) scope their account-state reads to a caller-supplied market id;
        whole-account protocols (the Aave family) publish no table. Framework
        consumers branch on this instead of naming a protocol.
        """
        return cls._normalize(protocol) in cls._dispatch().market_table_loaders

    @classmethod
    def declares_valuation_roles(cls, protocol: str) -> bool:
        """Return True when ``protocol``'s account-state spec declares valuation roles.

        Non-USD-native protocols (Morpho Blue, Silo V2, Euler V2) declare which
        market-params tokens the framework must price + inject; USD-native ones
        (the Aave family, BENQI) declare none. Distinct from
        :meth:`valuation_roles`, which also returns an empty tuple when the
        *market* cannot be resolved — this answers the protocol-level question
        so consumers can tell "no roles declared" apart from "market unknown".
        Imports only the owning connector's spec module (lazy, isolated).
        """
        spec = cls._load_account_state_spec(cls._normalize(protocol))
        return spec is not None and bool(spec.valuation_role_keys)

    @classmethod
    def _load_account_state_spec(cls, protocol: str) -> AccountStateReadSpec | None:
        """Resolve and cache one protocol's account-state spec.

        Imports ONLY the connector module that owns ``protocol`` (per the
        manifest-derived dispatch) — a broken sibling connector cannot block
        this lookup. Returns ``None`` when the protocol is unknown.
        """
        cached = cls._account_state_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._dispatch().account_state_loaders.get(protocol)
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
        if not spec.contract_kinds:
            # Market-scoped read target (VIB-4929 PR-3b, e.g. Compound V3): there is
            # no single per-chain address — the per-market Comet is bound later, in
            # ``resolve_account_state_plan``, from the market table's ``comet_address``.
            # Report chain-level existence (any published market on this chain) so the
            # framework reader's gate passes; the real target is resolved per-market.
            table = cls._load_market_table(key)
            return cls._MARKET_SCOPED_TARGET if (table and table.get(chain.lower())) else None
        return AddressRegistry.resolve_contract_address(key, chain, spec.contract_kinds)

    @classmethod
    def _load_market_table(cls, protocol: str) -> _MarketTable | None:
        """Resolve and cache one protocol's per-chain market table.

        Imports ONLY the connector module that owns ``protocol`` (per the
        manifest-derived dispatch), lazily on first access — a broken sibling
        connector cannot block this lookup, and the table is never derived
        eagerly at registry import (the VIB-4928 PR-1 xdist member-drop hazard).
        Returns ``None`` when the protocol publishes no market table.
        """
        cached = cls._market_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._dispatch().market_table_loaders.get(protocol)
        if entry is None:
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        table = getattr(module, attribute, None)
        if not isinstance(table, dict):
            raise TypeError(
                f"Registry maps {protocol!r} market table to {module_path}.{attribute}, "
                f"but that attribute is {type(table).__name__}, not a dict."
            )
        cls._market_cache[protocol] = table
        return table

    @classmethod
    def market_params(cls, protocol: str, chain: str, market_id: str) -> dict[str, object] | None:
        """Resolve the per-market parameters for ``(protocol, chain, market_id)``.

        For protocols whose account state is scoped to a single market (Morpho
        Blue), the reducer needs market parameters it cannot read on-chain cheaply
        (e.g. ``lltv``). The owning connector publishes the
        ``market_id -> params`` catalogue; the registry resolves it through the
        lazy manifest-derived market-table dispatch so the framework consumer can inject
        the params into an :class:`AccountStateQuery` without importing the
        connector itself.

        Returns ``None`` when the protocol publishes no market table, the chain
        has no markets, or the ``market_id`` is unknown — callers fail closed.

        Args:
            protocol: Protocol identifier (e.g. ``"morpho_blue"``).
            chain: Chain identifier (e.g. ``"ethereum"``).
            market_id: The market id (bytes32 hex, with or without ``0x``); matched
                case-insensitively against the published catalogue's keys.
        """
        key = cls._normalize(protocol)
        table = cls._load_market_table(key)
        if table is None:
            return None
        markets_for_chain = table.get(chain.lower())
        if not markets_for_chain:
            return None
        # Market-id normalisation is connector-declared (VIB-4929 PR-3b): a spec may
        # publish ``normalize_market_id`` (Compound V3 → ``str.lower`` for symbol ids
        # like "usdc"/"weth"). Default (``None``) keeps the Morpho-style 0x-prefixed,
        # lowercase, 32-byte ``zfill(64)`` shape.
        spec = cls._load_account_state_spec(key)
        normalizer = spec.normalize_market_id if spec is not None else None
        normalized = (
            normalizer(market_id) if normalizer is not None else "0x" + market_id.lower().replace("0x", "").zfill(64)
        )
        return markets_for_chain.get(normalized)

    @classmethod
    def market_health_reader(cls, protocol: str) -> Callable[..., LendingAccountState | None] | None:
        """Resolve the connector's multi-collateral market-health reader callable.

        VIB-4851 PR-2: dispatches ``(protocol) -> read_<protocol>_market_health`` via
        the lazy manifest-derived market-health dispatch, importing ONLY the owning connector
        module (a broken sibling cannot block this lookup) and caching the result. The
        framework consumer (:func:`~almanak.framework.accounting.lending_accounting.read_lending_market_health`)
        calls this instead of naming a connector function, so it stays protocol-agnostic
        — mirroring how ``read_lending_account_state`` resolves specs through the registry.

        Returns ``None`` when the protocol publishes no market-health reader, so the
        consumer falls through (→ no read) without fabricating a value.
        """
        key = cls._normalize(protocol)
        cached = cls._market_health_cache.get(key)
        if cached is not None:
            return cached
        entry = cls._dispatch().market_health_loaders.get(key)
        if entry is None:
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        reader = getattr(module, attribute, None)
        if not callable(reader):
            raise TypeError(
                f"Registry maps {protocol!r} market-health reader to {module_path}.{attribute}, "
                f"but that attribute is {type(reader).__name__}, not callable."
            )
        cls._market_health_cache[key] = reader
        return reader

    @classmethod
    def backtest_provider(cls, protocol: str | None) -> type | None:
        """Return the connector-owned ``HistoricalAPYProvider`` class for ``protocol``.

        The class is resolved lazily from the manifest ImportRef on first use
        and cached for the registry lifetime. Returns ``None`` when the
        connector has no declared historical APY provider or the protocol is
        unknown.
        """
        key = cls.backtest_provider_key(protocol)
        if key is None:
            return None
        if key in cls._backtest_provider_cache:
            return cls._backtest_provider_cache[key]
        entry = cls._dispatch().backtest_provider_loaders.get(key)
        if entry is None:
            cls._backtest_provider_cache[key] = None
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        provider = getattr(module, attribute, None)
        if not isinstance(provider, type):
            raise TypeError(
                f"Registry maps {protocol!r} backtest provider to {module_path}.{attribute}, "
                f"but that attribute is {type(provider).__name__}, not a class."
            )
        cls._backtest_provider_cache[key] = provider
        return provider

    @classmethod
    def backtest_provider_key(cls, protocol: str | None) -> str | None:
        """Return the canonical key for protocols with backtest APY providers.

        This is intentionally wider than :meth:`canonical`, which is scoped to
        single-reserve lending reads. Historical APY providers also exist for
        market/account-state protocols such as Compound V3 and Morpho Blue.
        """
        if not isinstance(protocol, str) or not protocol:
            return None
        key = cls._normalize(protocol)
        return key if key in cls._dispatch().backtest_provider_loaders else None

    @classmethod
    def backtest_provider_chains(cls, protocol: str | None) -> tuple[str, ...]:
        """Chains the connector's backtest ``HistoricalAPYProvider`` can serve.

        Resolved from the provider module's public ``SUPPORTED_CHAINS`` (its
        historical-subgraph coverage). This is the honest gate for the backtest
        lending-APY lane and is DELIBERATELY DISTINCT from
        :meth:`rate_history_chains` (the LIVE gateway rate lane): the on-chain
        ``getReserveData`` read can serve chains the historical subgraph does
        not index (e.g. ``aave_v3`` on ``bsc``, ``morpho_blue`` on
        ``arbitrum``), so the live lane is legitimately WIDER. A backtest that
        asked for historical APY on such a chain would silently degrade to
        fallback rates, so the support matrix must gate on this set, not the
        live one.

        Returns an empty tuple when the protocol declares no backtest provider,
        or when the provider module publishes no ``SUPPORTED_CHAINS`` (treated
        as "chain-agnostic" by the support matrix, matching the pre-existing
        ``not rate_chains`` fallthrough). Lazy import of the provider module
        (never at derivation time — the VIB-4928 hazard).
        """
        key = cls.backtest_provider_key(protocol)
        if key is None:
            return ()
        entry = cls._dispatch().backtest_provider_loaders.get(key)
        if entry is None:
            return ()
        module_path, _attribute = entry
        module = importlib.import_module(module_path)
        chains = getattr(module, "SUPPORTED_CHAINS", None)
        if not isinstance(chains, list | tuple):
            return ()
        return tuple(chains)

    @classmethod
    def market_health_inputs(cls, protocol: str, chain: str, market_id: str) -> dict[str, object] | None:
        """Resolve the multi-collateral health-read inputs for ``(protocol, chain, market_id)``.

        VIB-4851 PR-2: the position-health gate keeps the product-owner-chosen
        *summed* Compound V3 health factor
        ``HF = Σ_over_held_collaterals(value_usd × LCF) / borrow_value_usd``, which the
        single-leg account-state read (``resolve_account_state_plan``) cannot express
        (it reads one collateral). This accessor resolves the connector-owned market
        catalogue the parallel
        :func:`~almanak.connectors.compound_v3.lending_read.read_compound_v3_market_health`
        read needs, reusing the SAME lazy manifest-derived market-table dispatch
        (``COMPOUND_V3_ACCOUNT_STATE_MARKETS``) and connector-declared
        ``normalize_market_id`` (Compound → ``str.lower``) that :meth:`market_params`
        uses — so the registry stays generic (no Compound literal here beyond the
        shared market-table dispatch).

        Returns ``{comet_address, base_token, base_token_address, collaterals}``, or
        ``None`` when the protocol publishes no market table, the chain has no markets,
        or the ``market_id`` is unknown — callers fail closed (unknown chain/market →
        no read).

        Args:
            protocol: Protocol identifier (e.g. ``"compound_v3"``).
            chain: Chain identifier (e.g. ``"ethereum"``).
            market_id: The market id (a base-asset symbol for Compound, e.g. ``"usdc"``);
                normalized via the connector-declared normaliser before lookup.
        """
        params = cls.market_params(protocol, chain, market_id)
        if not params:
            return None
        return {
            "comet_address": params.get("comet_address"),
            "base_token": params.get("base_token"),
            "base_token_address": params.get("base_token_address"),
            "collaterals": params.get("collaterals"),
        }

    @classmethod
    def valuation_roles(
        cls,
        protocol: str,
        chain: str,
        market_id: str | None,
    ) -> tuple[tuple[str, str], ...]:
        """Resolve the ``(query_field, token_symbol)`` pairs to price + inject.

        For a non-USD-native protocol (Morpho Blue), the framework consumer must
        resolve a USD price + decimals for each valued token and inject them onto
        the :class:`AccountStateQuery`. *Which* tokens those are is connector
        knowledge: the spec declares ``valuation_role_keys`` as
        ``(query_field, market_params_key)`` pairs, and this method resolves each
        ``market_params_key`` against :meth:`market_params` to get the token
        symbol — returning ``(query_field, token_symbol)`` pairs the framework
        reader then prices.

        Returns an empty tuple when the protocol declares no valuation roles
        (the Aave family — USD-denominated on-chain), the protocol is unknown, or
        the market params / a declared role symbol cannot be resolved (so the
        consumer fails closed rather than pricing a wrong/empty set).

        Args:
            protocol: Protocol identifier (e.g. ``"morpho_blue"``, ``"aave_v3"``).
            chain: Chain identifier (e.g. ``"ethereum"``).
            market_id: The per-market id whose params name the valued tokens;
                ``None`` for whole-account protocols (which declare no roles).
        """
        key = cls._normalize(protocol)
        spec = cls._load_account_state_spec(key)
        if spec is None or not spec.valuation_role_keys:
            return ()
        if market_id is None:
            # A protocol that declares valuation roles is per-market by
            # construction; without a market id we cannot name its tokens.
            return ()
        params = cls.market_params(key, chain, market_id)
        if not params:
            return ()
        roles: list[tuple[str, str]] = []
        for query_field, params_key in spec.valuation_role_keys:
            symbol = params.get(params_key)
            if not isinstance(symbol, str) or not symbol:
                # A declared role with no resolvable symbol ⇒ fail closed (the
                # consumer must price every declared leg or read nothing).
                return ()
            roles.append((query_field, symbol))
        return tuple(roles)

    @classmethod
    def query_inputs(cls, protocol: str, intent: object) -> dict[str, Any] | None:
        """Derive the per-protocol query inputs for ``read_lending_account_state``.

        Delegates to the connector spec's
        :meth:`~almanak.connectors._strategy_base.lending_read_base.AccountStateReadSpec.query_inputs_from_intent`
        so the framework consumer does not hardcode which intent attributes a
        protocol's read needs (e.g. ``market_id`` for Morpho; possibly more for
        Compound V3 in PR-3b). The returned dict is splatted as keyword arguments
        into the generic reader.

        Returns ``None`` when ``protocol`` has no account-state read spec — i.e.
        it is not a (supported) lending protocol on the read path — so the
        consumer can fall through without fabricating inputs.
        """
        key = cls._normalize(protocol)
        spec = cls._load_account_state_spec(key)
        if spec is None:
            return None
        return spec.query_inputs_from_intent(intent)

    @classmethod
    def resolve_market_id(cls, ref: LendingPositionRef) -> str | None:
        """Reconstruct a protocol's ``market_id`` from a typed position ref (VIB-5775).

        The framework's valuation / position-health / teardown paths sometimes hold a
        :class:`~almanak.connectors._strategy_base.lending_read_base.LendingPositionRef`
        (protocol + chain + both leg tokens) but NO ``market_id`` — the case for
        synthetic-market protocols (Euler V2, Silo V2, BENQI) whose intents carry
        none. This resolves the connector-declared
        :attr:`~almanak.connectors._strategy_base.lending_read_base.AccountStateReadSpec.market_id_from_ref`
        (a PURE token-attribute function that shares the intent path's derivation, so
        the two ids can never drift) and returns the reconstructed id.

        Never guesses (Empty ≠ Zero): returns ``None`` — with a structured WARNING —
        when the protocol is unknown, publishes no account-state spec, declares no
        ``market_id_from_ref`` resolver, the resolver itself returns ``None``
        (ambiguous / uncatalogued tokens), or the resolver **raises** (a misbehaving
        connector must not crash the teardown/valuation guard — it fails CLOSED to
        ``None`` with a WARNING, honouring the same "never guess" contract). Callers
        fail closed on ``None``.

        If the ref already carries an explicit ``market_id`` (isolated-market
        protocols like Morpho, which DON'T declare a resolver), that id is returned
        verbatim — the ref already knows its market.
        """
        protocol = cls.normalize_protocol(ref.protocol)
        spec = cls._load_account_state_spec(protocol)
        if spec is None:
            logger.warning(
                "resolve_market_id: no account-state spec for protocol %r (chain=%s); "
                "cannot reconstruct market_id from ref — failing closed.",
                ref.protocol,
                ref.chain,
            )
            return None
        if spec.market_id_from_ref is None:
            # No ref resolver declared. An isolated-market protocol (Morpho) instead
            # carries an explicit market_id on the ref; honour it. Otherwise there is
            # nothing to derive — fail closed rather than guess.
            if ref.market_id:
                return ref.market_id
            # No resolver AND no explicit id. Two cases, logged differently so the
            # common/benign case is not mistaken for a fault:
            #   * WHOLE-ACCOUNT (no market table: Aave family) or TOKEN-KEYED (Fluid)
            #     protocols legitimately have NO synthetic market id — the caller
            #     proceeds with none (Aave drops the informational id; the guard's
            #     token fallback keys Fluid by token, VIB-5452). Expected → DEBUG.
            #   * A PER-MARKET, non-token-keyed protocol (Morpho / Compound / Fluid
            #     vault) that declares no resolver NEEDS an explicit id and got none —
            #     a real gap the caller must fail closed on → WARNING.
            if cls.publishes_market_table(protocol) and not cls.is_token_keyed(protocol):
                logger.warning(
                    "resolve_market_id: per-market protocol %r declares no market_id_from_ref "
                    "resolver and the ref carries no market_id (chain=%s, collateral=%s, loan=%s); "
                    "failing closed.",
                    protocol,
                    ref.chain,
                    ref.collateral_token,
                    ref.loan_token,
                )
            else:
                logger.debug(
                    "resolve_market_id: whole-account/token-keyed protocol %r has no synthetic "
                    "market_id to derive from a ref (chain=%s, collateral=%s, loan=%s); returning "
                    "None so the caller proceeds with none.",
                    protocol,
                    ref.chain,
                    ref.collateral_token,
                    ref.loan_token,
                )
            return None
        try:
            market_id = spec.market_id_from_ref(ref)
        except Exception:
            # A connector resolver is contracted PURE + non-raising (return None, never
            # guess). If one nevertheless raises, fail CLOSED rather than let it crash the
            # teardown/valuation guard that called us: return None (caller drops/keeps
            # conservatively — Empty ≠ Zero) and surface the fault at WARNING with a
            # traceback so the misbehaving connector is diagnosable.
            logger.warning(
                "resolve_market_id: %s resolver raised while reconstructing a market_id from ref "
                "(chain=%s, collateral=%s, loan=%s) — failing closed to None (never guessing).",
                protocol,
                ref.chain,
                ref.collateral_token,
                ref.loan_token,
                exc_info=True,
            )
            return None
        if not market_id:
            logger.warning(
                "resolve_market_id: %s resolver could not reconstruct a market_id from ref "
                "(chain=%s, collateral=%s, loan=%s) — tokens ambiguous or uncatalogued; "
                "failing closed (never guessing).",
                protocol,
                ref.chain,
                ref.collateral_token,
                ref.loan_token,
            )
            return None
        return market_id

    @classmethod
    def market_oracle_price_spec(cls, protocol: str) -> MarketOraclePriceSpec | None:
        """Resolve the connector-declared market-own-oracle price read, if any.

        Isolated-market protocols (Morpho Blue) may declare a
        :class:`~almanak.connectors._strategy_base.lending_read_base.MarketOraclePriceSpec`
        on their account-state spec — the pure description of how to read the
        market's OWN liquidation oracle. The framework's position-health
        default-pricing path
        (:func:`~almanak.framework.accounting.lending_reads.read_market_oracle_price`)
        dispatches through this accessor so it never hardcodes a protocol's
        oracle selector or scaling. Returns ``None`` when the protocol is
        unknown or declares no such read — callers fall back / fail closed.
        """
        spec = cls._load_account_state_spec(cls._normalize(protocol))
        return spec.market_oracle_price if spec is not None else None

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

        if spec.contract_kinds:
            target = AddressRegistry.resolve_contract_address(key, query.chain, spec.contract_kinds)
        else:
            # Market-scoped target (VIB-4929 PR-3b): the per-market read target (the
            # Compound Comet) rides on the injected market params, not the per-chain
            # AddressRegistry. ``market_params`` is resolved + injected by the
            # framework reader before planning.
            target = (query.market_params or {}).get("comet_address")
        if not target:
            logger.debug(
                "No %s account-state target for protocol %s on chain %s",
                spec.contract_kinds or "market-scoped",
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
    def rate_history_chains(cls, protocol: str | None) -> tuple[str, ...]:
        """Chains the framework rate consumers offer ``protocol``'s rates on.

        Manifest-derived (``LendingReadDecl.rate_history_chains``, VIB-4851
        Phase D); empty tuple for venues without a declared rate lane. A
        parity test pins each declaration as a subset of the connector's
        gateway-side ``lending_supported_chains()``.
        """
        key = cls.normalize_protocol(protocol)
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        for connector_manifest in CONNECTOR_REGISTRY.with_lending_read():
            if connector_manifest.name == key:
                decl = connector_manifest.lending_read
                assert decl is not None
                return decl.rate_history_chains
        return ()

    @classmethod
    def rate_history_protocols(cls) -> tuple[str, ...]:
        """Lending venues with a declared gateway rate lane, sorted."""
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        return tuple(
            sorted(
                c.name
                for c in CONNECTOR_REGISTRY.with_lending_read()
                if c.lending_read is not None and c.lending_read.rate_history_chains
            )
        )

    @classmethod
    def rate_history_protocols_for_chain(cls, chain: str | None) -> tuple[str, ...]:
        """Rate-lane venues declaring ``chain``, sorted (legacy PROTOCOL_CHAINS rows)."""
        if not isinstance(chain, str) or not chain:
            return ()
        chain_lower = chain.strip().lower()
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        return tuple(
            sorted(
                c.name
                for c in CONNECTOR_REGISTRY.with_lending_read()
                if c.lending_read is not None and chain_lower in c.lending_read.rate_history_chains
            )
        )

    @classmethod
    def all_rate_history_chains(cls) -> frozenset[str]:
        """Union of every declared rate-lane chain across lending venues."""
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        return frozenset(
            chain
            for c in CONNECTOR_REGISTRY.with_lending_read()
            if c.lending_read is not None
            for chain in c.lending_read.rate_history_chains
        )

    @classmethod
    def backtest_default_apys(cls, protocol: str | None) -> tuple[str | None, str | None]:
        """``(supply, borrow)`` offline-backtest fallback APYs for ``protocol``.

        Decimal strings from the manifest declaration; ``(None, None)`` when
        the venue declares none (consumers fail loud — there is no generic
        fabricated fallback rate).
        """
        key = cls.normalize_protocol(protocol)
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        for connector_manifest in CONNECTOR_REGISTRY.with_lending_read():
            if connector_manifest.name == key:
                decl = connector_manifest.lending_read
                assert decl is not None
                return (decl.backtest_default_supply_apy, decl.backtest_default_borrow_apy)
        return (None, None)

    @classmethod
    def accepts_is_collateral(cls, protocol: str) -> bool:
        """Return ``True`` when ``protocol`` accepts the ``is_collateral`` flag.

        Plan 027 Step 5: replaces the ``_normalize_protocol_key(protocol) in
        {"morpho", "morpho_blue"}`` inline set-membership guard in the executor
        and the ax CLI withdraw path. The fold (spaces + hyphens ->
        underscores) MUST run IN FRONT of this call so that display-cased
        inputs like ``"Morpho Blue"`` resolve via the alias map — this method
        only consults aliases after the fold, which maps ``"morpho_blue"``
        (the folded form of ``"Morpho Blue"``) back to ``"morpho_blue"`` via
        the aliases dict. Callers must pre-fold with
        ``_normalize_protocol_key`` before passing here; this method performs
        no folding of its own (the registry's ``_normalize`` does strip/lower/
        hyphen-fold, which is sufficient for alias resolution, but does NOT
        fold spaces -- pre-fold is the caller's responsibility).
        """
        return cls._normalize(protocol) in cls._dispatch().collateral_flag_protocols

    @classmethod
    def is_token_keyed(cls, protocol: str | None) -> bool:
        """Return ``True`` when ``protocol`` is a supply-only token-keyed surface.

        VIB-5493: a token-keyed lending protocol (Fluid fTokens) has one
        supply-only position per underlying token and carries NO ``market_id``;
        its position identity IS the token. The teardown lending guard
        (``lending_unwind_guard._position_key``) uses this to split the position
        key per token ONLY for these protocols, so two distinct Fluid supplies on
        the same chain are treated as two positions instead of collapsing to one
        ``(fluid, chain, "")`` key. Account/vault-keyed protocols (the Aave family
        with ``market_id=""``; Morpho / Compound / fluid_vault with an explicit
        per-market id) return ``False`` and stay grouped per account.

        Total by design: ``None`` / non-``str`` / unknown input normalises to a
        non-token-keyed (``False``) answer so callers fail closed onto the safe
        account-keyed grouping.
        """
        canonical = cls.normalize_protocol(protocol)
        return bool(canonical) and canonical in cls._dispatch().token_keyed_protocols

    @classmethod
    def is_market_isolated(cls, protocol: str | None) -> bool:
        """Return ``True`` when ``protocol`` is an ISOLATED-market lender (VIB-5418).

        An isolated market has exactly one collateral token and one loan token
        (Morpho Blue), so a per-market on-chain read's debt IS the whole-position
        debt. The teardown lending guard
        (``lending_unwind_guard._keep_withdraw``) uses this to KEEP a zero-debt
        collateral ``withdraw_all`` on a measured per-reserve read even when the
        account-level USD aggregate is unmeasured (empty snapshot prices for a
        cross-asset market) — a false strand it otherwise refuses.

        Deliberately NOT ``publishes_market_table``: Compound V3 publishes a
        per-market table but is MULTI-collateral against one base asset, so a zero
        collateral-reserve debt does not prove the account owes no base debt —
        lumping it in would KEEP an unsafe withdraw.

        Total by design: ``None`` / non-``str`` / unknown input normalises to
        ``False`` so callers fail closed onto the conservative non-isolated keep.
        """
        canonical = cls.normalize_protocol(protocol)
        return bool(canonical) and canonical in cls._dispatch().market_isolated_protocols

    @classmethod
    def collateral_earns_no_yield(cls, protocol: str | None) -> bool:
        """Return True when posted collateral on ``protocol`` earns exactly zero.

        Manifest-derived (``LendingReadDecl.collateral_earns_no_yield``, VIB-5729).
        True only where the protocol HOLDS collateral rather than lending it out
        (Morpho Blue's ``supplyCollateral``), which makes a collateral leg's supply
        APY a *measured* zero rather than an unmeasured one.

        Deliberately NOT inferable from ``market_isolated`` or from a market table
        naming a ``collateral_token``: Silo V2 / Euler V2 do both, yet their
        collateral IS lent out and accrues — stamping a measured zero there would
        fabricate a rate for a leg that is genuinely earning.

        Total by design: ``None`` / non-``str`` / unknown input normalises to
        ``False``, so callers fail closed onto honest-unmeasured.
        """
        canonical = cls.normalize_protocol(protocol)
        return bool(canonical) and canonical in cls._dispatch().collateral_no_yield_protocols

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-spec caches so the next call re-imports.

        Production code should never call this — it exists for narrow test
        setups that intentionally re-trigger a connector import.
        """
        cls._spec_cache.clear()
        cls._account_state_cache.clear()
        cls._market_cache.clear()
        cls._market_health_cache.clear()
        cls._backtest_provider_cache.clear()
        cls._dispatch_maps = None
