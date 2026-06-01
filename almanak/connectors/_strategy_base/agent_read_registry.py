"""Strategy-side agent-tool read-descriptor registry (VIB-4860 / W8).

Sibling of :mod:`almanak.connectors._strategy_base.gas_estimate_registry`
(W6) and :mod:`...receipt_parser_registry` (W2), scoped to the per-protocol
on-chain *read descriptors* the LLM-facing read tools need.

Why a strategy-side registry
============================

The agent-tool read handlers in
``almanak/framework/agent_tools/executor.py`` (pool-state, LP-position,
lending-account, and portfolio reads) do raw ``eth_call`` RPC through the
gateway and decode the result. Prior to W8 those handlers imported each
connector's address table directly
(``from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3``,
``from almanak.connectors.aave_v3.adapter import AAVE_V3_POOL_ADDRESSES``,
…) and carried per-protocol dispatch dicts (``_PROTOCOL_REGISTRIES``)
inline. Adding a new agent-callable read protocol meant editing the
central executor.

W8 (Alternative C of the plan) moves only the **protocol-specific
descriptors** — *which* contract address on *which* chain, and *which*
``getPool`` selector variant a fork uses — onto the owning connector. The
ABI-decode logic (``slot0()`` int24 two's-complement, ``positions()``
12-word layout, ``getUserAccountData()`` scaling) is **identical across
every V3 fork and stays generic in the executor**: duplicating it into N
connectors would re-introduce the fan-out W8 removes. The split is exactly
the W6 split — the connector owns "which address / which selector", the
executor owns the RPC round-trip and the decode.

PolicyEngine boundary (AGENTS.md mandate)
=========================================

These capability methods are **read/descriptor-only and pure**: they
return addresses, selectors, or ``None``. They MUST NOT call the gateway,
sign, or touch ``PolicyEngine``. The executor remains the sole owner of
the RPC round-trip *and* the policy gate — which runs in ``_execute_inner``
*before* any category dispatch, i.e. before any read handler resolves a
descriptor from this registry. The anti-bypass static guard
(``tests/static/test_agent_tools_policy_anti_bypass.py``) enforces this.

What lives here
===============

* :class:`AgentReadCapability` — a ``@runtime_checkable`` Protocol a
  connector declares. Methods (all cheap, metadata-only):

    - ``agent_read_keys() -> frozenset[str]`` — the read families the
      connector backs (``frozenset({"pool_state", "lp_position"})`` for a
      CL DEX; ``frozenset({"lending_account"})`` for a lending market).
      Non-empty; an empty set would silently disable the capability.
    - ``factory_address(chain) -> str | None`` — CL-DEX factory used by
      ``factory.getPool()``. ``None`` when the connector has no deployment
      on ``chain`` or does not back ``pool_state``.
    - ``position_manager_address(chain) -> str | None`` — CL-DEX NFT
      position manager used by ``positions(uint256)``. ``None`` when the
      connector does not back ``lp_position``.
    - ``get_pool_selector() -> str`` — the 4-byte ``getPool`` selector this
      fork uses (``0x1698ee82`` for the uint24-fee v3 family,
      ``0x28af8d0b`` for the int24-tick-spacing Slipstream family).
    - ``lending_pool_address(chain) -> str | None`` — lending-market Pool
      used by ``getUserAccountData(address)``. ``None`` for non-lending
      connectors.

  A connector implements only the methods relevant to the families it
  publishes; the others return ``None`` (or a sensible default selector).

* :class:`AgentReadConnector` — carrier base (``ProtocolName`` +
  ``ProtocolKind``), mirroring :class:`GasEstimateConnector`.

* :class:`AgentReadToolRegistry` — same shape as
  :class:`GasEstimateConnectorRegistry`: ``register`` (collision = hard
  error; instance, not class; must also implement the capability),
  ``get`` / ``all`` / ``with_capability`` / ``lookup``.

* :data:`STRATEGY_AGENT_READ_REGISTRY` — the single in-process instance.
  Concrete connectors are registered into it by
  :mod:`almanak.connectors._strategy_agent_tool_registry`.

Gateway-boundary note
=====================

Strategy-side. Imports ``ProtocolKind`` / ``ProtocolName`` from
``_base/types.py`` (the cross-boundary type leaf); does **not** touch
``_base/gateway_*``. The agent-tool executor runs strategy-side
(``tests/static/test_strategy_import_boundary.py`` lists
``almanak/framework/agent_tools`` as a strategy-side root) and consumes
this registry directly.
"""

from __future__ import annotations

from typing import ClassVar, Protocol, TypeVar, cast, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "STRATEGY_AGENT_READ_REGISTRY",
    "AgentReadCapability",
    "AgentReadConnector",
    "AgentReadRegistryError",
    "AgentReadToolRegistry",
]


class AgentReadRegistryError(Exception):
    """Registry contract violation (collision, unknown key, etc.)."""


@runtime_checkable
class AgentReadCapability(Protocol):
    """Connector publishes the on-chain *read descriptors* the agent-tool
    read handlers need. Pure data — NO RPC, NO signing, NO gateway, NO
    PolicyEngine.

    Contract
    --------

    * ``agent_read_keys() -> frozenset[str]`` — the read families this
      connector backs. Canonical keys: ``"pool_state"`` and
      ``"lp_position"`` (CL DEXes) and ``"lending_account"`` (lending
      markets). Returning an empty frozenset is **not** legal — a
      connector that declares the capability must publish at least one key.

    * ``factory_address(chain) -> str | None`` — CL-DEX factory contract on
      ``chain`` for ``factory.getPool(tokenA, tokenB, fee)``; ``None`` if
      the connector has no deployment on ``chain`` (or doesn't back pools).

    * ``position_manager_address(chain) -> str | None`` — CL-DEX NFT
      position manager on ``chain`` for ``positions(uint256)``; ``None``
      if the connector doesn't back LP positions on ``chain``.

    * ``get_pool_selector() -> str`` — the 4-byte ``getPool`` selector this
      fork uses. The v3 family encodes a ``uint24`` fee (``0x1698ee82``);
      the Aerodrome Slipstream family encodes an ``int24`` tick-spacing
      (``0x28af8d0b``). A flat per-connector constant.

    * ``lending_pool_address(chain) -> str | None`` — lending-market Pool
      on ``chain`` for ``getUserAccountData(address)``; ``None`` for
      non-lending connectors.

    Why descriptors, not decoders? The ABI-decode logic is identical across
    every v3 fork (same ``slot0()`` / ``positions()`` ABI) and across every
    Aave-V3 fork. The genuinely protocol-specific knowledge is *which
    address* and *which selector* — that is what moves here. The decode
    stays generic in the executor (byte-equivalent by construction).

    ``protocol`` is the canonical name the connector registers under; it is
    declared here so callers iterating the capability view
    (:meth:`AgentReadToolRegistry.with_capability`) can read it without
    narrowing back to :class:`AgentReadConnector`. Every concrete provider
    inherits it from :class:`AgentReadConnector`.
    """

    protocol: ClassVar[ProtocolName]

    def agent_read_keys(self) -> frozenset[str]: ...

    def factory_address(self, chain: str) -> str | None: ...

    def position_manager_address(self, chain: str) -> str | None: ...

    def get_pool_selector(self) -> str: ...

    def lending_pool_address(self, chain: str) -> str | None: ...


class AgentReadConnector:
    """Base class for strategy-side agent-read connector instances.

    Mirrors :class:`GasEstimateConnector`: a ``ProtocolName`` +
    ``ProtocolKind`` carrier whose capability surface is declared by also
    inheriting from :class:`AgentReadCapability`.

    Required class attributes
    -------------------------

    * ``protocol`` — canonical ``ProtocolName`` (registry key; collision is
      a hard error).
    * ``kind`` — static ``ProtocolKind`` for logging / dashboards.
    """

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class AgentReadToolRegistry:
    """In-process registry of strategy-side agent-read connectors.

    Same shape as :class:`GasEstimateConnectorRegistry`: keyed by
    ``ProtocolName``, collision is a hard error, instances (not classes)
    are stored so capability dispatch (``isinstance(connector, Cap)``)
    works.

    The :meth:`lookup` helper resolves a canonical ``protocol`` name to the
    connector's :class:`AgentReadCapability` (or ``None`` for an
    unregistered protocol). Read handlers call ``lookup`` *after* the
    policy gate has passed.
    """

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, AgentReadConnector] = {}

    def register(self, connector: AgentReadConnector) -> None:
        """Register a connector instance. Collision on protocol raises.

        Stores instances (not classes) so the registry can dispatch
        capability calls and read per-instance ``protocol``. Passing a
        class is rejected loudly here rather than failing at first lookup.
        """
        if not isinstance(connector, AgentReadConnector):
            raise AgentReadRegistryError(
                "register() expects an AgentReadConnector instance, got "
                f"{type(connector).__qualname__!s} ({connector!r}); did you "
                "forget to instantiate the class?"
            )
        # Fail fast on the silent-disable bug: a connector inherited from
        # ``AgentReadConnector`` but missing the ``AgentReadCapability``
        # mixin would pass ``register`` yet be unusable at ``lookup``.
        # Refuse it so the typo surfaces at boot. (Mirrors the W6 registry's
        # post-review hardening.)
        if not isinstance(connector, AgentReadCapability):
            raise AgentReadRegistryError(
                "register() expects a connector implementing AgentReadCapability "
                f"in addition to AgentReadConnector; {type(connector).__qualname__!s} "
                "is missing the mixin / required methods. Without the capability "
                "the connector's read families would be silently unreachable."
            )
        # Validate the published key set eagerly — an empty / malformed set
        # is a silent-disable bug, so reject at registration (the W6 registry
        # surfaces this lazily at lookup; agent-read has no per-key routing
        # map so the eager check is both possible and clearer).
        keys = connector.agent_read_keys()
        if not isinstance(keys, frozenset) or not keys:
            raise AgentReadRegistryError(
                f"{type(connector).__qualname__}.agent_read_keys() must return a non-empty frozenset, got {keys!r}"
            )
        for key in keys:
            if not isinstance(key, str) or not key:
                raise AgentReadRegistryError(
                    f"{type(connector).__qualname__}.agent_read_keys() returned an "
                    f"invalid key {key!r} (must be a non-empty str)"
                )
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            raise AgentReadRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite with "
                f"{type(connector).__qualname__}"
            )
        self._connectors[proto] = connector

    def get(self, protocol: ProtocolName) -> AgentReadConnector | None:
        """Return the connector registered under ``protocol`` (or ``None``)."""
        return self._connectors.get(protocol)

    def lookup(self, protocol: str) -> AgentReadCapability | None:
        """Return the :class:`AgentReadCapability` for ``protocol``, or ``None``.

        ``protocol`` must already be a canonical name (callers normalise via
        ``_strategy_base.protocol_aliases.normalize_protocol`` first).
        Returns ``None`` for an unregistered protocol — the read handler is
        expected to surface a user-facing "unsupported protocol" error.
        """
        connector = self._connectors.get(ProtocolName(protocol))
        if connector is None:
            return None
        # ``register`` guarantees capability implementation; the guard gives
        # mypy the proof and mirrors the W6 registry's defensive shape.
        if not isinstance(connector, AgentReadCapability):
            return None
        return connector

    def all(self) -> tuple[AgentReadConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def capabilities(self) -> tuple[AgentReadCapability, ...]:
        """Return every registered connector as its :class:`AgentReadCapability`.

        ``register`` guarantees every stored connector implements the
        capability, so this is a safe widening of :meth:`all` to the
        capability view — callers that need ``agent_read_keys`` /
        ``factory_address`` (not just the ``AgentReadConnector`` carrier
        attributes) iterate this instead. Order matches registration order.
        """
        return tuple(cast(AgentReadCapability, c) for c in self._connectors.values())

    def protocols(self) -> frozenset[ProtocolName]:
        """Return every registered protocol name."""
        return frozenset(self._connectors)

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``.

        ``capability`` must be a ``@runtime_checkable`` Protocol. Order
        matches registration order.
        """
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def clear(self) -> None:
        """Test helper — clear registrations. NOT used in production paths."""
        self._connectors.clear()


STRATEGY_AGENT_READ_REGISTRY: AgentReadToolRegistry = AgentReadToolRegistry()
