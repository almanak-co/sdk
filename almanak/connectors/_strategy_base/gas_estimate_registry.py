"""Strategy-side gas-estimate connector registry (VIB-4858 / W6).

Sibling of :mod:`almanak.connectors._strategy_base.receipt_parser_registry`,
scoped to the per-(protocol, action) gas-estimate concern.

Why a strategy-side registry
============================

Intent compilation runs **inside the strategy container** — the framework's
``Compiler`` builds ``TransactionData`` entries before the gateway ever sees
them, and each entry needs a ``gas_estimate`` integer. Prior to W6 those
integers lived in a central ``DEFAULT_GAS_ESTIMATES`` dict in
``almanak/framework/intents/compiler_constants.py`` keyed by action name
(``lp_mint``, ``lending_supply``, ``balancer_flash_loan``, …). The dict
mixed three concerns:

* **Chain-level common primitives** (``approve``, ``wrap_eth``,
  ``unwrap_eth``, ``swap_simple``, ``swap_multi_hop``) — these have no
  natural protocol owner and stay on the baseline table inside the
  framework.
* **Per-protocol estimates** (``lp_*`` for Uniswap V3 forks,
  ``lending_*`` and ``flash_loan*`` for Aave V3, ``balancer_flash_loan*``
  for Balancer V2, ``bridge_deposit`` for Across, ``vault_*`` for
  MetaMorpho). The W6 design is that each owning connector publishes its
  own values via this registry.
* **Per-chain overrides** — already moved onto
  ``ChainDescriptor.gas.operation_overrides`` by W5. The compiler's
  ``get_gas_estimate(chain, operation)`` still consults the descriptor
  first, then this registry, then the baseline.

This file mirrors the W2 receipt-parser pattern. A connector declares the
capability and gains the (protocol, action, chain) -> int routing for
free.

What lives here
===============

* :class:`GasEstimateCapability` — a ``@runtime_checkable`` Protocol a
  connector declares by implementing two methods:

    - ``gas_estimate_keys() -> frozenset[str]`` — every action key the
      connector publishes (``frozenset({"lp_mint", "lp_collect", ...})``).
      Cheap, metadata-only — does **not** import any heavy modules.
    - ``gas_estimate(action: str, chain: str) -> int`` — return the
      connector's gas estimate for ``action`` on ``chain``. ``chain`` is
      passed so connectors with per-chain knowledge (e.g. Aave V3's
      Arbitrum supply hooks) can specialise; most connectors ignore it
      and return a flat number identical to the previous central dict
      value.

* :class:`GasEstimateConnector` — base class for the lightweight
  strategy-side connector instances registered here. Mirrors
  :class:`almanak.connectors._strategy_base.receipt_parser_registry.ReceiptParserConnector`.

* :class:`GasEstimateConnectorRegistry` — the strategy-side registry
  itself. Same shape as :class:`ReceiptParserConnectorRegistry`
  (``register`` / ``get`` / ``all`` / ``with_capability``), plus a
  :meth:`lookup` helper that returns the resolved
  ``(action, chain) -> int`` answer or ``None`` when no connector
  publishes ``action``. The action -> connector map is built lazily on
  first lookup and cached; ``register`` invalidates the cache.

* :data:`STRATEGY_GAS_ESTIMATE_REGISTRY` — the single in-process
  instance. Concrete strategy-side connectors are registered into it by
  :func:`_register_all` in
  :mod:`almanak.connectors._strategy_gas_estimate_registry` (sibling of
  ``_strategy_receipt_registry.py``).

Byte-equivalence (VIB-4858 requirement)
=======================================

W6 is a refactor: the externally observable
``get_gas_estimate(chain, operation)`` -> ``int`` must return the same
number for every ``(chain, operation)`` pair before and after the
migration. Each connector's ``gas_estimate`` implementation MUST return
the exact integer the central ``DEFAULT_GAS_ESTIMATES[action]`` would
have returned. A verification script lives in ``tests/unit/intents/``
(``test_w6_gas_estimate_byte_equivalence.py``) and pins the contract
table.

Gateway-boundary note
=====================

This module is strategy-side. It imports ``ProtocolKind`` / ``ProtocolName``
from ``_base/types.py`` (the cross-boundary type module), but it does
**not** touch ``_base/gateway_*`` — those remain gateway-only. Compiler
runs strategy-side and consumes this registry directly.
"""

from __future__ import annotations

from typing import ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "STRATEGY_GAS_ESTIMATE_REGISTRY",
    "GasEstimateCapability",
    "GasEstimateConnector",
    "GasEstimateConnectorRegistry",
    "GasEstimateRegistryError",
]


class GasEstimateRegistryError(Exception):
    """Registry contract violation (collision, unknown key, etc.)."""


@runtime_checkable
class GasEstimateCapability(Protocol):
    """Connector publishes per-action gas estimates.

    Two methods so the registry can answer ``is this action published?``
    cheaply (no heavy module imports) and only consult the connector for
    the actual integer on a hit.

    Contract
    --------

    * ``gas_estimate_keys() -> frozenset[str]`` — every action key the
      connector claims. Multiple keys are legal: the Aave V3 connector
      publishes the full ``lending_*`` family plus ``flash_loan`` and
      ``flash_loan_simple``. Returning an empty frozenset is **not**
      legal — a connector that declares the capability must publish at
      least one key; an empty set would silently disable the capability.

    * ``gas_estimate(action: str, chain: str) -> int`` — return the
      connector's gas estimate for ``action`` on ``chain``. Raises
      ``KeyError`` if ``action`` is not in the connector's
      ``gas_estimate_keys()`` (programming error: the registry never
      asks for an unpublished key). The integer MUST be a positive
      number of gas units.

    Why pass ``chain`` even when most connectors ignore it? A few
    protocols genuinely have per-chain gas profiles (Aave V3 incentive
    hooks vary by chain; bridge_deposit on Across can exceed 675K on
    some destinations). Keeping ``chain`` in the capability signature
    means the consumer never has to call back into the connector to
    "ask if it cares about chain" — the connector decides internally.
    """

    def gas_estimate_keys(self) -> frozenset[str]: ...

    def gas_estimate(self, action: str, chain: str) -> int: ...


class GasEstimateConnector:
    """Base class for strategy-side gas-estimate connector instances.

    Mirrors :class:`almanak.connectors._strategy_base.receipt_parser_registry.ReceiptParserConnector`:
    a ``ProtocolName`` + ``ProtocolKind`` carrier with capability surface
    declared by also inheriting from :class:`GasEstimateCapability`.

    Required class attributes
    -------------------------

    * ``protocol`` — canonical ``ProtocolName`` for this connector. Used
      as the registry key (collision is a hard error).
    * ``kind`` — static ``ProtocolKind`` for logging / dashboards.

    Example::

        class AaveV3GasEstimateConnector(
            GasEstimateConnector, GasEstimateCapability,
        ):
            protocol: ClassVar[ProtocolName] = ProtocolName("aave_v3")
            kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

            _ESTIMATES: ClassVar[dict[str, int]] = {
                "lending_supply": 300000,
                "lending_borrow": 450000,
                "lending_repay": 250000,
                "lending_withdraw": 250000,
                "flash_loan": 500000,
                "flash_loan_simple": 300000,
            }

            def gas_estimate_keys(self) -> frozenset[str]:
                return frozenset(self._ESTIMATES)

            def gas_estimate(self, action: str, chain: str) -> int:
                return self._ESTIMATES[action]
    """

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class GasEstimateConnectorRegistry:
    """In-process registry of strategy-side gas-estimate connectors.

    Same shape as
    :class:`almanak.connectors._strategy_base.receipt_parser_registry.ReceiptParserConnectorRegistry`:
    keyed by ``ProtocolName``, collision is a hard error, instances (not
    classes) are stored so ``isinstance(connector, Cap)`` dispatch works.

    The :meth:`lookup` helper answers the (action, chain) -> int question
    used by the framework's ``get_gas_estimate(chain, operation)`` site.
    The internal ``{action: connector}`` map is built lazily on first
    call and cached; subsequent ``register`` calls invalidate the cache.
    """

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, GasEstimateConnector] = {}
        # Resolved {action: connector} map. None means "not built yet".
        self._action_to_connector: dict[str, GasEstimateConnector] | None = None

    def register(self, connector: GasEstimateConnector) -> None:
        """Register a connector instance. Collision on protocol raises.

        The registry stores instances so it can dispatch capability calls
        (``isinstance(connector, Cap)``) and read per-instance ``protocol``.
        Passing a class — a common slip — would break both, so reject it
        loudly at registration time rather than at first capability lookup.
        """
        if not isinstance(connector, GasEstimateConnector):
            raise GasEstimateRegistryError(
                "register() expects a GasEstimateConnector instance, got "
                f"{type(connector).__qualname__!s} "
                f"({connector!r}); did you forget to instantiate the class?"
            )
        # Fail fast on the silent-disable bug: a connector inherited from
        # ``GasEstimateConnector`` but forgot the ``GasEstimateCapability``
        # mixin would be quietly skipped by ``_resolve_action_map``'s
        # ``isinstance`` filter, leaving its actions unroutable. Refuse the
        # registration so the typo surfaces at boot rather than at the first
        # ``lookup`` call for the missing action. (Gemini review of PR #2477.)
        if not isinstance(connector, GasEstimateCapability):
            raise GasEstimateRegistryError(
                "register() expects a connector implementing GasEstimateCapability "
                f"in addition to GasEstimateConnector; {type(connector).__qualname__!s} "
                "is missing the mixin / required methods (gas_estimate_keys, gas_estimate). "
                "Without the capability the connector's actions would be silently "
                "ignored."
            )
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            raise GasEstimateRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite "
                f"with {type(connector).__qualname__}"
            )
        self._connectors[proto] = connector
        # Any cached resolution is now stale.
        self._action_to_connector = None

    def get(self, protocol: ProtocolName) -> GasEstimateConnector | None:
        """Return the connector registered under ``protocol`` (or ``None``)."""
        return self._connectors.get(protocol)

    def all(self) -> tuple[GasEstimateConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``.

        ``capability`` must be a ``@runtime_checkable`` Protocol. Order
        matches registration order.
        """
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def lookup(self, action: str, chain: str) -> int | None:
        """Return the gas estimate for ``action`` on ``chain``, or ``None``.

        Returns ``None`` when no registered connector publishes ``action`` —
        the caller is expected to fall back to a baseline default table.
        This is the **only** path the framework's
        ``get_gas_estimate(chain, operation)`` uses to consult connectors;
        per-chain overrides on ``ChainDescriptor.gas.operation_overrides``
        and the baseline-default table are out of scope for this registry.

        Collision detection (two connectors claiming the same action) runs
        at map-build time on the first lookup, not at registration — so
        registering a colliding pair raises here on first
        ``lookup(action, chain)`` rather than from inside ``register``.
        Module-level test
        ``tests/unit/connectors/test_gas_estimate_registry_completeness.py``
        forces an early lookup to surface collisions at import time.
        """
        if self._action_to_connector is None:
            self._action_to_connector = self._resolve_action_map()
        connector = self._action_to_connector.get(action)
        if connector is None:
            return None
        # ``isinstance`` guard mirrors the W2 receipt-parser registry's
        # defensive shape: ``_resolve_action_map`` only inserts capability-
        # implementing connectors, so this branch is unreachable in
        # practice, but it gives mypy the proof it needs that
        # ``connector.gas_estimate(...)`` is a valid call.
        if not isinstance(connector, GasEstimateCapability):
            return None
        return connector.gas_estimate(action, chain)

    def action_owner(self, action: str) -> GasEstimateConnector | None:
        """Return the connector that publishes ``action`` (or ``None``).

        Diagnostic helper for tests and tooling — production code should
        use :meth:`lookup` which folds owner resolution and integer
        retrieval into one call.
        """
        if self._action_to_connector is None:
            self._action_to_connector = self._resolve_action_map()
        return self._action_to_connector.get(action)

    def actions(self) -> frozenset[str]:
        """Return every action key any registered connector publishes."""
        if self._action_to_connector is None:
            self._action_to_connector = self._resolve_action_map()
        return frozenset(self._action_to_connector)

    def _resolve_action_map(self) -> dict[str, GasEstimateConnector]:
        """Build the ``{action: connector}`` map and validate keys + collisions.

        Two connectors publishing the same action is a hard error — the
        registry has no way to pick between them and the W6 design point
        is that exactly one connector owns each per-protocol action.
        """
        action_to_connector: dict[str, GasEstimateConnector] = {}
        for connector in self._connectors.values():
            if not isinstance(connector, GasEstimateCapability):
                continue
            keys = connector.gas_estimate_keys()
            if not isinstance(keys, frozenset) or not keys:
                raise GasEstimateRegistryError(
                    f"{type(connector).__qualname__}.gas_estimate_keys() "
                    f"must return a non-empty frozenset, got {keys!r}"
                )
            for key in keys:
                if not isinstance(key, str) or not key:
                    raise GasEstimateRegistryError(
                        f"{type(connector).__qualname__}.gas_estimate_keys() "
                        f"returned an invalid key {key!r} (must be a non-empty str)"
                    )
                existing_owner = action_to_connector.get(key)
                if existing_owner is not None and existing_owner is not connector:
                    raise GasEstimateRegistryError(
                        f"action key {key!r} claimed by both "
                        f"{type(existing_owner).__qualname__} and "
                        f"{type(connector).__qualname__}"
                    )
                action_to_connector[key] = connector
        return action_to_connector

    def clear(self) -> None:
        """Test helper — clear registrations. NOT used in production paths."""
        self._connectors.clear()
        self._action_to_connector = None


STRATEGY_GAS_ESTIMATE_REGISTRY: GasEstimateConnectorRegistry = GasEstimateConnectorRegistry()
