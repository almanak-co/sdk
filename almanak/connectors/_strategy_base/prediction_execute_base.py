"""Strategy-side base types for connector-owned prediction-market (CLOB) execution (VIB-4989).

Companion to :mod:`prediction_read_base`. Where the read seam publishes a
market-data provider, this seam publishes the **CLOB order-execution handler** — a
venue's order placement / status logic. Historically the Polymarket
``ClobActionHandler`` lived in the framework (``framework/execution/clob_handler.py``)
and was imported directly by ``strategy_runner.py``, leaking the connector into
the framework (VIB-4989, part of the VIB-4851 self-containment epic).

A prediction market is an **off-chain CLOB**, so order submission is a stateful
gRPC round-trip to a connector-owned ``PolymarketService`` (the gateway holds the
keys and signs server-side), not an ``Intent`` the generic on-chain orchestrator
executes. The seam therefore publishes a **handler factory** (``build_handler``);
the framework consumes the returned handler through its existing
:class:`~almanak.framework.execution.handler_registry.ExecutionHandler` Protocol
(``supported_protocols`` / ``can_handle`` / ``execute``). The neutral result and
persisted-state dataclasses (``ClobExecutionResult`` / ``ClobOrderState`` / …)
stay in the framework as the persisted-format contract; the connector imports
them back (connector→framework is allowed).

Gateway-boundary note: this module is ``_strategy_base`` and performs no network
egress; it references the framework ``ExecutionHandler`` only in annotations
(string-typed under ``TYPE_CHECKING``) to avoid a connectors→framework cycle. The
built handler signs nothing locally — it routes through the gateway.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Annotation-only: a runtime import would pull ``almanak.framework.execution``
    # into ``_strategy_base`` (a connectors→framework cycle).
    from almanak.framework.execution.handler_registry import ExecutionHandler

__all__ = ["PredictionExecuteSpec"]


@dataclass(frozen=True)
class PredictionExecuteSpec:
    """Connector-published descriptor: how to build a CLOB execution handler.

    Attributes:
        build_handler: ``(*, gateway_client, wallet) -> ExecutionHandler``. A
            factory the registry calls to construct the connector's CLOB handler,
            wired to the gateway client. Keyword-only at the registry boundary.
            The returned object implements the framework ``ExecutionHandler``
            Protocol (``supported_protocols`` / ``can_handle`` / ``execute``).
        chains: The chains this connector's CLOB execution supports (e.g.
            ``frozenset({"polygon"})``). Drives ``protocols_for_chain`` /
            ``supports_chain`` so the runner never hardcodes a chain for a venue
            (replaces the ``if chain == "polygon"`` gate).
    """

    build_handler: Callable[..., ExecutionHandler]
    chains: frozenset[str]

    def __post_init__(self) -> None:
        # Validate ``build_handler`` is callable so an invalid spec is caught at
        # registration/import, not late when the runner first builds the handler
        # (mirrors ``GatewayStubSpec``'s ``stub_factory`` check).
        if not callable(self.build_handler):
            raise TypeError(f"build_handler must be callable, got {type(self.build_handler).__name__}.")
        # Reject a bare str for ``chains`` (a str is iterable → each character would
        # register as a chain). Coerce other iterables to a frozenset of non-empty
        # strings (frozen dataclass → set via ``object.__setattr__``).
        chains = self.chains
        if isinstance(chains, str | bytes):
            raise TypeError(
                "chains must be a frozenset[str], not a bare "
                f"{type(chains).__name__} (a bare string is iterated character-by-character); "
                "pass e.g. frozenset({'polygon'})."
            )
        coerced = frozenset(chains)
        for member in coerced:
            if not isinstance(member, str) or not member:
                raise TypeError(f"chains members must be non-empty strings, got {member!r}.")
        object.__setattr__(self, "chains", coerced)
