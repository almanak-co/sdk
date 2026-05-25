"""Top-level connectors package — foundation for connector self-containment.

The connector self-containment program (PR 2169 / VIB-4121) moves each
protocol's strategy-side and gateway-side code into a single
self-contained ``almanak/connectors/<protocol>/`` folder. The foundation
that both halves share lives under ``_base/``.

Phase 0 — this PR — lands the gateway-side foundation only:

* ``_base.types`` — strategy-safe shared types (``ProtocolName``,
  ``ProtocolKind``, re-exported ``Chain`` / ``ChainFamily``).
* ``_base.gateway_connector`` — abstract base for gateway-side connectors.
* ``_base.gateway_capabilities`` — runtime-checkable Protocols a connector
  declares it implements.
* ``_base.gateway_registry`` — the ``GatewayConnectorRegistry`` the gateway
  boot loop talks to.
* ``_gateway_registry`` — outer registration point (imports every concrete
  gateway-side connector; empty until Phase 2 migrations land).

Strategy-side foundation (``_base.strategy_connector``,
``_base.strategy_capabilities``, ``_base.strategy_registry``) is part of the
broader 9-12 month program and is NOT included in this PR.

Concrete connectors continue to live under
``almanak/framework/connectors/<protocol>/`` for the duration of the
migration; their gateway-side bits move to
``almanak/connectors/<protocol>/gateway/`` in Phase 2 PRs.
"""
