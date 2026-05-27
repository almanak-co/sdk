"""Top-level connectors package — connector self-containment.

The connector self-containment program (VIB-4121, with the strategy-side
migration finalised in VIB-4835) keeps each protocol's strategy-side and
gateway-side code in a single self-contained ``almanak/connectors/<protocol>/``
folder. Foundation primitives shared by both halves live under
``_base/`` (gateway-side) and ``_strategy_base/`` (strategy-side).

Current state (post-VIB-4835 Phase 2):

* ``_base/`` — gateway-side foundation:
  * ``_base.types`` — strategy-safe shared types (``ProtocolName``,
    ``ProtocolKind``, re-exported ``Chain`` / ``ChainFamily``).
  * ``_base.gateway_connector`` — abstract base for gateway-side connectors.
  * ``_base.gateway_capabilities`` — runtime-checkable Protocols a connector
    declares it implements.
  * ``_base.gateway_registry`` — the ``GatewayConnectorRegistry`` the
    gateway boot loop talks to.
* ``_gateway_registry`` — outer registration point (imports every concrete
  gateway-side connector).
* ``_strategy_base/`` — strategy-side foundation (base receipt parser,
  registries, compilers, bridge base, contract registry, protocol aliases,
  capabilities registry).
* ``<protocol>/`` — every concrete connector. Each one ships ``adapter.py``,
  ``receipt_parser.py``, optional ``sdk.py``, optional
  ``permission_hints.py``, and an ``__init__.py`` that wires lazy PEP 562
  exports plus a ``_register_once()`` helper invoked on first attribute
  access. See ``docs/internal/blueprints/05-connectors.md`` for the canonical pattern.

``almanak.framework.connectors`` is no longer a valid import path; the CI
ratchet ``tests/static/test_legacy_connector_imports.py`` blocks regressions.
"""
