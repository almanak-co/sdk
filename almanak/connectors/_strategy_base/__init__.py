"""Strategy-side connector foundation.

This package holds the **strategy-side** foundation utilities shared
across protocol connectors — concentrated-liquidity math, base
compilers, ABI helpers, receipt parser scaffolding, bridge base, etc.
It is the destination for the per-connector foundation files that live
today under ``almanak/framework/connectors/`` (``base/``,
``bridge_base.py``, ``bridge_compiler.py``, ``compiler_registry.py``,
``capabilities_registry.py``, ``contract_registry.py``,
``protocol_aliases.py``, ``registry.py``, ``vaults/``).

Boundary semantics
------------------
Two foundation tiers sit side-by-side under ``almanak/connectors/``:

* :mod:`almanak.connectors._base` — **cross-boundary** contracts only.
  Holds ``GatewayConnector``, the ``Gateway*Capability`` Protocols,
  ``GatewayConnectorRegistry``, and ``types.py``. Both strategy- and
  gateway-side code may import its strategy-safe public surface; the
  ``gateway_*`` submodules are gateway-only.

* :mod:`almanak.connectors._strategy_base` (this package) —
  **strategy-only** foundation. Pulls in ``web3.py``, ABI decoders, and
  other strategy-container dependencies. Gateway code MUST NOT import
  from here; ``tests/static/test_gateway_connector_isolation.py``
  enforces that.

The directory boundary maps 1:1 to the trust boundary so the static
guards can discriminate by directory root rather than by submodule.

Phase 1 / Phase 2
-----------------
Phase 1 (this commit) scaffolds the empty package and lands the
strategy-side import ratchet (`tests/static/test_legacy_connector_imports.py`).
Phase 2 ``git mv``'s the foundation files into this package and rewrites
their import sites. See `linear.app/almanak/issue/VIB-4835`.
"""

from __future__ import annotations

__all__: list[str] = []
