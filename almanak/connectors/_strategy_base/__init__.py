"""Strategy-side connector foundation.

This package holds the **strategy-side** foundation utilities shared
across protocol connectors — concentrated-liquidity math, base
compilers, ABI helpers, receipt parser scaffolding, bridge base,
connector / compiler / capability / contract registries, and the
``vaults/`` shared adapter scaffolding. The protocol-leaf connectors
under ``almanak/connectors/<protocol>/`` consume these foundation
modules; nothing under ``almanak/connectors/<protocol>/`` is allowed to
appear as a re-export here.

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
"""

from __future__ import annotations

__all__: list[str] = []
