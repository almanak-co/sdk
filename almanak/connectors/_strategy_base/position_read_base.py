"""Shared vocabulary for connector-owned LP/vault position-read dispatch.

Sibling of ``lending_read_base`` / ``perps_read_base``. This module holds the
*pure* vocabulary the
:class:`~almanak.connectors._strategy_base.position_read_registry.PositionReadRegistry`
and the connector manifests share — the set of recognised position-read
**kinds** and the connector-side builder signature. It imports nothing from the
framework or the descriptor, so ``_connector_descriptor`` can import the kind
set for ``PositionReadDecl`` validation without a cycle.

A position-read *kind* names which framework on-chain repricer family owns a
protocol's LP/vault valuation. The manifest-driven seam (VIB-5126 / VIB-5420)
covers the two capability-gated LP readers that previously hardcoded a
protocol-name set in the framework:

* ``fungible_lp`` — fungible ERC-20-share, two-token LP (Fluid SmartLending),
  valued by a CONNECTOR-side builder (share balance → per-share token0/token1
  claim). Connectors of this kind MUST publish a ``builder`` ImportRef; the
  framework :class:`FungibleLpPositionReader` owns no per-protocol math.
* ``curve_lp`` — a single fungible Curve LP token over N coins, valued by the
  FRAMEWORK :class:`CurveLpPositionReader` (lp_balance × live virtual_price ×
  numeraire). The math is framework-owned, so no connector ``builder`` is
  published.

The Uniswap-V3-NFT LP read (the valuer's data-shape fall-through) and the
ERC-4626 vault read are NOT dispatched through this registry — they are not
protocol-name gated — so those connectors declare no ``position_read``. A new
protocol-name-gated repricer (e.g. a Pendle-LP marker) extends this set and
declares ``position_read`` on its manifest, rather than adding a
``protocol == "..."`` branch to the valuer.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

# Recognised position-read kinds. The descriptor validates a
# ``PositionReadDecl.kind`` against this set so a typo'd kind fails loudly at
# manifest-registration time rather than silently rendering a connector's
# repricer unreachable (which would let the position fall into the V3-NFT
# decode and corrupt the mark — the class of bug this seam exists to prevent).
FUNGIBLE_LP = "fungible_lp"
CURVE_LP = "curve_lp"

POSITION_READ_KINDS: frozenset[str] = frozenset({FUNGIBLE_LP, CURVE_LP})

# Kinds whose valuation math lives in a CONNECTOR-side builder and therefore
# REQUIRE a ``PositionReadDecl.builder`` ImportRef. Kinds outside this set
# (``curve_lp``) are valued by a framework-owned reader and must NOT declare a
# builder.
BUILDER_REQUIRED_KINDS: frozenset[str] = frozenset({FUNGIBLE_LP})

# Connector-side fungible-LP builder signature:
# ``(gateway_client, chain, wrapper, wallet_address) -> FungibleLpPosition | None``.
# Typed loosely (``Any``) so this strategy-side module need not import the
# framework ``FungibleLpPosition`` result type.
PositionReadBuilder = Callable[[Any, str, str, str], Any]

__all__ = [
    "BUILDER_REQUIRED_KINDS",
    "CURVE_LP",
    "FUNGIBLE_LP",
    "POSITION_READ_KINDS",
    "PositionReadBuilder",
]
