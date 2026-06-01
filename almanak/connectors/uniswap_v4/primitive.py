"""Uniswap V4 primitive declaration.

VIB-4477: Uniswap V4 LP positions roll up to :attr:`Primitive.LP_V4`, a
parallel LP primitive whose version stream is isolated from V3 / Aerodrome /
TraderJoe. The materializer's caller collapses ``LP_V4`` back to its shared
``"LP"`` materializer bucket (the LP position state machine is V3/V4-shared);
the split only matters at the version-stamping sites.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.LP_V4,
    position_type_aliases=frozenset({"UNI_V4", "UNISWAP_V4"}),
)
