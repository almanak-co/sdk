"""GMX V2 primitive declaration.

GMX V2 perpetual positions roll up to :attr:`Primitive.PERP`. Both the
canonical ``GMX_V2`` and the legacy short ``GMX`` label resolve here.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.PERP,
    position_type_aliases=frozenset({"GMX", "GMX_V2"}),
)
