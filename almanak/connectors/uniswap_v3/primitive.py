"""Uniswap V3 primitive declaration.

Uniswap V3 concentrated-liquidity positions roll up to :attr:`Primitive.LP`.
The position-state materializer (``accounting.position_state``) may observe
these positions under the protocol-name labels below; the registry resolves
them all to ``Primitive.LP``.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.LP,
    position_type_aliases=frozenset({"UNI_V3", "UNISWAP_V3"}),
)
