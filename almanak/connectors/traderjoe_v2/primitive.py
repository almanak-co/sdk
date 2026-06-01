"""Trader Joe V2 (Liquidity Book) primitive declaration.

Trader Joe V2 Liquidity-Book LP positions roll up to :attr:`Primitive.LP`.
The materializer observes them under the ``TRADERJOE_LP`` label.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.LP,
    position_type_aliases=frozenset({"TRADERJOE_LP"}),
)
