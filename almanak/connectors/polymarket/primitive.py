"""Polymarket primitive declaration.

Polymarket prediction-market positions roll up to
:attr:`Primitive.PREDICTION`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.PREDICTION,
    position_type_aliases=frozenset({"POLYMARKET"}),
)
