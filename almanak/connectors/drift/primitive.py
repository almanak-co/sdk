"""Drift primitive declaration.

Drift (Solana) perpetual positions roll up to :attr:`Primitive.PERP`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.PERP,
    position_type_aliases=frozenset({"DRIFT"}),
)
