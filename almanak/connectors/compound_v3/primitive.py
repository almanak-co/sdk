"""Compound V3 primitive declaration.

Compound V3 (Comet) money-market positions roll up to
:attr:`Primitive.LENDING`. Both the canonical ``COMPOUND_V3`` and the legacy
short ``COMPOUND`` label resolve here.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.LENDING,
    position_type_aliases=frozenset({"COMPOUND_V3", "COMPOUND"}),
)
