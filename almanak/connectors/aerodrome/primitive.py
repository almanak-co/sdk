"""Aerodrome primitive declaration.

Aerodrome (Solidly-fork concentrated/stable LP on Base) positions roll up to
:attr:`Primitive.LP`. Both the bare ``AERODROME`` and the ``AERODROME_LP``
spellings the materializer historically observed resolve here.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.LP,
    position_type_aliases=frozenset({"AERODROME", "AERODROME_LP"}),
)
