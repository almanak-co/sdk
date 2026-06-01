"""Morpho Blue primitive declaration.

Morpho Blue isolated-market lending positions roll up to
:attr:`Primitive.LENDING`. Both the legacy ``MORPHO`` alias and the canonical
``MORPHO_BLUE`` label resolve here.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.LENDING,
    position_type_aliases=frozenset({"MORPHO", "MORPHO_BLUE"}),
)
