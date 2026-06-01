"""Aave V3 primitive declaration.

Aave V3 money-market positions (collateral + debt) roll up to
:attr:`Primitive.LENDING`. The materializer observes them under both the
canonical ``AAVE_V3`` and the legacy short ``AAVE`` label.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.primitive_registry import PrimitiveDeclaration
from almanak.framework.primitives.types import Primitive

PRIMITIVE = PrimitiveDeclaration(
    primitive=Primitive.LENDING,
    position_type_aliases=frozenset({"AAVE_V3", "AAVE"}),
)
