"""Every teardown.models.PositionType has a corresponding Primitive.

VIB-4162 (T2). The teardown PositionType priority is teardown-protocol-
specific (risk-ordered close), but the underlying primitive mapping lives
in primitives.taxonomy.materializer_primitive_for. This test documents
the invariant: a new teardown PositionType cannot ship without a
corresponding primitive.
"""

from __future__ import annotations

import pytest

from almanak.framework.primitives.taxonomy import materializer_primitive_for
from almanak.framework.teardown.models import PositionType


@pytest.mark.parametrize("position_type", list(PositionType))
def test_every_position_type_has_primitive(position_type: PositionType) -> None:
    primitive = materializer_primitive_for(position_type.value)
    assert primitive is not None, (
        f"PositionType.{position_type.name} ({position_type.value!r}) has no "
        "corresponding Primitive in materializer_primitive_for. Add it to the "
        "taxonomy helper before introducing the new teardown position type."
    )
