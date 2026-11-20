"""The order in which a refused validation's errors are reported is the SDK's own."""

import json

import pytest
from pydantic import ValidationError

from almanak.models import Model, refusals


class Inner(Model):
    value: int


class Outer(Model):
    zeta: int
    alpha: int
    items: tuple[Inner, ...] = ()


DOCUMENT = {
    "zeta": "x",
    "items": ({"value": 1}, {"value": "x", "stray": 1}, {"value": "y"}),
    "unknown": 1,
}
# The undeclared keys first, the shortest path first, then the path, indexes as numbers.
EXPECTED = [
    ("unknown",),
    ("items", 1, "stray"),
    ("alpha",),
    ("zeta",),
    ("items", 1, "value"),
    ("items", 2, "value"),
]


@pytest.mark.parametrize(
    "validate",
    [Outer.model_validate, lambda value: Outer.model_validate_json(json.dumps(value))],
    ids=["python", "json"],
)
@pytest.mark.parametrize("reverse", [False, True])
def test_refusals_do_not_depend_on_the_validator_or_the_input_order(validate, reverse):
    document = dict(reversed(DOCUMENT.items())) if reverse else DOCUMENT
    with pytest.raises(ValidationError) as refused:
        validate(document)
    assert [item["loc"] for item in refusals(refused.value)] == EXPECTED
    assert all("input" not in item and "url" not in item for item in refusals(refused.value))
