"""Syntax-only author values; financial conversion waits for verified precision."""

from typing import Annotated

from pydantic import BeforeValidator, Field

from almanak.caip import AssetType
from almanak.configuration.values import quantity, seconds
from almanak.models import Model, UInt


class BaseUnits(Model):
    base_units: UInt


Quantity = Annotated[str, Field(pattern=r"^[0-9]+(?:\.[0-9]+)?$")] | BaseUnits
Duration = Annotated[UInt, BeforeValidator(seconds)]
# A duration the resolved parameters require to be positive (holds, deadlines).
PositiveDuration = Annotated[UInt, BeforeValidator(seconds), Field(gt=0)]


class AmountInput(Model):
    """An author quantity of one named asset, before its precision is verified."""

    asset: AssetType
    quantity: Quantity


def resolve_quantity(value: Quantity, decimals: int) -> int:
    return quantity(value.model_dump() if isinstance(value, BaseUnits) else value, decimals)
