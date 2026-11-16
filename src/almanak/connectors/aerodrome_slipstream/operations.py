"""Concrete connector operations retain their target through shared V3 validation."""

from typing import Annotated

from pydantic import Field

from almanak.connectors import v3_pool_operations as family
from almanak.connectors.aerodrome_slipstream.models import Target
from almanak.connectors.v3_pool_operations import validate_amounts as validate_amounts
from almanak.connectors.v3_pool_operations import validate_nft as validate_nft


class Operation(family.Operation[Target]):
    pass


class Approval(family.Approval[Target]):
    pass


class Swap(family.Swap[Target]):
    pass


class Addition(family.Addition[Target]):
    pass


class Mint(family.Mint[Target]):
    pass


class Increase(family.Increase[Target]):
    pass


class NftOperation(family.NftOperation[Target]):
    pass


class Collect(family.Collect[Target]):
    pass


class Removal(family.Removal[Target]):
    pass


class Decrease(family.Decrease[Target]):
    pass


class Close(family.Close[Target]):
    pass


ProtocolOperation = Annotated[
    Approval | Swap | Mint | Increase | Decrease | Collect | Close,
    Field(discriminator="kind"),
]
