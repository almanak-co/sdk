"""Every connector's authoritative parameter type, keyed by its connector tag.

The public root assembles one of these types from resolved markets; retained
artifacts decode through the tagged union. A composed parameter set keeps a null
connector tag, which selects a configuration codec and never a protocol.
"""

from typing import Annotated

from pydantic import Discriminator, Tag

from almanak.connectors.aave_v3.models import Parameters as AaveV3Parameters
from almanak.connectors.aave_v4.models import DebtAllocation as AaveV4Debt
from almanak.connectors.aave_v4.models import LendingParameters as AaveV4Parameters
from almanak.connectors.across.models import Parameters as AcrossParameters
from almanak.connectors.aerodrome.models import Parameters as AerodromeParameters
from almanak.connectors.aerodrome_slipstream.models import Parameters as SlipstreamParameters
from almanak.connectors.composed.models import OwnedParameters as ComposedParameters
from almanak.connectors.compound_v3.models import Parameters as CompoundParameters
from almanak.connectors.curve.models import Parameters as CurveParameters
from almanak.connectors.gmx_v2.models import Parameters as GmxParameters
from almanak.connectors.hyperliquid.models import Parameters as HyperliquidParameters
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.models import Parameters as MorphoParameters
from almanak.connectors.pancakeswap_v3.models import Parameters as PancakeParameters
from almanak.connectors.sparklend.models import Parameters as SparkParameters
from almanak.connectors.uniswap_v3.models import Parameters as UniswapV3Parameters
from almanak.connectors.uniswap_v4.models import Parameters as UniswapV4Parameters
from almanak.models import Model

COMPOSED = "composed"

PARAMETER_TYPES: dict[str, type[Model]] = {
    "aave_v4": AaveV4Parameters,
    "uniswap_v4": UniswapV4Parameters,
    "aave_v3": AaveV3Parameters,
    "sparklend": SparkParameters,
    "uniswap_v3": UniswapV3Parameters,
    "pancakeswap_v3": PancakeParameters,
    "aerodrome": AerodromeParameters,
    "aerodrome_slipstream": SlipstreamParameters,
    "morpho_blue": MorphoParameters,
    "compound_v3": CompoundParameters,
    "curve": CurveParameters,
    "gmx_v2": GmxParameters,
    "hyperliquid": HyperliquidParameters,
    "across": AcrossParameters,
    COMPOSED: ComposedParameters,
}


def parameter_tag(value) -> str:
    """The connector tag of a raw or typed parameter set.

    The wire convention of ``connector_from_wire`` applies: an absent tag is the
    Aave V4 parameter set, whose retained artifacts carry no tag, and a null tag
    is composed.
    """
    if isinstance(value, dict):
        tag = value.get("connector", "aave_v4")
    else:
        tag = getattr(value, "connector", "aave_v4")
    return COMPOSED if tag is None else str(tag)


def parameter_type(identity: ConnectorId | None) -> type[Model]:
    key = COMPOSED if identity is None else str(identity)
    try:
        return PARAMETER_TYPES[key]
    except KeyError:
        raise ValueError("configuration has no reviewed parameter binding") from None


def component_allocations(identity, items):
    if identity == ConnectorId("aave_v4"):
        return {
            "allocations": tuple(a for a in items if not isinstance(a, AaveV4Debt)),
            "debts": tuple(a for a in items if isinstance(a, AaveV4Debt)),
        }
    return {"allocations": items}


PARAMETERS = Annotated[
    Annotated[AaveV4Parameters, Tag("aave_v4")]
    | Annotated[UniswapV4Parameters, Tag("uniswap_v4")]
    | Annotated[AaveV3Parameters, Tag("aave_v3")]
    | Annotated[SparkParameters, Tag("sparklend")]
    | Annotated[UniswapV3Parameters, Tag("uniswap_v3")]
    | Annotated[PancakeParameters, Tag("pancakeswap_v3")]
    | Annotated[AerodromeParameters, Tag("aerodrome")]
    | Annotated[SlipstreamParameters, Tag("aerodrome_slipstream")]
    | Annotated[MorphoParameters, Tag("morpho_blue")]
    | Annotated[CompoundParameters, Tag("compound_v3")]
    | Annotated[CurveParameters, Tag("curve")]
    | Annotated[GmxParameters, Tag("gmx_v2")]
    | Annotated[HyperliquidParameters, Tag("hyperliquid")]
    | Annotated[AcrossParameters, Tag("across")]
    | Annotated[ComposedParameters, Tag(COMPOSED)],
    Discriminator(parameter_tag),
]
