"""The public author bindings of every convertible connector.

One binding names a connector's market selection, its resolved facts and its
position inputs. The public root types its markets and positions with the
aliases declared here; adding a connector adds one binding and one member to
each alias (discriminated on the connector tag) and never changes the root
shape. `tests/test_authoring_bindings.py` keeps the aliases and the registry
in lockstep.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any

from pydantic import Field

from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.aave_v3 import author_inputs as aave_v3_inputs
from almanak.connectors.aave_v3 import resolution as aave_v3_resolution
from almanak.connectors.aave_v4 import authoring as aave_authoring
from almanak.connectors.aave_v4 import resolution as aave_resolution
from almanak.connectors.across import author_inputs as across_inputs
from almanak.connectors.across import resolution as across_resolution
from almanak.connectors.aerodrome import author_inputs as aerodrome_inputs
from almanak.connectors.aerodrome import resolution as aerodrome_resolution
from almanak.connectors.aerodrome_slipstream import author_inputs as slipstream_inputs
from almanak.connectors.aerodrome_slipstream import resolution as slipstream_resolution
from almanak.connectors.compound_v3 import author_inputs as compound_v3_inputs
from almanak.connectors.compound_v3 import resolution as compound_v3_resolution
from almanak.connectors.curve import author_inputs as curve_inputs
from almanak.connectors.curve import resolution as curve_resolution
from almanak.connectors.gmx_v2 import author_inputs as gmx_v2_inputs
from almanak.connectors.gmx_v2 import resolution as gmx_v2_resolution
from almanak.connectors.hyperliquid import author_inputs as hyperliquid_inputs
from almanak.connectors.hyperliquid import resolution as hyperliquid_resolution
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue import author_inputs as morpho_blue_inputs
from almanak.connectors.morpho_blue import resolution as morpho_blue_resolution
from almanak.connectors.pancakeswap_v3 import author_inputs as pancakeswap_v3_inputs
from almanak.connectors.pancakeswap_v3 import resolution as pancakeswap_v3_resolution
from almanak.connectors.sparklend import author_inputs as sparklend_inputs
from almanak.connectors.sparklend import resolution as sparklend_resolution
from almanak.connectors.uniswap_v3 import author_inputs as uniswap_v3_inputs
from almanak.connectors.uniswap_v3 import resolution as uniswap_v3_resolution
from almanak.connectors.uniswap_v4 import author_inputs as uniswap_v4_inputs
from almanak.connectors.uniswap_v4 import resolution as uniswap_v4_resolution
from almanak.models import Model


@dataclass(frozen=True)
class AuthorBinding:
    connector: ConnectorId
    selection: type[Model]
    resolved: type[Model]
    resolve: Callable[..., Model]
    # One position type, or a union of this connector's position kinds discriminated
    # on `kind`; every member carries the connector tag the outer union selects on.
    allocation: Any
    debt: type[Model] | None = None
    # A market that spans two chains is completed on its second chain, with that chain's
    # own reader and anchor: `complete(reader, resolved, block)`. The selection names the
    # chain as `destination_chain` and the completed facts retain `destination_block`.
    complete: Callable[..., Model] | None = None
    # A check of freshly resolved facts against reviewed code, such as a pinned
    # implementation: `admit(resolved)`. It runs when a market is first resolved or
    # completed and never on the replay of retained reads, so a later change of the
    # reviewed code cannot refuse the restart of a running deployment.
    admit: Callable[[Any], None] | None = None
    # Replay of retained reads where the first reads depended on reviewed code, such as
    # a pinned address: `replay(reader, retained)` and, for a two-chain market,
    # `replay_complete(reader, observed, retained)`. They ask the retained reads about
    # what the retained facts name. Without them replay calls `resolve` and `complete`.
    replay: Callable[..., Model] | None = None
    replay_complete: Callable[..., Model] | None = None


BINDINGS: tuple[AuthorBinding, ...] = (
    AuthorBinding(
        connector=ConnectorId("aave_v4"),
        selection=aave_resolution.MarketSelection,
        resolved=aave_resolution.ResolvedReserve,
        resolve=aave_resolution.resolve_market,
        allocation=aave_authoring.SupplyInput,
        debt=aave_authoring.DebtInput,
    ),
    AuthorBinding(
        connector=ConnectorId("uniswap_v3"),
        selection=uniswap_v3_resolution.MarketSelection,
        resolved=uniswap_v3_resolution.ResolvedPool,
        resolve=uniswap_v3_resolution.resolve_market,
        allocation=uniswap_v3_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("aave_v3"),
        selection=aave_v3_resolution.MarketSelection,
        resolved=aave_v3_resolution.ResolvedReserve,
        resolve=aave_v3_resolution.resolve_market,
        allocation=aave_v3_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("sparklend"),
        selection=sparklend_resolution.MarketSelection,
        resolved=sparklend_resolution.ResolvedReserve,
        resolve=sparklend_resolution.resolve_market,
        allocation=sparklend_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("compound_v3"),
        selection=compound_v3_resolution.MarketSelection,
        resolved=compound_v3_resolution.ResolvedMarket,
        resolve=compound_v3_resolution.resolve_market,
        allocation=compound_v3_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("morpho_blue"),
        selection=morpho_blue_resolution.MarketSelection,
        resolved=morpho_blue_resolution.ResolvedMarket,
        resolve=morpho_blue_resolution.resolve_market,
        allocation=morpho_blue_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("uniswap_v4"),
        selection=uniswap_v4_resolution.MarketSelection,
        resolved=uniswap_v4_resolution.ResolvedPool,
        resolve=uniswap_v4_resolution.resolve_market,
        allocation=uniswap_v4_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("pancakeswap_v3"),
        selection=pancakeswap_v3_resolution.MarketSelection,
        resolved=pancakeswap_v3_resolution.ResolvedPool,
        resolve=pancakeswap_v3_resolution.resolve_market,
        allocation=pancakeswap_v3_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("aerodrome_slipstream"),
        selection=slipstream_resolution.MarketSelection,
        resolved=slipstream_resolution.ResolvedPool,
        resolve=slipstream_resolution.resolve_market,
        allocation=slipstream_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("aerodrome"),
        selection=aerodrome_resolution.MarketSelection,
        resolved=aerodrome_resolution.ResolvedPool,
        resolve=aerodrome_resolution.resolve_market,
        allocation=aerodrome_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("curve"),
        selection=curve_resolution.MarketSelection,
        resolved=curve_resolution.ResolvedPool,
        resolve=curve_resolution.resolve_market,
        allocation=curve_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("gmx_v2"),
        selection=gmx_v2_resolution.MarketSelection,
        resolved=gmx_v2_resolution.ResolvedMarket,
        resolve=gmx_v2_resolution.resolve_market,
        allocation=gmx_v2_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("hyperliquid"),
        selection=hyperliquid_resolution.MarketSelection,
        resolved=hyperliquid_resolution.ResolvedMarket,
        resolve=hyperliquid_resolution.resolve_market,
        allocation=hyperliquid_inputs.AllocationInput,
    ),
    AuthorBinding(
        connector=ConnectorId("across"),
        selection=across_resolution.MarketSelection,
        resolved=across_resolution.ResolvedRoute,
        resolve=across_resolution.resolve_market,
        allocation=across_inputs.AllocationInput,
        complete=across_resolution.complete_market,
        admit=across_resolution.admit_route,
        replay=across_resolution.replay_market,
        replay_complete=across_resolution.replay_completion,
    ),
)

MarketSelection = Annotated[
    aave_resolution.MarketSelection
    | uniswap_v3_resolution.MarketSelection
    | aave_v3_resolution.MarketSelection
    | sparklend_resolution.MarketSelection
    | compound_v3_resolution.MarketSelection
    | morpho_blue_resolution.MarketSelection
    | uniswap_v4_resolution.MarketSelection
    | pancakeswap_v3_resolution.MarketSelection
    | slipstream_resolution.MarketSelection
    | aerodrome_resolution.MarketSelection
    | curve_resolution.MarketSelection
    | gmx_v2_resolution.MarketSelection
    | hyperliquid_resolution.MarketSelection
    | across_resolution.MarketSelection,
    Field(discriminator="connector"),
]
ResolvedMarketFacts = Annotated[
    aave_resolution.ResolvedReserve
    | uniswap_v3_resolution.ResolvedPool
    | aave_v3_resolution.ResolvedReserve
    | sparklend_resolution.ResolvedReserve
    | compound_v3_resolution.ResolvedMarket
    | morpho_blue_resolution.ResolvedMarket
    | uniswap_v4_resolution.ResolvedPool
    | pancakeswap_v3_resolution.ResolvedPool
    | slipstream_resolution.ResolvedPool
    | aerodrome_resolution.ResolvedPool
    | curve_resolution.ResolvedPool
    | gmx_v2_resolution.ResolvedMarket
    | hyperliquid_resolution.ResolvedMarket
    | across_resolution.ResolvedRoute,
    Field(discriminator="resolver"),
]
AllocationInput = Annotated[
    aave_authoring.SupplyInput
    | uniswap_v3_inputs.AllocationInput
    | aave_v3_inputs.AllocationInput
    | sparklend_inputs.AllocationInput
    | compound_v3_inputs.AllocationInput
    | morpho_blue_inputs.AllocationInput
    | uniswap_v4_inputs.AllocationInput
    | pancakeswap_v3_inputs.AllocationInput
    | slipstream_inputs.AllocationInput
    | aerodrome_inputs.AllocationInput
    | curve_inputs.AllocationInput
    | gmx_v2_inputs.AllocationInput
    | hyperliquid_inputs.AllocationInput
    | across_inputs.AllocationInput,
    Field(discriminator="connector"),
]
DebtInput = aave_authoring.DebtInput


def binding_for(identity) -> AuthorBinding:
    for binding in BINDINGS:
        if str(binding.connector) == str(identity):
            return binding
    bound = ", ".join(str(binding.connector) for binding in BINDINGS)
    raise ConfigurationError(
        f"Connector {identity!r} has no public author binding yet. Bound connectors: {bound}.",
        code="unconverted_connector",
        remedy="Choose a connector whose references use the public author root.",
    )
