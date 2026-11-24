"""Typed cross-connector evidence retained by a composed V3 decision."""

from almanak.connectors.aave_v3.observations import Snapshot as LendingSnapshot
from almanak.connectors.aave_v4.observations import LendingSnapshot as AaveV4Snapshot
from almanak.connectors.across.observations import Snapshot as AcrossSnapshot
from almanak.connectors.aerodrome.observations import Snapshot as AerodromeSnapshot
from almanak.connectors.aerodrome_slipstream.observations import Snapshot as SlipstreamSnapshot
from almanak.connectors.compound_v3.observations import Snapshot as CompoundSnapshot
from almanak.connectors.curve.observations import Snapshot as CurveSnapshot
from almanak.connectors.gmx_v2.exchange_observations import (
    ExchangeSnapshot as GmxExchangeSnapshot,
)
from almanak.connectors.gmx_v2.observations import Snapshot as GmxPositionSnapshot
from almanak.connectors.hyperliquid.observations import Snapshot as HyperliquidSnapshot
from almanak.connectors.morpho_blue.observations import Snapshot as MorphoSnapshot
from almanak.connectors.pancakeswap_v3.observations import Snapshot as PancakeSnapshot
from almanak.connectors.sparklend.observations import Snapshot as SparkSnapshot
from almanak.connectors.uniswap_v3.observations import Snapshot as ExchangeSnapshot
from almanak.connectors.uniswap_v4.observation_models import Snapshot as V4Snapshot

Observation = (
    V4Snapshot
    | AaveV4Snapshot
    | LendingSnapshot
    | ExchangeSnapshot
    | SparkSnapshot
    | PancakeSnapshot
    | SlipstreamSnapshot
    | AerodromeSnapshot
    | CompoundSnapshot
    | MorphoSnapshot
    | CurveSnapshot
    | GmxExchangeSnapshot
    | GmxPositionSnapshot
    | HyperliquidSnapshot
    | AcrossSnapshot
)
