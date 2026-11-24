"""Economic effects update shared cash and every dependent currency view atomically."""

from uuid import uuid4

from almanak.backtest.economic_state import EconomicPosition as AaveV4Position
from almanak.connectors.aave_v3.economic_parameters import payer_wallet
from almanak.connectors.aave_v3.economic_parameters import position_key as lending_key
from almanak.connectors.aave_v3.economic_state import Position as LendingPosition
from almanak.connectors.aave_v3.observations import Ownership as LendingOwnership
from almanak.connectors.aave_v4.backtest import position_key as aave_v4_key
from almanak.connectors.aave_v4.models import Ownership as AaveV4Ownership
from almanak.connectors.across.economic_model import position_key as across_key
from almanak.connectors.across.economic_state import Position as AcrossPosition
from almanak.connectors.across.observations import Ownership as AcrossOwnership
from almanak.connectors.aerodrome.economic_parameters import position_key as aerodrome_key
from almanak.connectors.aerodrome.economic_state import EconomicPosition as AerodromePosition
from almanak.connectors.aerodrome.positions import Ownership as AerodromeOwnership
from almanak.connectors.aerodrome_slipstream.economic_parameters import (
    position_key as slipstream_key,
)
from almanak.connectors.aerodrome_slipstream.economic_state import (
    EconomicPosition as SlipstreamPosition,
)
from almanak.connectors.aerodrome_slipstream.positions import Ownership as SlipstreamOwnership
from almanak.connectors.composed.economic_state import Position, State
from almanak.connectors.composed.inventory import initial_owned, refresh
from almanak.connectors.composed.models import OwnedParameters
from almanak.connectors.compound_v3.economic_parameters import position_key as compound_key
from almanak.connectors.compound_v3.economic_state import Position as CompoundPosition
from almanak.connectors.compound_v3.observations import Ownership as CompoundOwnership
from almanak.connectors.curve.economic_parameters import position_key as curve_key
from almanak.connectors.curve.economic_state import EconomicPosition as CurvePosition
from almanak.connectors.curve.positions import Ownership as CurveOwnership
from almanak.connectors.gmx_v2.economic_component import initial_position as gmx_initial
from almanak.connectors.gmx_v2.economic_parameters import position_key as gmx_key
from almanak.connectors.gmx_v2.economic_perpetual_state import EconomicPerpetualPosition
from almanak.connectors.gmx_v2.economic_state import EconomicPosition as GmxPosition
from almanak.connectors.gmx_v2.exchange_observations import ExchangeOwnership as GmxOwnership
from almanak.connectors.gmx_v2.positions import Ownership as GmxPerpetualOwnership
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.economic_parameters import position_key as morpho_key
from almanak.connectors.morpho_blue.economic_positions import Position as MorphoPosition
from almanak.connectors.morpho_blue.observations import Ownership as MorphoOwnership
from almanak.connectors.pancakeswap_v3.economic_parameters import position_key as pancake_key
from almanak.connectors.pancakeswap_v3.economic_state import EconomicPosition as PancakePosition
from almanak.connectors.pancakeswap_v3.positions import Ownership as PancakeOwnership
from almanak.connectors.sparklend.economic_parameters import position_key as spark_key
from almanak.connectors.sparklend.economic_state import Position as SparkPosition
from almanak.connectors.sparklend.observations import Ownership as SparkOwnership
from almanak.connectors.uniswap_v3.economic_parameters import position_key as exchange_key
from almanak.connectors.uniswap_v3.economic_state import EconomicPosition
from almanak.connectors.uniswap_v4.backtest import HistoricalPosition as V4Position
from almanak.connectors.uniswap_v4.backtest import empty_allowances
from almanak.connectors.uniswap_v4.backtest import position_key as v4_key
from almanak.connectors.uniswap_v4.state import Ownership as V4Ownership
from almanak.effects import Effects, Portfolio, PositionChange
from almanak.effects import Position as HeldPosition
from almanak.models import AccountBinding, StrategyAccounts


def parameters(author, settings, interval):
    # Band fee attribution is proved for single-connector exchange runs only.
    if any(getattr(a, "band", None) is not None for a in author.allocations):
        raise ValueError(
            "banded liquidity allocations are not supported in composed economic backtests yet"
        )
    return OwnedParameters(
        decision_trigger=interval.effective,
        asset_metadata=author.asset_metadata,
        parameters=author.parameters,
        allocations=author.allocations,
        inventory=author.inventory,
        accounts=StrategyAccounts(
            bindings=tuple(AccountBinding(name=f.name, profile=f.profile) for f in settings.funds)
        ),
    )


def held_position(owned, config=None):
    if isinstance(owned, (GmxOwnership, GmxPerpetualOwnership)):
        component = config.component(ConnectorId("gmx_v2"))
        return HeldPosition[Position](
            key=gmx_key(owned.target),
            payload=gmx_initial(component, component.allocation(owned.target.allocation), owned),
        )
    if isinstance(owned, AaveV4Ownership):
        return HeldPosition[Position](
            key=aave_v4_key(owned.target), payload=AaveV4Position(target=owned.target)
        )
    if isinstance(owned, AcrossOwnership):
        return HeldPosition[Position](
            key=across_key(owned.target), payload=AcrossPosition(target=owned.target)
        )
    if isinstance(owned, V4Ownership):
        return HeldPosition[Position](
            key=v4_key(owned.target),
            payload=V4Position(owned=owned, allowances=empty_allowances(owned.target)),
        )
    if isinstance(owned, LendingOwnership):
        return HeldPosition[Position](
            key=lending_key(owned.target), payload=LendingPosition(target=owned.target)
        )
    if isinstance(owned, SparkOwnership):
        return HeldPosition[Position](
            key=spark_key(owned.target), payload=SparkPosition(target=owned.target)
        )
    if isinstance(owned, MorphoOwnership):
        return HeldPosition[Position](
            key=morpho_key(owned.target), payload=MorphoPosition(**owned.model_dump())
        )
    if isinstance(owned, CompoundOwnership):
        return HeldPosition[Position](
            key=compound_key(owned.target), payload=CompoundPosition(**owned.model_dump())
        )
    if isinstance(owned, CurveOwnership):
        return HeldPosition[Position](
            key=curve_key(owned.target), payload=CurvePosition(target=owned.target, owned=owned)
        )
    if isinstance(owned, PancakeOwnership):
        return HeldPosition[Position](
            key=pancake_key(owned.target), payload=PancakePosition(target=owned.target, owned=owned)
        )
    if isinstance(owned, AerodromeOwnership):
        return HeldPosition[Position](
            key=aerodrome_key(owned.target),
            payload=AerodromePosition(target=owned.target, owned=owned),
        )
    if isinstance(owned, SlipstreamOwnership):
        return HeldPosition[Position](
            key=slipstream_key(owned.target),
            payload=SlipstreamPosition(target=owned.target, owned=owned),
        )
    return HeldPosition[Position](
        key=exchange_key(owned.target), payload=EconomicPosition(target=owned.target, owned=owned)
    )


def initial(settings, config, interval):
    portfolio = Portfolio[Position](
        balances=tuple(b for fund in (*settings.funds, *settings.payers) for b in fund.inventory()),
        positions=tuple(held_position(owned, config) for owned in initial_owned(config)),
    )
    if any(portfolio.balance(b.key) != b.amount for b in config.inventory):
        raise ValueError("economic initial fund cash differs from the explicit author inventory")
    if set(settings.models) != set(config.parameter_components):
        raise ValueError("economic model policies must match configured connectors exactly")
    if {a.chain for a in config.positions} != {h.chain for h in settings.history}:
        raise ValueError("composed historical providers must cover configured chains exactly")
    for identity in config.parameter_components:
        native = config.component(identity)
        policy = settings.component(identity, native)
        if identity == ConnectorId("gmx_v2"):
            validate_gmx_initial(policy, native, portfolio)
        for allocation in native.positions:
            target = native.target(allocation.name)
            portfolio.balance(payer_wallet(policy, target, allocation.native_asset))
    return State(
        run_id=uuid4().hex,
        time=settings.schedule.start,
        schedule=settings.schedule,
        interval=interval,
        portfolio=portfolio,
    )


def validate_gmx_initial(settings, config, portfolio):
    from almanak.connectors.gmx_v2.economic_parameters import (
        validate_allocation,
        validate_publication_sources,
        verify_inventory,
    )

    validate_publication_sources(config, settings)
    for allocation in config.allocations:
        validate_allocation(allocation, settings)
    for position in portfolio.positions:
        if position.key.connector == ConnectorId("gmx_v2"):
            verify_inventory(settings, config, portfolio, position.payload)


def refresh_position(held, balances):
    if not isinstance(
        held.payload,
        (
            EconomicPosition,
            V4Position,
            CurvePosition,
            PancakePosition,
            AerodromePosition,
            SlipstreamPosition,
            GmxPosition,
            EconomicPerpetualPosition,
        ),
    ):
        return held
    owned = refresh((held.payload.owned,), balances)[0]
    return held.model_copy(update={"payload": held.payload.model_copy(update={"owned": owned})})


def validate_views(portfolio):
    if any(refresh_position(held, portfolio.balances) != held for held in portfolio.positions):
        raise ValueError("economic connector currency views differ from shared fund cash")


def shared_transition(state, transition):
    applied = state.portfolio.apply(transition.effects)
    current = {p.key: p for p in state.portfolio.positions}
    if {p.key for p in applied.positions} != set(current):
        raise ValueError("economic effects cannot replace configured position identities")
    refreshed = tuple(refresh_position(p, applied.balances) for p in applied.positions)
    effects = Effects[Position](
        assets=transition.effects.assets,
        positions=tuple(
            PositionChange[Position](before=current[p.key], after=p)
            for p in refreshed
            if current[p.key] != p
        ),
    )
    return transition.model_copy(update={"effects": effects})
