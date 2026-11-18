"""Bind author targets to explicit hypothetical fund and separate submitter inventory."""

from uuid import uuid4

from almanak.connectors.dex_economic_parameters import payer_wallet as payer_wallet
from almanak.connectors.dex_economic_parameters import wallet as wallet
from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.economic_component import initial_position
from almanak.connectors.gmx_v2.economic_state import Payload, State
from almanak.connectors.gmx_v2.exchange_models import SwapAllocation
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
from almanak.connectors.gmx_v2.models import Allocation, Parameters
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import Portfolio, Position, PositionKey
from almanak.models import AccountBinding, StrategyAccounts


def parameters(author, settings, interval):
    config = Parameters(
        decision_trigger=interval.effective,
        asset_metadata=author.asset_metadata,
        parameters=author.parameters,
        allocations=author.allocations,
        accounts=StrategyAccounts(
            bindings=tuple(AccountBinding(name=f.name, profile=f.profile) for f in settings.funds)
        ),
    )
    validate_publication_sources(config, settings)
    for allocation in config.allocations:
        validate_allocation(allocation, settings)
    return config


def validate_publication_sources(config, settings):
    if {binding.allocation for binding in settings.data.publications} != {
        a.name for a in config.allocations
    }:
        raise ValueError("GMX preparation requires one explicit publication source per allocation")


def validate_allocation(allocation, settings):
    if not isinstance(allocation, (Allocation, SwapAllocation, LiquidityAllocation)):
        raise ValueError("GMX prepared request model requires a supported allocation kind")
    if isinstance(allocation, LiquidityAllocation) != bool(settings.liquidity_model):
        raise ValueError("GMX prepared liquidity selection differs from its allocation kind")
    if isinstance(allocation, Allocation) != bool(settings.position_model):
        raise ValueError("GMX prepared position selection differs from its allocation kind")
    payment = next(
        value.payment
        for value in settings.keeper.payments
        if value.chain == allocation.chain.chain_id
    )
    if payment.amount > allocation.execution_fee.amount:
        raise ValueError("GMX modeled keeper payment exceeds the fund's wrapped-native fee escrow")


def position_key(target):
    return PositionKey(
        connector=ConnectorId("gmx_v2"),
        chain=target.fund.chain,
        account=target.account,
        reference=f"allocation:{target.allocation}:{target.market.share_asset}",
    )


def initial(settings, config, interval):
    positions = tuple(
        Position[Payload](
            key=position_key(config.target(allocation.name)),
            payload=initial_position(config, allocation),
        )
        for allocation in config.allocations
    )
    portfolio = Portfolio[Payload](
        balances=tuple(
            balance for fund in (*settings.funds, *settings.payers) for balance in fund.inventory()
        ),
        positions=positions,
    )
    for position in positions:
        verify_inventory(settings, config, portfolio, position.payload)
    from almanak.connectors.gmx_v2.economic_accounting import validate_inventory

    validate_inventory(portfolio)
    return State(
        run_id=uuid4().hex,
        time=settings.schedule.start,
        schedule=settings.schedule,
        interval=interval,
        portfolio=portfolio,
    )


def verify_inventory(settings, config, portfolio, payload):
    target = payload.target
    allocation = config.allocation(target.allocation)
    for value in payload.owned.inventory:
        if portfolio.balance(wallet(target, value.asset)) < value.amount:
            raise ValueError("GMX hypothetical fund inventory cannot cover its owned allocation")
    # Position references carry no GM share metadata and cannot own GM shares.
    if not isinstance(target, Target) and portfolio.balance(
        wallet(target, target.market.share_asset)
    ):
        raise ValueError("GMX prepared reference requires no unrelated initial GM shares")
    reserve = portfolio.balance(payer_wallet(settings, target, allocation.native_asset))
    if reserve < allocation.minimum_native_balance:
        raise ValueError("GMX hypothetical submitter inventory cannot cover its native reserve")
