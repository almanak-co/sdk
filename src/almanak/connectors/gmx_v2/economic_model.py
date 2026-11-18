"""Drive GMX request continuations through the shared durable economic simulator."""

from dataclasses import dataclass
from datetime import UTC, datetime

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Transition
from almanak.backtest.types import UnixTime
from almanak.caip import AssetType
from almanak.connectors.dex_economic_model import EconomicModel as SharedExchangeModel
from almanak.connectors.gmx_v2 import economic_data, economic_parameters, economic_state
from almanak.connectors.gmx_v2.authority import admit, validate
from almanak.connectors.gmx_v2.economic_accounting import (
    apply_fund_gas,
    effects,
    validate_inventory,
)
from almanak.connectors.gmx_v2.economic_component import protocol_closed
from almanak.connectors.gmx_v2.economic_liquidity_requests import validate_escrow
from almanak.connectors.gmx_v2.economic_parameters import payer_wallet, position_key, wallet
from almanak.connectors.gmx_v2.economic_perpetual_requests import (
    liquidate,
    liquidation_opportunity,
    next_liquidation,
)
from almanak.connectors.gmx_v2.economic_perpetual_requests import (
    validate_position as validate_perpetual_position,
)
from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicLiquidationBatch,
    EconomicLiquidationDeferral,
    EconomicLiquidationResult,
    EconomicPerpetualFailure,
    EconomicPerpetualPosition,
    EconomicPerpetualResult,
    EconomicPerpetualSnapshot,
    cleanup_outstanding,
)
from almanak.connectors.gmx_v2.economic_simulation import advance
from almanak.connectors.gmx_v2.economic_spending import spending_assets
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.connectors.gmx_v2.exchange_observations import inventory_assets
from almanak.connectors.gmx_v2.intents import RevokePosition
from almanak.connectors.gmx_v2.lifecycle import Lifecycle
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
from almanak.connectors.gmx_v2.state import Decision
from almanak.dex_intents import RevokeExchange, SwapExactInput
from almanak.effects import Effects, Position, PositionChange
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from almanak.strategy_api import SimulationEvidence


def perpetual(payload):
    return isinstance(payload, EconomicPerpetualPosition)


def open_position(payload):
    return perpetual(payload) and payload.owned.position is not None


@dataclass(frozen=True)
class Sweep:
    """One position's outcome at a liquidation publication."""

    result: EconomicLiquidationResult | None = None
    deferral: EconomicLiquidationDeferral | None = None
    change: PositionChange | None = None
    assets: tuple = ()


class EconomicModel(SharedExchangeModel):
    data = economic_data
    state_types = economic_state
    parameters = economic_parameters
    decision_type = Decision
    effects_type = Effects[economic_state.Payload]
    admit = staticmethod(admit)
    validate_capabilities = staticmethod(validate)
    increases_exposure = staticmethod(Lifecycle.increases_exposure)

    @property
    def model_name(self):
        return economic_data.revisions(self.config)[0]

    @staticmethod
    def cancel_at_end(intent):
        # The same intent identifies accepted escrow and must still be allowed to settle.
        return False

    @staticmethod
    def continuation_at(state, pending):
        # Publication eligibility has second precision; admission latency is paid once.
        return UnixTime(state.time + 1)

    def validate_state(self, state):
        if (
            state.schedule != self.settings.schedule
            or state.interval.effective != self.config.decision_trigger
        ):
            raise ValueError("GMX economic state differs from its frozen scheduling")
        expected = tuple(self.config.target(a.name) for a in self.config.allocations)
        if tuple(position.payload.target for position in state.portfolio.positions) != expected:
            raise ValueError("GMX economic state changed its retained allocation targets")
        for held in state.portfolio.positions:
            if held.key != position_key(held.payload.target):
                raise ValueError("GMX economic state changed its retained position identity")
            self.validate_position(held.payload, state.time)
        validate_inventory(state.portfolio)

    def validate_position(self, payload, time):
        allocation = self.config.allocation(payload.target.allocation)
        spending = validate_spending(allocation, payload)
        validate_escrow_spending(payload.escrow, spending)
        if perpetual(payload):
            validate_perpetual_position(allocation, payload, time)
        else:
            validate_exchange_escrow(allocation, payload.escrow, time)

    def snapshot(self, state, payload, time, *, quotes=True, entry=True):
        # GMX observations carry no pool entry quote, so `entry` changes nothing here.
        target = payload.target
        record = self.book.at(target, time)
        allocation = self.config.allocation(target.allocation)
        shared = dict(
            target=target,
            block=record.reference,
            owned=payload.owned,
            native_balance=AssetAmount(
                asset=allocation.native_asset,
                amount=state.portfolio.balance(
                    payer_wallet(self.settings, target, allocation.native_asset)
                ),
            ),
            payer=self.config.accounts.binding(target.account).profile.submitter,
            simulation_time=UnixTime(time),
            escrow=payload.escrow,
        )
        if perpetual(payload):
            return EconomicPerpetualSnapshot(
                **shared,
                wallets=tuple(
                    AssetAmount(
                        asset=value.asset,
                        amount=state.portfolio.balance(wallet(target, value.asset)),
                    )
                    for value in payload.owned.inventory
                ),
                allowance=payload.allowances[0].amount,
            )
        return economic_state.EconomicSnapshot(
            **shared,
            wallets=tuple(
                AssetAmount(asset=asset, amount=state.portfolio.balance(wallet(target, asset)))
                for asset in inventory_assets(target)
            ),
            market_tokens=AssetAmount(
                asset=target.market.share_asset,
                amount=state.portfolio.balance(wallet(target, target.market.share_asset)),
            ),
            allowances=payload.allowances,
        )

    def execute(self, state, pending):
        self.validate_state(state)
        self.route(pending.intent)
        held = next(
            position
            for position in state.portfolio.positions
            if position.key == position_key(pending.intent.target)
        )
        try:
            return self.apply_request(state, held, pending)
        except (ValueError, DataError) as error:
            return self.failure(held, pending, error)

    def failure(self, held, pending, error):
        complete = held.payload.escrow is None
        failure_type = (
            EconomicPerpetualFailure if perpetual(held.payload) else economic_state.EconomicFailure
        )
        return Transition(
            effects=self.effects_type(),
            complete=complete,
            result=failure_type(
                intent=pending.intent,
                complete=complete,
                effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
                simulation=SimulationEvidence(
                    execution_id=pending.identity, model=self.model_name, inputs=()
                ),
                error=str(error),
                unavailable=error.gap if isinstance(error, InputUnavailable) else None,
            ),
        )

    def apply_request(self, state, held, pending):
        if (
            held.payload.escrow is None
            and self.increases_exposure(pending.intent)
            and (state.mode == "teardown" or pending.eligible_at >= state.schedule.end)
        ):
            raise ValueError("GMX modeled exposure cannot be admitted at or after the run end")
        target = held.payload.target
        observed = self.snapshot(state, held.payload, pending.eligible_at)
        record = self.book.at(target, pending.eligible_at)
        step = advance(self.config, self.settings, held.payload, pending, record)
        if step.gas.amount > observed.native_balance.amount:
            raise ValueError("GMX economic submitter cannot cover the configured operation cost")
        after = apply_fund_gas(observed, step.after, step.gas)
        changes = effects(self.settings, held, after, step.assets, step.gas)
        validate_inventory(state.portfolio.apply(changes))
        cleanup = isinstance(pending.intent, (RevokeExchange, RevokePosition))
        complete = step.after.escrow is None and (not cleanup or not outstanding_cleanup(after))
        result_type = EconomicPerpetualResult if perpetual(after) else economic_state.EconomicResult
        result = result_type(
            intent=pending.intent,
            success=step.phase != "cancelled" or cleanup,
            complete=complete,
            phase=step.phase,
            reason=step.reason,
            effective_at=datetime.fromtimestamp(pending.eligible_at, UTC),
            simulation=SimulationEvidence(
                execution_id=pending.identity, model=self.model_name, inputs=record.evidence
            ),
            before=observed,
            after=after,
            assets=changes.assets,
            gas_paid=step.gas,
            keeper_paid=step.keeper,
            fund_refund=step.refund,
        )
        return Transition(effects=changes, result=result, complete=complete)

    def closed(self, state, observations):
        if len(observations) != len(state.portfolio.positions):
            return False
        return not any(self.remaining(position.payload) for position in state.portfolio.positions)

    def remaining(self, payload):
        if perpetual(payload):
            return not protocol_closed(payload)
        liquidity = isinstance(
            self.config.allocation(payload.target.allocation), LiquidityAllocation
        )
        return not protocol_closed(payload) or any(
            value.amount
            for value in payload.owned.inventory
            if not liquidity
            and value.asset in payload.target.assets
            and value.asset != payload.target.funding_asset
        )

    def next_event(self, state, until):
        """Earliest verified publication that liquidates a retained hypothetical position."""
        start = (
            state.time if state.event_cursor is None else max(state.time, state.event_cursor + 1)
        )
        if start > until:
            return None
        times = [
            next_liquidation(self.book.timelines[held.payload.target], held.payload, start, until)
            for held in state.portfolio.positions
            if open_position(held.payload)
        ]
        eligible = [time for time in times if time is not None]
        return UnixTime(min(eligible)) if eligible else None

    def external_event(self, state, time):
        sweeps = [
            sweep
            for held in state.portfolio.positions
            if open_position(held.payload)
            and (sweep := self.sweep_position(state, held, time)) is not None
        ]
        if not sweeps:
            return None
        return Transition(
            effects=self.effects_type(
                assets=tuple(asset for sweep in sweeps for asset in sweep.assets),
                positions=tuple(sweep.change for sweep in sweeps if sweep.change is not None),
            ),
            result=EconomicLiquidationBatch(
                results=tuple(sweep.result for sweep in sweeps if sweep.result is not None),
                deferrals=tuple(sweep.deferral for sweep in sweeps if sweep.deferral is not None),
            ),
        )

    def sweep_position(self, state, held, time):
        """Liquidate one position at ``time``, or record why the protocol could not."""
        target = held.payload.target
        common = dict(
            allocation=target.allocation,
            effective_at=datetime.fromtimestamp(time, UTC),
            simulation=SimulationEvidence(
                execution_id=f"{state.run_id}:event:{len(state.events)}",
                model=self.model_name,
                inputs=(),
            ),
        )
        try:
            record = self.book.at(target, time)
        except InputUnavailable as error:
            deferral = EconomicLiquidationDeferral(
                **common, eligibility=None, reason=str(error), unavailable=error.gap
            )
            return Sweep(deferral=deferral)
        found = liquidation_opportunity(held.payload, record, time)
        if found is None:
            return None
        opportunity, eligibility = found
        common["simulation"] = common["simulation"].model_copy(update={"inputs": record.evidence})
        try:
            after, assets, settlement = liquidate(held.payload, opportunity)
        except SwapConstraint as error:
            # The keeper's liquidation would revert here; the position stays and the
            # sweep re-evaluates it at the next publication.
            deferral = EconomicLiquidationDeferral(
                **common, eligibility=eligibility, reason=str(error)
            )
            return Sweep(deferral=deferral)
        result = EconomicLiquidationResult(
            **common,
            before=held.payload,
            after=after,
            assets=tuple(assets),
            reason=eligibility,
            unpaid_usd_e30=settlement.unpaid_usd_e30,
            fees_collected=settlement.fees_collected,
        )
        change = PositionChange[economic_state.Payload](
            before=held, after=Position[economic_state.Payload](key=held.key, payload=after)
        )
        return Sweep(result=result, change=change, assets=tuple(assets))

    @staticmethod
    def deliver_event(state, result):
        # Liquidation is an accounting event; the author hook only sees later observations.
        return state.memory


def outstanding_cleanup(payload):
    if perpetual(payload):
        return cleanup_outstanding(payload)
    return any(value.amount for value in payload.allowances)


def validate_spending(allocation, payload):
    """Cumulative inputs are recorded for exactly the allocation's input assets."""
    spending = {value.asset: value.amount for value in payload.spent}
    if tuple(spending) != spending_assets(allocation):
        raise ValueError("GMX economic state changed its recorded input assets")
    return spending


def validate_exchange_escrow(allocation, escrow, time):
    if isinstance(escrow, economic_state.LiquidityEscrow):
        if not isinstance(allocation, LiquidityAllocation):
            raise ValueError("GM modeled escrow requires its configured liquidity allocation")
        validate_escrow(allocation, escrow, time)
        return
    if escrow is not None:
        validate_swap_escrow(allocation, escrow, time)


def validate_swap_escrow(allocation, escrow, time):
    if (
        not isinstance(escrow.intent, SwapExactInput)
        or escrow.inputs != (escrow.intent.amount,)
        or escrow.execution_fee != allocation.execution_fee
        or escrow.accepted_at > time
    ):
        raise ValueError("GMX economic escrow differs from its retained admitted request")


def validate_escrow_spending(escrow, spending):
    if escrow is None:
        return
    retained: dict[AssetType, int] = {}
    for value in (*escrow.inputs, escrow.execution_fee):
        retained[value.asset] = retained.get(value.asset, 0) + value.amount
    if any(amount > spending.get(asset, 0) for asset, amount in retained.items()):
        raise ValueError("GMX retained escrow exceeds its recorded cumulative inputs")
