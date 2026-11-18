"""Current GMX exchange state must still cover exact ownership and admission validity."""

from almanak.connectors.gmx_v2.exchange_operations import CancelExchangeOrder
from almanak.connectors.gmx_v2.liquidity_operations import CancelLiquidityRequest
from almanak.connectors.gmx_v2.liquidity_reader import pending
from almanak.dex_intents import RevokeExchange
from almanak.gateway.ethereum import DataError


def validate_current(plan, current):
    if isinstance(plan.operation, (CancelExchangeOrder, CancelLiquidityRequest)):
        if pending(current) is None or not pending(current).cancellable:
            raise DataError("stale_plan", "GMX swap cancellation settled or became ineligible")
    elif not isinstance(plan.decision.intent, RevokeExchange):
        if plan.decision.intent.deadline <= current.block.timestamp:
            raise DataError("stale_plan", "GMX swap admission deadline expired before submission")
    if current.market_tokens != plan.before.owned.market_tokens:
        raise DataError("precondition_changed", "GMX owned share balance changed after preparation")
    if current.allowances != plan.before.allowances:
        raise DataError("precondition_changed", "GMX exchange allowances changed after preparation")
    balances = {value.asset: value.amount for value in current.wallets}
    if any(balances[value.asset] < value.amount for value in current.owned.inventory):
        raise DataError("inventory", "GMX fund no longer covers retained exchange inventory")
