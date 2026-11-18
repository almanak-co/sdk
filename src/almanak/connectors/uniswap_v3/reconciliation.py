"""Concrete receipt and outcome codecs bind shared V3 reconciliation."""

from almanak.connectors import v3_pool_reconciliation as family
from almanak.connectors.uniswap_v3.observations import snapshot
from almanak.connectors.uniswap_v3.receipts import movement
from almanak.connectors.uniswap_v3.state import Outcome
from almanak.connectors.v3_pool_reconciliation import allowances_after as allowances_after
from almanak.connectors.v3_pool_reconciliation import owned_after as owned_after
from almanak.connectors.v3_pool_reconciliation import (
    reconciled_liquidity as reconciled_liquidity,
)
from almanak.connectors.v3_pool_reconciliation import removal_settlement as removal_settlement
from almanak.connectors.v3_pool_reconciliation import retained_position as retained_position
from almanak.connectors.v3_pool_reconciliation import settlement as settlement
from almanak.connectors.v3_pool_reconciliation import validate_balances as validate_balances


def reconcile(backend, pending, receipt, block):
    return family.reconcile(
        backend,
        pending,
        receipt,
        block,
        snapshot=snapshot,
        movement=movement,
        outcome_type=Outcome,
    )
