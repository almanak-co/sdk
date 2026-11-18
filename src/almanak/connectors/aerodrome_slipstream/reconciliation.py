"""Concrete receipt and outcome codecs bind shared V3 reconciliation."""

from almanak.connectors import v3_pool_reconciliation as family
from almanak.connectors.aerodrome_slipstream.observations import snapshot
from almanak.connectors.aerodrome_slipstream.reader import nft_position
from almanak.connectors.aerodrome_slipstream.receipts import movement
from almanak.connectors.aerodrome_slipstream.state import Outcome
from almanak.connectors.v3_pool_reconciliation import allowances_after as allowances_after
from almanak.connectors.v3_pool_reconciliation import owned_after as owned_after
from almanak.connectors.v3_pool_reconciliation import removal_settlement as removal_settlement
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
        position_reader=nft_position,
    )


def reconciled_liquidity(backend, old, operation, effects, block):
    return family.reconciled_liquidity(
        backend, old, operation, effects, block, position_reader=nft_position
    )


def retained_position(backend, pending, effects, block, success):
    return family.retained_position(
        backend, pending, effects, block, success, position_reader=nft_position
    )
