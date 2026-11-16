"""Connector-specific swap events and shared verified V3 manager settlement."""

from almanak.connectors import v3_pool_receipts as family
from almanak.connectors.v3_pool_receipts import APPROVAL as APPROVAL
from almanak.connectors.v3_pool_receipts import COLLECT as COLLECT
from almanak.connectors.v3_pool_receipts import DECREASE as DECREASE
from almanak.connectors.v3_pool_receipts import INCREASE as INCREASE
from almanak.connectors.v3_pool_receipts import POOL_BURN as POOL_BURN
from almanak.connectors.v3_pool_receipts import POOL_COLLECT as POOL_COLLECT
from almanak.connectors.v3_pool_receipts import POOL_MINT as POOL_MINT
from almanak.connectors.v3_pool_receipts import TRANSFER as TRANSFER
from almanak.connectors.v3_pool_receipts import Effects as Effects
from almanak.connectors.v3_pool_receipts import added as added
from almanak.connectors.v3_pool_receipts import approval_effect as approval_effect
from almanak.connectors.v3_pool_receipts import collected as collected
from almanak.connectors.v3_pool_receipts import manager_event as manager_event
from almanak.connectors.v3_pool_receipts import nft_transition as nft_transition
from almanak.connectors.v3_pool_receipts import pool_range as pool_range
from almanak.connectors.v3_pool_receipts import removed as removed
from almanak.connectors.v3_pool_receipts import token_movement as token_movement
from almanak.connectors.v3_pool_receipts import topic as topic

SWAP = topic("Swap(address,address,int256,int256,uint160,uint128,int24)")
SWAP_TYPES = ("int256", "int256", "uint160", "uint128", "int24")


def swap_movement(receipt, operation, deltas):
    return family.swap_movement(receipt, operation, deltas, SWAP, SWAP_TYPES)


def movement(receipt, operation):
    return family.movement(receipt, operation, SWAP, SWAP_TYPES)


def validate_sequence(receipt, operation):
    return family.validate_sequence(receipt, operation, SWAP)
