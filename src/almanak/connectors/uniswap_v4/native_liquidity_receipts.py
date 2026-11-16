"""Receipts of liquidity actions whose native leg the PositionManager unwrapped or wrapped.

The token leg settles between the fund and the PoolManager as for any position. The fund's
wrapped-native leg moves only along the reviewed path, and the chain's wrap evidence for the
PositionManager ties it to the pool's native delta:

- Mint: the fund pays the pulled wrapped native to the PoolManager, which hands exactly that
  to the PositionManager; the PositionManager unwraps at least it, pays the native debt, and
  wraps and sweeps the rest to the fund.
- Collect and close: nothing is pulled or unwrapped; the PositionManager wraps the native it
  took and sweeps the wrapped native to the fund.

The PositionManager wraps its whole native balance and sweeps its whole wrapped balance, so
native or wrapped native that already sat in it only adds to what the fund receives.
"""

from almanak.chains import evm_network
from almanak.connectors.uniswap_v4.receipts import (
    TRANSFER,
    events,
    liquidity_movement,
    nft_transition,
    token_movement,
    transfer,
)
from almanak.connectors.uniswap_v4.state import Mint
from almanak.gateway.ethereum import DataError
from almanak.gateway.wrap_evidence import wrap_flows
from almanak.models import ZERO_ADDRESS


def native_liquidity_movement(receipt, operation):
    """Fund deltas in custody order and the minted or burned NFT of a native-leg position."""
    target = operation.target
    native = target.native_index
    deltas = [0, 0]
    deltas[1 - native] = token_movement(receipt, target, target.assets[1 - native])
    pulled, taken, swept = wrapped_path(receipt, target)
    network = evm_network(target.pool.chain.chain_id)
    wrapped, unwrapped = wrap_flows(receipt, network, target.contracts.position_manager)
    if isinstance(operation, Mint):
        if pulled > (operation.maximum0, operation.maximum1)[native].amount:
            raise DataError("receipt", "mint pulled more than its native maximum")
        if taken != pulled or unwrapped < taken:
            raise DataError("receipt", "position manager unwrap differs from the pulled native")
    elif pulled or taken or unwrapped:
        raise DataError("receipt", "a liquidity exit pulled or unwrapped wrapped native")
    if swept < wrapped:
        raise DataError("receipt", "position manager swept less than it wrapped")
    deltas[native] = swept - pulled
    nft = nft_transition(receipt, operation)
    # A surplus swept back to the fund is not a deposit: the mint's ceiling binds what the
    # fund paid, never below zero, while the recorded delta keeps the surplus.
    checked = list(deltas)
    if isinstance(operation, Mint):
        checked[native] = min(checked[native], 0)
    liquidity_movement(receipt, operation, tuple(checked), nft)
    return tuple(deltas), nft, None


def wrapped_path(receipt, target):
    """(fund to PoolManager, PoolManager to PositionManager, PositionManager to fund) amounts.

    Mints and burns of the wrapped native by the PositionManager are wrap evidence; any other
    transfer of the fund's wrapped native is refused.
    """
    fund, manager = target.fund.address, target.pool.pool_manager
    position_manager = target.contracts.position_manager
    path = {(fund, manager): 0, (manager, position_manager): 0, (position_manager, fund): 0}
    for event in events(receipt, target.assets[target.native_index].reference, TRANSFER):
        sender, receiver, amount = transfer(event)
        if ZERO_ADDRESS in (sender, receiver) and position_manager in (sender, receiver):
            continue
        if (sender, receiver) not in path:
            raise DataError("receipt", "wrapped native moved outside the native liquidity path")
        path[sender, receiver] += amount
    return tuple(path.values())
