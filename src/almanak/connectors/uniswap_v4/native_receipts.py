"""Receipts of exact-input swaps whose native leg the router wrapped or unwrapped.

The fund's wrapped-native leg moves only between the fund and the router; its ERC-20 leg
settles with the PoolManager as for any swap. The chain's wrap evidence ties the router's
wrapping to the pool's native delta:

- Native in: the fund paid exactly the input, the router unwrapped at least that, and the pool
  settled at least what was unwrapped. Any surplus is wrapped native that already sat in the
  router; it only adds to the fund's input.
- Native out: the router wrapped exactly what the fund received, its whole native balance. That
  is the pool's native output after an output-haircut hook's take, plus any native already
  in the router: native can be forced into any address (SELFDESTRUCT, block rewards and
  withdrawals), so a surplus only adds to the fund's output. Without a haircut the wrap must
  cover the gross output. With one, the take is measured as gross output minus the wrap,
  never below zero; a surplus understates it, and the reviewed-haircut check reports that.
"""

from eth_abi import decode

from almanak.chains import evm_network
from almanak.connectors.uniswap_v4.receipts import (
    SWAP,
    TRANSFER,
    events,
    hook_take,
    pool_event,
    taker,
    token_movement,
    transfer,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.wrap_evidence import wrap_flows
from almanak.models import ZERO_ADDRESS


def native_swap_movement(receipt, operation):
    """Fund deltas in custody order, no NFT, and an output-haircut swap's (take, gross)."""
    target = operation.target
    held, native, router = target.assets, target.native_index, target.contracts.router
    entering = 0 if operation.amount.asset == held[0] else 1
    token = 1 - native
    hook = taker(operation)
    deltas = [0, 0]
    deltas[token] = token_movement(
        receipt, target, held[token], hook if token != entering else None
    )
    deltas[native] = router_movement(receipt, target, held[native], router)
    wrapped, unwrapped = wrap_flows(receipt, evm_network(target.pool.chain.chain_id), router)
    pool = list(
        decode(
            ("int128", "int128", "uint160", "uint128", "int24", "uint24"),
            pool_event(receipt, operation, SWAP),
        )[:2]
    )
    if deltas[entering] != -operation.amount.amount:
        raise DataError("receipt", "swap transfers differ from the retained input")
    if entering == native:
        take = hook_take(receipt, operation, held[token]) if hook is not None else 0
        gross = pool[token]
        if wrapped or not -pool[native] >= unwrapped >= operation.amount.amount:
            raise DataError("receipt", "router unwrap differs from the native swap input")
        if pool[token] - take != deltas[token]:
            raise DataError("receipt", "swap event and transfers differ from the swap output")
    else:
        gross = pool[native]
        take = 0 if hook is None else max(gross - wrapped, 0)
        if unwrapped or wrapped != deltas[native] or wrapped < gross - take:
            raise DataError("receipt", "router wrap differs from the native swap output")
        if pool[token] != deltas[token]:
            raise DataError("receipt", "swap event and transfers differ from the swap input")
    if deltas[1 - entering] < operation.minimum.amount:
        raise DataError("receipt", "swap output is below the retained minimum")
    return tuple(deltas), None, (None if hook is None else (take, gross))


def router_movement(receipt, target, asset, router):
    """The fund's wrapped-native delta: transfers only with the router, mints and burns aside."""
    fund, delta = target.fund.address, 0
    for event in events(receipt, asset.reference, TRANSFER):
        sender, receiver, amount = transfer(event)
        if ZERO_ADDRESS in (sender, receiver) and router in (sender, receiver):
            continue
        if fund not in (sender, receiver) or router not in (sender, receiver):
            raise DataError("receipt", "wrapped native moved outside the fund and router")
        delta += amount if receiver == fund else -amount
    return delta
