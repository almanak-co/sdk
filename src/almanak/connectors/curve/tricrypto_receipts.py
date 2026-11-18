"""Retain exact pool wrapping and admin mints outside the fund's economic effects."""

from almanak.caip import AssetType
from almanak.chains import evm_network
from almanak.connectors.curve import crypto_receipts as two
from almanak.connectors.curve import receipts as shared
from almanak.connectors.curve.operations import Addition, Approval, Removal, Swap
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_logs import data, events, one_event, topic_address
from almanak.models import ZERO_ADDRESS

SWAP = two.SWAP
ADD = shared.topic("AddLiquidity(address,uint256[3],uint256,uint256,uint256)")
REMOVE = shared.topic("RemoveLiquidity(address,uint256[3],uint256)")
ONE = two.ONE
CLAIM = shared.topic("ClaimAdminFee(address,uint256)")
TOKEN_CLAIM = shared.topic("ClaimAdminFee(address,uint256[3])")


def sequence(receipt, operation):
    expected = None
    if isinstance(operation, Swap):
        expected = SWAP
    elif isinstance(operation, Addition):
        expected = ADD
    elif isinstance(operation, Removal):
        expected = REMOVE if operation.output_asset is None else ONE
    for candidate in (
        SWAP,
        ADD,
        REMOVE,
        ONE,
        two.ADD,
        two.REMOVE,
        shared.SWAP,
        shared.ADD,
        shared.REMOVE,
        shared.ONE,
        shared.IMBALANCE,
    ):
        if len(events(receipt, operation.target.pool.pool, candidate)) != int(
            candidate == expected
        ):
            raise DataError(
                "receipt", "TriCrypto receipt contains another protocol action sequence"
            )
    if events(receipt, operation.target.pool.pool, two.CLAIM):
        raise DataError("receipt", "TriCrypto receipt contains another family admin claim")


def admin_mints(receipt, operation, before):
    target = operation.target
    if not events(receipt, target.pool.pool, CLAIM):
        return ()
    if before is None or not (
        isinstance(operation, Addition)
        or (isinstance(operation, Removal) and operation.output_asset is not None)
    ):
        raise DataError("receipt", "unexpected TriCrypto admin claim without retained LP operation")
    event = one_event(receipt, target.pool.pool, CLAIM, 2)
    receiver = topic_address(event["topics"][1])
    if receiver != before.pool.accounting.receiver or receiver in (
        ZERO_ADDRESS,
        target.fund.address,
    ):
        raise DataError(
            "receipt", "TriCrypto admin receiver differs from independent retained fee scope"
        )
    (minted,) = data(event, ("uint256",))
    return ((ZERO_ADDRESS, receiver, minted),)


def addition_effect(receipt, operation, claims):
    event = shared.pool_event(receipt, operation, ADD)
    amounts, _, supply, scale = data(event, ("uint256[3]", "uint256", "uint256", "uint256"))
    if amounts != tuple(value.amount for value in operation.amounts.root):
        raise DataError("receipt", "TriCrypto mint differs from its ordered exact coin inputs")
    target = operation.target
    changes = shared.transfers(receipt, target.pool.shares_asset)
    for claim in claims:
        if claim not in changes:
            raise DataError("receipt", "TriCrypto admin claim lacks its exact LP mint")
        changes.remove(claim)
    if len(changes) != 1 or changes[0][:2] != (ZERO_ADDRESS, target.fund.address):
        raise DataError("receipt", "TriCrypto mint differs from its fund and admin recipients")
    minted = changes[0][2]
    if minted < operation.minimum_shares.amount or supply < minted or scale == 0:
        raise DataError("receipt", "TriCrypto mint violates its retained LP share minimum")
    return shared.effects(target, tuple(-amount for amount in amounts), minted)


def removal_effect(receipt, operation):
    target = operation.target
    if operation.output_asset is None:
        event = shared.pool_event(receipt, operation, REMOVE)
        amounts, _ = data(event, ("uint256[3]", "uint256"))
    else:
        event = shared.pool_event(receipt, operation, ONE)
        burned, index, amount, _, _ = data(event, ("uint256",) * 5)
        if (
            index != target.assets.index(operation.output_asset)
            or burned != operation.shares.amount
        ):
            raise DataError("receipt", "TriCrypto removal changed its selected coin or share burn")
        amounts = tuple(amount if coin == index else 0 for coin in range(3))
    if any(
        amount < minimum.amount
        for amount, minimum in zip(amounts, operation.minimums.root, strict=True)
    ):
        raise DataError("receipt", "TriCrypto removal violates its ordered exit minima")
    return shared.effects(target, amounts, -operation.shares.amount)


def wrapping_transfers(operation, result):
    network = evm_network(operation.target.pool.chain.chain_id)
    expected: dict[AssetType, list[tuple[str, str, int]]] = {
        asset: [] for asset in operation.target.assets
    }
    if not network.wrapping_emits_transfer:
        return expected
    pool = operation.target.pool.pool
    for change in result.asset_changes:
        if change.key.asset != network.wrapped_native:
            continue
        # OptimizedWETH pools retain native ETH, unwrapping inputs and wrapping exits.
        # aeWETH emits ERC-20 burn/mint events for these exact pool-owned quantities.
        if change.amount < 0:
            expected[change.key.asset].append((pool, ZERO_ADDRESS, -change.amount))
        elif change.amount > 0 or shared.receives_zero(operation, change.key.asset):
            expected[change.key.asset].append((ZERO_ADDRESS, pool, change.amount))
    return expected


def movement(receipt, operation, before):
    sequence(receipt, operation)
    token_revision = operation.target.pool.implementation_revision == "tricrypto-ng-2024-01-04"
    if events(receipt, operation.target.pool.pool, CLAIM if token_revision else TOKEN_CLAIM):
        raise DataError("receipt", "TriCrypto fee claim differs from its configured revision")
    claims = () if token_revision else admin_mints(receipt, operation, before)
    if isinstance(operation, Approval):
        result = shared.approval_effect(receipt, operation)
    elif isinstance(operation, Swap):
        result = two.swap_effect(receipt, operation)
    elif isinstance(operation, Addition):
        result = addition_effect(receipt, operation, claims)
    elif isinstance(operation, Removal):
        result = removal_effect(receipt, operation)
    else:
        raise ValueError("unsupported TriCrypto protocol operation")
    shared.settlement(
        receipt,
        operation,
        result,
        token_admin_transfers(receipt, operation, before)
        if token_revision
        else wrapping_transfers(operation, result),
        extra_share_transfers=claims,
    )
    return result


def token_admin_transfers(receipt, operation, before):
    target = operation.target
    if not events(receipt, target.pool.pool, TOKEN_CLAIM):
        return {asset: [] for asset in target.assets}
    if before is None or not isinstance(operation, Removal) or operation.output_asset is None:
        raise DataError(
            "receipt", "TriCrypto token admin claim requires retained single-coin withdrawal"
        )
    event = one_event(receipt, target.pool.pool, TOKEN_CLAIM, 2)
    receiver = topic_address(event["topics"][1])
    if receiver != before.pool.accounting.receiver or receiver == target.fund.address:
        raise DataError(
            "receipt", "TriCrypto token admin receiver differs from retained independent fee scope"
        )
    (amounts,) = data(event, ("uint256[3]",))
    return {
        asset: [(target.pool.pool, receiver, amount)]
        for asset, amount in zip(target.assets, amounts, strict=True)
    }
