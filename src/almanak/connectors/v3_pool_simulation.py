"""Validate protocol return data separately from Safe outer execution success."""

from eth_abi import decode, encode

from almanak.connectors.erc20_approval import approval_succeeded
from almanak.connectors.v3_pool_operations import (
    Approval,
    Close,
    Collect,
    Decrease,
    Increase,
    Mint,
    Swap,
)
from almanak.gateway.ethereum import DataError


def decoded(raw, types):
    try:
        values = decode(types, raw)
        if encode(types, values) != raw:
            raise ValueError("noncanonical return data")
    except Exception:
        raise DataError("simulation", "V3 call returned malformed protocol data") from None
    return values


def multicall(raw, count):
    (values,) = decoded(raw, ("bytes[]",))
    if len(values) != count:
        raise DataError("simulation", "V3 multicall returned another action sequence")
    return values


def validate_simulation(operation, result, before, decode_swap):
    try:
        raw = bytes.fromhex(result[2:])
    except (ValueError, TypeError):
        raise DataError("simulation", "V3 simulation returned malformed bytes") from None
    if isinstance(operation, Approval):
        if not approval_succeeded(raw):
            raise DataError("simulation", "token rejected the bounded approval")
        return
    if isinstance(operation, Swap):
        (amount,) = decode_swap(raw)
        if amount < operation.minimum.amount:
            raise DataError("simulation", "simulated swap does not satisfy its output minimum")
        return
    if isinstance(operation, (Mint, Increase)):
        validate_addition(operation, raw)
        return
    if isinstance(operation, Collect):
        decoded(raw, ("uint256", "uint256"))
        return
    validate_removal(operation, raw)


def validate_addition(operation, raw):
    if isinstance(operation, Mint):
        token, liquidity, amount0, amount1 = decoded(
            raw, ("uint256", "uint128", "uint256", "uint256")
        )
        if not token:
            raise DataError("simulation", "V3 mint returned no NFT identity")
    else:
        liquidity, amount0, amount1 = decoded(raw, ("uint128", "uint256", "uint256"))
    if liquidity < operation.minimum_liquidity or not (
        operation.minimum0.amount <= amount0 <= operation.maximum0.amount
        and operation.minimum1.amount <= amount1 <= operation.maximum1.amount
    ):
        raise DataError("simulation", "simulated liquidity addition violates its quantity bounds")


def validate_removal(operation, raw):
    if not isinstance(operation, (Decrease, Close)):
        raise ValueError("unsupported V3 protocol operation")
    values = multicall(raw, 3 if isinstance(operation, Close) else 2)
    principal = decoded(values[0], ("uint256", "uint256"))
    received = decoded(values[1], ("uint256", "uint256"))
    minimums = operation.minimum0.amount, operation.minimum1.amount
    if any(
        p < minimum or actual < minimum
        for p, actual, minimum in zip(principal, received, minimums, strict=True)
    ):
        raise DataError("simulation", "simulated principal or cash receipt violates exit minima")
    if isinstance(operation, Close) and values[2]:
        raise DataError("simulation", "NFT burn returned unexpected data")
