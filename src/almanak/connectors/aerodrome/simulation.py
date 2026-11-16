"""Validate inner protocol returns before submitting EOA or Safe execution."""

from eth_abi import decode, encode

from almanak.connectors.aerodrome.operations import Addition, Approval, Collect, Removal, Swap
from almanak.gateway.ethereum import DataError


def decoded(raw, types):
    try:
        values = decode(types, raw)
        if encode(types, values) != raw:
            raise ValueError("noncanonical return data")
    except Exception:
        raise DataError("simulation", "Aerodrome call returned malformed protocol data") from None
    return values


def validate_simulation(operation, result, before=None):
    try:
        if not result.startswith("0x"):
            raise ValueError("missing hexadecimal prefix")
        raw = bytes.fromhex(result[2:])
    except (ValueError, TypeError, AttributeError):
        raise DataError("simulation", "Aerodrome simulation returned malformed bytes") from None
    if isinstance(operation, Approval):
        if raw and decoded(raw, ("bool",)) != (True,):
            raise DataError("simulation", "token rejected the bounded approval")
    elif isinstance(operation, Swap):
        (amounts,) = decoded(raw, ("uint256[]",))
        if (
            len(amounts) != 2
            or amounts[0] != operation.amount.amount
            or amounts[1] < operation.minimum.amount
        ):
            raise DataError("simulation", "simulated swap violates its input or output bounds")
    elif isinstance(operation, Addition):
        validate_addition(operation, raw)
    elif isinstance(operation, Removal):
        values = decoded(raw, ("uint256", "uint256"))
        if values[0] < operation.minimum0.amount or values[1] < operation.minimum1.amount:
            raise DataError("simulation", "simulated liquidity removal violates its exit minima")
    elif isinstance(operation, Collect):
        decoded(raw, ("uint256", "uint256"))
    else:
        raise ValueError("unsupported Aerodrome protocol operation")


def validate_addition(operation, raw):
    amount0, amount1, shares = decoded(raw, ("uint256", "uint256", "uint256"))
    if shares < operation.minimum_shares.amount or not (
        operation.minimum0.amount <= amount0 <= operation.maximum0.amount
        and operation.minimum1.amount <= amount1 <= operation.maximum1.amount
    ):
        raise DataError("simulation", "simulated liquidity addition violates its quantity bounds")
