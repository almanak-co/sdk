"""Validate canonical pool returns against the retained economic bounds."""

from eth_abi import decode, encode

from almanak.connectors.curve.operations import Addition, Approval, Removal, Swap
from almanak.connectors.curve.pool_abi import layout
from almanak.connectors.erc20_approval import approval_succeeded
from almanak.gateway.ethereum import DataError


def decoded(raw, types):
    try:
        values = decode(types, raw)
        if encode(types, values) != raw:
            raise ValueError("noncanonical return data")
    except Exception:
        raise DataError("simulation", "Curve call returned malformed protocol data") from None
    return values


def validate_simulation(operation, result, before=None):
    try:
        if not result.startswith("0x"):
            raise ValueError("missing hexadecimal prefix")
        raw = bytes.fromhex(result[2:])
    except (ValueError, TypeError, AttributeError):
        raise DataError("simulation", "Curve simulation returned malformed bytes") from None
    if isinstance(operation, Approval):
        if not approval_succeeded(raw):
            raise DataError("simulation", "token rejected the bounded approval")
    elif isinstance(operation, Swap):
        require_minimum(raw, operation.minimum.amount)
    elif isinstance(operation, Addition):
        require_minimum(raw, operation.minimum_shares.amount)
    elif isinstance(operation, Removal):
        validate_removal(operation, raw)
    else:
        raise ValueError("unsupported Curve protocol operation")


def require_minimum(raw, minimum):
    (amount,) = decoded(raw, ("uint256",))
    if amount < minimum:
        raise DataError("simulation", "simulated Curve output violates its retained minimum")


def validate_removal(operation, raw):
    minimums = operation.minimums.root
    if operation.output_asset is not None:
        minimum = next(v.amount for v in minimums if v.asset == operation.output_asset)
        require_minimum(raw, minimum)
        return
    (amounts,) = decoded(raw, (layout(operation.target.pool).amounts,))
    if len(amounts) != len(minimums) or any(
        amount < minimum.amount for amount, minimum in zip(amounts, minimums, strict=True)
    ):
        raise DataError("simulation", "simulated Curve removal violates its ordered exit minima")
