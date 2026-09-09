"""Measured EVM receipt costs, retaining unknown supplementary fee evidence."""

from collections.abc import Mapping
from typing import Any


def _fee_quantity(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise ValueError("Receipt L1 fee must be a nonnegative integer quantity")
    try:
        amount = int(value, 16 if value.lower().startswith("0x") else 10) if isinstance(value, str) else value
    except ValueError as exc:
        raise ValueError("Receipt L1 fee must be a nonnegative integer quantity") from exc
    if amount < 0:
        raise ValueError("Receipt L1 fee must be a nonnegative integer quantity")
    return amount


def receipt_l1_fee_wei(receipt: Mapping[str, Any]) -> int | None:
    """Read an explicitly charged L1 fee; never derive one from L1 gas units.

    OP receipts expose an additive ``l1Fee``. Arbitrum's ``gasUsedForL1``
    describes gas already included in gasUsed and must not be added again.
    Missing/null evidence remains None; an explicit zero is measured zero.
    """
    values = [receipt[name] for name in ("l1_fee_wei", "l1Fee") if receipt.get(name) is not None]
    if not values:
        return None
    parsed = [_fee_quantity(value) for value in values]
    if len(set(parsed)) != 1:
        raise ValueError("Conflicting receipt L1 fee quantities")
    return parsed[0]


def measured_gas_cost_wei(gas_used: int, effective_gas_price: int, l1_fee_wei: int | None = None) -> int:
    """Execution cost plus an explicitly measured additive L1 charge.

    Without L1 evidence this is measured execution cost, not proof of an
    all-in fee on a chain that charges supplementary fees. It does not
    estimate missing L1/operator fees or double-count blob fee components.
    """
    execution_cost = gas_used * effective_gas_price
    return execution_cost if l1_fee_wei is None else execution_cost + _fee_quantity(l1_fee_wei)
