"""Receipt-only types — pure log/calldata/gas facts (Accounting-AttemptNo17 §3 D3).

These dataclasses describe ONLY what a transaction receipt directly tells you:
decoded log topics, decoded calldata, gas, tx hashes, primary tx, sub-tx hashes.

They contain NO chain reads. APR, health factor, mark price, interest index —
all of those come from `pre_state_json` / `post_state_json` captured by the
runner around execution and live on `LedgerEntry`. The composed input
(receipt + pre/post + prices) is `AccountingObservation` (see observations.py).

Why split? AccountingProcessor's no-live-chain-calls invariant
(processor.py:11) means the outbox writer cannot phone the chain hours later
on recovery. Smuggling chain-read fields into the "receipt" type would
re-introduce the dependency. Keeping receipts pure log facts and observations
the composed input matches the existing invariant exactly.

Track A2 of `docs/internal/Accounting-AttemptNo17.md`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal


@dataclass(frozen=True)
class LPReceipt:
    """Log-derived facts for an LP open / close / collect."""

    operation: Literal["OPEN", "CLOSE", "COLLECT"]
    position_id: str  # decoded from Mint/Burn/Collect log topic
    pool_address: str
    tick_lower: int  # from log
    tick_upper: int
    liquidity_delta: int  # signed; negative for CLOSE
    amount0_delta: Decimal  # decoded from Transfer logs, decimal-normalised
    amount1_delta: Decimal
    fees_token0: Decimal  # only populated for COLLECT / CLOSE-with-collect
    fees_token1: Decimal
    primary_tx_hash: str
    sub_tx_hashes: tuple[str, ...] = ()  # approve, mint, etc.
    # ``None`` = unmeasured (RPC didn't include the field). ``0`` = measured
    # zero. Defaulting to ``0`` would have made every unmeasured row look
    # like a free transaction. The downstream layers either skip ``None``
    # rows or treat them as missing data.
    gas_used: int | None = None
    effective_gas_price: int | None = None


@dataclass(frozen=True)
class LendingReceipt:
    """Log-derived facts for a SUPPLY / WITHDRAW / BORROW / REPAY."""

    operation: Literal["SUPPLY", "WITHDRAW", "BORROW", "REPAY"]
    asset: str  # ERC-20 address (lowercase) or canonical symbol
    amount_delta: Decimal  # decoded from Transfer logs
    primary_tx_hash: str
    sub_tx_hashes: tuple[str, ...] = ()
    # ``None`` = unmeasured (RPC didn't include the field). ``0`` = measured
    # zero. Defaulting to ``0`` would have made every unmeasured row look
    # like a free transaction. The downstream layers either skip ``None``
    # rows or treat them as missing data.
    gas_used: int | None = None
    effective_gas_price: int | None = None
    # interest_index, APR, HF — NOT here. They live in pre_state_json /
    # post_state_json and are merged via LendingObservation.


@dataclass(frozen=True)
class PerpReceipt:
    """Log-derived facts for an OPEN / CLOSE perp intent."""

    operation: Literal["OPEN", "CLOSE"]
    position_id: str
    size_delta: Decimal  # signed; sign convention from log
    is_long: bool
    realized_pnl_delta: Decimal | None  # CLOSE only — from log topic if available
    fee_delta: Decimal
    primary_tx_hash: str
    sub_tx_hashes: tuple[str, ...] = ()
    # ``None`` = unmeasured (RPC didn't include the field). ``0`` = measured
    # zero. Defaulting to ``0`` would have made every unmeasured row look
    # like a free transaction. The downstream layers either skip ``None``
    # rows or treat them as missing data.
    gas_used: int | None = None
    effective_gas_price: int | None = None


@dataclass(frozen=True)
class SwapReceipt:
    """Log-derived facts for a SWAP. Used for Track A's G6 reconciliation."""

    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    pool_address: str | None
    primary_tx_hash: str
    sub_tx_hashes: tuple[str, ...] = ()
    # ``None`` = unmeasured (RPC didn't include the field). ``0`` = measured
    # zero. Defaulting to ``0`` would have made every unmeasured row look
    # like a free transaction. The downstream layers either skip ``None``
    # rows or treat them as missing data.
    gas_used: int | None = None
    effective_gas_price: int | None = None


@dataclass(frozen=True)
class FailedReceipt:
    """A reverted intent. Receipts have a row even when the tx fails."""

    intent_type: str  # "LP_OPEN", "BORROW", "PERP_OPEN", ...
    chain: str
    primary_tx_hash: str
    sub_tx_hashes: tuple[str, ...] = ()
    gas_used: int | None = None  # failed txs still cost gas; None = unmeasured
    effective_gas_price: int | None = None
    revert_reason: str | None = None
    partial_state: dict[str, str] = field(default_factory=dict)
    # e.g. {"approval_to": "0x...", "approval_amount": "MAX"} when an approve
    # landed but the mint reverted — used by FailedObservation to mark the
    # outstanding-allowance risk surface.


# Discriminated union of all receipt shapes — the writer dispatches on type.
Receipt = LPReceipt | LendingReceipt | PerpReceipt | SwapReceipt | FailedReceipt
