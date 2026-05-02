"""Composed observations — receipt + pre/post state + prices (AttemptNo17 §3 D3).

An ``AccountingObservation`` is what the typed-column writers (D2) actually
consume. It composes:

- ``receipt`` — pure log facts (see ``receipts.py``).
- ``pre_state`` — protocol state BEFORE the tx, captured by the runner from
  ``transaction_ledger.pre_state_json``.
- ``post_state`` — protocol state AFTER the tx, captured by the runner from
  ``transaction_ledger.post_state_json``.
- ``prices`` — oracle snapshot at execution time, captured from
  ``transaction_ledger.price_inputs_json``.

Every derived USD field is a pure function of those four. No live chain calls
in the writer path — that invariant is what keeps the outbox processor safe
to run on startup-recovery hours after the original execution.

Confidence taxonomy (AttemptNo17 §1.0):
- HIGH: oracle ≤30s old at execution.
- ESTIMATED: derived without direct measurement (e.g. cross-rate).
- STALE: oracle 30s–5min old; usable but flagged.
- UNAVAILABLE: source unreachable / empty pre_state / unknown decimals.

Confidence is mandatory on every USD number. A null without confidence
fails the Accountant Test.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from almanak.framework.accounting.receipts import (
    FailedReceipt,
    LendingReceipt,
    LPReceipt,
    PerpReceipt,
    SwapReceipt,
)

ConfidenceLiteral = Literal["HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"]


@dataclass(frozen=True)
class PriceSnapshot:
    """Parsed view over ``transaction_ledger.price_inputs_json``.

    Shape: ``{symbol_or_address: {"price_usd": str, "oracle_source": str,
    "fetched_at": iso8601, "confidence": HIGH|ESTIMATED|STALE|UNAVAILABLE}}``.
    """

    raw: dict[str, dict[str, Any]]

    def usd(self, symbol_or_address: str) -> Decimal | None:
        """Return USD price, or None when missing/unparseable."""
        entry = self.raw.get(symbol_or_address) or self.raw.get(symbol_or_address.lower())
        if not entry:
            return None
        try:
            return Decimal(str(entry.get("price_usd")))
        except (InvalidOperation, TypeError):
            return None

    def confidence(self, symbol_or_address: str) -> ConfidenceLiteral:
        """Return the confidence tag for a priced asset.

        An unrecognised confidence string (e.g. a future enum value the SDK
        doesn't know about, a typo'd tag from an external feed) collapses
        to ``UNAVAILABLE`` rather than ``ESTIMATED`` — defaulting to
        ``ESTIMATED`` would let an unknown / malformed tag look like a
        measured-but-derived value, which is the opposite of the safe
        default. Operators can recognise ``UNAVAILABLE`` rows and either
        fix the source feed or extend the taxonomy; ``ESTIMATED`` would
        bury the drift.
        """
        entry = self.raw.get(symbol_or_address) or self.raw.get(symbol_or_address.lower())
        if not entry:
            return "UNAVAILABLE"
        c = entry.get("confidence")
        if c in ("HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"):
            return c
        return "UNAVAILABLE"

    def oracle_source(self, symbol_or_address: str) -> str | None:
        entry = self.raw.get(symbol_or_address) or self.raw.get(symbol_or_address.lower())
        return None if entry is None else entry.get("oracle_source")

    def is_empty(self) -> bool:
        return not self.raw

    @classmethod
    def from_json(cls, s: str) -> PriceSnapshot:
        if not s:
            return cls(raw={})
        try:
            d = json.loads(s)
        except (json.JSONDecodeError, TypeError):
            return cls(raw={})
        if not isinstance(d, dict):
            return cls(raw={})
        # Drop entries that are not the expected nested-dict shape — without
        # this, ``usd()`` / ``confidence()`` later call ``.get()`` on whatever
        # the writer dropped in (a bare number, a string, a list) and crash
        # the read path. The writer-side normalization in
        # ``observability/ledger.py`` should already ensure shape, but the
        # reader is defensive against historical or external rows.
        cleaned: dict[str, dict[str, Any]] = {k: v for k, v in d.items() if isinstance(v, dict)}
        return cls(raw=cleaned)


@dataclass(frozen=True)
class LendingState:
    """Parsed view over ``pre_state_json`` / ``post_state_json`` for lending."""

    collateral_usd: Decimal | None
    debt_usd: Decimal | None
    health_factor: Decimal | None
    supply_balance: Decimal | None
    borrow_balance: Decimal | None
    supply_apr_pct: Decimal | None
    borrow_apr_pct: Decimal | None
    principal_debt: Decimal | None  # for principal-vs-interest split at REPAY
    total_debt: Decimal | None
    raw: dict[str, Any]

    @classmethod
    def from_json(cls, s: str) -> LendingState:
        if not s:
            return cls(None, None, None, None, None, None, None, None, None, raw={})
        try:
            d = json.loads(s) if isinstance(s, str) else s
        except (json.JSONDecodeError, TypeError):
            return cls(None, None, None, None, None, None, None, None, None, raw={})
        if not isinstance(d, dict):
            return cls(None, None, None, None, None, None, None, None, None, raw={})

        def _dec(k: str) -> Decimal | None:
            v = d.get(k)
            if v is None or v == "":
                return None
            try:
                return Decimal(str(v))
            except (InvalidOperation, TypeError):
                return None

        return cls(
            collateral_usd=_dec("collateral_usd"),
            debt_usd=_dec("debt_usd"),
            health_factor=_dec("health_factor"),
            supply_balance=_dec("supply_balance"),
            borrow_balance=_dec("borrow_balance"),
            supply_apr_pct=_dec("supply_apr_pct"),
            borrow_apr_pct=_dec("borrow_apr_pct"),
            principal_debt=_dec("principal_debt"),
            total_debt=_dec("total_debt"),
            raw=d,
        )


@dataclass(frozen=True)
class LPState:
    """Parsed pre/post state for an LP position."""

    position_id: str | None
    tick_current: int | None
    liquidity: int | None
    in_range: bool | None
    fees_token0_owed: Decimal | None
    fees_token1_owed: Decimal | None
    sqrt_price_x96: int | None
    raw: dict[str, Any]

    @classmethod
    def from_json(cls, s: str) -> LPState:
        if not s:
            return cls(None, None, None, None, None, None, None, raw={})
        try:
            d = json.loads(s) if isinstance(s, str) else s
        except (json.JSONDecodeError, TypeError):
            return cls(None, None, None, None, None, None, None, raw={})
        if not isinstance(d, dict):
            return cls(None, None, None, None, None, None, None, raw={})

        def _int(k: str) -> int | None:
            v = d.get(k)
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        def _dec(k: str) -> Decimal | None:
            v = d.get(k)
            if v is None or v == "":
                return None
            try:
                return Decimal(str(v))
            except (InvalidOperation, TypeError):
                return None

        return cls(
            position_id=d.get("position_id"),
            tick_current=_int("tick_current"),
            liquidity=_int("liquidity"),
            in_range=d.get("in_range"),
            fees_token0_owed=_dec("fees_token0_owed"),
            fees_token1_owed=_dec("fees_token1_owed"),
            sqrt_price_x96=_int("sqrt_price_x96"),
            raw=d,
        )


@dataclass(frozen=True)
class PerpState:
    """Parsed pre/post state for a perp position."""

    position_id: str | None
    size: Decimal | None
    collateral: Decimal | None
    entry_price: Decimal | None
    mark_price: Decimal | None
    unrealized_pnl: Decimal | None
    cumulative_funding: Decimal | None
    liquidation_price: Decimal | None
    raw: dict[str, Any]

    @classmethod
    def from_json(cls, s: str) -> PerpState:
        if not s:
            return cls(None, None, None, None, None, None, None, None, raw={})
        try:
            d = json.loads(s) if isinstance(s, str) else s
        except (json.JSONDecodeError, TypeError):
            return cls(None, None, None, None, None, None, None, None, raw={})
        if not isinstance(d, dict):
            return cls(None, None, None, None, None, None, None, None, raw={})

        def _dec(k: str) -> Decimal | None:
            v = d.get(k)
            if v is None or v == "":
                return None
            try:
                return Decimal(str(v))
            except (InvalidOperation, TypeError):
                return None

        return cls(
            position_id=d.get("position_id"),
            size=_dec("size"),
            collateral=_dec("collateral"),
            entry_price=_dec("entry_price"),
            mark_price=_dec("mark_price"),
            unrealized_pnl=_dec("unrealized_pnl"),
            cumulative_funding=_dec("cumulative_funding"),
            liquidation_price=_dec("liquidation_price"),
            raw=d,
        )


# ─── Composed observation types ────────────────────────────────────────────


@dataclass(frozen=True)
class LendingObservation:
    receipt: LendingReceipt
    pre_state: LendingState
    post_state: LendingState
    prices: PriceSnapshot

    @property
    def amount_usd(self) -> Decimal | None:
        # ``or`` would treat ``Decimal("0")`` as falsy and fall through to the
        # lowercase lookup — but a measured zero price (rare but legitimate
        # for fully-debased assets) is meaningful, while ``None`` means the
        # asset wasn't priced. Disambiguate explicitly.
        p = self.prices.usd(self.receipt.asset)
        if p is None:
            p = self.prices.usd(self.receipt.asset.lower())
        return None if p is None else self.receipt.amount_delta * p

    @property
    def amount_usd_confidence(self) -> ConfidenceLiteral:
        if self.prices.is_empty():
            return "UNAVAILABLE"
        return self.prices.confidence(self.receipt.asset)

    @property
    def borrow_apr_at_execution(self) -> Decimal | None:
        return self.pre_state.borrow_apr_pct

    @property
    def supply_apr_at_execution(self) -> Decimal | None:
        return self.pre_state.supply_apr_pct

    @property
    def health_factor_after(self) -> Decimal | None:
        return self.post_state.health_factor

    @property
    def principal_repaid_and_interest_paid(self) -> tuple[Decimal | None, Decimal | None]:
        """REPAY-only: split the total amount into principal and interest.

        Uses ``pre_state.principal_debt`` (set by the lending state capture
        path) to derive ``interest_paid = total_debt − principal_debt`` at
        the time of the repay. Returns ``(None, None)`` for non-REPAY
        operations or when the lending state didn't expose the principal.
        """
        if self.receipt.operation != "REPAY":
            return (None, None)
        if self.pre_state.principal_debt is None or self.pre_state.total_debt is None:
            return (None, None)
        # Interest accrued is whatever portion of total_debt exceeds principal_debt.
        # Stale or buggy state may report ``total_debt < principal_debt`` (e.g.
        # the connector emitted total before the index update). Clamp to zero
        # so we never report negative interest, and never report principal
        # exceeding the actual amount repaid.
        amount_repaid = self.receipt.amount_delta
        if amount_repaid is None or amount_repaid < 0:
            return (None, None)
        interest_outstanding = max(
            self.pre_state.total_debt - self.pre_state.principal_debt,
            Decimal("0"),
        )
        # Of the amount actually repaid, the principal portion is whatever's
        # left after the interest portion is paid first (Aave's behaviour).
        # Clamp ``interest_paid`` and ``principal_repaid`` so they sum to
        # ``amount_repaid`` and neither side can go negative.
        interest_paid = min(amount_repaid, interest_outstanding)
        principal_repaid = amount_repaid - interest_paid
        return (principal_repaid, interest_paid)


@dataclass(frozen=True)
class LPObservation:
    receipt: LPReceipt
    pre_state: LPState
    post_state: LPState
    prices: PriceSnapshot
    token0: str
    token1: str

    @property
    def amount0_usd(self) -> Decimal | None:
        p = self.prices.usd(self.token0)
        return None if p is None else self.receipt.amount0_delta * p

    @property
    def amount1_usd(self) -> Decimal | None:
        p = self.prices.usd(self.token1)
        return None if p is None else self.receipt.amount1_delta * p

    @property
    def total_value_usd(self) -> Decimal | None:
        a0, a1 = self.amount0_usd, self.amount1_usd
        if a0 is None or a1 is None:
            return None
        return a0 + a1

    @property
    def fees_total_usd(self) -> Decimal | None:
        p0 = self.prices.usd(self.token0)
        p1 = self.prices.usd(self.token1)
        if p0 is None or p1 is None:
            return None
        return self.receipt.fees_token0 * p0 + self.receipt.fees_token1 * p1

    @property
    def confidence(self) -> ConfidenceLiteral:
        if self.prices.is_empty():
            return "UNAVAILABLE"
        c0 = self.prices.confidence(self.token0)
        c1 = self.prices.confidence(self.token1)
        # Worst confidence wins.
        order = ["HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"]
        return order[max(order.index(c0), order.index(c1))]  # type: ignore[return-value]


@dataclass(frozen=True)
class PerpObservation:
    receipt: PerpReceipt
    pre_state: PerpState
    post_state: PerpState
    prices: PriceSnapshot

    @property
    def realized_pnl_usd(self) -> Decimal | None:
        # CLOSE only. Prefer the receipt log topic when available; else derive
        # from pre_state.entry_price + post_state.mark_price (which equals exit).
        if self.receipt.operation != "CLOSE":
            return None
        if self.receipt.realized_pnl_delta is not None:
            return self.receipt.realized_pnl_delta
        ep = self.pre_state.entry_price
        xp = self.post_state.mark_price
        sz = self.pre_state.size
        if ep is None or xp is None or sz is None:
            return None
        side = Decimal(1) if self.receipt.is_long else Decimal(-1)
        return (xp - ep) * sz * side

    @property
    def funding_total_usd(self) -> Decimal | None:
        if self.pre_state.cumulative_funding is None or self.post_state.cumulative_funding is None:
            return None
        return self.post_state.cumulative_funding - self.pre_state.cumulative_funding


@dataclass(frozen=True)
class SwapObservation:
    receipt: SwapReceipt
    prices: PriceSnapshot

    @property
    def amount_in_usd(self) -> Decimal | None:
        p = self.prices.usd(self.receipt.token_in)
        return None if p is None else self.receipt.amount_in * p

    @property
    def amount_out_usd(self) -> Decimal | None:
        p = self.prices.usd(self.receipt.token_out)
        return None if p is None else self.receipt.amount_out * p

    @property
    def realized_slippage_usd(self) -> Decimal | None:
        a_in, a_out = self.amount_in_usd, self.amount_out_usd
        if a_in is None or a_out is None:
            return None
        return a_in - a_out

    @property
    def confidence(self) -> ConfidenceLiteral:
        if self.prices.is_empty():
            return "UNAVAILABLE"
        ci = self.prices.confidence(self.receipt.token_in)
        co = self.prices.confidence(self.receipt.token_out)
        order = ["HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"]
        return order[max(order.index(ci), order.index(co))]  # type: ignore[return-value]


@dataclass(frozen=True)
class FailedObservation:
    receipt: FailedReceipt
    prices: PriceSnapshot  # native token price for gas_usd

    @property
    def gas_usd(self) -> Decimal | None:
        # The runner will resolve the chain's native token symbol and look it up.
        # This is computed in the centralized `gas_pricing.compute_gas_usd` path;
        # we keep the field here for FailedObservation completeness.
        return None  # downstream writers compute this


Observation = LendingObservation | LPObservation | PerpObservation | SwapObservation | FailedObservation
