"""Strategy-side types for connector-owned venue-account reads.

Some venues hold the strategy's money in an account of their own rather than in
the wallet: an off-chain order book's margin balance, for example. That money
leaves the wallet on deposit, so wallet balances alone undercount NAV. A
venue-account read reports the account's equity — cash plus unrealized PnL of
cross-margined positions — so valuation can count it as one account-level row.

Reads go through the gateway client the framework passes in; this module makes
no network calls itself.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

__all__ = [
    "VENUE_ACCOUNT_VALUATION_SOURCE",
    "VenueAccountPosition",
    "SettledVenueTransfer",
    "VenueAccountRead",
    "VenueAccountReadSpec",
]

#: ``PositionValue.details["valuation_source"]`` for an account-level row.
VENUE_ACCOUNT_VALUATION_SOURCE = "venue_account"


@dataclass(frozen=True)
class VenueAccountPosition:
    """One open position inside the account, for display and audit only.

    Its PnL is already inside the account equity; valuation never adds it again.
    """

    market: str
    is_long: bool
    size: Decimal
    entry_price: Decimal | None
    mark_price: Decimal | None
    unrealized_pnl_usd: Decimal | None
    leverage: Decimal | None


@dataclass(frozen=True)
class VenueAccountRead:
    """Outcome of one venue-account read.

    ``ok=False`` means unmeasured (read failed, or a balance could not be valued
    in USD); ``equity_usd`` is then ``None`` and callers must not treat the
    account as empty.
    """

    ok: bool
    equity_usd: Decimal | None = None
    cash_usd: Decimal | None = None
    unrealized_pnl_usd: Decimal | None = None
    positions: tuple[VenueAccountPosition, ...] = ()
    error: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        """Measured empty: no equity and no open positions."""
        return self.ok and not self.positions and self.equity_usd == 0


@dataclass(frozen=True)
class SettledVenueTransfer:
    """One exact, gateway-confirmed payout log for a ledger withdrawal."""

    transfer_id: str
    chain: str
    tx_hash: str
    log_index: int
    token_address: str
    receiver: str
    raw_amount: int
    block_number: int

    def matches(self, transfer: Any) -> bool:
        return (
            transfer.chain == self.chain
            and transfer.tx_hash.lower() == self.tx_hash.lower()
            and transfer.log_index == self.log_index
            and transfer.token_address.lower() == self.token_address.lower()
            and transfer.raw_amount == self.raw_amount
            and str(transfer.direction) == "IN"
        )


@dataclass(frozen=True)
class VenueAccountReadSpec:
    """Connector-published descriptor for a venue-account read.

    Attributes:
        read_account: ``(*, gateway_client, chain, wallet_address) -> VenueAccountRead``.
            Must not raise: failures return ``ok=False``.
        chains: Chains whose wallets own an account on the venue.
        read_settled_transfers: Optional gateway-backed confirmation of exact payout logs.
        validate_execution_receipt: Optional pure validator returning a unique execution identity.
    """

    read_account: Callable[..., VenueAccountRead]
    chains: frozenset[str]
    read_settled_transfers: Callable[..., tuple[SettledVenueTransfer, ...]] | None = None
    validate_execution_receipt: Callable[[Any, dict[str, Any]], str | None] | None = None

    def __post_init__(self) -> None:
        if not callable(self.read_account):
            raise TypeError(f"read_account must be callable, got {type(self.read_account).__name__}.")
        for callback in (self.read_settled_transfers, self.validate_execution_receipt):
            if callback is not None and not callable(callback):
                raise TypeError("venue evidence callbacks must be callable or None")
        if isinstance(self.chains, str | bytes):
            raise TypeError("chains must be a frozenset[str], not a bare string.")
        coerced = frozenset(self.chains)
        if not coerced or any(not isinstance(c, str) or not c for c in coerced):
            raise TypeError(f"chains must be a non-empty set of non-empty strings, got {self.chains!r}.")
        object.__setattr__(self, "chains", coerced)
