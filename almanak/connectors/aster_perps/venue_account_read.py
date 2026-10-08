"""Aster Pro account equity read (wallet balance + every position's unrealized PnL).

Margin deposited into Aster leaves the wallet, so valuation reads the account
through the gateway and counts its equity as one account-level row.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.venue_account_read_base import (
    SettledVenueTransfer,
    VenueAccountPosition,
    VenueAccountRead,
    VenueAccountReadSpec,
)
from almanak.connectors.aster_perps.accounting_receipts import venue_receipt_identity
from almanak.connectors.aster_perps.compiler import SUPPORTED_CHAINS
from almanak.connectors.aster_perps.gateway_client import (
    AsterGatewayError,
    AsterPendingTransfer,
    GatewayAsterPerpsClient,
)

# Margin assets valued at par; any other non-zero asset leaves equity unmeasured.
_USD_PAR_ASSETS = frozenset({"USDT", "USDC", "USD1"})


def _in_flight_usd(pending: list[AsterPendingTransfer]) -> Decimal | str:
    """Value moving between the wallet and the account, owned by neither side yet.

    A pending deposit has left the wallet but is not credited; a pending
    withdrawal is debited from the account but not yet paid, and pays its amount
    minus the venue fee. Returns an error string when a transfer cannot be valued.
    """
    total = Decimal(0)
    for transfer in pending:
        if transfer.asset.upper() not in _USD_PAR_ASSETS or transfer.amount is None:
            return f"in-flight {transfer.type} of {transfer.asset} cannot be valued in USD"
        if transfer.type == "DEPOSIT":
            total += transfer.amount
        elif transfer.type == "WITHDRAW":
            if transfer.fee is None:
                return "in-flight withdrawal without a known venue fee"
            total += transfer.amount - transfer.fee
        else:
            return f"unknown in-flight transfer type {transfer.type!r}"
    return total


def read_aster_account(*, gateway_client: Any, chain: str, wallet_address: str) -> VenueAccountRead:
    client = GatewayAsterPerpsClient(gateway_client)
    try:
        account = client.get_account(wallet_address=wallet_address)
        positions = client.get_positions(wallet_address=wallet_address)
    except AsterGatewayError as exc:
        return VenueAccountRead(ok=False, error=str(exc))
    balances = account.balances
    in_flight = _in_flight_usd(account.pending_transfers)
    if isinstance(in_flight, str):
        return VenueAccountRead(ok=False, error=in_flight)

    unpriced = sorted(b.asset for b in balances if b.asset.upper() not in _USD_PAR_ASSETS and b.balance != 0)
    if unpriced:
        return VenueAccountRead(ok=False, error=f"Aster account holds assets without a USD mark: {unpriced}")

    cash = sum((b.balance for b in balances), Decimal(0))
    # Per position, not the balance's cross PnL: that omits isolated-margin
    # positions, whose margin the wallet balance already includes.
    if any(p.unrealized_pnl is None for p in positions):
        return VenueAccountRead(ok=False, error="Aster position without unrealized PnL")
    unrealized = sum((p.unrealized_pnl or Decimal(0) for p in positions), Decimal(0))
    return VenueAccountRead(
        ok=True,
        equity_usd=cash + unrealized + in_flight,
        cash_usd=cash,
        unrealized_pnl_usd=unrealized,
        details={"in_flight_usd": str(in_flight)} if in_flight else {},
        positions=tuple(
            VenueAccountPosition(
                market=p.symbol,
                is_long=p.position_amt > 0,
                size=abs(p.position_amt),
                entry_price=p.entry_price,
                mark_price=p.mark_price,
                unrealized_pnl_usd=p.unrealized_pnl,
                leverage=p.leverage,
            )
            for p in positions
        ),
    )


def read_settled_transfers(
    *, gateway_client: Any, chain: str, wallet_address: str, transfers: list[dict[str, Any]]
) -> tuple[SettledVenueTransfer, ...]:
    client = GatewayAsterPerpsClient(gateway_client)
    result = []
    for transfer in transfers:
        response = client.get_withdrawal_payout(wallet_address=wallet_address, transfer=transfer)
        if not response.settled:
            continue
        if response.withdrawal_id != transfer["transfer_id"] or response.receiver.lower() != wallet_address.lower():
            raise AsterGatewayError("payout identity differs from ledger withdrawal")
        result.append(
            SettledVenueTransfer(
                transfer_id=response.withdrawal_id,
                chain=chain,
                tx_hash=response.tx_hash,
                log_index=response.log_index,
                token_address=response.token_address,
                receiver=response.receiver,
                raw_amount=int(response.raw_amount),
                block_number=response.block_number,
            )
        )
    return tuple(result)


VENUE_ACCOUNT_READ_SPEC = VenueAccountReadSpec(
    read_account=read_aster_account,
    chains=SUPPORTED_CHAINS,
    read_settled_transfers=read_settled_transfers,
    validate_execution_receipt=venue_receipt_identity,
)

__all__ = ["VENUE_ACCOUNT_READ_SPEC", "read_aster_account", "read_settled_transfers"]
