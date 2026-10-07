"""Strategy-side Aster Pro client: thin wrapper over the gateway gRPC stub.

Holds no keys and makes no network calls of its own — every request goes
through the gateway's ``AsterPerpsService``.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import grpc

from almanak.connectors.aster_perps.proto import aster_perps_pb2

SERVICE_NAME = "aster_perps"
_DEFAULT_TIMEOUT_SECONDS = 30.0


def _optional_decimal(value: str) -> Decimal | None:
    if value in ("", None):
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


@dataclass(frozen=True)
class AsterPosition:
    symbol: str
    position_amt: Decimal
    entry_price: Decimal | None
    mark_price: Decimal | None
    unrealized_pnl: Decimal | None
    leverage: Decimal | None
    notional: Decimal | None


@dataclass(frozen=True)
class AsterBalance:
    asset: str
    balance: Decimal
    available_balance: Decimal | None
    cross_unrealized_pnl: Decimal | None


@dataclass(frozen=True)
class AsterPendingTransfer:
    type: str
    asset: str
    amount: Decimal | None
    fee: Decimal | None


@dataclass(frozen=True)
class AsterAccount:
    balances: list[AsterBalance]
    pending_transfers: list[AsterPendingTransfer]


class AsterGatewayError(RuntimeError):
    """The gateway could not complete an Aster read."""


class GatewayAsterPerpsClient:
    """Calls the gateway's ``AsterPerpsService``."""

    def __init__(self, gateway_client: Any, *, timeout: float = _DEFAULT_TIMEOUT_SECONDS) -> None:
        self._gateway_client = gateway_client
        self._timeout = timeout

    def _stub(self) -> Any:
        return self._gateway_client.connector_stub(SERVICE_NAME)

    def place_market_order(self, order_request: dict[str, Any], *, wallet_address: str) -> Any:
        request = aster_perps_pb2.AsterPlaceMarketOrderRequest(
            symbol=str(order_request["symbol"]),
            is_long=bool(order_request["is_long"]),
            notional_usd=str(order_request.get("notional_usd") or ""),
            close_position=bool(order_request.get("close_position", False)),
            leverage=int(order_request.get("leverage") or 0),
            client_order_id=str(order_request["client_order_id"]),
            wallet_address=wallet_address,
            max_slippage=str(order_request.get("max_slippage") or ""),
        )
        return self._stub().PlaceMarketOrder(request, timeout=self._timeout)

    def get_order(self, *, symbol: str, client_order_id: str, wallet_address: str, close_position: bool = False) -> Any:
        request = aster_perps_pb2.AsterGetOrderRequest(
            symbol=symbol, client_order_id=client_order_id, wallet_address=wallet_address, close_position=close_position
        )
        return self._stub().GetOrder(request, timeout=self._timeout)

    def withdraw(self, *, asset: str, amount: str, wallet_address: str, client_request_id: str = "") -> Any:
        request = aster_perps_pb2.AsterWithdrawRequest(
            asset=asset,
            amount=amount,
            wallet_address=wallet_address,
            client_request_id=client_request_id,
        )
        return self._stub().Withdraw(request, timeout=self._timeout)

    def find_withdrawal(self, *, since_ms: int, wallet_address: str, client_request_id: str = "") -> Any:
        request = aster_perps_pb2.AsterFindWithdrawalRequest(
            since_ms=since_ms, wallet_address=wallet_address, client_request_id=client_request_id
        )
        return self._stub().FindWithdrawal(request, timeout=self._timeout)

    def get_positions(self, *, wallet_address: str, symbol: str = "") -> list[AsterPosition]:
        try:
            response = self._stub().GetPositions(
                aster_perps_pb2.AsterGetPositionsRequest(symbol=symbol, wallet_address=wallet_address),
                timeout=self._timeout,
            )
        except grpc.RpcError as exc:
            raise AsterGatewayError(f"GetPositions RPC failed: {exc.details()}") from exc
        if not response.success:
            raise AsterGatewayError(f"GetPositions failed: {response.error}")
        return [
            AsterPosition(
                symbol=p.symbol,
                position_amt=_optional_decimal(p.position_amt) or Decimal(0),
                entry_price=_optional_decimal(p.entry_price),
                mark_price=_optional_decimal(p.mark_price),
                unrealized_pnl=_optional_decimal(p.unrealized_pnl),
                leverage=_optional_decimal(p.leverage),
                notional=_optional_decimal(p.notional),
            )
            for p in response.positions
        ]

    def get_balances(self, *, wallet_address: str) -> list[AsterBalance]:
        return self.get_account(wallet_address=wallet_address).balances

    def get_account(self, *, wallet_address: str) -> AsterAccount:
        try:
            response = self._stub().GetBalances(
                aster_perps_pb2.AsterGetBalancesRequest(wallet_address=wallet_address), timeout=self._timeout
            )
        except grpc.RpcError as exc:
            raise AsterGatewayError(f"GetBalances RPC failed: {exc.details()}") from exc
        if not response.success:
            raise AsterGatewayError(f"GetBalances failed: {response.error}")
        return AsterAccount(
            balances=[
                AsterBalance(
                    asset=b.asset,
                    balance=_optional_decimal(b.balance) or Decimal(0),
                    available_balance=_optional_decimal(b.available_balance),
                    cross_unrealized_pnl=_optional_decimal(b.cross_unrealized_pnl),
                )
                for b in response.balances
            ],
            pending_transfers=[
                AsterPendingTransfer(
                    type=t.type, asset=t.asset, amount=_optional_decimal(t.amount), fee=_optional_decimal(t.fee)
                )
                for t in response.pending_transfers
            ],
        )


__all__ = [
    "SERVICE_NAME",
    "AsterAccount",
    "AsterBalance",
    "AsterGatewayError",
    "AsterPendingTransfer",
    "AsterPosition",
    "GatewayAsterPerpsClient",
]
