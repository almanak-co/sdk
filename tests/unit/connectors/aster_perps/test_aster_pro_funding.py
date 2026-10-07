"""Aster Pro funding: on-chain vault deposit, gateway-signed withdrawal."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from eth_abi import decode as abi_decode
from eth_account import Account

from almanak.connectors._strategy_base.base.compiler import PerpCompilerContext
from almanak.connectors.aster_perps.addresses import ASTER_PRO, FUTURES_BROKER_ID
from almanak.connectors.aster_perps.compiler import AsterPerpsCompiler
from almanak.connectors.aster_perps.execution import ASTER_WITHDRAW_KEY, AsterOrderHandler
from almanak.connectors.aster_perps.gateway.api_client import AsterApiError, AsterProApiClient
from almanak.connectors.aster_perps.gateway.service import AsterPerpsServiceServicer
from almanak.connectors.aster_perps.proto import aster_perps_pb2
from almanak.framework.intents.compiler_models import CompilationStatus, TokenInfo, TransactionData
from almanak.framework.intents.vocabulary import Intent

WALLET = "0x" + "ab" * 20
USDT = "0x55d398326f99059fF775485246999027B3197955"
VAULT = ASTER_PRO["bsc"]["vault"]
APPROVE = TransactionData(to=USDT, value=0, data="0x095ea7b3", gas_estimate=50_000, description="approve", tx_type="approve")


def _ctx(chain: str = "bsc") -> PerpCompilerContext:
    services = MagicMock()
    services.resolve_token.return_value = TokenInfo(symbol="USDT", address=USDT, decimals=18)
    services.build_approve_tx.return_value = [APPROVE]
    return PerpCompilerContext(
        chain=chain, wallet_address=WALLET, rpc_url=None, rpc_timeout=10.0, permission_discovery=False,
        allow_placeholder_prices=False, token_resolver=None, gateway_client=None, price_oracle=None, cache={},
        services=services, default_protocol="aster_perps", protocol="aster_perps",
    )


def test_deposit_approves_the_vault_then_deposits_to_the_futures_account() -> None:
    ctx = _ctx()
    result = AsterPerpsCompiler().compile(ctx, Intent.perp_deposit(amount=Decimal("4.5"), asset="USDT", protocol="aster_perps"))
    assert result.status == CompilationStatus.SUCCESS
    approve, deposit = result.transactions
    assert approve is APPROVE
    ctx.services.build_approve_tx.assert_called_once_with(USDT, VAULT, 4_500_000_000_000_000_000)
    assert deposit.to == VAULT and deposit.value == 0
    assert deposit.data.startswith("0x0efe6a8b")
    token, amount, broker = abi_decode(["address", "uint256", "uint256"], bytes.fromhex(deposit.data[10:]))
    assert (token.lower(), amount, broker) == (USDT.lower(), 4_500_000_000_000_000_000, FUTURES_BROKER_ID)


@pytest.mark.parametrize(
    ("intent", "chain", "error"),
    [
        (Intent.perp_deposit(amount=Decimal("1"), asset="BNB", protocol="aster_perps"), "bsc", "margin is USDT"),
        (Intent.perp_deposit(amount=Decimal("1"), asset="USDT", protocol="aster_perps"), "arbitrum", "BSC"),
        (Intent.perp_deposit(amount="all", asset="USDT", protocol="aster_perps"), "bsc", "resolved"),
    ],
)
def test_deposit_rejects_what_the_vault_cannot_credit(intent: Any, chain: str, error: str) -> None:
    result = AsterPerpsCompiler().compile(_ctx(chain), intent)
    assert result.status == CompilationStatus.FAILED and error in (result.error or "")


def test_withdraw_compiles_to_an_offchain_request_paid_to_the_wallet() -> None:
    result = AsterPerpsCompiler().compile(
        _ctx(), Intent.perp_withdraw(amount=Decimal("4.4"), asset="USDT", protocol="aster_perps")
    )
    assert result.status == CompilationStatus.SUCCESS
    assert result.action_bundle.transactions == []
    assert result.action_bundle.metadata["withdraw_request"]["amount"] == "4.4"


def test_withdraw_to_a_foreign_destination_is_refused() -> None:
    intent = Intent.perp_withdraw(
        amount=Decimal("4.4"), asset="USDT", protocol="aster_perps", destination="0x" + "11" * 20
    )
    assert AsterPerpsCompiler().compile(_ctx(), intent).status == CompilationStatus.FAILED


MAIN = Account.create()


class _WithdrawClient:
    def __init__(self, *, max_amount: str = "4.6", fee: str = "0.11") -> None:
        self.user_address = MAIN.address
        self.max_amount, self.fee = max_amount, fee
        self.calls: list[dict] = []

    async def withdraw_info(self) -> dict:
        return {"balances": {"USDT": {"chainBalances": {"56": {"perpMaxWithdrawAmount": self.max_amount,
                                                                "withdrawFee": self.fee}}}}}

    async def withdraw(self, **kwargs: Any) -> dict:
        self.calls.append(kwargs)
        return {"withdrawId": "77", "hash": "0xh"}


def _servicer(client: _WithdrawClient) -> AsterPerpsServiceServicer:
    servicer = AsterPerpsServiceServicer(SimpleNamespace(private_key=MAIN.key.hex(), safe_mode=None))
    servicer._client = client  # type: ignore[assignment]
    return servicer


def _withdraw_request(amount: str, wallet: str = "") -> aster_perps_pb2.AsterWithdrawRequest:
    return aster_perps_pb2.AsterWithdrawRequest(asset="USDT", amount=amount, wallet_address=wallet or MAIN.address)


@pytest.mark.asyncio
async def test_withdraw_uses_the_venue_fee_and_pays_the_main_wallet() -> None:
    client = _WithdrawClient()
    response = await _servicer(client).Withdraw(_withdraw_request("4.5"), None)
    assert response.success and response.withdraw_id == "77" and response.receiver == MAIN.address
    assert client.calls == [{"asset": "USDT", "amount": "4.5", "fee": "0.11"}]


@pytest.mark.asyncio
async def test_withdraw_all_takes_the_full_withdrawable_balance() -> None:
    client = _WithdrawClient(max_amount="4.59670747")
    response = await _servicer(client).Withdraw(_withdraw_request("all"), None)
    assert response.success and client.calls == [{"asset": "USDT", "amount": "4.59670747", "fee": "0.11"}]


@pytest.mark.asyncio
async def test_withdraw_all_of_dust_below_the_fee_is_refused() -> None:
    client = _WithdrawClient(max_amount="0.05")
    response = await _servicer(client).Withdraw(_withdraw_request("all"), None)
    assert not response.success and not client.calls


def test_withdraw_all_compiles_for_the_gateway_to_resolve() -> None:
    result = AsterPerpsCompiler().compile(_ctx(), Intent.perp_withdraw(amount="all", asset="USDT", protocol="aster_perps"))
    assert result.status == CompilationStatus.SUCCESS
    assert result.action_bundle.metadata["withdraw_request"]["amount"] == "all"


@pytest.mark.asyncio
@pytest.mark.parametrize(("amount", "error"), [("5", "exceeds withdrawable"), ("0.1", "does not cover")])
async def test_withdraw_refuses_amounts_the_venue_would_reject(amount: str, error: str) -> None:
    client = _WithdrawClient()
    response = await _servicer(client).Withdraw(_withdraw_request(amount), None)
    assert not response.success and error in response.error and not client.calls


@pytest.mark.asyncio
async def test_withdraw_for_a_foreign_wallet_is_refused() -> None:
    client = _WithdrawClient()
    response = await _servicer(client).Withdraw(_withdraw_request("4.5", wallet="0x" + "11" * 20), None)
    assert not response.success and not client.calls


@pytest.mark.asyncio
async def test_withdraw_without_a_configured_egress_ip_never_registers_an_agent() -> None:
    client = AsterProApiClient(MAIN)
    with pytest.raises(AsterApiError, match="WITHDRAW_IP_WHITELIST"):
        await client.withdraw(asset="USDT", amount="4.5", fee="0.11")
    assert client._withdraw_agent is None


class _GatewayWithdraw:
    def __init__(self, response: Any) -> None:
        self.response = response

    def withdraw(
        self, *, asset: str, amount: str, wallet_address: str, client_request_id: str = ""
    ) -> Any:
        self.client_request_id = client_request_id
        return self.response


@pytest.mark.asyncio
async def test_handler_records_the_withdrawal_as_venue_data() -> None:
    response = aster_perps_pb2.AsterWithdrawResponse(success=True, withdraw_id="77", amount="4.5", fee="0.11",
                                                     receiver=MAIN.address)
    gateway = _GatewayWithdraw(response)
    handler = AsterOrderHandler(gateway, wallet_address=MAIN.address)  # type: ignore[arg-type]
    intent = Intent.perp_withdraw(amount=Decimal("4.5"), asset="USDT", protocol="aster_perps")
    bundle = AsterPerpsCompiler().compile(_ctx(), intent).action_bundle
    assert handler.can_handle(bundle)
    result = await handler.execute(bundle)
    assert result.success and result.order_id == "77"
    assert gateway.client_request_id == bundle.metadata["withdraw_request"]["client_request_id"] != ""
    assert result.venue_data[ASTER_WITHDRAW_KEY]["fee"] == "0.11"
