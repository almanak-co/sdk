"""Four-layer GMX V2 PERP_CLOSE intent coverage on an Avalanche fork.

The test first creates and settles a real GMX increase order on managed Anvil,
then proves the close path end to end:

1. ``PerpCloseIntent`` compiles to the ExchangeRouter decrease-order multicall.
2. The strategy transaction and managed-Anvil keeper transaction both succeed.
3. The keeper receipt contains a decoded ``PositionDecrease`` whose raw
   collateral delta matches the position measured immediately before close.
4. The wallet receives collateral and the position is measured closed on-chain.

Production uses GMX keepers and signed oracle data. The managed-Anvil executor
reproduces the privileged on-chain execution entrypoint with prices supplied by
the same measured price fixture used to compile the intents.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.gmx_v2 import GMXv2ReceiptParser
from almanak.connectors.gmx_v2.addresses import GMX_V2, GMX_V2_TOKENS
from almanak.connectors.gmx_v2.anvil_order_executor import execute_pending_orders_on_anvil
from almanak.connectors.gmx_v2.teardown_reads import read_open_positions
from almanak.framework.execution.extracted_data import AsyncOrderKind
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.execution.result_enricher import ResultEnricher
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.perp_intents import PerpCloseIntent, PerpOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from almanak.gateway.proto import gateway_pb2
from tests.intents._gmx_v2_perp_support import gmx_oracle_price_map
from tests.intents.conftest import fund_native_token, get_token_balance

CHAIN_NAME = "avalanche"
USDC_ADDRESS = GMX_V2_TOKENS[CHAIN_NAME]["USDC"]
EXCHANGE_ROUTER_ADDRESS = GMX_V2[CHAIN_NAME]["exchange_router"]
_GMX_USD_SCALE = Decimal(10**30)


@dataclass(frozen=True)
class _PriceService:
    prices_by_token: dict[str, Decimal]

    def GetPrice(self, request: Any) -> gateway_pb2.PriceResponse:  # noqa: N802 - gRPC API compatibility
        price = self.prices_by_token.get(str(request.token).lower())
        if price is None:
            raise AssertionError(f"No measured test price for GMX oracle token {request.token}")
        return gateway_pb2.PriceResponse(
            price=str(price),
            source="intent-test-price-oracle",
            stale=False,
        )


@dataclass(frozen=True)
class _RpcService:
    web3: Web3

    def Call(self, request: Any, timeout: float | None = None) -> gateway_pb2.RpcResponse:  # noqa: N802
        del timeout
        params = json.loads(request.params)
        response = self.web3.provider.make_request(request.method, params)
        error = response.get("error")
        if error is not None:
            return gateway_pb2.RpcResponse(
                id=request.id,
                error=json.dumps(error),
                success=False,
            )
        return gateway_pb2.RpcResponse(
            id=request.id,
            result=json.dumps(response.get("result")),
            success=True,
        )


class _AnvilGateway:
    """Gateway-shaped adapter that routes every mutation to the test Anvil."""

    config = None

    def __init__(self, web3: Web3, prices: dict[str, Decimal]) -> None:
        self.web3 = web3
        self.rpc = _RpcService(web3)
        # Address AND symbol keyed: the executor prices collateral by address
        # and a market's index token by base symbol (ALM-3108).
        self.market = _PriceService(gmx_oracle_price_map(CHAIN_NAME, prices))

    def eth_call(
        self,
        *,
        chain: str,
        to: str,
        data: str,
        block: int | str | None = None,
    ) -> str:
        assert chain == CHAIN_NAME
        result = self.web3.eth.call(
            {
                "to": Web3.to_checksum_address(to),
                "data": data,
            },
            block_identifier=block if block is not None else "latest",
        )
        return Web3.to_hex(result)


def _build_compiler(
    *,
    wallet: str,
    orchestrator: ExecutionOrchestrator,
    prices: dict[str, Decimal],
) -> IntentCompiler:
    return IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=wallet,
        price_oracle=prices,
        rpc_url=orchestrator.rpc_url,
    )


def _receipt_dict(execution: Any, transaction_index: int = -1) -> dict[str, Any]:
    result = execution.transaction_results[transaction_index]
    assert result.receipt is not None
    receipt = result.receipt.to_dict()
    status = receipt.get("status")
    status_int = int(status, 16) if isinstance(status, str) else int(status)
    assert status_int == 1
    return receipt


@pytest.mark.avalanche
@pytest.mark.asyncio
class TestGmxV2PerpCloseIntentAvalanche:
    @pytest.mark.intent(IntentType.PERP_CLOSE)
    async def test_recent_fork_close_eth_long_returns_collateral(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url: str,
        price_oracle_avalanche: dict[str, Decimal],
    ) -> None:
        fork_age_seconds = int(time.time()) - int(web3.eth.get_block("latest")["timestamp"])
        assert abs(fork_age_seconds) <= 7 * 24 * 60 * 60, (
            f"GMX PERP_CLOSE intent proof requires a fork no older than seven days; age={fork_age_seconds}s"
        )
        parser = GMXv2ReceiptParser()
        compiler = _build_compiler(
            wallet=funded_wallet,
            orchestrator=orchestrator,
            prices=price_oracle_avalanche,
        )
        gateway = _AnvilGateway(web3, price_oracle_avalanche)

        # Establish the real position that the PERP_CLOSE intent will consume.
        open_intent = PerpOpenIntent(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("300"),
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol="gmx_v2",
            leverage=Decimal("3"),
        )
        open_compilation = compiler.compile(open_intent)
        assert open_compilation.action_bundle is not None, open_compilation.error
        open_execution = await orchestrator.execute(open_compilation.action_bundle)
        assert open_execution.success, open_execution.error
        open_submission_receipt = _receipt_dict(open_execution)
        native_balance_after_submission = web3.eth.get_balance(funded_wallet)
        assert native_balance_after_submission > 0

        open_orders = parser.extract_async_orders(
            open_submission_receipt,
            intent_type=IntentType.PERP_OPEN.value,
        )
        assert open_orders is not None and len(open_orders) == 1
        assert open_orders[0].kind is AsyncOrderKind.INCREASE
        open_settlement = execute_pending_orders_on_anvil(
            gateway_client=gateway,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            orders=tuple(open_orders),
            network="anvil",
        )
        assert open_settlement.ok, open_settlement.reason
        assert len(open_settlement.execution_receipts) == 1

        positions_before = read_open_positions(gateway, CHAIN_NAME, funded_wallet)
        assert positions_before.ok
        assert len(positions_before.positions) == 1
        position_before = positions_before.positions[0]
        assert position_before.collateral_token.lower() == USDC_ADDRESS.lower()
        assert position_before.collateral_amount > 0

        # Ensure the Safe retains at least its measured post-submission gas
        # reserve. Keeper settlement may also refund unused execution fee, so a
        # larger balance is valid and must not be overwritten.
        fund_native_token(funded_wallet, native_balance_after_submission, anvil_rpc_url)
        assert web3.eth.get_balance(funded_wallet) >= native_balance_after_submission

        # Layer 1: compile the measured full position size into a decrease order.
        close_intent = PerpCloseIntent(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_usd=Decimal(position_before.size_in_usd) / _GMX_USD_SCALE,
            max_slippage=Decimal("0.01"),
            protocol="gmx_v2",
        )
        close_compilation = compiler.compile(close_intent)
        assert close_compilation.action_bundle is not None, close_compilation.error
        close_transactions = close_compilation.action_bundle.transactions
        assert len(close_transactions) == 1
        assert close_transactions[0]["to"].lower() == EXCHANGE_ROUTER_ADDRESS.lower()
        assert int(close_transactions[0]["value"]) > 0
        assert close_compilation.action_bundle.metadata["protocol"] == "gmx_v2"

        # Layer 2: submit the decrease order, then execute its exact key as keeper.
        close_execution = await orchestrator.execute(close_compilation.action_bundle)
        assert close_execution.success, close_execution.error
        close_submission_receipt = _receipt_dict(close_execution)
        close_orders = parser.extract_async_orders(
            close_submission_receipt,
            intent_type=IntentType.PERP_CLOSE.value,
        )
        assert close_orders is not None and len(close_orders) == 1
        assert close_orders[0].kind is AsyncOrderKind.DECREASE

        usdc_before_settlement = get_token_balance(web3, USDC_ADDRESS, funded_wallet)
        close_settlement = execute_pending_orders_on_anvil(
            gateway_client=gateway,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            orders=tuple(close_orders),
            network="anvil",
        )
        assert close_settlement.ok, close_settlement.reason
        assert close_settlement.executed_order_keys == (close_orders[0].order_id,)
        assert len(close_settlement.execution_receipts) == 1
        keeper_receipt = close_settlement.execution_receipts[0]

        # Layer 3: run the production enrichment path over the terminal keeper
        # receipt. It normalizes raw JSON-RPC numerics before the GMX parser
        # decodes PositionDecrease and attaches collateral_returned.
        enriched = ResultEnricher().enrich(
            close_execution,
            close_intent,
            ExecutionContext(
                chain=CHAIN_NAME,
                wallet_address=funded_wallet,
                protocol="gmx_v2",
            ),
            bundle_metadata=close_compilation.action_bundle.metadata,
            additional_receipts=(keeper_receipt,),
        )
        collateral_returned = enriched.extracted_data.get("collateral_returned")
        assert collateral_returned is not None
        assert collateral_returned == Decimal(position_before.collateral_amount)

        # Layer 4: collateral reached the wallet and the exact position is closed.
        usdc_after_settlement = get_token_balance(web3, USDC_ADDRESS, funded_wallet)
        assert usdc_after_settlement > usdc_before_settlement
        positions_after = read_open_positions(gateway, CHAIN_NAME, funded_wallet)
        assert positions_after.ok
        assert positions_after.positions == ()
