"""Four-layer GMX V2 PERP_CLOSE intent coverage on an Arbitrum fork.

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
from web3.eth import AsyncEth, Eth

from almanak.connectors.gmx_v2 import GMXv2ReceiptParser
from almanak.connectors.gmx_v2.addresses import GMX_V2
from tests.support.gmx_v2 import GMX_V2_TOKENS
from tests.unit.connectors.gmx_v2.market_fixtures import fake_dynamic_gateway, market_address

# Address-first primary spelling: the strategy-declared market-token address
# (the label path is covered separately by the core-alias unit tests).
_ETH_USD_MARKET = market_address("arbitrum", "ETH/USD")
from almanak.connectors.gmx_v2.anvil_order_executor import execute_pending_orders_on_anvil
from almanak.connectors.gmx_v2.teardown_reads import read_open_positions
from almanak.framework.execution.extracted_data import AsyncOrderKind
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.execution.result_enricher import ResultEnricher
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.perp_intents import PerpCloseIntent, PerpOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from almanak.gateway.proto import gateway_pb2
from tests.intents._gmx_v2_perp_support import (
    assert_gmx_event_key,
    assert_gmx_event_name,
    gmx_oracle_price_map,
)
from tests.intents.conftest import fund_native_token, get_token_balance

CHAIN_NAME = "arbitrum"
USDC_ADDRESS = GMX_V2_TOKENS[CHAIN_NAME]["USDC"]
EXCHANGE_ROUTER_ADDRESS = GMX_V2[CHAIN_NAME]["exchange_router"]
_GMX_USD_SCALE = Decimal(10**30)
_HISTORICAL_CLOSE_GAS_PRICE_WEI = 20_076_000
_HISTORICAL_FAILURE_WINDOW_START_WEI = 3_000_000_000_000_000
_HISTORICAL_FAILURE_WINDOW_END_WEI = 3_137_572_000_000_000


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
        gateway_client=fake_dynamic_gateway("arbitrum", rpc_url=orchestrator.rpc_url),
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


@pytest.mark.arbitrum
@pytest.mark.asyncio
class TestGmxV2PerpCloseIntent:
    @pytest.mark.intent(IntentType.PERP_CLOSE)
    @pytest.mark.no_zodiac(reason="historical regression must make the funded wallet the actual gas payer")
    async def test_recent_fork_close_eth_long_returns_collateral(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url: str,
        price_oracle_arbitrum: dict[str, Decimal],
        monkeypatch: pytest.MonkeyPatch,
        intent_evidence,
    ) -> None:
        fork_age_seconds = int(time.time()) - int(web3.eth.get_block("latest")["timestamp"])
        assert abs(fork_age_seconds) <= 7 * 24 * 60 * 60, (
            f"GMX PERP_CLOSE intent proof requires a fork no older than seven days; age={fork_age_seconds}s"
        )
        parser = GMXv2ReceiptParser(chain=CHAIN_NAME)
        compiler = _build_compiler(
            wallet=funded_wallet,
            orchestrator=orchestrator,
            prices=price_oracle_arbitrum,
        )
        gateway = _AnvilGateway(web3, price_oracle_arbitrum)

        # Reproduce the incident's Arbitrum fee suggestion for both the sync
        # compiler SDK and the async EIP-1559 execution path. The fork still
        # estimates and executes every transaction against real GMX state.
        async def historical_priority_fee(_eth: AsyncEth) -> int:
            return _HISTORICAL_CLOSE_GAS_PRICE_WEI

        monkeypatch.setattr(Eth, "gas_price", property(lambda _eth: _HISTORICAL_CLOSE_GAS_PRICE_WEI))
        monkeypatch.setattr(Eth, "max_priority_fee", property(lambda _eth: _HISTORICAL_CLOSE_GAS_PRICE_WEI))
        monkeypatch.setattr(AsyncEth, "max_priority_fee", property(historical_priority_fee))

        # Pin the real gas payer inside the historical failure window. The old
        # 0.001 ETH keeper fee + 0.002 ETH fixed reserve admitted this open at
        # equality, then refused the still-affordable close after open gas.
        funding_response = web3.provider.make_request(
            "anvil_setBalance",
            [funded_wallet, hex(_HISTORICAL_FAILURE_WINDOW_START_WEI)],
        )
        assert funding_response.get("error") is None, funding_response
        opening_native_balance = web3.eth.get_balance(funded_wallet)
        assert _HISTORICAL_FAILURE_WINDOW_START_WEI <= opening_native_balance
        assert opening_native_balance < _HISTORICAL_FAILURE_WINDOW_END_WEI

        # Establish the real position that the PERP_CLOSE intent will consume.
        open_intent = PerpOpenIntent(
            market=_ETH_USD_MARKET,
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("300"),
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol="gmx_v2",
            leverage=Decimal("3"),
            chain=CHAIN_NAME,
        )
        open_compilation = compiler.compile(open_intent)
        assert open_compilation.status.value == "SUCCESS", open_compilation.error
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
        native_balance_before_close = web3.eth.get_balance(funded_wallet)
        assert native_balance_before_close < _HISTORICAL_FAILURE_WINDOW_START_WEI

        # Layer 1: compile the measured full position size into a decrease order.
        close_intent = PerpCloseIntent(
            market=_ETH_USD_MARKET,
            collateral_token="USDC",
            is_long=True,
            size_usd=Decimal(position_before.size_in_usd) / _GMX_USD_SCALE,
            max_slippage=Decimal("0.01"),
            protocol="gmx_v2",
            chain=CHAIN_NAME,
        )
        close_compilation = compiler.compile(close_intent)
        assert close_compilation.status.value == "SUCCESS", close_compilation.error
        assert close_compilation.action_bundle is not None, close_compilation.error
        close_transactions = close_compilation.action_bundle.transactions
        assert len(close_transactions) == 1
        assert close_transactions[0]["to"].lower() == EXCHANGE_ROUTER_ADDRESS.lower()
        assert int(close_transactions[0]["value"]) > 0
        assert close_compilation.action_bundle.metadata["protocol"] == "gmx_v2"
        old_fixed_requirement = int(close_transactions[0]["value"]) + 2_000_000_000_000_000
        assert native_balance_before_close < old_fixed_requirement

        # Layer 2: submit the decrease order, then execute its exact key as keeper.
        close_execution = await orchestrator.execute(close_compilation.action_bundle)
        assert close_execution.success, close_execution.error
        assert len(close_execution.transaction_results) == 1
        close_submission_receipt = _receipt_dict(close_execution)
        close_orders = intent_evidence.capture_parse(
            intent=close_intent,
            transaction_result=close_execution.transaction_results[-1],
            parser=lambda receipt: parser.extract_async_orders(receipt, intent_type=IntentType.PERP_CLOSE.value),
            parser_method="extract_async_orders",
            receipt_role="submission",
        )
        assert close_orders is not None and len(close_orders) == 1
        assert close_orders[0].kind is AsyncOrderKind.DECREASE
        submission_witness = assert_gmx_event_key(
            close_submission_receipt,
            chain=CHAIN_NAME,
            event_name="OrderCreated",
            key=close_orders[0].order_id,
        )

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

        fill = intent_evidence.capture_parse(
            intent=close_intent,
            transaction_result=keeper_receipt,
            parser=lambda receipt: parser.extract_perp_fill(
                receipt,
                order_key=close_orders[0].order_id,
                account=funded_wallet,
            ),
            parser_method="extract_perp_fill",
            receipt_role="settlement",
        )
        assert fill is not None
        assert fill.is_open is False
        assert fill.order_key is not None
        assert fill.order_key.lower() == close_orders[0].order_id.lower()
        assert fill.position_key is not None
        settlement_witness = assert_gmx_event_name(
            keeper_receipt,
            chain=CHAIN_NAME,
            event_name="PositionDecrease",
        )

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
        intent_evidence.record_fidelity(
            receipt_role="submission",
            hard=True,
            flags={
                "parser_order_key_eq_raw_event_key": (
                    submission_witness["matched_key"] == close_orders[0].order_id.lower()
                ),
                "decrease_kind_measured_from_receipt": close_orders[0].kind is AsyncOrderKind.DECREASE,
            },
            witnesses=[submission_witness],
            notes=["Submission proves the exact queued decrease order; settlement is sealed separately."],
        )
        native_balance_after_close = web3.eth.get_balance(funded_wallet)
        intent_evidence.record_balance_deltas(
            receipt_role="submission",
            checks={"native_execution_fee_debited": native_balance_after_close < native_balance_before_close},
            native_execution_fee={
                "symbol": "ETH wei",
                "before": native_balance_before_close,
                "after": native_balance_after_close,
                "delta": native_balance_after_close - native_balance_before_close,
            },
        )
        intent_evidence.record_fidelity(
            receipt_role="settlement",
            hard=True,
            flags={
                "raw_position_decrease_event_present": (
                    settlement_witness["matched_event_name_topic"] == settlement_witness["event_name_topic"]
                ),
                "parser_order_key_eq_submitted_key": (fill.order_key.lower() == close_orders[0].order_id.lower()),
                "enriched_collateral_eq_preclose_position": (
                    collateral_returned == Decimal(position_before.collateral_amount)
                ),
                "position_measured_closed_onchain": positions_after.positions == (),
            },
            witnesses=[settlement_witness],
        )
        intent_evidence.record_balance_deltas(
            receipt_role="settlement",
            checks={
                "wallet_collateral_increased": usdc_after_settlement > usdc_before_settlement,
                "position_measured_closed_onchain": positions_after.positions == (),
            },
            wallet_usdc={
                "symbol": "USDC",
                "before": usdc_before_settlement,
                "after": usdc_after_settlement,
                "delta": usdc_after_settlement - usdc_before_settlement,
            },
            position_collateral={
                "symbol": "USDC raw",
                "before": position_before.collateral_amount,
                "after": 0,
                "delta": -position_before.collateral_amount,
            },
        )
