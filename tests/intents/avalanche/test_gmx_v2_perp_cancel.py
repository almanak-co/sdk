"""Four-layer strategy-authored GMX V2 PERP_CANCEL_ORDER proof on Avalanche."""

from __future__ import annotations

from decimal import Decimal

import pytest
from eth_utils import function_signature_to_4byte_selector
from web3 import Web3

from almanak.connectors.gmx_v2 import GMXv2ReceiptParser
from almanak.connectors.gmx_v2.addresses import GMX_V2, GMX_V2_TOKENS
from almanak.connectors.gmx_v2.teardown_reads import read_open_positions, read_pending_orders
from almanak.framework.execution.extracted_data import AsyncOrderKind
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.perp_intents import PerpCancelIntent, PerpOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._gmx_v2_perp_support import (
    AnvilGateway,
    advance_past_cancel_age,
    assert_recent_fork,
    build_compiler,
    receipt_dict,
)
from tests.intents.conftest import get_token_balance

CHAIN_NAME = "avalanche"
USDC_ADDRESS = GMX_V2_TOKENS[CHAIN_NAME]["USDC"]
ORDER_VAULT_ADDRESS = GMX_V2[CHAIN_NAME]["order_vault"]
EXCHANGE_ROUTER_ADDRESS = GMX_V2[CHAIN_NAME]["exchange_router"]
_CANCEL_SELECTOR = "0x" + function_signature_to_4byte_selector("cancelOrder(bytes32)").hex()


@pytest.mark.avalanche
@pytest.mark.asyncio
class TestGmxV2PerpCancelIntent:
    @pytest.mark.intent(IntentType.PERP_OPEN, IntentType.PERP_CANCEL_ORDER)
    async def test_cancel_exact_pending_order_refunds_collateral(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle_avalanche: dict[str, Decimal],
    ) -> None:
        assert_recent_fork(web3)
        parser = GMXv2ReceiptParser(chain=CHAIN_NAME)
        compiler = build_compiler(
            chain=CHAIN_NAME,
            wallet=funded_wallet,
            orchestrator=orchestrator,
            prices=price_oracle_avalanche,
        )
        gateway = AnvilGateway(web3, CHAIN_NAME, price_oracle_avalanche)
        collateral_amount = Decimal("100")
        collateral_raw = 100 * 10**6

        wallet_usdc_initial = get_token_balance(web3, USDC_ADDRESS, funded_wallet)
        vault_usdc_initial = get_token_balance(web3, USDC_ADDRESS, ORDER_VAULT_ADDRESS)

        open_intent = PerpOpenIntent(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=collateral_amount,
            size_usd=Decimal("300"),
            is_long=True,
            leverage=Decimal("3"),
            max_slippage=Decimal("0.01"),
            protocol="gmx_v2",
            chain=CHAIN_NAME,
        )
        open_compilation = compiler.compile(open_intent)
        assert open_compilation.status.value == "SUCCESS", open_compilation.error
        assert open_compilation.action_bundle is not None
        open_execution = await orchestrator.execute(open_compilation.action_bundle)
        assert open_execution.success, open_execution.error
        open_receipt = receipt_dict(open_execution)
        created = parser.extract_async_orders(open_receipt, intent_type=IntentType.PERP_OPEN.value)
        assert created is not None and len(created) == 1
        assert created[0].kind is AsyncOrderKind.INCREASE
        order_key = created[0].order_key

        pending_before = read_pending_orders(gateway, CHAIN_NAME, funded_wallet)
        assert pending_before.ok
        assert pending_before.order_keys == [order_key]
        assert get_token_balance(web3, USDC_ADDRESS, funded_wallet) == wallet_usdc_initial - collateral_raw
        assert get_token_balance(web3, USDC_ADDRESS, ORDER_VAULT_ADDRESS) == vault_usdc_initial + collateral_raw

        advance_past_cancel_age(web3)
        wallet_native_before_cancel = web3.eth.get_balance(funded_wallet)
        cancel_intent = PerpCancelIntent(order_key=order_key, protocol="gmx_v2", chain=CHAIN_NAME)

        # Layer 1: public compiler emits the real direct cancel selector.
        cancellation = compiler.compile(cancel_intent)
        assert cancellation.status.value == "SUCCESS", cancellation.error
        assert cancellation.action_bundle is not None
        assert len(cancellation.action_bundle.transactions) == 1
        cancel_tx = cancellation.action_bundle.transactions[0]
        assert cancel_tx["to"].lower() == EXCHANGE_ROUTER_ADDRESS.lower()
        assert cancel_tx["data"][:10].lower() == _CANCEL_SELECTOR
        assert int(cancel_tx.get("value", 0)) == 0

        # Layer 2: default-on Zodiac executes the authored cancellation.
        cancel_execution = await orchestrator.execute(cancellation.action_bundle)
        assert cancel_execution.success, cancel_execution.error
        cancel_receipt = receipt_dict(cancel_execution)

        # Layer 3: OrderCancelled correlates to the exact submitted key.
        parsed = parser.parse_receipt(cancel_receipt)
        assert parsed.success, parsed.error
        cancelled = [event for event in parsed.events if event.event_name == "OrderCancelled"]
        assert len(cancelled) == 1
        assert cancelled[0].data["key"].lower() == order_key.lower()
        assert parsed.position_increases == []
        assert parsed.position_decreases == []

        # Layer 4: the exact order disappears and its money returns without a
        # fabricated position-close lifecycle.
        pending_after = read_pending_orders(gateway, CHAIN_NAME, funded_wallet)
        assert pending_after.ok and pending_after.order_keys == []
        assert get_token_balance(web3, USDC_ADDRESS, funded_wallet) == wallet_usdc_initial
        assert get_token_balance(web3, USDC_ADDRESS, ORDER_VAULT_ADDRESS) == vault_usdc_initial
        assert web3.eth.get_balance(funded_wallet) > wallet_native_before_cancel
        positions = read_open_positions(gateway, CHAIN_NAME, funded_wallet)
        assert positions.ok and positions.positions == ()
