"""Four-layer proofs for full-key V4 routes with explicit native identity."""

from decimal import Decimal

import pytest

from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance

pytestmark = [
    pytest.mark.base,
    pytest.mark.swap,
    pytest.mark.no_zodiac(
        reason="EOA operation-artifact qualification; hooked Safe routes have explicit refusal tests"
    ),
]
NATIVE = "0x" + "0" * 40


@pytest.mark.parametrize("native,fee,spacing", [(True, 1000, 20), (False, 500, 10)])
@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.asyncio
async def test_full_key_roundtrip(web3, funded_wallet, orchestrator, anvil_eth_call_adapter, native, fee, spacing):
    tokens = CHAIN_CONFIGS["base"]["tokens"]
    usdc = tokens["USDC"]
    other = NATIVE if native else tokens["WETH"]
    key = PoolKey(other, usdc, fee, spacing)
    gateway = anvil_eth_call_adapter
    orchestrator.operation_observer_factory = lambda: gateway
    compiler = IntentCompiler(
        chain="base",
        wallet_address=funded_wallet,
        gateway_client=gateway,
        venue_verification_gateway_factory=lambda: gateway,
        price_oracle={usdc: Decimal(1), other: Decimal(2500)},
    )
    parser = UniswapV4ReceiptParser(chain="base")

    def balance(token):
        return web3.eth.get_balance(funded_wallet) if token == NATIVE else get_token_balance(web3, token, funded_wallet)

    amount = 3_200_000
    for token_in, token_out, decimals in ((usdc, other, 6), (other, usdc, 18)):
        before_in, before_out = balance(token_in), balance(token_out)
        intent = SwapIntent(
            from_token=token_in,
            to_token=token_out,
            amount=Decimal(amount) / Decimal(10**decimals),
            protocol="uniswap_v4",
            chain="base",
            swap_params={"pool_key": key.to_wire()},
            max_slippage=Decimal("0.005"),
        )
        compilation = compiler.compile(intent)
        assert compilation.status.value == "SUCCESS", compilation.error
        assert compilation.action_bundle.metadata["pool_id"] == key.pool_id
        result = await orchestrator.execute(compilation.action_bundle)
        assert result.success, result.error
        receipts = [entry.receipt.to_dict() for entry in result.transaction_results if entry.receipt is not None]
        swaps = [
            parser.parse_receipt(receipt, swap_token_meta=compilation.action_bundle.metadata["swap_token_meta"])
            for receipt in receipts
        ]
        parsed = next(value for value in swaps if value.swap_result is not None)
        assert parsed.swap_result.amount_in > 0 and parsed.swap_result.amount_out > 0
        assert parsed.swap_events[0].pool_id.lower() == key.pool_id
        gas = sum(int(receipt["gas_used"]) * int(receipt["effective_gas_price"]) for receipt in receipts)
        spent = before_in - balance(token_in) - (gas if token_in == NATIVE else 0)
        received = balance(token_out) - before_out + (gas if token_out == NATIVE else 0)
        assert spent == amount
        assert received > 0
        assert parsed.swap_result.amount_out == received
        amount = received
