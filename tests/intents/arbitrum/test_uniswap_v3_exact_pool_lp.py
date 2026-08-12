"""Four-layer exact-address Uniswap V3 LP lifecycle on Arbitrum Anvil."""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._lp_setup_helpers import query_position_liquidity
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals

CHAIN_NAME = "arbitrum"
POSITION_MANAGER = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"
# Canonical Uniswap V3 WETH/USDC 0.05% pool on Arbitrum.
EXACT_POOL = "0xC6962004f452bE9203591991D15f6b388e09E8D0"
LP_AMOUNT_WETH = Decimal("0.2")
LP_AMOUNT_USDC = Decimal("500")
RANGE_LOWER = Decimal("200")
RANGE_UPPER = Decimal("20000")


@pytest.mark.arbitrum
@pytest.mark.lp
class TestUniswapV3ExactPoolLP:
    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_exact_pool_address_open_close_roundtrip(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        """Compile, execute, parse, and balance-check one address-bound lifecycle."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        open_intent = LPOpenIntent(
            pool=EXACT_POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
            gateway_client=anvil_eth_call_adapter,
        )

        # Layers 1-2: exact-address compilation and managed-Anvil execution.
        open_compilation = compiler.compile(open_intent)
        assert open_compilation.status.value == "SUCCESS", (
            f"Exact-pool LP_OPEN compilation failed: {open_compilation.error}"
        )
        assert open_compilation.action_bundle is not None
        assert open_compilation.action_bundle.metadata["pool"].lower() == EXACT_POOL.lower()
        open_execution = await orchestrator.execute(open_compilation.action_bundle)
        assert open_execution.success, f"Exact-pool LP_OPEN execution failed: {open_execution.error}"

        # Layer 3: the production parser must recover the minted position.
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        position_id = None
        for tx_result in open_execution.transaction_results:
            if tx_result.receipt is None:
                continue
            parsed = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parsed.success, f"LP_OPEN receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
            position_id = parser.extract_position_id(tx_result.receipt.to_dict()) or position_id
        assert position_id is not None, "Exact-pool LP_OPEN receipt must contain the minted position id"
        assert query_position_liquidity(web3, POSITION_MANAGER, position_id) > 0

        # Layer 4: deposited assets leave the wallet without exceeding the
        # requested amounts.
        usdc_after_open = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_open = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_spent = usdc_before - usdc_after_open
        weth_spent = weth_before - weth_after_open
        assert usdc_spent > 0 or weth_spent > 0
        assert usdc_spent <= int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        assert weth_spent <= int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))

        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=EXACT_POOL,
            collect_fees=True,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )
        close_compilation = compiler.compile(close_intent)
        assert close_compilation.status.value == "SUCCESS", (
            f"Exact-pool LP_CLOSE compilation failed: {close_compilation.error}"
        )
        assert close_compilation.action_bundle is not None
        close_execution = await orchestrator.execute(close_compilation.action_bundle)
        assert close_execution.success, f"Exact-pool LP_CLOSE execution failed: {close_execution.error}"

        close_data = None
        for tx_result in close_execution.transaction_results:
            if tx_result.receipt is None:
                continue
            parsed = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parsed.success, f"LP_CLOSE receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
            close_data = parser.extract_lp_close_data(tx_result.receipt.to_dict()) or close_data
        assert close_data is not None, "Exact-pool LP_CLOSE receipt must contain measured close data"
        assert (close_data.amount0_collected or 0) > 0 or (close_data.amount1_collected or 0) > 0
        assert (
            get_token_balance(web3, usdc_addr, funded_wallet) > usdc_after_open
            or get_token_balance(web3, weth_addr, funded_wallet) > weth_after_open
        )
