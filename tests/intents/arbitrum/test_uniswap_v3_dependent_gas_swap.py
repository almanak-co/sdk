"""Uniswap V3 SWAP behind a fresh approval gets a measured gas limit on a managed fork.

Regression for ALM-10046: the local simulator executed the approval on a fork
snapshot for state setup and then handed the dependent swap the static swap
constant anyway. A swap that crosses many ticks needs several times that
constant, so it burned the whole limit inside the pool. The swap below is sized
so that the static limit cannot pay for it: the test can only pass when the
limit comes from a measurement taken after the approval.
"""

from decimal import Decimal

import pytest

from almanak.connectors.uniswap_v3.receipt_parser import SWAP_EVENT_TOPIC, UniswapV3ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.execution.signer import LocalKeySigner
from almanak.framework.execution.simulator.local import LocalSimulator
from almanak.framework.execution.submitter import PublicMempoolSubmitter
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.compiler_constants import get_gas_estimate
from almanak.framework.intents.compiler_models import IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType
from qa_lab.operator_gateway import OperatorGatewayClient
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    TEST_SUBMITTER_MAX_RETRIES,
    TEST_TX_TIMEOUT_SECONDS,
    fund_erc20_token,
    get_token_balance,
)

CHAIN = "arbitrum"
FEE_TIER = 500
# Large enough to walk far past the active tick range of the 0.05% pool.
SWAP_AMOUNT_USDC = Decimal("3000000")
FUNDING_USDC = 4_000_000


@pytest.mark.arbitrum
@pytest.mark.swap
class TestUniswapV3DependentGasSwap:
    @pytest.mark.no_zodiac(
        reason="LocalSimulator state setup measures the EOA bundle; a Safe module call cannot be estimated as a direct wallet call"
    )
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_tick_crossing_swap_behind_fresh_approval_gets_measured_gas(
        self, web3, anvil_rpc_url, funded_wallet, test_private_key, price_oracle
    ):
        tokens = CHAIN_CONFIGS[CHAIN]["tokens"]
        usdc, weth = tokens["USDC"], tokens["WETH"]
        fund_erc20_token(
            funded_wallet, usdc, FUNDING_USDC * 10**6, CHAIN_CONFIGS[CHAIN]["balance_slots"]["USDC"], anvil_rpc_url
        )
        # The pipeline under test only engages when the fork can be advanced in place.
        orchestrator = ExecutionOrchestrator(
            signer=LocalKeySigner(private_key=test_private_key),
            submitter=PublicMempoolSubmitter(
                rpc_url=anvil_rpc_url, max_retries=TEST_SUBMITTER_MAX_RETRIES, timeout_seconds=TEST_TX_TIMEOUT_SECONDS
            ),
            simulator=LocalSimulator(rpc_url=anvil_rpc_url),
            chain=CHAIN,
            rpc_url=anvil_rpc_url,
            tx_timeout_seconds=TEST_TX_TIMEOUT_SECONDS,
        )
        static_swap_limit = int(get_gas_estimate(CHAIN, "swap_simple") * orchestrator.gas_buffer_multiplier**2)

        intent = SwapIntent(
            from_token=usdc,
            to_token=weth,
            amount=SWAP_AMOUNT_USDC,
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v3",
            chain=CHAIN,
            swap_params={"fee_tier": FEE_TIER},
        )
        compiler = IntentCompiler(
            chain=CHAIN,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            config=IntentCompilerConfig(managed_fork=False),
            rpc_url=anvil_rpc_url,
            gateway_client=OperatorGatewayClient(web3, CHAIN),
        )
        compiled = compiler.compile(intent)
        assert compiled.status.value == "SUCCESS", f"SWAP compilation failed: {compiled.error}"
        assert compiled.action_bundle is not None
        assert len(compiled.action_bundle.transactions) >= 2, "expected an approval before the swap"

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)
        context = ExecutionContext(chain=CHAIN, wallet_address=funded_wallet, simulation_enabled=True)
        executed = await orchestrator.execute(compiled.action_bundle, context)
        assert executed.success, f"SWAP execution failed: {executed.error}"

        swap_txs = [
            t
            for t in executed.transaction_results
            if t.receipt is not None
            and any(
                log.get("topics") and str(log["topics"][0]).lower() == SWAP_EVENT_TOPIC
                for log in t.receipt.to_dict().get("logs", [])
            )
        ]
        assert len(swap_txs) == 1, f"expected exactly one Swap-emitting receipt, got {len(swap_txs)}"
        swap_receipt = swap_txs[0].receipt
        assert swap_receipt is not None
        receipt = swap_receipt.to_dict()
        gas_used = int(receipt["gas_used"])
        gas_limit = int(web3.eth.get_transaction(receipt["tx_hash"])["gas"])
        assert int(receipt["status"]) == 1
        # Negative control: the static constant cannot pay for this swap, so a pass
        # proves the limit was measured after the approval, not guessed.
        assert gas_used > static_swap_limit, (
            f"swap used {gas_used} <= static limit {static_swap_limit}; test no longer discriminates"
        )
        assert gas_limit >= int(gas_used * orchestrator.gas_buffer_multiplier), (
            f"gas limit {gas_limit} is below the buffered measurement of {gas_used}"
        )

        parsed = UniswapV3ReceiptParser(chain=CHAIN).parse_receipt(receipt)
        assert parsed.success and parsed.swap_result is not None, f"receipt parse failed: {parsed}"
        assert parsed.swap_result.token_in.lower() == usdc.lower()
        assert parsed.swap_result.token_out.lower() == weth.lower()

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before
        assert usdc_spent == int(SWAP_AMOUNT_USDC * 10**6)
        assert weth_received > 0
        assert int(parsed.swap_result.amount_in) == usdc_spent
        assert int(parsed.swap_result.amount_out) == weth_received
