"""Four-layer exact Curve Tricrypto admission proof on a managed Ethereum fork."""

from __future__ import annotations

import os
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import CHAIN_CONFIGS, SWAP_MAX_SLIPPAGE, fund_erc20_token, get_token_balance

CHAIN = "ethereum"
POOL = "0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
USDT_BALANCE_SLOT = 2
EXPECTED_BINDING_HASH = "4d64d8b73304e417286da27346327b1953c73f20dac733f9947d61b2d9639b2c"
CRYPTOSWAP_EXCHANGE_SELECTOR = "0x5b41b908"


@pytest.mark.ethereum
@pytest.mark.swap
@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.asyncio
async def test_exact_curve_tricrypto_executes_at_bound_pool(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    anvil_rpc_url: str,
    anvil_eth_call_adapter,
) -> None:
    """Compile, execute, parse, and balance-check an exact Tricrypto2 swap."""
    amount = Decimal("100")
    fund_erc20_token(funded_wallet, USDT, int(Decimal("10000") * Decimal(10**6)), USDT_BALANCE_SLOT, anvil_rpc_url)
    tokens = CHAIN_CONFIGS[CHAIN]["tokens"]
    usdt_before = get_token_balance(web3, tokens["USDT"], funded_wallet)
    weth_before = get_token_balance(web3, tokens["WETH"], funded_wallet)
    assert usdt_before > 0

    compiler = IntentCompiler(
        chain=CHAIN,
        wallet_address=funded_wallet,
        price_oracle={"USDT": Decimal("1"), "WETH": Decimal("3000")},
        rpc_url=anvil_rpc_url,
        venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
    )
    compiled = compiler.compile(
        SwapIntent(
            from_token="USDT",
            to_token="WETH",
            amount=amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="curve",
            chain=CHAIN,
            swap_params={"pool": POOL},
        )
    )

    assert compiled.status is CompilationStatus.SUCCESS, compiled.error
    assert compiled.action_bundle is not None
    metadata = compiled.action_bundle.metadata or {}
    assert metadata["venue_binding_hash"] == EXPECTED_BINDING_HASH
    assert metadata["venue_binding"]["identityRefs"][0]["reference"].lower() == POOL.lower()
    pool_transactions = [tx for tx in compiled.transactions if tx.to.lower() == POOL.lower()]
    assert len(pool_transactions) == 1
    assert pool_transactions[0].data[:10].lower() == CRYPTOSWAP_EXCHANGE_SELECTOR

    executed = await orchestrator.execute(compiled.action_bundle)
    assert executed.success, executed.error
    assert executed.transaction_results
    fork_block = int(os.environ["ANVIL_FORK_BLOCK_ETHEREUM"])
    parser = CurveReceiptParser(chain=CHAIN)
    found_bound_swap = False
    for tx_result in executed.transaction_results:
        assert tx_result.tx_hash.startswith("0x") and len(tx_result.tx_hash) == 66
        assert tx_result.receipt is not None
        assert tx_result.receipt.status == 1
        assert tx_result.receipt.block_number >= fork_block
        parsed = parser.parse_receipt(tx_result.receipt.to_dict())
        assert parsed.success, parsed.error
        found_bound_swap |= any(
            event.event_type is CurveEventType.TOKEN_EXCHANGE
            and event.contract_address.lower() == POOL.lower()
            and event.data["tokens_sold"] > 0
            and event.data["tokens_bought"] > 0
            for event in parsed.events
        )
    assert found_bound_swap

    usdt_after = get_token_balance(web3, tokens["USDT"], funded_wallet)
    weth_after = get_token_balance(web3, tokens["WETH"], funded_wallet)
    assert usdt_before - usdt_after == int(amount * Decimal(10**6))
    assert weth_after - weth_before > 0
