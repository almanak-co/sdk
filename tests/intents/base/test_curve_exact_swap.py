"""Four-layer exact Curve StableSwap admission proof on a managed Base fork."""

from __future__ import annotations

import os
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import CHAIN_CONFIGS, SWAP_MAX_SLIPPAGE, get_token_balance

CHAIN = "base"
POOL = "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f"
EXPECTED_BINDING_HASH = "48b66ede1f325b446cb605fac5e59b58544b7416c13355c741823d71063ee02c"
STABLESWAP_EXCHANGE_SELECTOR = "0x3df02124"


@pytest.mark.base
@pytest.mark.swap
@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.asyncio
async def test_exact_curve_stableswap_executes_at_bound_pool(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    anvil_rpc_url: str,
    anvil_eth_call_adapter,
) -> None:
    """Compile, execute, parse, and balance-check an exact Base 4pool swap."""
    tokens = CHAIN_CONFIGS[CHAIN]["tokens"]
    usdc = tokens["USDC"]
    usdbc = tokens["USDbC"]
    amount = Decimal("100")
    usdc_before = get_token_balance(web3, usdc, funded_wallet)
    usdbc_before = get_token_balance(web3, usdbc, funded_wallet)
    assert usdc_before > 0

    compiler = IntentCompiler(
        chain=CHAIN,
        wallet_address=funded_wallet,
        price_oracle={"USDC": Decimal("1"), "USDbC": Decimal("1")},
        rpc_url=anvil_rpc_url,
        venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
    )
    compiled = compiler.compile(
        SwapIntent(
            from_token="USDC",
            to_token="USDbC",
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
    assert pool_transactions[0].data[:10].lower() == STABLESWAP_EXCHANGE_SELECTOR

    executed = await orchestrator.execute(compiled.action_bundle)
    assert executed.success, executed.error
    assert executed.transaction_results
    fork_block = int(os.environ["ANVIL_FORK_BLOCK_BASE"])
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

    usdc_after = get_token_balance(web3, usdc, funded_wallet)
    usdbc_after = get_token_balance(web3, usdbc, funded_wallet)
    assert usdc_before - usdc_after == int(amount * Decimal(10**6))
    assert usdbc_after - usdbc_before > 0
