"""Chain-neutral atomic evidence scenarios for Uniswap V3 Swap Intents."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from web3 import Web3

from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3
from almanak.connectors.uniswap_v3.receipt_parser import SWAP_EVENT_TOPIC, UniswapV3ReceiptParser
from almanak.connectors.uniswap_v3.sdk import compute_pool_address
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.compiler_models import IntentCompilerConfig
from tests.intents.conftest import CHAIN_CONFIGS, SWAP_MAX_SLIPPAGE, get_token_balance, get_token_decimals
from tests.intents.intent_evidence import decode_explorer_view
from tests.intents.pool_helpers import fail_if_v3_pool_missing

FEE_TIER = 500
SWAP_AMOUNT = Decimal("10")


@dataclass(frozen=True)
class SwapTargetResult:
    intent: SwapIntent
    execution_result: Any
    transaction_result: Any
    amount_in_raw: int
    amount_out_raw: int
    compile_metadata: dict[str, Any]


def _swap_transaction(execution_result: Any) -> Any:
    matches = []
    for transaction in execution_result.transaction_results:
        if transaction.receipt is None:
            continue
        if any(
            log.get("topics") and str(log["topics"][0]).lower() == SWAP_EVENT_TOPIC
            for log in transaction.receipt.to_dict().get("logs", [])
        ):
            matches.append(transaction)
    assert len(matches) == 1, f"Expected one Swap-emitting target receipt, got {len(matches)}"
    return matches[0]


async def run_uniswap_v3_swap_exact_proof(
    *,
    chain: str,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    intent_evidence: Any,
    amount: Decimal = SWAP_AMOUNT,
    execution_context: ExecutionContext | None = None,
    compiler_config: IntentCompilerConfig | None = None,
    rpc_url: str | None = None,
    gateway_client: Any | None = None,
    max_slippage: Decimal = SWAP_MAX_SLIPPAGE,
    max_price_impact: Decimal | None = None,
) -> SwapTargetResult:
    """Prove one exact USDC→WETH swap through receipt and bilateral state."""
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    token_in = tokens["USDC"]
    token_out = tokens["WETH"]
    factory = UNISWAP_V3[chain]["factory"]
    pool = compute_pool_address(factory, token_in, token_out, FEE_TIER)
    fail_if_v3_pool_missing(web3, chain, "uniswap_v3", token_in, token_out, FEE_TIER)

    input_decimals = get_token_decimals(web3, token_in)
    output_decimals = get_token_decimals(web3, token_out)
    requested_raw = int(amount * Decimal(10**input_decimals))
    input_before = get_token_balance(web3, token_in, funded_wallet)
    output_before = get_token_balance(web3, token_out, funded_wallet)

    intent = SwapIntent(
        from_token=token_in,
        to_token=token_out,
        amount=amount,
        max_slippage=max_slippage,
        max_price_impact=max_price_impact,
        protocol="uniswap_v3",
        chain=chain,
        swap_params={"pool": Web3.to_checksum_address(pool)},
    )
    intent_evidence.bind(intent)
    compiled = IntentCompiler(
        chain=chain,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        config=compiler_config,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
    ).compile(intent)
    assert compiled.status.value == "SUCCESS", f"SWAP compilation failed: {compiled.error}"
    assert compiled.action_bundle is not None
    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success, f"SWAP execution failed: {executed.error}"

    transaction = _swap_transaction(executed)
    parsed = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=transaction,
        parser=lambda receipt: UniswapV3ReceiptParser(chain=chain).parse_receipt(receipt),
    )
    assert parsed.success and parsed.swap_result is not None
    result = parsed.swap_result

    input_after = get_token_balance(web3, token_in, funded_wallet)
    output_after = get_token_balance(web3, token_out, funded_wallet)
    input_spent = input_before - input_after
    output_received = output_after - output_before
    raw_receipt = transaction.receipt.to_dict()
    swap_emitters = {
        str(log.get("address") or "").lower()
        for log in raw_receipt.get("logs", [])
        if log.get("topics") and str(log["topics"][0]).lower() == SWAP_EVENT_TOPIC
    }
    explorer_logs = decode_explorer_view(raw_receipt)["logs"]
    wallet = funded_wallet.lower()
    input_transfers = [
        log
        for log in explorer_logs
        if log.get("name") == "Transfer"
        and str(log.get("address", "")).lower() == token_in.lower()
        and str((log.get("args") or {}).get("from", "")).lower() == wallet
        and int((log.get("args") or {}).get("value", -1)) == input_spent
    ]
    output_transfers = [
        log
        for log in explorer_logs
        if log.get("name") == "Transfer"
        and str(log.get("address", "")).lower() == token_out.lower()
        and str((log.get("args") or {}).get("to", "")).lower() == wallet
        and int((log.get("args") or {}).get("value", -1)) == output_received
    ]
    flags = {
        "single_exact_pool_emitter": swap_emitters == {pool.lower()},
        "input_spent_matches_request": input_spent == requested_raw,
        "output_received_positive": output_received > 0,
        "parser_input_matches_wallet": int(result.amount_in) == input_spent,
        "parser_output_matches_wallet": int(result.amount_out) == output_received,
        "parser_token_in_matches": result.token_in.lower() == token_in.lower(),
        "parser_token_out_matches": result.token_out.lower() == token_out.lower(),
        "single_input_transfer": len(input_transfers) == 1,
        "single_output_transfer": len(output_transfers) == 1,
    }
    assert all(flags.values()), f"Uniswap V3 exact SWAP predicates failed: {flags}"
    intent_evidence.record_fidelity(
        hard=True,
        flags=flags,
        witnesses=[
            {"kind": "swap_pool_emitter", "pool": pool, "emitters": sorted(swap_emitters)},
            {"kind": "independent_transfer_logs", "input": input_transfers, "output": output_transfers},
        ],
        notes=[],
    )
    intent_evidence.record_balance_deltas(
        checks={"bilateral_wallet_flow_verified": True},
        token_in={
            "address": token_in,
            "before": input_before,
            "after": input_after,
            "delta": -input_spent,
        },
        token_out={
            "address": token_out,
            "before": output_before,
            "after": output_after,
            "delta": output_received,
        },
    )
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile="swap.v1",
        intent="SWAP",
        account=funded_wallet,
        asset_address=token_in,
        asset_decimals=input_decimals,
        output_asset_address=token_out,
        output_asset_decimals=output_decimals,
        resource_address=pool,
        factory_address=factory,
        fee_tier=FEE_TIER,
        requested_amount_raw=requested_raw,
        wallet_before_raw=input_before,
        wallet_after_raw=input_after,
        output_wallet_before_raw=output_before,
        output_wallet_after_raw=output_after,
        parser_amount_raw=int(result.amount_in),
        parser_output_amount_raw=int(result.amount_out),
    )
    return SwapTargetResult(
        intent=intent,
        execution_result=executed,
        transaction_result=transaction,
        amount_in_raw=input_spent,
        amount_out_raw=output_received,
        compile_metadata=dict(compiled.action_bundle.metadata),
    )


async def execute_uniswap_v3_exact_reverse_cleanup(
    *,
    chain: str,
    web3: Web3,
    wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    execution_context: ExecutionContext | None,
    compiler_config: IntentCompilerConfig,
    rpc_url: str,
    gateway_client: Any,
    amount_in_raw: int,
    max_slippage: Decimal = SWAP_MAX_SLIPPAGE,
    max_price_impact: Decimal | None = None,
) -> SwapTargetResult:
    """Reverse only the measured WETH output from the target swap."""
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    token_in = tokens["WETH"]
    token_out = tokens["USDC"]
    factory = UNISWAP_V3[chain]["factory"]
    pool = compute_pool_address(factory, token_in, token_out, FEE_TIER)
    input_decimals = get_token_decimals(web3, token_in)
    if amount_in_raw <= 0:
        raise AssertionError("Reverse cleanup requires a positive measured WETH output")
    input_before = get_token_balance(web3, token_in, wallet)
    if input_before < amount_in_raw:
        raise AssertionError("Reverse cleanup output exceeds the wallet balance")
    output_before = get_token_balance(web3, token_out, wallet)
    intent = SwapIntent(
        from_token=token_in,
        to_token=token_out,
        amount=Decimal(amount_in_raw) / (Decimal(10) ** input_decimals),
        max_slippage=max_slippage,
        max_price_impact=max_price_impact,
        protocol="uniswap_v3",
        chain=chain,
        swap_params={"pool": Web3.to_checksum_address(pool)},
    )
    compiled = IntentCompiler(
        chain=chain,
        wallet_address=wallet,
        price_oracle=price_oracle,
        config=compiler_config,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
    ).compile(intent)
    assert compiled.status.value == "SUCCESS", f"Reverse cleanup compilation failed: {compiled.error}"
    assert compiled.action_bundle is not None
    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success, f"Reverse cleanup execution failed: {executed.error}"
    transaction = _swap_transaction(executed)
    parsed = UniswapV3ReceiptParser(chain=chain).parse_receipt(transaction.receipt.to_dict())
    assert parsed.success and parsed.swap_result is not None
    result = parsed.swap_result
    output_after = get_token_balance(web3, token_out, wallet)
    assert get_token_balance(web3, token_in, wallet) == input_before - amount_in_raw
    assert int(result.amount_in) == amount_in_raw
    assert int(result.amount_out) == output_after - output_before > 0
    assert result.token_in.lower() == token_in.lower()
    assert result.token_out.lower() == token_out.lower()
    return SwapTargetResult(
        intent=intent,
        execution_result=executed,
        transaction_result=transaction,
        amount_in_raw=amount_in_raw,
        amount_out_raw=int(result.amount_out),
        compile_metadata=dict(compiled.action_bundle.metadata),
    )


__all__ = [
    "SwapTargetResult",
    "execute_uniswap_v3_exact_reverse_cleanup",
    "run_uniswap_v3_swap_exact_proof",
]
