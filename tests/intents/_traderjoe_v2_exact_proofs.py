"""Chain-neutral exact-runtime proofs for Trader Joe Liquidity Book swaps."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from web3 import Web3

from almanak.connectors.traderjoe_v2.addresses import TRADERJOE_V2, TRADERJOE_V2_LBPAIRS
from almanak.connectors.traderjoe_v2.receipt_parser import TraderJoeV2ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.compiler_models import IntentCompilerConfig
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals
from tests.intents.intent_evidence import decode_explorer_view

BIN_STEP = 20
SWAP_AMOUNT = Decimal("0.01")
MAX_SLIPPAGE = Decimal("0.01")


@dataclass(frozen=True)
class TraderJoeSwapTargetResult:
    intent: SwapIntent
    execution_result: Any
    transaction_result: Any
    amount_in_raw: int
    amount_out_raw: int
    compile_metadata: dict[str, Any]


def _pair_address(chain: str) -> str:
    rows = [
        row
        for row in TRADERJOE_V2_LBPAIRS[chain]
        if row["tokenX"] == "WAVAX" and row["tokenY"] == "USDT" and row["bin_step"] == BIN_STEP
    ]
    assert len(rows) == 1, "Exact Trader Joe WAVAX/USDT/20 registry identity is ambiguous"
    return Web3.to_checksum_address(str(rows[0]["address"]))


def _factory_witness(web3: Web3, *, chain: str, token_x: str, token_y: str, pair: str) -> dict[str, Any]:
    factory = TRADERJOE_V2[chain]["factory"]
    calldata = (
        "0x704037bd"
        + token_x.removeprefix("0x").lower().zfill(64)
        + token_y.removeprefix("0x").lower().zfill(64)
        + f"{BIN_STEP:064x}"
    )
    block = web3.eth.get_block("latest")
    number = int(block["number"])
    result = web3.eth.call({"to": Web3.to_checksum_address(factory), "data": calldata}, block_identifier=number)
    raw = bytes(result)
    assert len(raw) >= 128
    assert int.from_bytes(raw[:32], "big") == BIN_STEP
    assert Web3.to_checksum_address("0x" + raw[44:64].hex()) == pair
    return {
        "block_number": number,
        "block_hash": Web3.to_hex(block["hash"]),
        "to": factory,
        "calldata": calldata,
        "raw_result": Web3.to_hex(result),
    }


def _target_transaction(execution: Any, *, wallet: str, token_in: str, token_out: str, pair: str) -> Any:
    """Select the economic target receipt across direct EOA and Safe wrappers.

    A Safe/Zodiac receipt targets the Roles module, not the nested Trader Joe
    router. The uniquely identifying invariant shared by both paths is the
    bilateral token flow: input leaves the tested wallet and output enters it.
    Router authorization is proved independently by the Safe permission layer.
    """
    matches = []
    for row in execution.transaction_results:
        if row.receipt is None:
            continue
        logs = decode_explorer_view(row.receipt.to_dict())["logs"]
        has_input = any(
            log.get("name") == "Transfer"
            and str(log.get("address") or "").lower() == token_in.lower()
            and str((log.get("args") or {}).get("from") or "").lower() == wallet.lower()
            and str((log.get("args") or {}).get("to") or "").lower() == pair.lower()
            for log in logs
        )
        has_output = any(
            log.get("name") == "Transfer"
            and str(log.get("address") or "").lower() == token_out.lower()
            and str((log.get("args") or {}).get("from") or "").lower() == pair.lower()
            and str((log.get("args") or {}).get("to") or "").lower() == wallet.lower()
            for log in logs
        )
        if has_input and has_output:
            matches.append(row)
    assert len(matches) == 1, f"Expected one bilateral Trader Joe target receipt, got {len(matches)}"
    return matches[0]


async def run_traderjoe_v2_swap_exact_proof(
    *,
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
) -> TraderJoeSwapTargetResult:
    """Prove WAVAX→USDT through the exact live LBPair and bilateral flow."""
    chain = "avalanche"
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    token_in, token_out = tokens["WAVAX"], tokens["USDT"]
    router = TRADERJOE_V2[chain]["router"]
    factory = TRADERJOE_V2[chain]["factory"]
    pair = _pair_address(chain)
    input_decimals = get_token_decimals(web3, token_in)
    output_decimals = get_token_decimals(web3, token_out)
    requested_raw = int(amount * Decimal(10**input_decimals))
    input_before = get_token_balance(web3, token_in, funded_wallet)
    output_before = get_token_balance(web3, token_out, funded_wallet)

    intent = SwapIntent(
        from_token=token_in,
        to_token=token_out,
        amount=amount,
        max_slippage=MAX_SLIPPAGE,
        max_price_impact=Decimal("0.02"),
        protocol="traderjoe_v2",
        chain=chain,
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
    assert compiled.status.value == "SUCCESS", f"Trader Joe SWAP compilation failed: {compiled.error}"
    assert compiled.action_bundle is not None
    metadata = dict(compiled.action_bundle.metadata)
    assert metadata.get("bin_step") == BIN_STEP
    assert str(metadata.get("router") or "").lower() == router.lower()
    for field in ("amount_out_min_wei", "oracle_expected_wei", "quoter_amount_wei"):
        assert int(metadata.get(field) or 0) > 0, f"Trader Joe compile guard omitted {field}"
    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success, f"Trader Joe SWAP execution failed: {executed.error}"
    transaction = _target_transaction(executed, wallet=funded_wallet, token_in=token_in, token_out=token_out, pair=pair)
    parsed = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=transaction,
        parser=lambda receipt: TraderJoeV2ReceiptParser(chain=chain).extract_swap_amounts(receipt),
        parser_method="extract_swap_amounts",
    )
    assert parsed is not None

    input_after = get_token_balance(web3, token_in, funded_wallet)
    output_after = get_token_balance(web3, token_out, funded_wallet)
    input_spent = input_before - input_after
    output_received = output_after - output_before
    assert input_spent == requested_raw and output_received > 0
    assert int(parsed.amount_in) == input_spent and int(parsed.amount_out) == output_received
    witness = _factory_witness(web3, chain=chain, token_x=token_in, token_y=token_out, pair=pair)
    logs = decode_explorer_view(transaction.receipt.to_dict())["logs"]
    wallet = funded_wallet.lower()
    input_transfers = [
        row
        for row in logs
        if row.get("name") == "Transfer"
        and str(row.get("address") or "").lower() == token_in.lower()
        and str((row.get("args") or {}).get("from") or "").lower() == wallet
        and str((row.get("args") or {}).get("to") or "").lower() == pair.lower()
        and int((row.get("args") or {}).get("value", -1)) == input_spent
    ]
    output_transfers = [
        row
        for row in logs
        if row.get("name") == "Transfer"
        and str(row.get("address") or "").lower() == token_out.lower()
        and str((row.get("args") or {}).get("to") or "").lower() == wallet
        and str((row.get("args") or {}).get("from") or "").lower() == pair.lower()
        and int((row.get("args") or {}).get("value", -1)) == output_received
    ]
    flags = {
        "factory_pair_identity": witness["to"].lower() == factory.lower(),
        "input_spent_matches_request": input_spent == requested_raw,
        "output_received_positive": output_received > 0,
        "parser_matches_wallet": int(parsed.amount_in) == input_spent and int(parsed.amount_out) == output_received,
        "bilateral_transfer_logs": len(input_transfers) == len(output_transfers) == 1,
    }
    assert all(flags.values()), f"Trader Joe exact-proof predicates failed: {flags}"
    intent_evidence.record_fidelity(
        hard=True,
        flags=flags,
        witnesses=[{"kind": "lb_factory_pair", **witness}],
    )
    intent_evidence.record_balance_deltas(
        checks={"bilateral_wallet_flow_verified": flags["bilateral_transfer_logs"] and flags["parser_matches_wallet"]},
        token_in={"address": token_in, "before": input_before, "after": input_after, "delta": -input_spent},
        token_out={
            "address": token_out,
            "before": output_before,
            "after": output_after,
            "delta": output_received,
        },
    )
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile="liquidity_book_swap.v1",
        intent="SWAP",
        account=funded_wallet,
        asset_address=token_in,
        asset_decimals=input_decimals,
        output_asset_address=token_out,
        output_asset_decimals=output_decimals,
        resource_address=pair,
        factory_address=factory,
        router_address=router,
        bin_step=BIN_STEP,
        requested_amount_raw=requested_raw,
        wallet_before_raw=input_before,
        wallet_after_raw=input_after,
        output_wallet_before_raw=output_before,
        output_wallet_after_raw=output_after,
        parser_amount_raw=int(parsed.amount_in),
        parser_output_amount_raw=int(parsed.amount_out),
        factory_witness=witness,
    )
    return TraderJoeSwapTargetResult(
        intent=intent,
        execution_result=executed,
        transaction_result=transaction,
        amount_in_raw=input_spent,
        amount_out_raw=output_received,
        compile_metadata=metadata,
    )


async def execute_traderjoe_v2_reverse_cleanup(
    *,
    web3: Web3,
    wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    execution_context: ExecutionContext | None,
    compiler_config: IntentCompilerConfig,
    rpc_url: str,
    gateway_client: Any,
    amount_in_raw: int,
) -> TraderJoeSwapTargetResult:
    """Swap only the measured forward USDT output back to WAVAX."""
    chain = "avalanche"
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    token_in, token_out = tokens["USDT"], tokens["WAVAX"]
    pair = _pair_address(chain)
    decimals = get_token_decimals(web3, token_in)
    amount_raw = amount_in_raw
    if amount_raw <= 0:
        raise AssertionError("Trader Joe reverse cleanup requires measured USDT")
    input_before = get_token_balance(web3, token_in, wallet)
    if input_before < amount_raw:
        raise AssertionError("Trader Joe reverse cleanup output exceeds the wallet balance")
    output_before = get_token_balance(web3, token_out, wallet)
    intent = SwapIntent(
        from_token=token_in,
        to_token=token_out,
        amount=Decimal(amount_raw) / Decimal(10**decimals),
        max_slippage=MAX_SLIPPAGE,
        max_price_impact=Decimal("0.02"),
        protocol="traderjoe_v2",
        chain=chain,
    )
    compiled = IntentCompiler(
        chain=chain,
        wallet_address=wallet,
        price_oracle=price_oracle,
        config=compiler_config,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
    ).compile(intent)
    assert compiled.status.value == "SUCCESS" and compiled.action_bundle is not None
    assert compiled.action_bundle.metadata.get("bin_step") == BIN_STEP
    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success
    transaction = _target_transaction(executed, wallet=wallet, token_in=token_in, token_out=token_out, pair=pair)
    parsed = TraderJoeV2ReceiptParser(chain=chain).extract_swap_amounts(transaction.receipt.to_dict())
    output_after = get_token_balance(web3, token_out, wallet)
    assert get_token_balance(web3, token_in, wallet) == input_before - amount_raw
    assert parsed is not None and int(parsed.amount_in) == amount_raw
    assert int(parsed.amount_out) == output_after - output_before > 0
    return TraderJoeSwapTargetResult(
        intent=intent,
        execution_result=executed,
        transaction_result=transaction,
        amount_in_raw=amount_raw,
        amount_out_raw=output_after - output_before,
        compile_metadata=dict(compiled.action_bundle.metadata),
    )
