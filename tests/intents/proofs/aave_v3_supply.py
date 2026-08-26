"""Shared Aave V3 SUPPLY execution and raw-evidence capture.

The helper deliberately does not decide whether the observations prove the
claim.  Both Anvil and Mainnet runners use it to produce the ``lending.v1``
measurements that the independent seal-time validator re-derives from the raw
receipt.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.connectors.aave_v3.receipt_parser import EVENT_TOPICS, AaveV3ReceiptParser
from almanak.framework.intents import IntentCompiler, SupplyIntent


@dataclass(frozen=True)
class AaveSupplyProofResult:
    """Target execution facts needed by accounting or Mainnet cleanup."""

    intent: SupplyIntent
    compilation_result: Any
    execution_result: Any
    transaction_result: Any
    requested_amount_raw: int
    wallet_before_raw: int
    wallet_after_raw: int
    position_before: int
    position_after: int


async def execute_aave_supply_proof(
    *,
    chain: str,
    wallet: str,
    token_reference: str,
    token_symbol: str | None = None,
    token_address: str,
    token_decimals: int,
    pool_address: str,
    amount: Decimal,
    orchestrator: Any,
    execution_context: Any,
    price_oracle: dict[str, Decimal],
    compiler_config: Any | None = None,
    rpc_url: str | None = None,
    gateway_client: Any | None = None,
    intent_evidence: Any,
    read_token_balance: Callable[[], int],
    read_position: Callable[[], int],
    explorer_tx_base: str | None = None,
) -> AaveSupplyProofResult:
    """Compile, execute and observe one exact Aave V3 SUPPLY target.

    ``read_position`` must return the same authoritative position quantity
    before and after execution.  The Arbitrum proof uses Aave account collateral
    base, while the Mainnet lifecycle additionally records reserve-scoped aToken
    state for its terminal cleanup gate.
    """
    wallet_before = read_token_balance()
    position_before = read_position()
    intent = SupplyIntent(
        protocol="aave_v3",
        token=token_reference,
        amount=amount,
        chain=chain,
        expected_pool=pool_address,
    )
    intent_evidence.bind(intent)
    compiler = IntentCompiler(
        chain=chain,
        wallet_address=wallet,
        price_oracle=price_oracle,
        config=compiler_config,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
    )
    compilation_result = compiler.compile(intent)
    if compilation_result.status.value != "SUCCESS" or compilation_result.action_bundle is None:
        raise AssertionError(f"Aave SUPPLY compilation failed: {compilation_result.error}")

    execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
    if not execution_result.success:
        raise AssertionError(f"Aave SUPPLY execution failed: {execution_result.error}")

    matching = []
    for transaction_result in execution_result.transaction_results:
        if transaction_result.receipt is None:
            continue
        receipt = transaction_result.receipt.to_dict()
        if any(
            log.get("topics") and str(log["topics"][0]).lower() == EVENT_TOPICS["Supply"]
            for log in receipt.get("logs", [])
        ):
            matching.append(transaction_result)
    if len(matching) != 1:
        raise AssertionError(f"Expected exactly one Aave Supply-emitting receipt, got {len(matching)}")

    target_tx = matching[0]
    parser = AaveV3ReceiptParser()
    explorer_url = f"{explorer_tx_base}{target_tx.tx_hash}" if explorer_tx_base else None
    parse_result = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=target_tx,
        parser=parser.parse_receipt,
        explorer_url=explorer_url,
    )
    if not parse_result.success:
        raise AssertionError(f"Aave SUPPLY receipt parsing failed: {parse_result.error}")
    supplies = [event for event in parse_result.supplies if event.reserve.lower() == token_address.lower()]
    if len(supplies) != 1:
        raise AssertionError(f"Expected one parsed reserve Supply event, found {len(supplies)}")
    event = supplies[0]

    wallet_after = read_token_balance()
    position_after = read_position()
    requested_raw = int(amount * Decimal(10**token_decimals))
    spent = wallet_before - wallet_after
    if spent != requested_raw or int(event.amount) != requested_raw:
        raise AssertionError(
            f"Aave SUPPLY amount mismatch: request={requested_raw}, wallet={spent}, parser={event.amount}"
        )
    if event.reserve.lower() != token_address.lower():
        raise AssertionError("Aave SUPPLY parser reserve does not match the target asset")
    if event.user.lower() != wallet.lower() or event.on_behalf_of.lower() != wallet.lower():
        raise AssertionError("Aave SUPPLY parser account does not match the executing wallet")
    if position_after <= position_before:
        raise AssertionError("Aave SUPPLY authoritative position did not increase")

    receipt_logs = target_tx.receipt.to_dict().get("logs", [])
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    wallet_topic = wallet.lower().removeprefix("0x").rjust(64, "0")
    transfers = [
        log
        for log in receipt_logs
        if str(log.get("address") or "").lower() == token_address.lower()
        and len(log.get("topics") or []) == 3
        and str(log["topics"][0]).lower() == transfer_topic
        and str(log["topics"][1]).lower().removeprefix("0x") == wallet_topic
        and int(str(log.get("data") or "0x0"), 16) == requested_raw
    ]
    transfer_unambiguous = len(transfers) == 1
    intent_evidence.record_fidelity(
        hard=transfer_unambiguous,
        flags={
            "parse_success": True,
            "single_reserve_supply_event": True,
            "reserve_match": True,
            "user_match": True,
            "beneficiary_match": True,
            "amount_eq_wallet_delta": int(event.amount) == spent,
            "input_transfer_unambiguous": transfer_unambiguous,
            "amount_eq_transfer": transfer_unambiguous,
        },
        witnesses=[
            {"kind": "wallet_balance_delta", "token": token_address, "amount_raw": spent},
            {"kind": "independent_transfer_logs", "input_matches": transfers},
        ],
        notes=[] if transfer_unambiguous else ["Wallet-directed token Transfer was ambiguous."],
    )
    intent_evidence.record_balance_deltas(
        checks={"wallet_delta_eq_independent_transfer_log": transfer_unambiguous},
        token={
            "address": token_address,
            "symbol": token_symbol or token_reference,
            "before": wallet_before,
            "after": wallet_after,
            "delta": -spent,
        },
    )
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile="lending.v1",
        intent="SUPPLY",
        account=wallet,
        asset_address=token_address,
        asset_decimals=token_decimals,
        resource_address=pool_address,
        requested_amount_raw=requested_raw,
        wallet_before_raw=wallet_before,
        wallet_after_raw=wallet_after,
        position_before=position_before,
        position_after=position_after,
        parser_amount_raw=int(event.amount),
    )
    return AaveSupplyProofResult(
        intent=intent,
        compilation_result=compilation_result,
        execution_result=execution_result,
        transaction_result=target_tx,
        requested_amount_raw=requested_raw,
        wallet_before_raw=wallet_before,
        wallet_after_raw=wallet_after,
        position_before=position_before,
        position_after=position_after,
    )
