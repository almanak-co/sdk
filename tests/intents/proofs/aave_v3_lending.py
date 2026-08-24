"""Shared atomic Aave V3 target proof for synchronous Intent runners.

The helper records observations; the independent ``lending.v1`` sealer remains
the authority.  Setup and cleanup deliberately do not use this helper, so they
cannot be mistaken for the one target Intent claimed by a cell.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.connectors.aave_v3.receipt_parser import EVENT_TOPICS, AaveV3ReceiptParser
from almanak.framework.intents import IntentCompiler

_COLLECTION = {"SUPPLY": "supplies", "WITHDRAW": "withdraws", "BORROW": "borrows", "REPAY": "repays"}
_DIRECTION = {"SUPPLY": -1, "WITHDRAW": 1, "BORROW": 1, "REPAY": -1}
_POSITION = {"SUPPLY": 1, "WITHDRAW": -1, "BORROW": 1, "REPAY": -1}
_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


@dataclass(frozen=True)
class AaveLendingProofResult:
    intent: Any
    compilation_result: Any
    execution_result: Any
    transaction_result: Any
    requested_amount_raw: int


def _event_account_matches(intent_name: str, event: Any, wallet: str) -> bool:
    account = wallet.lower()
    if event.user.lower() != account:
        return False
    if intent_name in {"SUPPLY", "BORROW"}:
        return event.on_behalf_of.lower() == account
    if intent_name == "WITHDRAW":
        return event.to.lower() == account
    return event.repayer.lower() == account and event.use_atokens is False


def _hex_str(value: Any) -> str:
    """Normalize Web3 receipt hex fields across str and HexBytes shapes."""
    if isinstance(value, bytes | bytearray | memoryview):
        return "0x" + bytes(value).hex()
    return str(value or "").lower()


def _wallet_transfers(
    *, receipt: dict[str, Any], wallet: str, token_address: str, direction: int, amount_raw: int
) -> list[dict[str, Any]]:
    wallet_topic = wallet.lower().removeprefix("0x").rjust(64, "0")
    direction_topic = 2 if direction > 0 else 1
    return [
        log
        for log in receipt.get("logs", [])
        if str(log.get("address") or "").lower() == token_address.lower()
        and len(log.get("topics") or []) == 3
        and _hex_str(log["topics"][0]) == _TRANSFER_TOPIC
        and _hex_str(log["topics"][direction_topic]).removeprefix("0x") == wallet_topic
        and int(_hex_str(log.get("data")) or "0x0", 16) == amount_raw
    ]


async def execute_aave_lending_target(
    *,
    intent_name: str,
    intent: Any,
    chain: str,
    wallet: str,
    token_address: str,
    token_symbol: str,
    token_decimals: int,
    pool_address: str,
    amount: Decimal,
    orchestrator: Any,
    execution_context: Any,
    price_oracle: dict[str, Decimal],
    compiler_config: Any,
    rpc_url: str,
    gateway_client: Any,
    intent_evidence: Any,
    read_token_balance: Callable[[], int],
    read_position: Callable[[], int],
    explorer_tx_base: str | None = None,
) -> AaveLendingProofResult:
    """Execute one target action and record independently checkable facts."""
    intent_name = intent_name.upper()
    if intent_name not in _COLLECTION:
        raise ValueError(f"Unsupported Aave lending proof target {intent_name!r}")
    wallet_before = read_token_balance()
    position_before = read_position()
    intent_evidence.bind(intent)
    compilation = IntentCompiler(
        chain=chain,
        wallet_address=wallet,
        price_oracle=price_oracle,
        config=compiler_config,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
    ).compile(intent)
    if compilation.status.value != "SUCCESS" or compilation.action_bundle is None:
        raise AssertionError(f"Aave {intent_name} compilation failed: {compilation.error}")
    execution = await orchestrator.execute(compilation.action_bundle, execution_context)
    if not execution.success:
        raise AssertionError(f"Aave {intent_name} execution failed: {execution.error}")

    matches = []
    topic = EVENT_TOPICS[intent_name.title()]
    for transaction in execution.transaction_results:
        if transaction.receipt is None:
            continue
        if any(
            log.get("topics") and _hex_str(log["topics"][0]) == topic.lower()
            for log in transaction.receipt.to_dict().get("logs", [])
        ):
            matches.append(transaction)
    if len(matches) != 1:
        raise AssertionError(f"Expected exactly one Aave {intent_name} receipt, got {len(matches)}")
    target = matches[0]
    parsed = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=target,
        parser=AaveV3ReceiptParser().parse_receipt,
        explorer_url=f"{explorer_tx_base}{target.tx_hash}" if explorer_tx_base else None,
    )
    if not parsed.success:
        raise AssertionError(f"Aave {intent_name} receipt parsing failed: {parsed.error}")
    events = [
        event for event in getattr(parsed, _COLLECTION[intent_name]) if event.reserve.lower() == token_address.lower()
    ]
    if len(events) != 1:
        raise AssertionError(f"Expected one parsed {intent_name} event for the target reserve")
    event = events[0]
    requested_raw = int(amount * Decimal(10**token_decimals))
    wallet_after = read_token_balance()
    position_after = read_position()
    wallet_delta = wallet_after - wallet_before
    if wallet_delta != _DIRECTION[intent_name] * requested_raw or int(event.amount) != requested_raw:
        raise AssertionError(
            f"Aave {intent_name} value flow mismatch: request={requested_raw}, "
            f"wallet_delta={wallet_delta}, parser={event.amount}"
        )
    if _POSITION[intent_name] > 0 and position_after <= position_before:
        raise AssertionError(f"Aave {intent_name} position did not increase")
    if _POSITION[intent_name] < 0 and position_after >= position_before:
        raise AssertionError(f"Aave {intent_name} position did not decrease")

    account_ok = _event_account_matches(intent_name, event, wallet)
    if not account_ok:
        raise AssertionError(f"Aave {intent_name} event account binding failed")

    transfers = _wallet_transfers(
        receipt=target.receipt.to_dict(),
        wallet=wallet,
        token_address=token_address,
        direction=_DIRECTION[intent_name],
        amount_raw=requested_raw,
    )
    transfer_ok = len(transfers) == 1
    intent_evidence.record_fidelity(
        hard=transfer_ok,
        flags={
            "parse_success": True,
            "single_reserve_event": True,
            "reserve_match": True,
            "account_match": account_ok,
            "amount_eq_wallet_delta": True,
            "wallet_transfer_unambiguous": transfer_ok,
        },
        witnesses=[
            {"kind": "wallet_balance_delta", "token": token_address, "amount_raw": wallet_delta},
            {"kind": "independent_transfer_logs", "matches": transfers},
        ],
        notes=[] if transfer_ok else ["Wallet-directed token Transfer was ambiguous."],
    )
    intent_evidence.record_balance_deltas(
        checks={"wallet_delta_eq_independent_transfer_log": transfer_ok},
        token={
            "address": token_address,
            "symbol": token_symbol,
            "before": wallet_before,
            "after": wallet_after,
            "delta": wallet_delta,
        },
    )
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile="lending.v1",
        intent=intent_name,
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
    return AaveLendingProofResult(intent, compilation, execution, target, requested_raw)
