"""Shared exact-cell scientific evidence helpers for Euler V2 Intent tests."""

from __future__ import annotations

from typing import Any

from web3 import Web3

from almanak.connectors.euler_v2.adapter import DEBT_OF_SELECTOR
from almanak.connectors.euler_v2.receipt_parser import EulerV2ReceiptParser
from tests.intents.conftest import get_token_balance

_AMOUNT_FIELD = {
    "SUPPLY": "deposit_amount",
    "WITHDRAW": "withdraw_amount",
    "BORROW": "borrow_amount",
    "REPAY": "repay_amount",
}


def euler_position_raw(web3: Web3, *, vault: str, account: str, debt: bool) -> int:
    """Read the protocol position directly from the authoritative EVault."""
    if not debt:
        return get_token_balance(web3, vault, account)
    calldata = DEBT_OF_SELECTOR + account.lower().removeprefix("0x").rjust(64, "0")
    result = web3.eth.call({"to": Web3.to_checksum_address(vault), "data": calldata})
    return int.from_bytes(bytes(result), byteorder="big")


def record_euler_lending_proof(
    *,
    intent_evidence: Any,
    intent: Any,
    execution_result: Any,
    vault: str,
    decimals: int,
    wallet_before_raw: int,
    wallet_after_raw: int,
    position_before: int,
    position_after: int,
    requested_amount_raw: int,
    asset_address: str,
    account: str,
) -> None:
    """Record one target receipt plus raw lending value-flow measurements.

    Setup intents are deliberately ignored.  Exactly one receipt must contain
    the target Euler semantic event; the independent sealer later re-derives
    the event emitter, amount, account, ERC-20 transfer, wallet delta, parser
    amount, and position direction from the sealed values.
    """
    intent_name = intent.intent_type.value
    amount_field = _AMOUNT_FIELD[intent_name]

    matches: list[tuple[Any, Any]] = []
    for transaction_result in execution_result.transaction_results:
        if transaction_result.receipt is None:
            continue
        parser = EulerV2ReceiptParser(underlying_decimals=decimals)
        parsed = parser.parse_receipt(transaction_result.receipt.to_dict(), vault_address=vault)
        if parsed.success and int(getattr(parsed, amount_field)) > 0:
            matches.append((transaction_result, parsed))
    assert len(matches) == 1, f"Expected exactly one Euler V2 {intent_name} receipt from {vault}, found {len(matches)}"
    transaction_result, parsed = matches[0]
    parser_amount = int(getattr(parsed, amount_field))

    intent_evidence.bind(intent)
    captured = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=transaction_result,
        parser=lambda receipt: EulerV2ReceiptParser(underlying_decimals=decimals).parse_receipt(
            receipt,
            vault_address=vault,
        ),
    )
    assert captured.success
    assert int(getattr(captured, amount_field)) == parser_amount

    wallet_delta = wallet_after_raw - wallet_before_raw
    expected_delta = requested_amount_raw if intent_name in {"WITHDRAW", "BORROW"} else -requested_amount_raw
    position_direction_ok = (
        position_after > position_before if intent_name in {"SUPPLY", "BORROW"} else position_after < position_before
    )
    flags = {
        "parse_success": bool(captured.success),
        "single_target_receipt": len(matches) == 1,
        "parser_amount_matches_request": parser_amount == requested_amount_raw,
        "wallet_delta_matches_request": wallet_delta == expected_delta,
        "position_direction_matches_intent": position_direction_ok,
    }
    # Assert before recording, as the Uniswap/Aave/Trader Joe helpers do.
    # record_fidelity is a no-op whenever evidence is disabled, which is the
    # default in CI, so a flags-only Euler helper proved nothing there. The
    # EVault position direction is the one property no call site re-checks.
    assert all(flags.values()), f"Euler V2 {intent_name} proof failed: {sorted(k for k, v in flags.items() if not v)}"
    intent_evidence.record_fidelity(
        hard=True,
        flags=flags,
        witnesses=[
            {"kind": "wallet_balance", "before_raw": wallet_before_raw, "after_raw": wallet_after_raw},
            {"kind": "protocol_position", "before": position_before, "after": position_after},
        ],
    )
    intent_evidence.record_balance_deltas(
        checks={"wallet_delta_matches_requested_amount": wallet_delta == expected_delta},
        token={
            "address": asset_address,
            "before": wallet_before_raw,
            "after": wallet_after_raw,
            "delta": wallet_delta,
        },
    )
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile="lending.v1",
        intent=intent_name,
        account=account,
        asset_address=asset_address,
        asset_decimals=decimals,
        resource_address=vault,
        requested_amount_raw=requested_amount_raw,
        wallet_before_raw=wallet_before_raw,
        wallet_after_raw=wallet_after_raw,
        position_before=position_before,
        position_after=position_after,
        parser_amount_raw=parser_amount,
    )


__all__ = ["euler_position_raw", "record_euler_lending_proof"]
