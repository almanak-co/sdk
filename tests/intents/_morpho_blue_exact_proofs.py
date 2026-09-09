"""Chain-neutral atomic evidence scenarios for Morpho Blue lending Intents.

Morpho Blue is a singleton: every market settles through one contract, so the
market id rather than the emitter identifies the resource. Each proof therefore
binds the market it settled against in addition to the singleton address.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from web3 import Web3

from almanak.connectors.morpho_blue.addresses import MORPHO_BLUE
from almanak.connectors.morpho_blue.adapter import MORPHO_MARKETS
from almanak.connectors.morpho_blue.receipt_parser import (
    EVENT_TOPICS,
    MorphoBlueEvent,
    MorphoBlueEventType,
    MorphoBlueReceiptParser,
)
from almanak.connectors.morpho_blue.sdk import MorphoBlueSDK
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals
from tests.intents.intent_evidence import decode_explorer_view

# 100 USDe against a 20 USDG borrow is 20% LTV on a 91.5% LLTV market, well
# inside the 30% ceiling intent tests hold to so live oracle drift cannot
# liquidate the setup mid-run.
COLLATERAL_AMOUNT = Decimal("100")
BORROW_AMOUNT = Decimal("20")
SUPPLY_AMOUNT = Decimal("10")
WITHDRAW_SETUP_AMOUNT = Decimal("20")
WITHDRAW_AMOUNT = Decimal("10")
REPAY_AMOUNT = Decimal("4")

_TARGET_EVENT: dict[IntentType, tuple[str, MorphoBlueEventType]] = {
    IntentType.SUPPLY: ("Supply", MorphoBlueEventType.SUPPLY),
    IntentType.WITHDRAW: ("Withdraw", MorphoBlueEventType.WITHDRAW),
    IntentType.BORROW: ("Borrow", MorphoBlueEventType.BORROW),
    IntentType.REPAY: ("Repay", MorphoBlueEventType.REPAY),
}

# Wallet gains the asset on WITHDRAW and BORROW; it pays on SUPPLY and REPAY.
_WALLET_INFLOW = {IntentType.WITHDRAW, IntentType.BORROW}
# Supply-side intents move supply shares; debt-side intents move borrow shares.
_POSITION_FIELD = {
    IntentType.SUPPLY: "supply_shares",
    IntentType.WITHDRAW: "supply_shares",
    IntentType.BORROW: "borrow_shares",
    IntentType.REPAY: "borrow_shares",
}


def select_market_id(chain: str, market_name: str) -> str:
    for market_id, info in MORPHO_MARKETS.get(chain, {}).items():
        if info.get("name") == market_name:
            return market_id
    raise AssertionError(f"Expected Morpho market {market_name!r} on chain {chain!r}")


def _position(sdk: MorphoBlueSDK, market_id: str, wallet: str, field: str) -> int:
    return int(getattr(sdk.get_position(market_id, wallet), field))


async def _execute(
    compiler: IntentCompiler,
    orchestrator: ExecutionOrchestrator,
    execution_context: ExecutionContext,
    intent: Any,
):
    compiled = compiler.compile(intent)
    assert compiled.status.value == "SUCCESS", f"{intent.intent_type.value} compilation failed: {compiled.error}"
    assert compiled.action_bundle is not None
    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success, f"{intent.intent_type.value} execution failed: {executed.error}"
    return executed


def _target_transaction(execution_result: Any, event_name: str) -> Any:
    topic = EVENT_TOPICS[event_name].lower()
    matches = []
    for transaction in execution_result.transaction_results:
        if transaction.receipt is None:
            continue
        for log in transaction.receipt.to_dict().get("logs", []):
            topics = log.get("topics") or []
            if not topics:
                continue
            first = topics[0]
            first_hex = first.lower() if isinstance(first, str) else Web3.to_hex(first).lower()
            if first_hex == topic:
                matches.append(transaction)
                break
    assert len(matches) == 1, f"Expected one {event_name}-emitting target receipt, got {len(matches)}"
    return matches[0]


def _parsed_event(parse_result: Any, event_type: MorphoBlueEventType, market_id: str) -> MorphoBlueEvent:
    matches = [
        event
        for event in parse_result.events
        if event.event_type == event_type and str(event.data.get("market_id", "")).lower() == market_id.lower()
    ]
    assert len(matches) == 1, f"Expected one parsed {event_type} event for market {market_id}, got {len(matches)}"
    return matches[0]


async def run_morpho_blue_exact_proof(
    *,
    target: IntentType,
    chain: str,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    execution_context: ExecutionContext,
    price_oracle: dict[str, Decimal],
    intent_evidence: Any,
    rpc_url: str,
    market_name: str,
) -> None:
    """Execute setup separately, then emit evidence for exactly one target Intent."""
    market_id = select_market_id(chain, market_name)
    market = MORPHO_MARKETS[chain][market_id]
    loan_symbol = market["loan_token"]
    collateral_symbol = market["collateral_token"]
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    loan_token = tokens[loan_symbol]
    compiler = IntentCompiler(chain=chain, wallet_address=funded_wallet, price_oracle=price_oracle, rpc_url=rpc_url)
    sdk = MorphoBlueSDK(chain=chain, rpc_url=rpc_url)

    def supply_collateral() -> SupplyIntent:
        return SupplyIntent(
            protocol="morpho_blue",
            token=collateral_symbol,
            amount=COLLATERAL_AMOUNT,
            use_as_collateral=True,
            market_id=market_id,
            chain=chain,
        )

    def borrow(amount: Decimal) -> BorrowIntent:
        return BorrowIntent(
            protocol="morpho_blue",
            collateral_token=collateral_symbol,
            collateral_amount=Decimal("0"),
            borrow_token=loan_symbol,
            borrow_amount=amount,
            market_id=market_id,
            chain=chain,
        )

    if target is IntentType.SUPPLY:
        amount = SUPPLY_AMOUNT
        intent: Any = SupplyIntent(
            protocol="morpho_blue",
            token=loan_symbol,
            amount=amount,
            use_as_collateral=False,
            market_id=market_id,
            chain=chain,
        )
    elif target is IntentType.WITHDRAW:
        await _execute(
            compiler,
            orchestrator,
            execution_context,
            SupplyIntent(
                protocol="morpho_blue",
                token=loan_symbol,
                amount=WITHDRAW_SETUP_AMOUNT,
                use_as_collateral=False,
                market_id=market_id,
                chain=chain,
            ),
        )
        amount = WITHDRAW_AMOUNT
        intent = WithdrawIntent(
            protocol="morpho_blue",
            token=loan_symbol,
            amount=amount,
            withdraw_all=False,
            # WithdrawIntent.is_collateral defaults to True, which routes Morpho
            # to withdrawCollateral(). This target withdraws the loan token it
            # supplied, and the wallet holds no collateral in this asset, so the
            # default would underflow the collateral position with Panic(17).
            is_collateral=False,
            market_id=market_id,
            chain=chain,
        )
    elif target is IntentType.BORROW:
        await _execute(compiler, orchestrator, execution_context, supply_collateral())
        amount = BORROW_AMOUNT
        intent = borrow(amount)
    else:
        await _execute(compiler, orchestrator, execution_context, supply_collateral())
        await _execute(compiler, orchestrator, execution_context, borrow(BORROW_AMOUNT))
        amount = REPAY_AMOUNT
        intent = RepayIntent(
            protocol="morpho_blue",
            token=loan_symbol,
            amount=amount,
            repay_full=False,
            market_id=market_id,
            chain=chain,
        )

    event_name, event_type = _TARGET_EVENT[target]
    position_field = _POSITION_FIELD[target]
    decimals = get_token_decimals(web3, loan_token)
    requested_raw = int(amount * Decimal(10**decimals))
    wallet_before = get_token_balance(web3, loan_token, funded_wallet)
    position_before = _position(sdk, market_id, funded_wallet, position_field)

    intent_evidence.bind(intent)
    execution_result = await _execute(compiler, orchestrator, execution_context, intent)
    target_tx = _target_transaction(execution_result, event_name)
    parser = MorphoBlueReceiptParser()
    parse_result = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=target_tx,
        parser=lambda receipt: parser.parse_receipt(receipt),
    )
    assert parse_result.success, f"{target.value} receipt parsing failed: {parse_result.error}"
    event = _parsed_event(parse_result, event_type, market_id)
    parser_amount = int(Decimal(str(event.data["assets"])))

    wallet_after = get_token_balance(web3, loan_token, funded_wallet)
    position_after = _position(sdk, market_id, funded_wallet, position_field)
    expected_delta = requested_raw if target in _WALLET_INFLOW else -requested_raw
    wallet_delta = wallet_after - wallet_before

    explorer_logs = decode_explorer_view(target_tx.receipt.to_dict())["logs"]
    wallet = funded_wallet.lower()
    direction_key = "to" if expected_delta > 0 else "from"
    transfers = [
        log
        for log in explorer_logs
        if log.get("name") == "Transfer"
        and str(log.get("address", "")).lower() == loan_token.lower()
        and str((log.get("args") or {}).get(direction_key, "")).lower() == wallet
        and int((log.get("args") or {}).get("value", -1)) == requested_raw
    ]
    flags = {
        "single_target_protocol_event": True,
        "market_matches": str(event.data.get("market_id", "")).lower() == market_id.lower(),
        "account_matches": str(event.data.get("on_behalf_of", "")).lower() == wallet,
        "parser_amount_matches_request": parser_amount == requested_raw,
        "wallet_delta_matches_request": wallet_delta == expected_delta,
        "single_independent_asset_transfer": len(transfers) == 1,
    }
    assert all(flags.values()), f"{target.value} exact-proof predicates failed: {flags}"
    position_moved = (
        position_after > position_before
        if target in {IntentType.SUPPLY, IntentType.BORROW}
        else position_after < position_before
    )
    assert position_moved, f"{target.value} did not move {position_field} in the required direction"

    intent_evidence.record_fidelity(
        hard=True,
        flags=flags,
        witnesses=[
            {"kind": "wallet_balance_delta", "token": loan_token, "amount_raw": wallet_delta},
            {"kind": "independent_transfer_logs", "matches": transfers},
        ],
        notes=[],
    )
    intent_evidence.record_balance_deltas(
        checks={"wallet_delta_matches_request": wallet_delta == expected_delta},
        asset={
            "address": loan_token,
            "symbol": loan_symbol,
            "before": wallet_before,
            "after": wallet_after,
            "delta": wallet_delta,
        },
    )
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile="lending.v1",
        intent=target.value,
        account=funded_wallet,
        asset_address=loan_token,
        asset_decimals=decimals,
        resource_address=MORPHO_BLUE[chain]["morpho"],
        market_id=market_id,
        requested_amount_raw=requested_raw,
        wallet_before_raw=wallet_before,
        wallet_after_raw=wallet_after,
        position_before=position_before,
        position_after=position_after,
        parser_amount_raw=parser_amount,
    )


__all__ = ["run_morpho_blue_exact_proof", "select_market_id"]
