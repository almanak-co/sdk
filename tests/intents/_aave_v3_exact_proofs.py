"""Chain-neutral atomic evidence scenarios for Aave V3 lending Intents."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from web3 import Web3

from almanak.connectors.aave_v3.adapter import AAVE_V3_POOL_ADDRESSES
from almanak.connectors.aave_v3.receipt_parser import EVENT_TOPICS, AaveV3ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals
from tests.intents.intent_evidence import decode_explorer_view

AAVE_POOL_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
        "name": "getUserAccountData",
        "outputs": [
            {"internalType": "uint256", "name": "totalCollateralBase", "type": "uint256"},
            {"internalType": "uint256", "name": "totalDebtBase", "type": "uint256"},
            {"internalType": "uint256", "name": "availableBorrowsBase", "type": "uint256"},
            {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
            {"internalType": "uint256", "name": "ltv", "type": "uint256"},
            {"internalType": "uint256", "name": "healthFactor", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]


def _account_data(web3: Web3, chain: str, wallet: str) -> dict[str, int]:
    pool = web3.eth.contract(
        address=Web3.to_checksum_address(AAVE_V3_POOL_ADDRESSES[chain]),
        abi=AAVE_POOL_ABI,
    )
    result = pool.functions.getUserAccountData(Web3.to_checksum_address(wallet)).call()
    return {
        "totalCollateralBase": int(result[0]),
        "totalDebtBase": int(result[1]),
        "healthFactor": int(result[5]),
    }


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


_TARGET_EVENT = {
    IntentType.SUPPLY: "Supply",
    IntentType.WITHDRAW: "Withdraw",
    IntentType.BORROW: "Borrow",
    IntentType.REPAY: "Repay",
}


def _target_transaction(execution_result: Any, target: IntentType) -> Any:
    event_name = _TARGET_EVENT[target]
    matches = []
    topic = EVENT_TOPICS[event_name].lower()
    for transaction in execution_result.transaction_results:
        if transaction.receipt is None:
            continue
        receipt = transaction.receipt.to_dict()
        if any(
            log.get("topics")
            and (
                log["topics"][0].lower() if isinstance(log["topics"][0], str) else Web3.to_hex(log["topics"][0]).lower()
            )
            == topic
            for log in receipt.get("logs", [])
        ):
            matches.append(transaction)
    assert len(matches) == 1, f"Expected one {event_name}-emitting target receipt, got {len(matches)}"
    return matches[0]


def _parsed_event(parse_result: Any, target: IntentType, asset: str) -> Any:
    collection = {
        IntentType.SUPPLY: parse_result.supplies,
        IntentType.WITHDRAW: parse_result.withdraws,
        IntentType.BORROW: parse_result.borrows,
        IntentType.REPAY: parse_result.repays,
    }[target]
    matches = [event for event in collection if event.reserve.lower() == asset.lower()]
    assert len(matches) == 1, f"Expected one parsed {target.value} event for {asset}, got {len(matches)}"
    return matches[0]


def _account_matches(event: Any, target: IntentType, wallet: str) -> bool:
    expected = wallet.lower()
    if target is IntentType.SUPPLY:
        return event.user.lower() == expected and event.on_behalf_of.lower() == expected
    if target is IntentType.WITHDRAW:
        return event.user.lower() == expected and event.to.lower() == expected
    if target is IntentType.BORROW:
        return event.user.lower() == expected and event.on_behalf_of.lower() == expected
    return event.user.lower() == expected and event.repayer.lower() == expected and event.use_atokens is False


async def run_aave_v3_exact_proof(
    *,
    target: IntentType,
    chain: str,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    execution_context: ExecutionContext,
    price_oracle: dict[str, Decimal],
    intent_evidence: Any,
) -> None:
    """Execute setup separately, then emit evidence for exactly one target Intent."""
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    usdc = tokens["USDC"]
    wsteth = tokens["wstETH"]
    compiler = IntentCompiler(chain=chain, wallet_address=funded_wallet, price_oracle=price_oracle)

    if target is IntentType.SUPPLY:
        amount = Decimal("10")
        intent = SupplyIntent(protocol="aave_v3", token=usdc, amount=amount, chain=chain)
        position_key = "totalCollateralBase"
    elif target is IntentType.WITHDRAW:
        await _execute(
            compiler,
            orchestrator,
            execution_context,
            SupplyIntent(protocol="aave_v3", token=usdc, amount=Decimal("20"), chain=chain),
        )
        amount = Decimal("10")
        intent = WithdrawIntent(protocol="aave_v3", token=usdc, amount=amount, chain=chain)
        position_key = "totalCollateralBase"
    else:
        await _execute(
            compiler,
            orchestrator,
            execution_context,
            SupplyIntent(protocol="aave_v3", token=wsteth, amount=Decimal("0.1"), chain=chain),
        )
        if target is IntentType.BORROW:
            amount = Decimal("10")
            intent = BorrowIntent(
                protocol="aave_v3",
                collateral_token=wsteth,
                collateral_amount=Decimal("0"),
                borrow_token=usdc,
                borrow_amount=amount,
                interest_rate_mode="variable",
                chain=chain,
            )
        else:
            await _execute(
                compiler,
                orchestrator,
                execution_context,
                BorrowIntent(
                    protocol="aave_v3",
                    collateral_token=wsteth,
                    collateral_amount=Decimal("0"),
                    borrow_token=usdc,
                    borrow_amount=Decimal("10"),
                    interest_rate_mode="variable",
                    chain=chain,
                ),
            )
            amount = Decimal("4")
            intent = RepayIntent(protocol="aave_v3", token=usdc, amount=amount, chain=chain)
        position_key = "totalDebtBase"

    decimals = get_token_decimals(web3, usdc)
    requested_raw = int(amount * Decimal(10**decimals))
    wallet_before = get_token_balance(web3, usdc, funded_wallet)
    account_before = _account_data(web3, chain, funded_wallet)

    intent_evidence.bind(intent)
    execution_result = await _execute(compiler, orchestrator, execution_context, intent)
    target_tx = _target_transaction(execution_result, target)
    parser = AaveV3ReceiptParser(chain=chain)
    parse_result = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=target_tx,
        parser=lambda receipt: parser.parse_receipt(receipt),
    )
    assert parse_result.success, f"{target.value} receipt parsing failed: {parse_result.error}"
    event = _parsed_event(parse_result, target, usdc)

    wallet_after = get_token_balance(web3, usdc, funded_wallet)
    account_after = _account_data(web3, chain, funded_wallet)
    expected_delta = requested_raw if target in {IntentType.WITHDRAW, IntentType.BORROW} else -requested_raw
    wallet_delta = wallet_after - wallet_before

    explorer_logs = decode_explorer_view(target_tx.receipt.to_dict())["logs"]
    wallet = funded_wallet.lower()
    direction_key = "to" if expected_delta > 0 else "from"
    transfers = [
        log
        for log in explorer_logs
        if log.get("name") == "Transfer"
        and str(log.get("address", "")).lower() == usdc.lower()
        and str((log.get("args") or {}).get(direction_key, "")).lower() == wallet
        and int((log.get("args") or {}).get("value", -1)) == requested_raw
    ]
    flags = {
        "single_target_protocol_event": True,
        "asset_matches": event.reserve.lower() == usdc.lower(),
        "account_matches": _account_matches(event, target, funded_wallet),
        "parser_amount_matches_request": int(event.amount) == requested_raw,
        "wallet_delta_matches_request": wallet_delta == expected_delta,
        "single_independent_asset_transfer": len(transfers) == 1,
    }
    assert all(flags.values()), f"{target.value} exact-proof predicates failed: {flags}"
    before_position = account_before[position_key]
    after_position = account_after[position_key]
    position_changed = (
        after_position > before_position
        if target in {IntentType.SUPPLY, IntentType.BORROW}
        else after_position < before_position
    )
    assert position_changed, f"{target.value} did not move {position_key} in the required direction"

    intent_evidence.record_fidelity(
        hard=True,
        flags=flags,
        witnesses=[
            {"kind": "wallet_balance_delta", "token": usdc, "amount_raw": wallet_delta},
            {"kind": "independent_transfer_logs", "matches": transfers},
        ],
        notes=[],
    )
    intent_evidence.record_balance_deltas(
        checks={"wallet_delta_matches_request": wallet_delta == expected_delta},
        asset={
            "address": usdc,
            "symbol": "USDC",
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
        asset_address=usdc,
        asset_decimals=decimals,
        resource_address=AAVE_V3_POOL_ADDRESSES[chain],
        requested_amount_raw=requested_raw,
        wallet_before_raw=wallet_before,
        wallet_after_raw=wallet_after,
        position_before=before_position,
        position_after=after_position,
        parser_amount_raw=int(event.amount),
    )


__all__ = ["run_aave_v3_exact_proof"]
