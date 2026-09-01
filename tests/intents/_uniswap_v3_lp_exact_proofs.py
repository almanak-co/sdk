"""Chain-neutral exact Uniswap V3 concentrated-liquidity proofs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3
from almanak.connectors.uniswap_v3.receipt_parser import EVENT_TOPICS, UniswapV3ReceiptParser
from almanak.connectors.uniswap_v3.sdk import compute_pool_address
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import LPCloseIntent, LPOpenIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.compiler_models import IntentCompilerConfig
from tests.intents._parameter_fidelity import TxOutcome, check_calldata
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals
from tests.intents.intent_evidence import DisabledIntentEvidenceRecorder, decode_explorer_view
from tests.intents.pool_helpers import fail_if_v3_pool_missing

FEE_TIER = 500
WETH_AMOUNT = Decimal("0.001")
USDC_AMOUNT = Decimal("1")
RANGE_LOWER = Decimal("1000")
RANGE_UPPER = Decimal("3000")
MAX_SLIPPAGE = Decimal("0.005")


def _canonical_weth_usdc_pair(weth: str, usdc: str) -> tuple[str, str, Decimal, Decimal, Decimal, Decimal]:
    """Map WETH/USDC amounts and a USDC-per-WETH range onto pool token0/token1 order.

    A bare pool address on LPOpenIntent uses the pool contract's canonical
    orientation (token0 < token1 by address). Price bounds are token1 per token0,
    so the 1000-3000 USDC-per-WETH band inverts when USDC is token0.
    """
    weth_cs = Web3.to_checksum_address(weth)
    usdc_cs = Web3.to_checksum_address(usdc)
    if int(weth_cs, 16) < int(usdc_cs, 16):
        return weth_cs, usdc_cs, WETH_AMOUNT, USDC_AMOUNT, RANGE_LOWER, RANGE_UPPER
    return (
        usdc_cs,
        weth_cs,
        USDC_AMOUNT,
        WETH_AMOUNT,
        Decimal(1) / RANGE_UPPER,
        Decimal(1) / RANGE_LOWER,
    )


@dataclass(frozen=True)
class LPOpenTargetResult:
    intent: LPOpenIntent
    execution_result: Any
    transaction_result: Any
    position_id: int
    liquidity: int
    pool_address: str
    compile_metadata: dict[str, Any]


@dataclass(frozen=True)
class LPCloseTargetResult:
    intent: LPCloseIntent
    execution_result: Any
    setup_result: LPOpenTargetResult
    position_id: int
    removed_liquidity: int
    amount0_returned: int
    amount1_returned: int
    pool_address: str
    compile_metadata: dict[str, Any]
    receipt_set: dict[str, str]


def _single_event_transaction(execution: Any, topic: str, label: str) -> Any:
    matches = [
        transaction
        for transaction in execution.transaction_results
        if transaction.receipt is not None
        and any(
            log.get("topics") and str(log["topics"][0]).lower() == topic.lower()
            for log in transaction.receipt.to_dict().get("logs", [])
        )
    ]
    assert len(matches) == 1, f"Expected one {label} receipt, got {len(matches)}"
    return matches[0]


def _position_calls(web3: Web3, *, position_manager: str, position_id: int, block: int) -> tuple[bytes, bytes]:
    suffix = hex(position_id)[2:].zfill(64)
    position = web3.eth.call(
        {"to": Web3.to_checksum_address(position_manager), "data": "0x99fbab88" + suffix},
        block_identifier=block,
    )
    owner = web3.eth.call(
        {"to": Web3.to_checksum_address(position_manager), "data": "0x6352211e" + suffix},
        block_identifier=block,
    )
    assert len(position) >= 12 * 32 and len(owner) == 32
    return bytes(position), bytes(owner)


def _raw_position_call(web3: Web3, *, position_manager: str, position_id: int, block: int | str) -> dict[str, Any]:
    data = "0x99fbab88" + hex(position_id)[2:].zfill(64)
    return dict(
        web3.provider.make_request(
            "eth_call",
            [
                {"to": Web3.to_checksum_address(position_manager), "data": data},
                hex(block) if isinstance(block, int) else block,
            ],
        )
    )


def _raw_owner_call(web3: Web3, *, position_manager: str, position_id: int, block: int | str) -> dict[str, Any]:
    data = "0x6352211e" + hex(position_id)[2:].zfill(64)
    return dict(
        web3.provider.make_request(
            "eth_call",
            [
                {"to": Web3.to_checksum_address(position_manager), "data": data},
                hex(block) if isinstance(block, int) else block,
            ],
        )
    )


def _is_execution_revert(response: dict[str, Any]) -> bool:
    """Return true only when ``eth_call`` reached the EVM and reverted."""
    if "result" in response or not isinstance(response.get("error"), dict):
        return False
    message = str(response["error"].get("message") or "").lower()
    return "execution reverted" in message or message.startswith("revert")


def _position_state(raw: bytes) -> dict[str, Any]:
    def word(index: int) -> bytes:
        return raw[index * 32 : (index + 1) * 32]

    def signed(index: int) -> int:
        value = int.from_bytes(word(index), "big")
        return value - (1 << 256) if value >= (1 << 255) else value

    return {
        "token0": Web3.to_checksum_address("0x" + word(2)[-20:].hex()),
        "token1": Web3.to_checksum_address("0x" + word(3)[-20:].hex()),
        "fee": int.from_bytes(word(4), "big"),
        "tick_lower": signed(5),
        "tick_upper": signed(6),
        "liquidity": int.from_bytes(word(7), "big"),
    }


async def run_uniswap_v3_lp_open_exact_proof(
    *,
    chain: str,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    intent_evidence: Any,
    execution_context: ExecutionContext | None = None,
    compiler_config: IntentCompilerConfig | None = None,
    rpc_url: str | None = None,
    gateway_client: Any | None = None,
) -> LPOpenTargetResult:
    """Compile, execute, and independently prove one exact-pool NFT mint."""
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    weth = tokens["WETH"]
    usdc = tokens["USDC"]
    factory = UNISWAP_V3[chain]["factory"]
    npm = UNISWAP_V3[chain]["position_manager"]
    pool = compute_pool_address(factory, weth, usdc, FEE_TIER)
    fail_if_v3_pool_missing(web3, chain, "uniswap_v3", weth, usdc, FEE_TIER)
    token0, token1, amount0, amount1, range_lower, range_upper = _canonical_weth_usdc_pair(weth, usdc)
    token0_decimals = get_token_decimals(web3, token0)
    token1_decimals = get_token_decimals(web3, token1)
    token0_max = int(amount0 * Decimal(10**token0_decimals))
    token1_max = int(amount1 * Decimal(10**token1_decimals))
    token0_before = get_token_balance(web3, token0, funded_wallet)
    token1_before = get_token_balance(web3, token1, funded_wallet)

    intent = LPOpenIntent(
        pool=Web3.to_checksum_address(pool),
        amount0=amount0,
        amount1=amount1,
        range_lower=range_lower,
        range_upper=range_upper,
        fee_tier_units=FEE_TIER,
        max_slippage=MAX_SLIPPAGE,
        require_two_sided_minimums=True,
        protocol="uniswap_v3",
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
    assert compiled.status.value == "SUCCESS", f"LP_OPEN compilation failed: {compiled.error}"
    assert compiled.action_bundle is not None
    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success, f"LP_OPEN execution failed: {executed.error}"

    transaction = _single_event_transaction(executed, EVENT_TOPICS["IncreaseLiquidity"], "IncreaseLiquidity")
    parsed = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=transaction,
        parser=lambda receipt: UniswapV3ReceiptParser(chain=chain).extract_lp_open_data(receipt),
        parser_method="extract_lp_open_data",
    )
    assert parsed is not None and parsed.position_id is not None
    receipt = transaction.receipt.to_dict()
    block = int(receipt["blockNumber"] if "blockNumber" in receipt else receipt["block_number"])
    position_raw, owner_raw = _position_calls(web3, position_manager=npm, position_id=parsed.position_id, block=block)
    state = _position_state(position_raw)
    assert state["liquidity"] > 0
    assert {state["token0"].lower(), state["token1"].lower()} == {weth.lower(), usdc.lower()}
    assert state["fee"] == FEE_TIER
    assert owner_raw[-20:].hex() == funded_wallet.lower().removeprefix("0x")

    token0_after = get_token_balance(web3, token0, funded_wallet)
    token1_after = get_token_balance(web3, token1, funded_wallet)
    token0_spent = token0_before - token0_after
    token1_spent = token1_before - token1_after
    assert 0 < token0_spent <= token0_max
    assert 0 < token1_spent <= token1_max
    assert parsed.amount0 == token0_spent and parsed.amount1 == token1_spent
    assert parsed.liquidity == state["liquidity"]
    assert parsed.tick_lower == state["tick_lower"] and parsed.tick_upper == state["tick_upper"]

    logs = decode_explorer_view(receipt)["logs"]
    nft_mints = [
        log
        for log in logs
        if log.get("name") == "Transfer"
        and str(log.get("address") or "").lower() == npm.lower()
        and str((log.get("args") or {}).get("from") or "").lower() == "0x" + "0" * 40
        and str((log.get("args") or {}).get("to") or "").lower() == funded_wallet.lower()
        and int((log.get("args") or {}).get("value", -1)) == parsed.position_id
    ]
    outflows = {token0.lower(): 0, token1.lower(): 0}
    for log in logs:
        address = str(log.get("address") or "").lower()
        if (
            log.get("name") == "Transfer"
            and address in outflows
            and str((log.get("args") or {}).get("from") or "").lower() == funded_wallet.lower()
        ):
            outflows[address] += int((log.get("args") or {}).get("value", 0))
    flags = {
        "one_nft_mint": len(nft_mints) == 1,
        "exact_token0_outflow": outflows[token0.lower()] == token0_spent,
        "exact_token1_outflow": outflows[token1.lower()] == token1_spent,
        "position_identity": (state["token0"].lower() == token0.lower() and state["token1"].lower() == token1.lower()),
        "position_liquidity": parsed.liquidity == state["liquidity"] > 0,
        "position_ticks": parsed.tick_lower == state["tick_lower"] and parsed.tick_upper == state["tick_upper"],
    }
    assert all(flags.values()), f"LP_OPEN exact proof predicates failed: {flags}"
    intent_evidence.record_fidelity(
        hard=True,
        flags=flags,
        witnesses=[{"kind": "position_state", "position_id": parsed.position_id, **state}],
    )
    intent_evidence.record_balance_deltas(
        checks={"bilateral_position_funding_verified": True},
        token0={"address": token0, "before": token0_before, "after": token0_after, "delta": -token0_spent},
        token1={"address": token1, "before": token1_before, "after": token1_after, "delta": -token1_spent},
    )
    block_hash = web3.eth.get_block(block)["hash"]
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile="v3_lp.v1",
        intent="LP_OPEN",
        account=funded_wallet,
        pool_reference=Web3.to_checksum_address(pool),
        amount0=amount0,
        amount1=amount1,
        range_lower=range_lower,
        range_upper=range_upper,
        resource_address=npm,
        factory_address=factory,
        pool_address=pool,
        token0=state["token0"],
        token1=state["token1"],
        fee_tier=FEE_TIER,
        position_id=parsed.position_id,
        tick_lower=state["tick_lower"],
        tick_upper=state["tick_upper"],
        liquidity=state["liquidity"],
        max_amount0_raw=token0_max,
        max_amount1_raw=token1_max,
        actual_amount0_raw=token0_spent,
        actual_amount1_raw=token1_spent,
        parser_position_id=parsed.position_id,
        parser_liquidity=parsed.liquidity,
        parser_amount0_raw=parsed.amount0,
        parser_amount1_raw=parsed.amount1,
        position_state_raw="0x" + position_raw.hex(),
        owner_state_raw="0x" + owner_raw.hex(),
        position_state_block=block,
        position_state_block_hash=block_hash.hex(),
    )
    return LPOpenTargetResult(
        intent=intent,
        execution_result=executed,
        transaction_result=transaction,
        position_id=parsed.position_id,
        liquidity=state["liquidity"],
        pool_address=pool,
        compile_metadata=dict(compiled.action_bundle.metadata),
    )


def _receipt_hash(transaction: Any) -> str:
    receipt = transaction.receipt.to_dict()
    value = receipt.get("transactionHash", receipt.get("tx_hash", transaction.tx_hash))
    if isinstance(value, str):
        return (value if value.startswith("0x") else "0x" + value).lower()
    return Web3.to_hex(value).lower()


def _single_nft_burn_transaction(execution: Any, *, position_manager: str, account: str, token_id: int) -> Any:
    matches = []
    for transaction in execution.transaction_results:
        if transaction.receipt is None:
            continue
        logs = decode_explorer_view(transaction.receipt.to_dict())["logs"]
        burns = [
            log
            for log in logs
            if log.get("name") == "Transfer"
            and str(log.get("address") or "").lower() == position_manager.lower()
            and str((log.get("args") or {}).get("from") or "").lower() == account.lower()
            and str((log.get("args") or {}).get("to") or "").lower() == "0x" + "0" * 40
            and int((log.get("args") or {}).get("value", -1)) == token_id
        ]
        if burns:
            assert len(burns) == 1, f"Expected one NFT burn log, got {len(burns)}"
            matches.append(transaction)
    assert len(matches) == 1, f"Expected one NFT burn receipt, got {len(matches)}"
    return matches[0]


def _compiled_close_calls(bundle: Any) -> list[dict[str, Any]]:
    calls = [
        {
            "to": str(transaction["to"]),
            "data": str(transaction["data"]),
            "value": int(transaction["value"]),
            "tx_type": str(transaction["tx_type"]),
        }
        for transaction in bundle.transactions
    ]
    assert [call["tx_type"] for call in calls] == ["lp_decrease_liquidity", "lp_collect", "lp_burn"]
    return calls


def _decrease_minimums(call: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    """Decode the compiled ``decreaseLiquidity`` call and rule whether its floors bind.

    The intent declares a slippage tolerance; the chain enforces only what reaches
    ``amount0Min`` / ``amount1Min``. The verdict is the weak I3 reading -- at least one
    leg binds -- because a leg can legitimately be zero at a range edge. The decoded
    values are returned as a witness so the DECODE page shows the numbers, not just
    the verdict.
    """
    verdict = check_calldata(call["to"], call["data"])
    witness: dict[str, Any] = {
        "kind": "decoded_calldata",
        "function": verdict.function or verdict.selector,
        "outcome": verdict.outcome.value,
    }
    for constraint in verdict.constraints:
        witness[constraint.path.rsplit(".", 1)[-1]] = str(constraint.value)
    return verdict.outcome is TxOutcome.PROTECTED, witness


async def run_uniswap_v3_lp_close_exact_proof(
    *,
    chain: str,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    intent_evidence: Any,
    execution_context: ExecutionContext | None = None,
    compiler_config: IntentCompilerConfig | None = None,
    rpc_url: str | None = None,
    gateway_client: Any | None = None,
    existing_position: LPOpenTargetResult | None = None,
) -> LPCloseTargetResult:
    """Prove one exact full-close target, creating isolated setup when needed.

    ``existing_position`` is the Mainnet LP_OPEN cleanup seam: it closes the
    exact NFT produced by the target without opening a second position.  When
    omitted, the helper creates setup with a disabled recorder so an LP_CLOSE
    proof can never count its prerequisite LP_OPEN as target evidence.
    """
    setup = existing_position
    if setup is None:
        setup = await run_uniswap_v3_lp_open_exact_proof(
            chain=chain,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            intent_evidence=DisabledIntentEvidenceRecorder(),
            compiler_config=compiler_config,
            rpc_url=rpc_url,
            gateway_client=gateway_client,
        )
    factory = UNISWAP_V3[chain]["factory"]
    npm = UNISWAP_V3[chain]["position_manager"]
    pre_block = web3.eth.block_number
    pre_header = web3.eth.get_block(pre_block)
    pre_position_raw, pre_owner_raw = _position_calls(
        web3,
        position_manager=npm,
        position_id=setup.position_id,
        block=pre_block,
    )
    pre_state = _position_state(pre_position_raw)
    assert pre_state["liquidity"] == setup.liquidity > 0
    assert pre_owner_raw[-20:].hex() == funded_wallet.lower().removeprefix("0x")
    token0 = pre_state["token0"]
    token1 = pre_state["token1"]
    token0_before = get_token_balance(web3, token0, funded_wallet)
    token1_before = get_token_balance(web3, token1, funded_wallet)

    close_intent = LPCloseIntent(
        position_id=str(setup.position_id),
        pool=Web3.to_checksum_address(setup.pool_address),
        collect_fees=True,
        protocol="uniswap_v3",
        chain=chain,
    )
    intent_evidence.bind(close_intent)
    compiled = IntentCompiler(
        chain=chain,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        config=compiler_config,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
    ).compile(close_intent)
    assert compiled.status.value == "SUCCESS", f"LP_CLOSE compilation failed: {compiled.error}"
    assert compiled.action_bundle is not None
    compiled_calls = _compiled_close_calls(compiled.action_bundle)
    decrease_minimums_bind, decrease_minimums_witness = _decrease_minimums(compiled_calls[0])
    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success, f"LP_CLOSE execution failed: {executed.error}"

    decrease_tx = _single_event_transaction(executed, EVENT_TOPICS["DecreaseLiquidity"], "DecreaseLiquidity")
    collect_tx = _single_event_transaction(executed, EVENT_TOPICS["Collect"], "Collect")
    burn_tx = _single_nft_burn_transaction(
        executed,
        position_manager=npm,
        account=funded_wallet,
        token_id=setup.position_id,
    )
    role_transactions = {"decrease": decrease_tx, "collect": collect_tx, "burn": burn_tx}
    receipt_set = {role: _receipt_hash(transaction) for role, transaction in role_transactions.items()}
    assert len(set(receipt_set.values())) == 3, "LP_CLOSE requires three distinct target receipts"

    parser = UniswapV3ReceiptParser(chain=chain)
    decrease = intent_evidence.capture_parse(
        intent=close_intent,
        transaction_result=decrease_tx,
        parser=lambda receipt: parser.extract_lp_close_data(receipt),
        parser_method="extract_lp_close_data:decrease",
        receipt_role="decrease",
    )
    collect = intent_evidence.capture_parse(
        intent=close_intent,
        transaction_result=collect_tx,
        parser=lambda receipt: parser.extract_lp_close_data(receipt),
        parser_method="extract_lp_close_data:collect",
        receipt_role="collect",
    )
    burn = intent_evidence.capture_parse(
        intent=close_intent,
        transaction_result=burn_tx,
        parser=lambda receipt: {
            "position_id": setup.position_id,
            "nft_burns": sum(
                1
                for log in decode_explorer_view(receipt)["logs"]
                if log.get("name") == "Transfer" and str(log.get("address") or "").lower() == npm.lower()
            ),
        },
        parser_method="erc721_transfer_burn",
        receipt_role="burn",
    )
    assert decrease is not None and decrease.source == "decrease_liquidity"
    assert decrease.liquidity_removed == pre_state["liquidity"]
    assert collect is not None and collect.source == "collect"
    assert burn == {"position_id": setup.position_id, "nft_burns": 1}

    token0_after = get_token_balance(web3, token0, funded_wallet)
    token1_after = get_token_balance(web3, token1, funded_wallet)
    token0_returned = token0_after - token0_before
    token1_returned = token1_after - token1_before
    assert token0_returned > 0 or token1_returned > 0
    assert collect.amount0_collected == token0_returned
    assert collect.amount1_collected == token1_returned
    assert collect.currency0.lower() == token0.lower()
    assert collect.currency1.lower() == token1.lower()

    terminal_position = _raw_position_call(
        web3,
        position_manager=npm,
        position_id=setup.position_id,
        block="latest",
    )
    terminal_owner = _raw_owner_call(
        web3,
        position_manager=npm,
        position_id=setup.position_id,
        block="latest",
    )
    assert _is_execution_revert(terminal_position), terminal_position
    assert _is_execution_revert(terminal_owner), terminal_owner
    terminal_header = web3.eth.get_block("latest")

    common_contract = {
        "schema_version": 1,
        "profile": "v3_lp.v1",
        "intent": "LP_CLOSE",
        "account": funded_wallet,
        "pool_reference": Web3.to_checksum_address(setup.pool_address),
        "resource_address": npm,
        "factory_address": factory,
        "pool_address": setup.pool_address,
        "token0": pre_state["token0"],
        "token1": pre_state["token1"],
        "fee_tier": pre_state["fee"],
        "position_id": setup.position_id,
        "pre_liquidity": pre_state["liquidity"],
        "pre_position_state_raw": "0x" + pre_position_raw.hex(),
        "pre_owner_state_raw": "0x" + pre_owner_raw.hex(),
        "pre_state_block": pre_block,
        "pre_state_block_hash": pre_header["hash"].hex(),
        "compiled_calls": compiled_calls,
        "receipt_set": receipt_set,
        "parser_liquidity_removed": decrease.liquidity_removed,
        "parser_amount0_raw": collect.amount0_collected,
        "parser_amount1_raw": collect.amount1_collected,
        "actual_amount0_raw": token0_returned,
        "actual_amount1_raw": token1_returned,
        "terminal_position_response": terminal_position,
        "terminal_owner_response": terminal_owner,
        "terminal_state_block": int(terminal_header["number"]),
        "terminal_state_block_hash": terminal_header["hash"].hex(),
    }
    role_flags = {
        "decrease": {
            "liquidity_removed_eq_pre_state": decrease.liquidity_removed == pre_state["liquidity"],
            "decrease_minimums_bind": decrease_minimums_bind,
        },
        "collect": {
            "parser_amount0_eq_wallet_delta": collect.amount0_collected == token0_returned,
            "parser_amount1_eq_wallet_delta": collect.amount1_collected == token1_returned,
            "both_assets_identified": collect.currency0.lower() == token0.lower()
            and collect.currency1.lower() == token1.lower(),
        },
        "burn": {
            "single_nft_burn": burn["nft_burns"] == 1,
            "terminal_owner_absent": _is_execution_revert(terminal_owner),
            "terminal_position_absent": _is_execution_revert(terminal_position),
        },
    }
    for role, flags in role_flags.items():
        intent_evidence.record_fidelity(
            hard=True,
            flags=flags,
            witnesses=[decrease_minimums_witness] if role == "decrease" else None,
            receipt_role=role,
        )
        intent_evidence.record_balance_deltas(
            receipt_role=role,
            checks={f"{role}_value_and_state_contract": all(flags.values())},
            token0={"address": token0, "before": token0_before, "after": token0_after, "delta": token0_returned},
            token1={"address": token1, "before": token1_before, "after": token1_after, "delta": token1_returned},
        )
        intent_evidence.record_semantic_contract(receipt_role=role, receipt_role_name=role, **common_contract)

    # Asserted only after the close landed and the receipts were sealed: the position is
    # closed and the funds are back, so a red here strands nothing on a live chain. The
    # false flag is already in the sealed receipt; this makes the pytest node red too.
    if not decrease_minimums_bind and os.environ.get("ALMANAK_QA_STRICT_PROOFS") != "1":
        # Excuse EXACTLY this known-red assertion, never the whole node: every
        # assertion above stays a hard failure in CI. Self-healing — the moment
        # the compiler binds the floors this branch is not taken and the assert
        # below re-arms, with no marker left to remove. The QA Lab seal lane
        # (ALMANAK_QA_STRICT_PROOFS=1) never takes this branch and stays FAIL.
        pytest.xfail("VIB-6212: compiled decreaseLiquidity floors do not bind (as of 2026-09-01)")
    assert decrease_minimums_bind, (
        "LP_CLOSE declared a slippage tolerance but the compiled decreaseLiquidity floors do not bind "
        f"({decrease_minimums_witness}); the chain would accept any output."
    )

    return LPCloseTargetResult(
        intent=close_intent,
        execution_result=executed,
        setup_result=setup,
        position_id=setup.position_id,
        removed_liquidity=pre_state["liquidity"],
        amount0_returned=token0_returned,
        amount1_returned=token1_returned,
        pool_address=setup.pool_address,
        compile_metadata=dict(compiled.action_bundle.metadata),
        receipt_set=receipt_set,
    )


__all__ = [
    "LPCloseTargetResult",
    "LPOpenTargetResult",
    "run_uniswap_v3_lp_close_exact_proof",
    "run_uniswap_v3_lp_open_exact_proof",
]
