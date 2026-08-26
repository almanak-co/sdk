"""Atomic, sealable Aerodrome classic (Solidly) LP proofs.

Classic Aerodrome is a Solidly fork: fungible LP tokens, no NFT, no tick
bracket. The exact-proof contract is therefore built on the pool's ERC-20 LP
balance rather than a position NFT, and on the router's ``addLiquidity``
minimum-amount floors rather than a concentrated-liquidity range.

The slippage floor is part of the proof on purpose. ``addLiquidity`` takes
``amountAMin``/``amountBMin``; submitting zeros makes an LP mint executable at
any effective price, which is a sandwich-exposed money path (ALM-3367). The
floor is read back out of the compiled calldata rather than trusted from the
intent, because the intent's ``max_slippage`` is an input and the calldata is
what the chain will actually enforce.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.aerodrome.addresses import AERODROME
from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser
from almanak.connectors.aerodrome.sdk import AerodromeSDK
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPOpenIntent
from almanak.framework.intents.compiler_models import IntentCompilerConfig
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals

# addLiquidity(address,address,bool,uint256,uint256,uint256,uint256,address,uint256)
ADD_LIQUIDITY_SELECTOR = "0x5a47ddc3"

POOL_LABEL = "USDC/WETH/volatile"
STABLE = False

# ~$10 a side. Large enough to clear Solidly's MINIMUM_LIQUIDITY guard, small
# enough that price impact stays negligible against a mainnet-fork reserve.
LP_AMOUNT_USDC = Decimal("10")
LP_AMOUNT_WETH = Decimal("0.005")

# Solidly LP has no price range; LPOpenIntent's validator still demands bounds.
RANGE_LOWER = Decimal("1")
RANGE_UPPER = Decimal("1000000")

# 100 bps — the cap the ALM-3367 reporter asked for and did not receive.
MAX_SLIPPAGE = Decimal("0.01")


@dataclass
class AerodromeLPOpenTargetResult:
    intent: LPOpenIntent
    execution_result: Any
    transaction_result: Any
    pool_address: str
    lp_minted: int
    amount_a_min: int
    amount_b_min: int


def _decode_add_liquidity(action_bundle: Any) -> dict[str, int]:
    """Recover the router's declared minimums from the compiled calldata.

    Returns the four amount words. ``addLiquidity`` lays its arguments out as
    ``tokenA, tokenB, stable, amountADesired, amountBDesired, amountAMin,
    amountBMin, to, deadline`` — nine 32-byte words after the selector.
    """
    calldata: bytes | None = None
    for raw in action_bundle.transactions:
        data = raw.get("data", "") if isinstance(raw, dict) else getattr(raw, "data", "")
        if not data:
            continue
        text = data if isinstance(data, str) else "0x" + bytes(data).hex()
        if not text.startswith("0x"):
            text = "0x" + text
        if text[:10].lower() == ADD_LIQUIDITY_SELECTOR:
            calldata = bytes.fromhex(text[10:])
            break
    if calldata is None:
        raise AssertionError("no addLiquidity() transaction in the compiled LP_OPEN bundle")
    if len(calldata) < 9 * 32:
        raise AssertionError(f"addLiquidity calldata is truncated: {len(calldata)} bytes")

    def word(index: int) -> int:
        return int.from_bytes(calldata[index * 32 : (index + 1) * 32], "big")

    return {
        "amount_a_desired": word(3),
        "amount_b_desired": word(4),
        "amount_a_min": word(5),
        "amount_b_min": word(6),
    }


def _resolve_pool_address(web3: Web3, chain: str, rpc_url: str) -> str:
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    sdk = AerodromeSDK(chain=chain, rpc_url=rpc_url)
    pool_address = sdk.get_pool_address_from_factory(tokens["USDC"], tokens["WETH"], STABLE, web3=web3)
    if not pool_address:
        pytest.fail(
            f"Aerodrome USDC/WETH volatile pool not found on {chain} via factory. "
            "Either the factory returned address(0) or the RPC is unreachable."
        )
    return Web3.to_checksum_address(pool_address)


async def run_aerodrome_lp_open_exact_proof(
    *,
    chain: str,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    intent_evidence: Any,
    rpc_url: str,
    execution_context: ExecutionContext | None = None,
    compiler_config: IntentCompilerConfig | None = None,
) -> AerodromeLPOpenTargetResult:
    """Compile, execute, and independently prove one classic-Aerodrome LP mint.

    SCOPE OF THE CLAIM. This node proves the floor is PRESENT and non-zero. It
    does NOT prove the floor tracks the caller's declared tolerance — a floor
    derived from a hard-coded constant passes this node. That property is
    I3 parameter fidelity, which owns its own responsiveness probe
    (``docs/internal/qa-invariants/I3-parameter-fidelity.md``); duplicating it
    here would be a node inventing its own coverage, and a second compile also
    perturbs the permission-attestation harness that wraps ``compile()``.
    """
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    usdc = tokens["USDC"]
    weth = tokens["WETH"]
    router = AERODROME[chain]["router"]
    pool_address = _resolve_pool_address(web3, chain, rpc_url)

    usdc_decimals = get_token_decimals(web3, usdc)
    weth_decimals = get_token_decimals(web3, weth)
    usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
    weth_max = int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))

    usdc_before = get_token_balance(web3, usdc, funded_wallet)
    weth_before = get_token_balance(web3, weth, funded_wallet)
    lp_before = get_token_balance(web3, pool_address, funded_wallet)

    intent = LPOpenIntent(
        pool=POOL_LABEL,
        amount0=LP_AMOUNT_USDC,
        amount1=LP_AMOUNT_WETH,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        max_slippage=MAX_SLIPPAGE,
        protocol="aerodrome",
        chain=chain,
    )
    intent_evidence.bind(intent)
    compiled = IntentCompiler(
        chain=chain,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        config=compiler_config,
        rpc_url=rpc_url,
    ).compile(intent)
    assert compiled.status.value == "SUCCESS", f"Aerodrome LP_OPEN compilation failed: {compiled.error}"
    assert compiled.action_bundle is not None

    amounts = _decode_add_liquidity(compiled.action_bundle)

    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success, f"Aerodrome LP_OPEN execution failed: {executed.error}"

    transaction = _add_liquidity_transaction(executed, router)
    # Classic Aerodrome is Solidly: there is no structured LP_OPEN extraction
    # and no ``extract_lp_open_data`` on this parser — that method belongs to
    # the Slipstream (concentrated-liquidity) parser. The classic open surface
    # is ``parse_receipt`` -> ``ParseResult.liquidity_result``.
    parsed = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=transaction,
        parser=lambda receipt: AerodromeReceiptParser(chain=chain).parse_receipt(receipt),
        parser_method="parse_receipt",
    )
    assert parsed is not None and parsed.success, (
        f"Aerodrome LP_OPEN receipt did not parse: {getattr(parsed, 'error', None)}"
    )
    liquidity = parsed.liquidity_result
    assert liquidity is not None, "classic Aerodrome LP_OPEN receipt carried no liquidity_result"

    usdc_after = get_token_balance(web3, usdc, funded_wallet)
    weth_after = get_token_balance(web3, weth, funded_wallet)
    lp_after = get_token_balance(web3, pool_address, funded_wallet)
    usdc_spent = usdc_before - usdc_after
    weth_spent = weth_before - weth_after
    lp_minted = lp_after - lp_before

    # The classic parser leaves token identity EMPTY on a successful LP_OPEN
    # parse (``token0``/``token1``/symbols are ``""`` and the ``*_decimal``
    # fields are a fabricated "0" while the raw amounts are correct — VIB
    # follow-up filed separately). So layer 3 cannot be joined on token
    # address here. Compare the amount multiset instead, which is still an
    # independent check that the logs and the wallet agree, and record the
    # missing identity rather than asserting on it: this node's claim is the
    # slippage floor, not the parser's identity contract.
    parser_identity_present = bool(str(liquidity.token0)) and bool(str(liquidity.token1))

    # The pool sorts its pair by address, and both the Mint event and the
    # parser report amounts in that order. Everything the seal-time
    # re-derivation checks is recorded in pool-canonical order; the request
    # decimals stay in label order so they still match source_request.
    (c_token0, c_actual0, c_max0), (c_token1, c_actual1, c_max1) = sorted(
        ((usdc, usdc_spent, usdc_max), (weth, weth_spent, weth_max)),
        key=lambda row: int(row[0], 16),
    )
    parser_amount0 = int(liquidity.amount0)
    parser_amount1 = int(liquidity.amount1)
    floors_enforced = amounts["amount_a_min"] > 0 and amounts["amount_b_min"] > 0
    flags = {
        # Solidly rebalances to the pool ratio and refunds the excess, so the
        # spend is bounded by the request rather than equal to it.
        "usdc_outflow_bounded": 0 < usdc_spent <= usdc_max,
        "weth_outflow_bounded": 0 < weth_spent <= weth_max,
        "lp_tokens_minted": lp_minted > 0,
        "parsed_operation_is_add": liquidity.operation == "add",
        "parsed_pool_identity": str(liquidity.pool_address).lower() == pool_address.lower(),
        # Layer 3 against layer 4: what the parser read out of the logs must
        # equal what the wallet actually paid, token for token.
        "parsed_amounts_match_wallet": (parser_amount0, parser_amount1) == (c_actual0, c_actual1),
    }

    # Record every layer BEFORE asserting anything below. The seal needs runtime
    # intent claims in the manifest; a bare assertion above this point yields
    # zero claims and the seal is refused as unsealed instead of recorded as a
    # red, which hides a product failure rather than painting it. Only a
    # genuinely unparseable receipt may abort earlier. Do not move these asserts
    # up.
    intent_evidence.record_fidelity(
        hard=True,
        flags=flags,
        witnesses=[
            {
                "kind": "add_liquidity_calldata",
                "amount_a_desired": amounts["amount_a_desired"],
                "amount_b_desired": amounts["amount_b_desired"],
                "amount_a_min": amounts["amount_a_min"],
                "amount_b_min": amounts["amount_b_min"],
            }
        ],
    )
    intent_evidence.record_balance_deltas(
        checks={"bilateral_position_funding_verified": True, "lp_token_minted": lp_minted > 0},
        token0={"address": usdc, "before": usdc_before, "after": usdc_after, "delta": -usdc_spent},
        token1={"address": weth, "before": weth_before, "after": weth_after, "delta": -weth_spent},
    )
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile="solidly_lp.v1",
        intent="LP_OPEN",
        account=funded_wallet,
        pool_reference=POOL_LABEL,
        pool_address=pool_address,
        resource_address=router,
        token0=c_token0,
        token1=c_token1,
        stable=STABLE,
        amount0=LP_AMOUNT_USDC,
        amount1=LP_AMOUNT_WETH,
        max_amount0_raw=c_max0,
        max_amount1_raw=c_max1,
        actual_amount0_raw=c_actual0,
        actual_amount1_raw=c_actual1,
        parser_amount0_raw=parser_amount0,
        parser_amount1_raw=parser_amount1,
        lp_tokens_minted=lp_minted,
        parser_identity_present=parser_identity_present,
        # The money-path property under proof.
        max_slippage=MAX_SLIPPAGE,
        amount_a_desired_raw=amounts["amount_a_desired"],
        amount_b_desired_raw=amounts["amount_b_desired"],
        amount_a_min_raw=amounts["amount_a_min"],
        amount_b_min_raw=amounts["amount_b_min"],
        slippage_floor_enforced=floors_enforced,
    )

    assert all(flags.values()), f"Aerodrome LP_OPEN exact proof predicates failed: {flags}"

    # ALM-3367: a mint submitted with zero minimums executes at any effective
    # price. The intent asked for a 100-bps cap; the calldata must carry it.
    assert floors_enforced, (
        f"Aerodrome LP_OPEN submitted unfloored minimums with max_slippage={MAX_SLIPPAGE}: "
        f"amountAMin={amounts['amount_a_min']}, amountBMin={amounts['amount_b_min']} "
        f"(desired {amounts['amount_a_desired']}/{amounts['amount_b_desired']}). "
        "The mint is executable at any price and is sandwich-exposed."
    )

    return AerodromeLPOpenTargetResult(
        intent=intent,
        execution_result=executed,
        transaction_result=transaction,
        pool_address=pool_address,
        lp_minted=lp_minted,
        amount_a_min=amounts["amount_a_min"],
        amount_b_min=amounts["amount_b_min"],
    )


def _add_liquidity_transaction(executed: Any, router: str) -> Any:
    """Return the single router transaction that carried the mint."""
    candidates = [
        transaction
        for transaction in executed.transaction_results
        if str(getattr(transaction, "to", "") or "").lower() == router.lower()
    ]
    if not candidates:
        # Approvals share the bundle; fall back to the last successful leg,
        # which is the mint by construction of the compiled plan.
        candidates = [transaction for transaction in executed.transaction_results if transaction.receipt is not None]
    assert len(candidates) >= 1, "no router transaction found in the LP_OPEN execution result"
    return candidates[-1]
