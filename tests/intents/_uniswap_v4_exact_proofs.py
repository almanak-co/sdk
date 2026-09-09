"""Chain-neutral atomic evidence scenarios for Uniswap V4 Swap Intents.

Uniswap V4 holds every pool in one PoolManager, so a pool is identified by the
keccak of its PoolKey rather than by a deployed address. The connector derives
tick spacing from a welded fee->spacing map, which is an assumption inherited
from V3 rather than a fact about the pool; this helper therefore refuses to
proceed unless the derived key names a pool that is actually initialised
on-chain, so a swap can never be proven against a guessed identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from web3 import Web3

from almanak.connectors._strategy_base.v4_pool_abi import compute_v4_pool_id
from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.receipt_parser import SWAP_EVENT_TOPIC, UniswapV4ReceiptParser
from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, PERMIT2_ADDRESS, UniswapV4SDK
from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.compiler_models import IntentCompilerConfig
from tests.intents.conftest import CHAIN_CONFIGS, SWAP_MAX_SLIPPAGE, get_token_balance, get_token_decimals

FEE_TIER = 3000
SWAP_AMOUNT = Decimal("10")

# The pool currencies are exactly the wallet's two ERC-20 assets.
V4_SWAP_PROFILE = "v4_swap.v1"
# The pool pays the native currency and the wallet is credited the wrapper.
V4_SWAP_ROUTE_PROFILE = "v4_swap_route.v1"


@dataclass(frozen=True)
class V4SwapTargetResult:
    intent: SwapIntent
    execution_result: Any
    transaction_result: Any
    amount_in_raw: int
    amount_out_raw: int
    pool_id: str


def _swap_transaction(execution_result: Any, *, pool_manager: str) -> tuple[Any, list[str]]:
    """Return the receipt carrying the PoolManager's Swap event, and the pools it names.

    Selection is deliberately on the singleton emitter alone, never on the pool
    id the caller expected. Filtering by the expected id would turn a routing
    disagreement into a missing receipt, and the cell would paint HARNESS_FAIL
    instead of handing the sealer the evidence that the pools differ.
    """
    matches: list[tuple[Any, list[str]]] = []
    for transaction in execution_result.transaction_results:
        if transaction.receipt is None:
            continue
        emitted = []
        for log in transaction.receipt.to_dict().get("logs", []):
            topics = log.get("topics") or []
            if len(topics) < 2:
                continue
            first = topics[0] if isinstance(topics[0], str) else Web3.to_hex(topics[0])
            second = topics[1] if isinstance(topics[1], str) else Web3.to_hex(topics[1])
            if (
                str(log.get("address") or "").lower() == pool_manager.lower()
                and first.lower() == SWAP_EVENT_TOPIC.lower()
            ):
                emitted.append(second.lower())
        if emitted:
            matches.append((transaction, emitted))
    assert len(matches) == 1, f"Expected one Swap-emitting target receipt from {pool_manager}, got {len(matches)}"
    return matches[0]


def expected_pool_key(
    sdk: UniswapV4SDK,
    *,
    chain: str,
    token_in: str,
    token_out: str,
    fee_tier: int,
    profile: str,
) -> PoolKey:
    """Return the PoolKey the declared contract requires the swap to settle in.

    The contract is declared by the cell, never inferred from the route the run
    happened to take: a node that chose its own profile from its own observation
    could silently drop the conversion proof the moment the connector stopped
    converting, and nothing downstream would notice the coverage had gone.

    ``v4_swap.v1`` names the pool of the two requested assets. ``v4_swap_route.v1``
    names the pool reached by substituting the chain's native currency for the
    wrapped-native leg, and has no meaning for a pair that holds no wrapper.
    """
    if profile == V4_SWAP_PROFILE:
        return sdk.compute_pool_key(token_in, token_out, fee=fee_tier)
    if profile != V4_SWAP_ROUTE_PROFILE:
        raise ValueError(f"Unsupported V4 swap contract profile: {profile!r}")
    wrapper = WRAPPED_NATIVE[chain].lower()
    legs = [NATIVE_CURRENCY if token.lower() == wrapper else token for token in (token_in, token_out)]
    if legs == [token_in, token_out]:
        raise ValueError(
            f"{V4_SWAP_ROUTE_PROFILE} requires one leg to be {chain}'s wrapped native token "
            f"{wrapper}; {token_in} -> {token_out} converts nothing"
        )
    return sdk.compute_pool_key(legs[0], legs[1], fee=fee_tier)


async def run_uniswap_v4_swap_exact_proof(
    *,
    chain: str,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    intent_evidence: Any,
    rpc_url: str,
    profile: str,
    amount: Decimal = SWAP_AMOUNT,
    fee_tier: int = FEE_TIER,
    execution_context: ExecutionContext | None = None,
    compiler_config: IntentCompilerConfig | None = None,
    gateway_client: Any | None = None,
    max_slippage: Decimal = SWAP_MAX_SLIPPAGE,
    from_symbol: str = "USDC",
    to_symbol: str = "WETH",
) -> V4SwapTargetResult:
    """Prove one exact-pool V4 swap through receipt, pool identity, and bilateral state."""
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    token_in = tokens[from_symbol]
    token_out = tokens[to_symbol]
    pool_manager = UNISWAP_V4[chain]["pool_manager"]

    sdk = UniswapV4SDK(chain=chain, rpc_url=rpc_url)
    pool_key = expected_pool_key(
        sdk, chain=chain, token_in=token_in, token_out=token_out, fee_tier=fee_tier, profile=profile
    )
    sqrt_price = sdk.get_pool_sqrt_price(pool_key, rpc_url=rpc_url)
    assert sqrt_price, (
        f"No initialised V4 pool for {from_symbol}/{to_symbol} fee {fee_tier} "
        f"tick_spacing {pool_key.tick_spacing} hooks {pool_key.hooks} on {chain}: "
        "the connector's resolved PoolKey does not name a live pool"
    )
    pool_id = compute_v4_pool_id(
        pool_key.currency0, pool_key.currency1, pool_key.fee, pool_key.tick_spacing, pool_key.hooks
    )

    input_decimals = get_token_decimals(web3, token_in)
    requested_raw = int(amount * Decimal(10**input_decimals))
    input_before = get_token_balance(web3, token_in, funded_wallet)
    output_before = get_token_balance(web3, token_out, funded_wallet)

    intent = SwapIntent(
        from_token=token_in,
        to_token=token_out,
        amount=amount,
        max_slippage=max_slippage,
        protocol="uniswap_v4",
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
    assert compiled.status.value == "SUCCESS", f"V4 SWAP compilation failed: {compiled.error}"
    assert compiled.action_bundle is not None
    # V4 bundle entries carry a free-text description rather than V3's tx_type,
    # and encode value as a string.
    compiled_calls = [
        {
            "to": str(transaction["to"]),
            "data": str(transaction["data"]),
            "value": int(transaction["value"]),
            "description": str(transaction.get("description") or ""),
        }
        for transaction in compiled.action_bundle.transactions
    ]
    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success, f"V4 SWAP execution failed: {executed.error}"

    transaction, emitted_pool_ids = _swap_transaction(executed, pool_manager=pool_manager)
    parsed = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=transaction,
        parser=lambda receipt: UniswapV4ReceiptParser(chain=chain).parse_receipt(receipt),
    )
    assert parsed.swap_result is not None, f"V4 receipt parsing produced no swap result: {parsed.error}"
    result = parsed.swap_result

    input_after = get_token_balance(web3, token_in, funded_wallet)
    output_after = get_token_balance(web3, token_out, funded_wallet)
    input_spent = input_before - input_after
    output_received = output_after - output_before

    flags = {
        "input_spent_matches_request": input_spent == requested_raw,
        "output_received_is_positive": output_received > 0,
        "parser_input_matches_request": int(result.amount_in) == requested_raw,
        "parser_output_matches_wallet_inflow": int(result.amount_out) == output_received,
        "pool_id_matches_resolved_pool_key": emitted_pool_ids == [pool_id.lower()],
    }

    intent_evidence.record_fidelity(
        hard=True,
        flags=flags,
        witnesses=[
            {"kind": "wallet_balance_delta", "token": token_in, "amount_raw": -input_spent},
            {"kind": "wallet_balance_delta", "token": token_out, "amount_raw": output_received},
        ],
        notes=[],
    )
    intent_evidence.record_balance_deltas(
        checks={
            "input_spent_matches_request": input_spent == requested_raw,
            "output_received_is_positive": output_received > 0,
        },
        asset={
            "address": token_in,
            "symbol": from_symbol,
            "before": input_before,
            "after": input_after,
            "delta": -input_spent,
        },
    )
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile=profile,
        intent="SWAP",
        account=funded_wallet,
        asset_address=token_in,
        asset_decimals=input_decimals,
        output_asset_address=token_out,
        output_asset_decimals=get_token_decimals(web3, token_out),
        resource_address=pool_manager,
        wrapper_address=WRAPPED_NATIVE[chain],
        permit2_address=PERMIT2_ADDRESS,
        compiled_calls=compiled_calls,
        pool_id=pool_id,
        currency0=pool_key.currency0,
        currency1=pool_key.currency1,
        fee_tier=pool_key.fee,
        tick_spacing=pool_key.tick_spacing,
        hooks=pool_key.hooks,
        requested_amount_raw=requested_raw,
        parser_amount_raw=int(result.amount_in),
        parser_output_amount_raw=int(result.amount_out),
        wallet_before_raw=input_before,
        wallet_after_raw=input_after,
        output_wallet_before_raw=output_before,
        output_wallet_after_raw=output_after,
    )
    # Asserted only AFTER the contract is recorded: raising before the record
    # would withhold the receipt and the run would be judged on missing evidence
    # rather than on what the swap did. On a converting route the parser books
    # the pool's native payout while the wallet keeps what the router forwarded,
    # so those two legitimately differ by the amount a leaking router strands;
    # deciding whether that gap is acceptable belongs to the sealer.
    hard = dict(flags)
    if profile == V4_SWAP_ROUTE_PROFILE:
        hard.pop("parser_output_matches_wallet_inflow")
    assert all(hard.values()), f"V4 SWAP exact-proof predicates failed: {hard}"
    return V4SwapTargetResult(
        intent=intent,
        execution_result=executed,
        transaction_result=transaction,
        amount_in_raw=input_spent,
        amount_out_raw=output_received,
        pool_id=pool_id,
    )


__all__ = [
    "V4_SWAP_PROFILE",
    "V4_SWAP_ROUTE_PROFILE",
    "V4SwapTargetResult",
    "expected_pool_key",
    "run_uniswap_v4_swap_exact_proof",
]
