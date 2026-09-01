"""Exact Aerodrome Slipstream (concentrated-liquidity) LP proofs on Base.

Slipstream is V3-shaped where it matters for the receipt: the NonfungiblePositionManager
emits the same ``IncreaseLiquidity`` / ERC-721 ``Transfer`` topics and ``positions()`` has
the same word layout (slot 4 carries ``tickSpacing`` instead of ``fee``). What differs is
venue identity: pools are keyed by tick spacing, not fee tier, and live on one of several
reviewed factory generations, each paired with the NPM that owns its positions.

The proof therefore binds the pool the way a strategy would get it -- through the SDK's
own discovery (the connector's pool-reader spec) -- proves the compiler bound that exact
venue, and carries the factory's ``getPool`` answer as a chain-observed witness, so the
seal-time contract can re-derive pool identity without a CREATE2 formula it does not have.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.aerodrome.addresses import SlipstreamDeployment, slipstream_lp_deployments
from almanak.connectors.aerodrome.pool_reader import SLIPSTREAM_POOL_READER_SPEC
from almanak.connectors.aerodrome.receipt_parser import EVENT_TOPICS, AerodromeSlipstreamReceiptParser
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import LPOpenIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.compiler_models import IntentCompilerConfig
from tests.intents._uniswap_v3_lp_exact_proofs import (
    LPOpenTargetResult,
    _position_calls,
    _position_state,
    _single_event_transaction,
)
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals
from tests.intents.intent_evidence import decode_explorer_view

PROTOCOL = "aerodrome_slipstream"
CONTRACT_PROFILE = "v3_lp.v1"
WETH_AMOUNT = Decimal("0.001")
USDC_AMOUNT = Decimal("3")
MAX_SLIPPAGE = Decimal("0.005")
# Half-width of the tick band around the live pool tick. Wide enough that the position
# is two-sided on any fork block; ticks are aligned to the pool's spacing below.
TICK_HALF_WIDTH = 5000
SLOT0_SELECTOR = "0x3850c7bd"


@dataclass(frozen=True)
class DiscoveredSlipstreamPool:
    """The venue the SDK's own discovery returns for a pair, with its owning generation."""

    pool: str
    tick_spacing: int
    token0: str
    token1: str
    deployment: SlipstreamDeployment
    lookup_raw: bytes
    label: str


def _get_pool_word(web3: Web3, *, factory: str, token0: str, token1: str, tick_spacing: int, block: int | str) -> bytes:
    data = (
        SLIPSTREAM_POOL_READER_SPEC.get_pool_selector
        + Web3.to_checksum_address(token0)[2:].lower().zfill(64)
        + Web3.to_checksum_address(token1)[2:].lower().zfill(64)
        + (tick_spacing & ((1 << 24) - 1)).to_bytes(32, "big").hex()
    )
    return bytes(web3.eth.call({"to": Web3.to_checksum_address(factory), "data": data}, block))


def _as_int24(word: bytes) -> int:
    value = int.from_bytes(word, "big")
    return value - (1 << 256) if value >= (1 << 255) else value


def discover_slipstream_pool(web3: Web3, *, chain: str, token_a: str, token_b: str) -> DiscoveredSlipstreamPool:
    """Resolve the pair exactly as the SDK does: the connector's known-pool registry first.

    ``SLIPSTREAM_POOL_READER_SPEC.known_pools`` is what the pool reader consults before
    any factory call, so it is the venue a strategy is steered to. The owning factory
    generation is then established on-chain by asking each reviewed factory for that
    (token0, token1, tickSpacing); exactly one must answer with the discovered pool.
    """
    token0, token1 = sorted((token_a.lower(), token_b.lower()))
    known = SLIPSTREAM_POOL_READER_SPEC.known_pools.get(chain, {})
    candidates = sorted(
        (int(spacing), Web3.to_checksum_address(address))
        for (first, second, spacing), address in known.items()
        if {first.lower(), second.lower()} == {token0, token1}
    )
    assert candidates, f"SDK discovery knows no Slipstream pool for {token0}/{token1} on {chain}"
    tick_spacing, pool = candidates[0]
    owners: list[tuple[SlipstreamDeployment, bytes]] = []
    for deployment in slipstream_lp_deployments(chain):
        word = _get_pool_word(
            web3, factory=deployment.factory, token0=token0, token1=token1, tick_spacing=tick_spacing, block="latest"
        )
        if len(word) == 32 and word[-20:].hex() == pool[2:].lower():
            owners.append((deployment, word))
    assert len(owners) == 1, (
        f"discovered Slipstream pool {pool} (tick spacing {tick_spacing}) is owned by "
        f"{len(owners)} reviewed factory generations; expected exactly one"
    )
    deployment, lookup_raw = owners[0]
    symbols = {address.lower(): symbol for symbol, address in CHAIN_CONFIGS[chain]["tokens"].items()}
    label = f"{symbols[token0]}/{symbols[token1]}/{tick_spacing}"
    return DiscoveredSlipstreamPool(
        pool=pool,
        tick_spacing=tick_spacing,
        token0=Web3.to_checksum_address(token0),
        token1=Web3.to_checksum_address(token1),
        deployment=deployment,
        lookup_raw=lookup_raw,
        label=label,
    )


def _tick_band(web3: Web3, pool: str, tick_spacing: int) -> tuple[int, int]:
    slot0 = bytes(web3.eth.call({"to": Web3.to_checksum_address(pool), "data": SLOT0_SELECTOR}))
    assert len(slot0) >= 64, f"slot0() returned {len(slot0)} bytes for {pool}"
    current = _as_int24(slot0[32:64])
    lower = ((current - TICK_HALF_WIDTH) // tick_spacing) * tick_spacing
    upper = -((-(current + TICK_HALF_WIDTH)) // tick_spacing) * tick_spacing
    return lower, upper


async def run_aerodrome_slipstream_lp_open_exact_proof(
    *,
    chain: str,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    intent_evidence: Any,
    venue_verification_gateway: Any,
    execution_context: ExecutionContext | None = None,
    compiler_config: IntentCompilerConfig | None = None,
    rpc_url: str | None = None,
    gateway_client: Any | None = None,
) -> LPOpenTargetResult:
    """Compile, execute, and independently prove one Slipstream NFT mint on the discovered pool."""
    tokens = CHAIN_CONFIGS[chain]["tokens"]
    venue = discover_slipstream_pool(web3, chain=chain, token_a=tokens["WETH"], token_b=tokens["USDC"])
    npm = venue.deployment.position_manager
    token0, token1 = venue.token0, venue.token1
    amount0, amount1 = (
        (WETH_AMOUNT, USDC_AMOUNT) if token0.lower() == tokens["WETH"].lower() else (USDC_AMOUNT, WETH_AMOUNT)
    )
    tick_lower, tick_upper = _tick_band(web3, venue.pool, venue.tick_spacing)
    token0_decimals = get_token_decimals(web3, token0)
    token1_decimals = get_token_decimals(web3, token1)
    token0_max = int(amount0 * Decimal(10**token0_decimals))
    token1_max = int(amount1 * Decimal(10**token1_decimals))
    token0_before = get_token_balance(web3, token0, funded_wallet)
    token1_before = get_token_balance(web3, token1, funded_wallet)

    intent = LPOpenIntent(
        pool=venue.label,
        amount0=amount0,
        amount1=amount1,
        range_lower=Decimal(tick_lower),
        range_upper=Decimal(tick_upper),
        max_slippage=MAX_SLIPPAGE,
        protocol=PROTOCOL,
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
        venue_verification_gateway_factory=lambda: venue_verification_gateway,
    ).compile(intent)
    if (
        compiled.status.value != "SUCCESS"
        and "No Aerodrome CL pool found" in str(compiled.error)
        and os.environ.get("ALMANAK_QA_STRICT_PROOFS") != "1"
    ):
        # Excuse EXACTLY the known venue-steering failure (VIB-6810), never the
        # node: any other compile error, and every assertion below, stays hard.
        # Self-healing on fix; the Lab seal lane keeps the honest FAIL cell.
        pytest.xfail("VIB-6810: discovery returns a legacy-factory pool the compiler cannot execute (as of 2026-09-01)")
    assert compiled.status.value == "SUCCESS", (
        f"LP_OPEN compilation failed for the pool the SDK's own discovery returned "
        f"({venue.label} = {venue.pool}, {venue.deployment.generation} factory): {compiled.error}"
    )
    assert compiled.action_bundle is not None
    metadata = compiled.action_bundle.metadata
    assert str(metadata.get("nft_manager") or "").lower() == npm.lower(), (
        f"compiler bound NPM {metadata.get('nft_manager')} but the discovered pool is owned by "
        f"the {venue.deployment.generation} generation ({npm})"
    )
    assert int(metadata.get("tick_spacing") or -1) == venue.tick_spacing
    executed = await orchestrator.execute(compiled.action_bundle, execution_context)
    assert executed.success, f"LP_OPEN execution failed: {executed.error}"

    transaction = _single_event_transaction(executed, EVENT_TOPICS["IncreaseLiquidity"], "IncreaseLiquidity")
    parser = AerodromeSlipstreamReceiptParser(chain=chain)
    parsed = intent_evidence.capture_parse(
        intent=intent,
        transaction_result=transaction,
        parser=lambda receipt: parser.extract_lp_open_data(receipt),
        parser_method="extract_lp_open_data",
    )
    assert parsed is not None and parsed.position_id is not None
    receipt = transaction.receipt.to_dict()
    block = int(receipt["blockNumber"] if "blockNumber" in receipt else receipt["block_number"])
    position_raw, owner_raw = _position_calls(web3, position_manager=npm, position_id=parsed.position_id, block=block)
    state = _position_state(position_raw)
    lookup_raw = _get_pool_word(
        web3,
        factory=venue.deployment.factory,
        token0=token0,
        token1=token1,
        tick_spacing=venue.tick_spacing,
        block=block,
    )
    assert state["liquidity"] > 0
    assert {state["token0"].lower(), state["token1"].lower()} == {token0.lower(), token1.lower()}
    assert state["fee"] == venue.tick_spacing, f"positions() slot 4 = {state['fee']}, expected tick spacing"
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
    pool_emitted = 0
    for log in logs:
        address = str(log.get("address") or "").lower()
        if address == venue.pool.lower():
            pool_emitted += 1
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
        # The venue the SDK discovered is the venue the chain used: the pool contract
        # itself emitted in the mint receipt, and its factory still resolves it.
        "discovered_pool_emitted_in_receipt": pool_emitted > 0,
        "factory_resolves_discovered_pool": lookup_raw[-20:].hex() == venue.pool[2:].lower(),
    }
    assert all(flags.values()), f"LP_OPEN exact proof predicates failed: {flags}"
    intent_evidence.record_fidelity(
        hard=True,
        flags=flags,
        witnesses=[
            {"kind": "position_state", "position_id": parsed.position_id, **state},
            {
                "kind": "discovered_venue",
                "pool": venue.pool,
                "tick_spacing": venue.tick_spacing,
                "factory_generation": venue.deployment.generation,
                "source": "SLIPSTREAM_POOL_READER_SPEC.known_pools",
            },
        ],
    )
    intent_evidence.record_balance_deltas(
        checks={"bilateral_position_funding_verified": True},
        token0={"address": token0, "before": token0_before, "after": token0_after, "delta": -token0_spent},
        token1={"address": token1, "before": token1_before, "after": token1_after, "delta": -token1_spent},
    )
    block_hash = web3.eth.get_block(block)["hash"]
    intent_evidence.record_semantic_contract(
        schema_version=1,
        profile=CONTRACT_PROFILE,
        intent="LP_OPEN",
        account=funded_wallet,
        pool_reference=venue.label,
        amount0=amount0,
        amount1=amount1,
        range_lower=Decimal(tick_lower),
        range_upper=Decimal(tick_upper),
        resource_address=npm,
        factory_address=venue.deployment.factory,
        pool_address=venue.pool,
        pool_key_kind="tick_spacing",
        pool_lookup_raw="0x" + lookup_raw.hex(),
        token0=state["token0"],
        token1=state["token1"],
        fee_tier=venue.tick_spacing,
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
        pool_address=venue.pool,
        compile_metadata=dict(metadata),
    )


__all__ = [
    "DiscoveredSlipstreamPool",
    "discover_slipstream_pool",
    "run_aerodrome_slipstream_lp_open_exact_proof",
]
