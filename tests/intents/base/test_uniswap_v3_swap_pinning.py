"""Four-layer intent tests for per-intent V3 pool pinning on Base.

Acceptance evidence for ``SwapIntent.swap_params`` ``fee_tier`` / ``pool``
(PR #3644). Unit tests already cover every compile-level branch with a mocked
``eth_call``; what they cannot show is the property the feature actually
claims:

    **A pinned swap executes against exactly the pinned pool, and nothing else.**

A balance-delta assertion cannot prove that on its own — an auto-routed swap
moves the same balances. So each positive test reads the Uniswap V3 ``Swap``
event back out of the receipt and asserts on the **emitting contract address**,
which is the pool the router actually traded against. The fee-tier test
additionally runs the same swap UNPINNED first and requires the two to land on
DIFFERENT pools, so the assertion cannot pass by coincidence when the pin and
auto-selection happen to agree.

The four layers are written out inline in every test rather than hidden behind
a shared helper: this file exists to be read as evidence, and a delegated layer
is one a reviewer has to go looking for.

NO MOCKING. All positive tests execute real on-chain swaps against an Anvil
fork of Base and verify state changes.

To run:
    uv run pytest tests/intents/base/test_uniswap_v3_swap_pinning.py -v -s
"""

import os
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v3.pool_validation import validate_v3_pool
from almanak.connectors.uniswap_v3.receipt_parser import (
    SWAP_EVENT_TOPIC,
    UniswapV3ReceiptParser,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.venues import correlate_verified_venue_receipts
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_v3_pool_missing

CHAIN_NAME = "base"
PROTOCOL = "uniswap_v3"
SWAP_USDC = Decimal("100")

# Base USDC/WETH exists at 100, 500, 3000 and 10000. Having several LIVE tiers
# for one pair is what makes the pin falsifiable: pinning is only meaningful if
# some other pool would otherwise have been chosen.
CANDIDATE_TIERS = (100, 500, 3000)


# =============================================================================
# Helpers (pool resolution and event decoding only — never a test layer)
# =============================================================================


def _resolve_pool(web3: Web3, token_a: str, token_b: str, fee_tier: int) -> str:
    """Resolve the canonical pool address for a tier, or fail the test."""
    rpc_url = web3.provider.endpoint_uri  # type: ignore[attr-defined]
    result = validate_v3_pool(CHAIN_NAME, PROTOCOL, token_a, token_b, fee_tier, rpc_url)
    assert result.exists, (
        f"Precondition: {PROTOCOL} pool for {token_a}/{token_b} fee {fee_tier} must exist "
        f"on {CHAIN_NAME} (validate_v3_pool returned exists={result.exists})"
    )
    assert result.pool_address, "validate_v3_pool confirmed the pool but returned no address"
    return result.pool_address.lower()


def _normalize_hexish(value: Any) -> str:
    """Normalize a web3 log field (bytes or str) to a lowercase 0x string."""
    if isinstance(value, bytes):
        return "0x" + value.hex().lower()
    text = str(value).lower()
    return text if text.startswith("0x") else "0x" + text


def _executed_swap_pools(execution_result: Any) -> set[str]:
    """Return every pool that emitted a V3 ``Swap`` event in this execution.

    The emitting contract of a Uniswap V3 ``Swap`` log IS the pool. This is the
    ground truth for "which pool did the router actually trade against" — the
    router's calldata is an input, the event is the outcome.
    """
    pools: set[str] = set()
    swap_topic = SWAP_EVENT_TOPIC.lower()
    for tx_result in execution_result.transaction_results:
        if not tx_result.receipt:
            continue
        for log in tx_result.receipt.to_dict().get("logs", []) or []:
            topics = log.get("topics", []) if hasattr(log, "get") else getattr(log, "topics", [])
            if not topics:
                continue
            if _normalize_hexish(topics[0]) != swap_topic:
                continue
            address = log.get("address", "") if hasattr(log, "get") else getattr(log, "address", "")
            pools.add(_normalize_hexish(address))
    return pools


# =============================================================================
# Positive: the pin is load-bearing
# =============================================================================


@pytest.mark.base
@pytest.mark.swap
class TestUniswapV3SwapPinning:
    """Per-intent pool pinning executes against exactly the pinned pool."""

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_pinned_fee_tier_redirects_execution_to_the_pinned_pool(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """A ``fee_tier`` pin moves execution to a pool auto-selection did NOT pick.

        This is the discriminating test. It first runs the swap UNPINNED to
        learn which pool auto-selection prefers, then pins a DIFFERENT live
        tier and requires execution to land there. Without the unpinned
        control, a passing pinned assertion would be indistinguishable from
        auto-selection happening to choose the same pool.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc, weth = tokens["USDC"], tokens["WETH"]
        for tier in CANDIDATE_TIERS:
            fail_if_v3_pool_missing(web3, CHAIN_NAME, PROTOCOL, usdc, weth, tier)

        usdc_decimals = get_token_decimals(web3, usdc)
        weth_decimals = get_token_decimals(web3, weth)
        expected_spent = int(SWAP_USDC * Decimal(10**usdc_decimals))
        tier_pools = {tier: _resolve_pool(web3, usdc, weth, tier) for tier in CANDIDATE_TIERS}
        print(f"\nLive USDC/WETH pools on {CHAIN_NAME}: {tier_pools}")

        # ---- Control: UNPINNED swap, to learn what auto-selection picks ----
        print("\n--- Control: UNPINNED swap ---")
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        control_intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=SWAP_USDC,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol=PROTOCOL,
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )
        control_compilation = compiler.compile(control_intent)
        assert control_compilation.status.value == "SUCCESS", (
            f"Layer 1 (control): compilation failed: {control_compilation.error}"
        )
        assert control_compilation.action_bundle is not None

        control_execution = await orchestrator.execute(control_compilation.action_bundle)
        assert control_execution.success, f"Layer 2 (control): {control_execution.error}"

        # Layer 3 (control): receipt parsing
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        decoded_in = Decimal("0")
        decoded_out = Decimal("0")
        for tx_result in control_execution.transaction_results:
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, (
                f"Layer 3 (control): parser failed on {tx_result.tx_hash}: {parse_result.error}"
            )
            if parse_result.swap_result:
                decoded_in = max(decoded_in, parse_result.swap_result.amount_in_decimal)
                decoded_out = max(decoded_out, parse_result.swap_result.amount_out_decimal)
        assert decoded_in > 0, "Layer 3 (control): parser must decode amount_in > 0"
        assert decoded_out > 0, "Layer 3 (control): parser must decode amount_out > 0"

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        assert usdc_before - usdc_after == expected_spent, (
            f"Layer 4 (control): USDC spent must equal {expected_spent}, got {usdc_before - usdc_after}"
        )
        assert weth_after - weth_before > 0, "Layer 4 (control): must receive positive WETH"
        print(
            f"  control: spent {format_token_amount(usdc_before - usdc_after, usdc_decimals)} USDC, "
            f"received {format_token_amount(weth_after - weth_before, weth_decimals)} WETH"
        )

        auto_pools = _executed_swap_pools(control_execution)
        assert len(auto_pools) == 1, f"Control swap must touch exactly one V3 pool, got {auto_pools}"
        auto_pool = next(iter(auto_pools))
        print(f"  auto-selected pool: {auto_pool}")

        pin_tier = next((tier for tier, pool in tier_pools.items() if pool != auto_pool), None)
        assert pin_tier is not None, (
            "Test is vacuous unless a second live tier exists whose pool differs from the "
            f"auto-selected one ({auto_pool}); live pools were {tier_pools}"
        )
        pin_pool = tier_pools[pin_tier]

        # ---- Pinned: a DIFFERENT tier than auto-selection chose ----
        print(f"\n--- Pinned: fee_tier={pin_tier} (pool {pin_pool}, != auto {auto_pool}) ---")
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        pinned_intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=SWAP_USDC,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol=PROTOCOL,
            chain=CHAIN_NAME,
            swap_params={"fee_tier": pin_tier},
        )
        pinned_compilation = compiler.compile(pinned_intent)
        assert pinned_compilation.status.value == "SUCCESS", (
            f"Layer 1 (pinned): compilation failed: {pinned_compilation.error}"
        )
        assert pinned_compilation.action_bundle is not None

        pinned_execution = await orchestrator.execute(pinned_compilation.action_bundle)
        assert pinned_execution.success, f"Layer 2 (pinned): {pinned_execution.error}"

        # Layer 3 (pinned): receipt parsing
        pinned_decoded_in = Decimal("0")
        pinned_decoded_out = Decimal("0")
        for tx_result in pinned_execution.transaction_results:
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, f"Layer 3 (pinned): parser failed on {tx_result.tx_hash}: {parse_result.error}"
            if parse_result.swap_result:
                pinned_decoded_in = max(pinned_decoded_in, parse_result.swap_result.amount_in_decimal)
                pinned_decoded_out = max(pinned_decoded_out, parse_result.swap_result.amount_out_decimal)
        assert pinned_decoded_in > 0, "Layer 3 (pinned): parser must decode amount_in > 0"
        assert pinned_decoded_out > 0, "Layer 3 (pinned): parser must decode amount_out > 0"

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        assert usdc_before - usdc_after == expected_spent, (
            f"Layer 4 (pinned): USDC spent must equal {expected_spent}, got {usdc_before - usdc_after}"
        )
        assert weth_after - weth_before > 0, "Layer 4 (pinned): must receive positive WETH"
        print(
            f"  pinned: spent {format_token_amount(usdc_before - usdc_after, usdc_decimals)} USDC, "
            f"received {format_token_amount(weth_after - weth_before, weth_decimals)} WETH"
        )

        # ---- The acceptance assertion ----
        pinned_pools = _executed_swap_pools(pinned_execution)
        assert pinned_pools == {pin_pool}, (
            f"A pinned swap must execute against exactly the pinned pool. "
            f"Pinned fee_tier={pin_tier} (pool {pin_pool}) but the Swap event(s) came from {pinned_pools}"
        )
        assert auto_pool not in pinned_pools, (
            f"Pin is not load-bearing: execution still landed on the auto-selected pool {auto_pool}"
        )

        metadata = pinned_compilation.action_bundle.metadata or {}
        assert metadata.get("fee_selection_source") == "intent_pinned", (
            f"Bundle metadata must report the pin as its fee source, got {metadata.get('fee_selection_source')!r}"
        )
        assert int(metadata.get("selected_fee_tier")) == pin_tier, (
            f"Bundle metadata selected_fee_tier must be the pinned tier {pin_tier}, "
            f"got {metadata.get('selected_fee_tier')!r}"
        )
        print("\nPINNED FEE TIER VERIFIED ON-CHAIN")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_pinned_pool_address_executes_against_that_exact_pool(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_eth_call_adapter,
    ):
        """A ``pool`` address pin resolves on-chain and executes against that pool."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc, weth = tokens["USDC"], tokens["WETH"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, PROTOCOL, usdc, weth, 3000)
        pinned = _resolve_pool(web3, usdc, weth, 3000)
        print(f"\nPinning pool address {pinned} (USDC/WETH 0.3%)")

        usdc_decimals = get_token_decimals(web3, usdc)
        weth_decimals = get_token_decimals(web3, weth)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=SWAP_USDC,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol=PROTOCOL,
            chain=CHAIN_NAME,
            swap_params={"pool": Web3.to_checksum_address(pinned)},
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle={"USDC": Decimal("1"), "WETH": Decimal("3000")},
            rpc_url=orchestrator.rpc_url,
            venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Layer 1: compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None
        expected_binding_hash = compilation_result.action_bundle.metadata["venue_binding_hash"]
        assert isinstance(expected_binding_hash, str)

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Layer 2: {execution_result.error}"
        fork_block = int(os.environ["ANVIL_FORK_BLOCK_BASE"])
        assert execution_result.transaction_results
        for tx_result in execution_result.transaction_results:
            assert tx_result.tx_hash.startswith("0x") and len(tx_result.tx_hash) == 66
            assert tx_result.receipt is not None
            assert tx_result.receipt.status == 1
            assert tx_result.receipt.block_number >= fork_block
        metadata = compilation_result.action_bundle.metadata or {}
        operational_targets = {ref["reference"].lower() for ref in metadata["venue_operational_refs"]}
        assert any(
            (tx["to"] if isinstance(tx, dict) else tx.to).lower() in operational_targets
            for tx in compilation_result.action_bundle.transactions
        )
        assert correlate_verified_venue_receipts(
            bundle_metadata=metadata,
            expected_binding_hash=expected_binding_hash,
            receipts=tuple(tx.receipt.to_dict() for tx in execution_result.transaction_results if tx.receipt),
        ) == metadata["venue_binding_hash"]

        # Layer 3: receipt parsing
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        decoded_in = Decimal("0")
        decoded_out = Decimal("0")
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, f"Layer 3: parser failed on {tx_result.tx_hash}: {parse_result.error}"
            if parse_result.swap_result:
                decoded_in = max(decoded_in, parse_result.swap_result.amount_in_decimal)
                decoded_out = max(decoded_out, parse_result.swap_result.amount_out_decimal)
        assert decoded_in > 0, "Layer 3: parser must decode amount_in > 0"
        assert decoded_out > 0, "Layer 3: parser must decode amount_out > 0"

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        expected_spent = int(SWAP_USDC * Decimal(10**usdc_decimals))
        assert usdc_before - usdc_after == expected_spent, (
            f"Layer 4: USDC spent must equal {expected_spent}, got {usdc_before - usdc_after}"
        )
        assert weth_after - weth_before > 0, "Layer 4: must receive positive WETH"
        print(
            f"  spent {format_token_amount(usdc_before - usdc_after, usdc_decimals)} USDC, "
            f"received {format_token_amount(weth_after - weth_before, weth_decimals)} WETH"
        )

        pools = _executed_swap_pools(execution_result)
        assert pools == {pinned}, (
            f"A pool-address pin must execute against exactly that pool. "
            f"Pinned {pinned}, Swap event(s) came from {pools}"
        )
        assert (metadata.get("pinned_pool") or "").lower() == pinned, (
            f"Bundle metadata must record the pinned pool, got {metadata.get('pinned_pool')!r}"
        )
        assert metadata.get("fee_selection_source") == "intent_pinned", (
            f"Bundle metadata must report source=intent_pinned, got {metadata.get('fee_selection_source')!r}"
        )
        assert int(metadata.get("selected_fee_tier")) == 3000, (
            f"Pool pin must resolve the pool's own tier (3000), got {metadata.get('selected_fee_tier')!r}"
        )
        assert isinstance(metadata.get("venue_binding_hash"), str)
        print("\nPINNED POOL ADDRESS VERIFIED ON-CHAIN")


# =============================================================================
# Negative: an unusable pin FAILS compilation and never routes elsewhere
# =============================================================================


@pytest.mark.base
@pytest.mark.swap
class TestUniswapV3PinningRefusals:
    """A pin that cannot be honoured must fail compilation, never fall back.

    ``# noqa: layers`` on each test below: these assert a compile-time REFUSAL,
    so there is no transaction, no receipt and no balance delta to verify.
    Layer 1 is the assertion itself (status FAILED with a specific cause) and
    balance conservation stands in for layers 2-4 — the whole point is that
    nothing executed. The corresponding successful-execution paths are covered
    by ``TestUniswapV3SwapPinning`` above.
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_pinned_pool_for_a_different_pair_fails_compilation(  # noqa: layers
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_eth_call_adapter,
    ):
        """Pinning a USDC/WETH pool on a wstETH->USDC swap must be refused."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc, weth, wsteth = tokens["USDC"], tokens["WETH"], tokens["wstETH"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, PROTOCOL, usdc, weth, 500)
        wrong_pair_pool = _resolve_pool(web3, usdc, weth, 500)

        wsteth_before = get_token_balance(web3, wsteth, funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        intent = SwapIntent(
            from_token="wstETH",
            to_token="USDC",
            amount=Decimal("0.01"),
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol=PROTOCOL,
            chain=CHAIN_NAME,
            swap_params={"pool": Web3.to_checksum_address(wrong_pair_pool)},
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle={"USDC": Decimal("1"), "WETH": Decimal("3000"), "wstETH": Decimal("3500")},
            rpc_url=orchestrator.rpc_url,
            venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
        )
        result = compiler.compile(intent)

        assert result.status.value == "FAILED", (
            f"A pool pinned for the wrong pair must fail compilation, got {result.status.value}"
        )
        assert "does not match the swap pair" in (result.error or ""), (
            f"Refusal must name the pair mismatch, got: {result.error}"
        )
        assert result.action_bundle is None, "A refused pin must not produce an ActionBundle"

        assert get_token_balance(web3, wsteth, funded_wallet) == wsteth_before, (
            "Balance conservation: a refused compilation must not move wstETH"
        )
        assert get_token_balance(web3, usdc, funded_wallet) == usdc_before, (
            "Balance conservation: a refused compilation must not move USDC"
        )
        print(f"\nREFUSED AS EXPECTED: {result.error}")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_pool_from_a_different_forks_factory_is_rejected(  # noqa: layers
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_eth_call_adapter,
    ):
        """A real PancakeSwap V3 pool must not satisfy a ``uniswap_v3`` pin.

        This is the cross-fork confusion the factory cross-check exists for:
        the address IS a valid V3-shaped pool holding the right pair, so the
        token/fee reads all succeed. Only ``factory.getPool`` can tell that the
        router would never trade against it.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc, weth = tokens["USDC"], tokens["WETH"]
        foreign = validate_v3_pool(
            CHAIN_NAME,
            "pancakeswap_v3",
            usdc,
            weth,
            500,
            web3.provider.endpoint_uri,  # type: ignore[attr-defined]
        )
        if not foreign.exists or not foreign.pool_address:
            pytest.skip("No PancakeSwap V3 USDC/WETH 0.05% pool on Base to borrow as a foreign address")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=SWAP_USDC,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol=PROTOCOL,  # uniswap_v3, but the pool belongs to pancakeswap_v3
            chain=CHAIN_NAME,
            swap_params={"pool": Web3.to_checksum_address(foreign.pool_address)},
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle={"USDC": Decimal("1"), "WETH": Decimal("3000")},
            rpc_url=orchestrator.rpc_url,
            venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
        )
        result = compiler.compile(intent)

        assert result.status.value == "FAILED", (
            f"A pool from another fork's factory must be rejected, got {result.status.value}"
        )
        assert "different protocol or chain" in (result.error or ""), (
            f"Refusal must name the factory mismatch, got: {result.error}"
        )
        assert result.action_bundle is None, "A refused pin must not produce an ActionBundle"
        assert get_token_balance(web3, usdc, funded_wallet) == usdc_before, (
            "Balance conservation: a refused compilation must not move USDC"
        )
        print(f"\nREFUSED AS EXPECTED: {result.error}")
