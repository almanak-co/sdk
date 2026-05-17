"""4-layer intent tests for Uniswap V4 LP_CLOSE on Polygon Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
closing V4 LP positions via PositionManager on Polygon:
1. Open a WETH/USDC LP position (LP_OPEN as setup)
2. Create LPCloseIntent with position_id and protocol_params
3. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
4. Execute via ExecutionOrchestrator (full production pipeline)
5. Parse receipts using UniswapV4ReceiptParser (liquidity removed, tokens returned)
6. Verify bilateral balance deltas: WETH and USDC both strictly positive

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

VIB-4364 / VIB-4343: registry edit (adding "polygon" to uniswap_v4 declared
chains) is OUT OF SCOPE for this ticket. The ``no_zodiac`` marker is required
because uniswap_v4 is not in the synthetic_intents manifest matrix.

To run:
    uv run pytest tests/intents/polygon/test_uniswap_v4_lp_close.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, LPCloseIntent, LPOpenIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

pytestmark = pytest.mark.no_zodiac(
    reason="VIB-4343: uniswap_v4 not yet in synthetic_intents matrix"
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "polygon"

# WETH/USDC pool with 0.3% fee tier (3000) — same shape as the arbitrum /
# ethereum golden tests and the Polygon LP_OPEN sibling (VIB-4363). Verified
# 2026-05-14 that this pool is initialized on Polygon V4 with non-zero
# liquidity. USDC here is the native (Circle) USDC token 0x3c499c..., which
# sorts before WETH 0x7ceB23.. so it is currency0.
LP_POOL = "WETH/USDC/3000"

# Small amounts for setup LP_OPEN
LP_AMOUNT_WETH = Decimal("0.01")
LP_AMOUNT_USDC = Decimal("25")
LP_RANGE_LOWER = Decimal("1000")
LP_RANGE_UPPER = Decimal("10000")

# =============================================================================
# Helper: Open a position (setup for close tests)
# =============================================================================


async def _open_v4_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> tuple[int, int, str, str]:
    """Open a V4 LP position and return (position_id, liquidity, currency0, currency1).

    Self-sufficient setup that mirrors the arbitrum / base / optimism / ethereum
    LP_CLOSE goldens so VIB-4364 can land without depending on VIB-4363's
    parallel LP_OPEN file. Uses the WETH/USDC/3000 pool key (see ``LP_POOL``
    comment) which matches the Polygon LP_OPEN sibling.

    Raises AssertionError if the setup LP_OPEN fails.
    """
    intent = LPOpenIntent(
        pool=LP_POOL,
        amount0=LP_AMOUNT_WETH,
        amount1=LP_AMOUNT_USDC,
        range_lower=LP_RANGE_LOWER,
        range_upper=LP_RANGE_UPPER,
        protocol="uniswap_v4",
        chain=CHAIN_NAME,
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
    )

    compilation_result = compiler.compile(intent)
    assert compilation_result.status.value == "SUCCESS", (
        f"Setup LP_OPEN compilation failed: {compilation_result.error}"
    )
    bundle = compilation_result.action_bundle
    assert bundle is not None

    execution_result = await orchestrator.execute(bundle)
    assert execution_result.success, f"Setup LP_OPEN execution failed: {execution_result.error}"

    # Extract position_id and liquidity from receipt.
    # Iterate until both are found, then stop -- avoids spamming the
    # "no position ID found" parser warning for approval txs.
    parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
    position_id: int | None = None
    liquidity: int | None = None

    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            receipt_dict = tx_result.receipt.to_dict()
            if position_id is None:
                position_id = parser.extract_position_id(receipt_dict)
            if liquidity is None:
                liquidity = parser.extract_liquidity(receipt_dict)
        if position_id is not None and liquidity is not None:
            break

    assert position_id is not None, "Setup LP_OPEN must yield a position_id"
    assert liquidity is not None and liquidity > 0, "Setup LP_OPEN must yield positive liquidity"

    # Get currency addresses from bundle metadata
    token0 = bundle.metadata.get("token0", {})
    token1 = bundle.metadata.get("token1", {})
    currency0 = token0.get("address", "")
    currency1 = token1.get("address", "")

    assert currency0 and currency1, "Must extract currency addresses from bundle metadata"

    return position_id, liquidity, currency0, currency1


# =============================================================================
# LPCloseIntent Tests -- Uniswap V4 on Polygon
# =============================================================================


@pytest.mark.polygon
@pytest.mark.lp
class TestUniswapV4LPCloseIntent:
    """Test Uniswap V4 LP_CLOSE using LPCloseIntent on Polygon.

    These tests verify the full LP close flow:
    - First open a position (setup)
    - LPCloseIntent creation with position_id and protocol_params
    - IntentCompiler routes to UniswapV4Adapter.compile_lp_close_intent()
    - Transactions execute successfully on-chain via PositionManager
    - UniswapV4ReceiptParser correctly extracts close data
    - Balance changes match expected token returns
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-4426 V0 (PR #2335) requires pool_key_lookup callable on UniswapV4ReceiptParser at LP_CLOSE per T07; intent-test harness wiring lands with PR-2 (VIB-4478 lp_v4 fixture). as of 2026-05-17.",
        strict=True,
    )
    async def test_lp_close_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test full LP_OPEN -> LP_CLOSE lifecycle for WETH/USDC via V4 on Polygon.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> lp_close_data extracted
        4. Balance Deltas: WETH and USDC returned from pool (principal + fees)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]

        weth_decimals = get_token_decimals(web3, weth_addr)
        usdc_decimals = get_token_decimals(web3, usdc_addr)

        # Fail-fast funding check: surface infra/fixture funding regressions
        # before LP_OPEN runs and produces a less-actionable error.
        weth_available = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_available = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_required = int(LP_AMOUNT_WETH * (Decimal(10) ** weth_decimals))
        usdc_required = int(LP_AMOUNT_USDC * (Decimal(10) ** usdc_decimals))
        assert weth_available >= weth_required, (
            f"Insufficient WETH funding for setup LP_OPEN: "
            f"have={weth_available}, need>={weth_required}"
        )
        assert usdc_available >= usdc_required, (
            f"Insufficient USDC funding for setup LP_OPEN: "
            f"have={usdc_available}, need>={usdc_required}"
        )

        print(f"\n{'=' * 80}")
        print("Test: LP_CLOSE WETH/USDC via Uniswap V4 on Polygon")
        print(f"{'=' * 80}")

        # Setup: Open a position first
        print("\n--- Setup: Opening LP position ---")
        position_id, liquidity, currency0, currency1 = await _open_v4_position(
            web3, funded_wallet, orchestrator, price_oracle,
        )
        print(f"Opened position: id={position_id}, liquidity={liquidity}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # Record balances before close
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        print("\n--- Closing LP position ---")
        print(f"WETH before close: {format_token_amount(weth_before, weth_decimals)}")
        print(f"USDC before close: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compilation
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            protocol_params={
                "liquidity": liquidity,
                "currency0": currency0,
                "currency1": currency1,
            },
        )

        print(f"Created LPCloseIntent: position_id={close_intent.position_id}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"LP_CLOSE compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        bundle = compilation_result.action_bundle
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")

        # Layer 2: Execution
        print("\nExecuting LP_CLOSE via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"LP_CLOSE execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        lp_close_data = None

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                # Exercise parse_receipt() entrypoint — this is the surface
                # ResultEnricher consumes in production via extract_lp_amounts,
                # so the intent-test contract requires calling it here
                # (.claude/rules/intent-tests.md Layer 3).
                parser.parse_receipt(receipt_dict)
                close_data = parser.extract_lp_close_data(receipt_dict)
                if close_data is not None:
                    lp_close_data = close_data
                    print("  LP Close Data:")
                    print(f"    amount0_collected: {close_data.amount0_collected}")
                    print(f"    amount1_collected: {close_data.amount1_collected}")
                    print(f"    liquidity_removed: {close_data.liquidity_removed}")

        assert lp_close_data is not None, "Must extract LP close data from receipt"
        assert lp_close_data.liquidity_removed is not None and lp_close_data.liquidity_removed > 0, (
            "Must remove positive liquidity"
        )
        # Parser MUST report principal collected on both sides (amounts0 + amounts1).
        # Fees0/fees1 are reported within the same lp_close_data structure as a
        # subcomponent of the collected totals — the parser's collected amounts
        # already include any accrued fees over the position lifetime.
        assert lp_close_data.amount0_collected is not None and lp_close_data.amount0_collected > 0, (
            "Parser must extract positive amount0_collected from LP_CLOSE receipt"
        )
        assert lp_close_data.amount1_collected is not None and lp_close_data.amount1_collected > 0, (
            "Parser must extract positive amount1_collected from LP_CLOSE receipt"
        )

        # Layer 4: Balance Deltas — wallet gains BOTH tokens (principal + any fees)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_received = weth_after - weth_before
        usdc_received = usdc_after - usdc_before

        print("\n--- Balance Deltas ---")
        print(f"WETH received: {format_token_amount(weth_received, weth_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, usdc_decimals)}")

        # MANDATORY bilateral delta (see .claude/rules/intent-tests.md and #1691):
        # the position was opened with both tokens, so closing it MUST return
        # both. Permitting `or` here would let a V4 one-sided-close bug pass.
        assert weth_received > 0 and usdc_received > 0, (
            f"LP_CLOSE on a two-token position must return BOTH tokens (no-op guard). "
            f"weth_received={weth_received}, usdc_received={usdc_received}"
        )

        print(f"\nPosition {position_id} successfully closed")
        print("\nALL 4 LAYERS PASSED")

    @pytest.mark.intent(IntentType.LP_CLOSE)  # noqa: layers
    @pytest.mark.asyncio
    async def test_lp_close_without_liquidity_fails_compilation(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
    ):
        """Test that LP_CLOSE without liquidity in protocol_params fails at compilation.

        V4 LP_CLOSE requires on-chain position data (liquidity, currencies).

        Intentional layer exception (``# noqa: layers``) — this test stops at
        Layer 1 by design. The failure-path contract from
        ``.claude/rules/intent-tests.md`` is still honoured by snapshotting
        WETH/USDC around ``compiler.compile(...)`` and asserting both
        balances are unchanged after the failed compilation.
        """
        print(f"\n{'=' * 80}")
        print("Test: LP_CLOSE without liquidity (should fail compilation)")
        print(f"{'=' * 80}")

        # Snapshot balances BEFORE compilation so we can assert conservation
        # after the compile-time failure (no transaction should be sent).
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        # Use a token id well above any minted position on Polygon V4 at fork
        # time so the on-chain ``get_position_liquidity`` query returns 0 and
        # the compiler must fall back to the protocol_params-required error
        # path. (Matches the deliberately out-of-range value used in
        # ``tests/intents/base/test_uniswap_v4_lp_close.py`` and the
        # optimism sibling.)
        close_intent = LPCloseIntent(
            position_id="999999999999",
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            # No protocol_params -- missing liquidity
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail without liquidity in protocol_params"
        )
        assert compilation_result.action_bundle is None, (
            "Failed compilation must not produce an ActionBundle"
        )
        assert "liquidity" in compilation_result.error.lower(), (
            f"Error should mention liquidity requirement, got: {compilation_result.error}"
        )

        # Failure-path balance conservation: no on-chain tx fired, balances unchanged.
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        assert weth_after == weth_before, (
            f"WETH balance must be unchanged after compile-time failure. "
            f"before={weth_before}, after={weth_after}"
        )
        assert usdc_after == usdc_before, (
            f"USDC balance must be unchanged after compile-time failure. "
            f"before={usdc_before}, after={usdc_after}"
        )

        print(f"Compilation failed as expected: {compilation_result.error}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
