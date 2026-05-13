"""4-layer intent tests for Uniswap V4 LP_COLLECT_FEES on Optimism Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
collecting fees from V4 LP positions via PositionManager on Optimism:
1. Open an ETH/USDC LP position (LP_OPEN as setup -- ``ETH`` symbol so
   currency0 resolves to ``address(0)``, matching the V4 swap router's
   WETH -> native ETH remapping)
2. Generate fees by counter-swapping through the SAME native-ETH pool
3. Create CollectFeesIntent with position_id and protocol_params
4. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
5. Execute via ExecutionOrchestrator (full production pipeline)
6. Parse receipts -- fees (Transfer from PoolManager for USDC, native
   delta for ETH) separate from principal (ModifyLiquidity delta == 0)
7. Verify position liquidity is unchanged on-chain after collection
8. Verify at least one side of the position accrued strictly positive
   fees (bilateral no-op guard)

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

VIB-4361 / VIB-4343: registry edit (adding "optimism" to uniswap_v4 declared
chains) is OUT OF SCOPE for this ticket. The ``no_zodiac`` marker is
required because uniswap_v4 is not in the synthetic_intents manifest matrix.

Pool selection: ``ETH/USDC/3000``. Optimism V4 does NOT have the
``WETH/USDC/3000`` (ERC-20-keyed) pool used by arbitrum / ethereum LP
goldens, but the native-keyed ``(NATIVE_ETH, USDC, 3000, 60, 0x0)`` pool IS
initialized at fork time (verified 2026-05-14 via direct StateView.getSlot0
against PoolManager 0x9a13F98C..., liquidity ~5.2e14). Using the ``ETH``
symbol means ``UniswapV4Adapter._resolve_token(for_v4_pool=True)`` resolves
currency0 to ``address(0)`` at LP_OPEN time, and the V4 SwapIntent path
already defaults to fee=3000 and unconditionally remaps WETH -> native ETH
at the pool layer (``UniswapV4SDK.build_swap_tx``), so the LP and the
counter-swap genuinely share the SAME pool key. This is the same
native-ETH strategy the just-merged ``tests/intents/base/test_uniswap_v4_collect_fees.py``
uses; fee tier 3000 keeps tick spacing 60 consistent with the arbitrum /
ethereum goldens.

The bilateral fee assertion uses OR across the two sides (ETH fee OR USDC
fee strictly positive) so a partial native-ETH return path -- which on
Optimism's deployed V4 PositionManager has been observed to leave principal
ETH inside the PoolManager for LP_CLOSE (see VIB-4360) -- does not silently
mask a real no-op.

To run:
    uv run pytest tests/intents/optimism/test_uniswap_v4_collect_fees.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.connectors.uniswap_v4.sdk import UniswapV4SDK
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType, LPOpenIntent
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

CHAIN_NAME = "optimism"

# ETH/USDC pool with 0.3% fee tier. V4 pools support native ETH as
# currency0 (address(0)); ``UniswapV4Adapter.swap_exact_input`` and
# ``UniswapV4SDK.build_swap_tx`` ALWAYS substitute WETH -> native ETH
# for the V4 pool key (see ``sdk.py:_is_wrapped_native`` handling).
# Opening the LP with ``ETH/USDC/3000`` resolves token0 to address(0)
# via ``_resolve_token(..., for_v4_pool=True)``, so the LP position
# and the counter-swaps end up on the SAME pool key and the position
# can actually accrue fees.
#
# Fee tier 3000 (matches arbitrum / ethereum goldens) — the
# ``WETH/USDC/3000`` ERC-20-keyed pool is uninitialized on Optimism V4
# (see VIB-4359 / VIB-4360), but the ``(NATIVE_ETH, USDC, 3000, 60, 0x0)``
# pool IS initialized at fork time (verified 2026-05-14: liquidity ~5.2e14
# via StateView.getSlot0). The V4 SwapIntent path also defaults to
# fee=3000 and unconditionally remaps WETH -> native ETH, so the LP and
# counter-swap end up on the same key without any per-test fee override.
LP_POOL = "ETH/USDC/3000"

# Token symbols for the fee-generation counter-swap. ``ETH`` symbol in
# the SwapIntent maps to native ETH at the V4 pool layer, matching the
# LP pool key. Wallet balance checks below still read WETH (the wrapped
# native token the test wallet actually holds).
SWAP_TOKEN0_SYMBOL = "ETH"
SWAP_TOKEN1_SYMBOL = "USDC"

# Small amounts for setup LP_OPEN
LP_AMOUNT_ETH = Decimal("0.01")
LP_AMOUNT_USDC = Decimal("25")
LP_RANGE_LOWER = Decimal("1000")
LP_RANGE_UPPER = Decimal("10000")

# Counter-swap amount used to generate fees in the LP_OPEN pool.
# A round-trip swap through the same pool key forces the LP position to
# accrue fees from both legs (USDC -> ETH and back).
COUNTER_SWAP_USDC = Decimal("100")
COUNTER_SWAP_ETH = Decimal("0.05")
SWAP_MAX_SLIPPAGE = Decimal("0.05")


# =============================================================================
# Helper: Open a position (setup for collect fees tests)
# =============================================================================


async def _open_v4_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> tuple[int, int, str, str]:
    """Open a V4 LP position and return ``(position_id, liquidity, currency0, currency1)``.

    Raises ``AssertionError`` if the setup LP_OPEN fails or yields no
    position id / no positive liquidity.
    """
    intent = LPOpenIntent(
        pool=LP_POOL,
        amount0=LP_AMOUNT_ETH,
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
    assert liquidity is not None and liquidity > 0, (
        "Setup LP_OPEN must yield positive liquidity"
    )

    # Get currency addresses from bundle metadata
    token0 = bundle.metadata.get("token0", {})
    token1 = bundle.metadata.get("token1", {})
    currency0 = token0.get("address", "")
    currency1 = token1.get("address", "")

    assert currency0 and currency1, (
        "Must extract currency addresses from bundle metadata"
    )

    return position_id, liquidity, currency0, currency1


async def _counter_swap_to_generate_fees(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> bool:
    """Run a round-trip swap through the V4 connector to accrue fees.

    Two SwapIntents (USDC -> ETH, then ETH -> USDC) routed via the V4
    UniversalRouter. Using the ``ETH`` symbol makes the swap path
    resolve to native ETH at the V4 pool key, which matches the
    ``ETH/USDC/3000`` LP_OPEN pool exactly -- the position then accrues
    fees from both legs.

    Returns ``True`` if both legs compiled AND executed successfully,
    ``False`` otherwise.
    """
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
    )

    for from_token, to_token, amount in (
        (SWAP_TOKEN1_SYMBOL, SWAP_TOKEN0_SYMBOL, COUNTER_SWAP_USDC),
        (SWAP_TOKEN0_SYMBOL, SWAP_TOKEN1_SYMBOL, COUNTER_SWAP_ETH),
    ):
        swap_intent = SwapIntent(
            from_token=from_token,
            to_token=to_token,
            amount=amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
        )
        swap_compilation = compiler.compile(swap_intent)
        if swap_compilation.status.value != "SUCCESS" or swap_compilation.action_bundle is None:
            return False
        swap_result = await orchestrator.execute(swap_compilation.action_bundle)
        if not swap_result.success:
            return False
    return True


# =============================================================================
# CollectFeesIntent Tests -- Uniswap V4 on Optimism
# =============================================================================


@pytest.mark.optimism
@pytest.mark.lp
class TestUniswapV4CollectFeesIntent:
    """Test Uniswap V4 LP_COLLECT_FEES using CollectFeesIntent on Optimism.

    These tests verify the fee collection flow:
    - First open a position (setup)
    - Generate trading fees via counter-swap through the same pool key
    - CollectFeesIntent creation with protocol_params
    - IntentCompiler routes to ``_compile_collect_fees_uniswap_v4()``
    - Transactions execute successfully on-chain via PositionManager
    - Position liquidity is unchanged after fee collection (fees-only)
    - Wallet gains ONLY the fee amounts; principal stays locked in the pool
    """

    @pytest.mark.intent(
        IntentType.LP_OPEN, IntentType.SWAP, IntentType.LP_COLLECT_FEES
    )
    @pytest.mark.asyncio
    async def test_collect_fees_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Collect fees from an ETH/USDC LP position via V4 on Optimism.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle.
        2. Execution: ExecutionOrchestrator -> success.
        3. Receipt Parsing: ModifyLiquidity event with ``liquidity_delta == 0``
           (fees-only path; principal is NOT removed). USDC fee amount
           surfaces as a PoolManager -> wallet Transfer; ETH fee is the
           native balance delta (TAKE flows native ETH directly with no
           Transfer event).
        4. Balance Deltas: Wallet gains ONLY the fee amounts (USDC delta
           equals the parsed Transfer amount exactly; native ETH delta
           net of gas equals fee0), on-chain position liquidity is
           unchanged, and the counter-swap proved at least one side of
           the position accrued strictly positive fees.

        Pool selection: V4's swap router always remaps WETH -> native
        ETH at the pool key (see ``UniswapV4SDK.build_swap_tx``); LP_OPEN
        with ``ETH/USDC/3000`` resolves currency0 to ``address(0)`` via
        ``_resolve_token(..., for_v4_pool=True)``, so LP and swap share
        the SAME ``(NATIVE_ETH, USDC, 3000, 60, 0x0)`` pool key and the
        position genuinely accrues fees from the counter-swap.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        usdc_decimals = get_token_decimals(web3, usdc_addr)

        # Fail-fast funding check: surface infra/fixture funding regressions
        # before LP_OPEN / counter-swap runs and produces a less-actionable error.
        eth_before_setup = web3.eth.get_balance(funded_wallet)
        usdc_before_setup = get_token_balance(web3, usdc_addr, funded_wallet)
        required_usdc = int((LP_AMOUNT_USDC + COUNTER_SWAP_USDC) * (10 ** usdc_decimals))
        required_eth = int((LP_AMOUNT_ETH + COUNTER_SWAP_ETH + Decimal("0.01")) * (10**18))
        assert eth_before_setup >= required_eth, (
            f"Insufficient ETH funding for test setup: "
            f"have={eth_before_setup}, need>={required_eth}"
        )
        assert usdc_before_setup >= required_usdc, (
            f"Insufficient USDC funding for test setup: "
            f"have={usdc_before_setup}, need>={required_usdc}"
        )

        print(f"\n{'=' * 80}")
        print("Test: LP_COLLECT_FEES ETH/USDC via Uniswap V4 on Optimism")
        print(f"{'=' * 80}")

        # Setup: Open a position first
        print("\n--- Setup: Opening LP position ---")
        position_id, liquidity_before, currency0, currency1 = await _open_v4_position(
            web3, funded_wallet, orchestrator, price_oracle,
        )
        print(f"Opened position: id={position_id}, liquidity={liquidity_before}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # Verify the LP pool key is the native-ETH pool (currency0 must be
        # address(0)). If LP_OPEN ever shifted to a non-native pool key,
        # the same-pool fee-accrual invariant below would silently break.
        assert int(currency0, 16) == 0, (
            f"LP_OPEN must use native ETH as currency0 so swap and LP "
            f"share the same V4 pool key (currency0=0x0). Got: {currency0}"
        )

        # Fee generation: a round-trip swap through the SAME pool key as
        # the LP position. ``UniswapV4SDK.build_swap_tx`` always remaps
        # WETH -> native ETH at the pool layer, so by using "ETH" symbols
        # in both the LP and the swap, both legs route through the same
        # (NATIVE_ETH, USDC, fee=3000, tickSpacing=60, hooks=0x0) pool.
        print("\n--- Counter-swap to accrue fees (USDC <-> ETH) ---")
        counter_swap_executed = await _counter_swap_to_generate_fees(
            funded_wallet, orchestrator, price_oracle,
        )
        print(f"Counter-swap executed: {counter_swap_executed}")
        assert counter_swap_executed, (
            "Counter-swap must succeed -- the same-pool fee-accrual cycle "
            "is the only way to prove COLLECT actually transfers fees "
            "(not just principal) on this chain."
        )

        # Cross-check that the on-chain liquidity is unchanged after the
        # counter-swaps (swaps must not touch the LP position liquidity).
        v4_sdk = UniswapV4SDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        liquidity_after_swap = v4_sdk.get_position_liquidity(position_id)
        assert liquidity_after_swap == liquidity_before, (
            f"Counter-swap must not change LP position liquidity. "
            f"before={liquidity_before}, after_swap={liquidity_after_swap}"
        )

        # Record balances BEFORE fee collection.
        # Native ETH is currency0 of the LP pool; USDC is currency1.
        # ``web3.eth.get_balance`` returns the native ETH balance; gas
        # spent by COLLECT_FEES is accounted for explicitly below.
        eth_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        print("\n--- Collecting fees ---")
        print(f"ETH  before: {format_token_amount(eth_before, 18)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compilation
        collect_intent = CollectFeesIntent(
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            protocol_params={
                "position_id": position_id,
                "currency0": currency0,
                "currency1": currency1,
            },
        )

        print(
            f"Created CollectFeesIntent: pool={collect_intent.pool}, "
            f"position_id={position_id}"
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"COLLECT_FEES compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        bundle = compilation_result.action_bundle
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")

        # Layer 2: Execution
        print("\nExecuting COLLECT_FEES via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, (
            f"COLLECT_FEES execution failed: {execution_result.error}"
        )
        print(
            f"Execution successful! "
            f"{len(execution_result.transaction_results)} transactions confirmed"
        )

        # Layer 3: Receipt Parsing -- fees0 / fees1 separate from principal.
        #
        # V4 COLLECT_FEES is implemented as DECREASE_LIQUIDITY(0) + TAKE_PAIR,
        # so the receipt MUST contain:
        #   * exactly one ModifyLiquidity event with liquidity_delta == 0
        #     (fees-only; no principal removed), and
        #   * USDC: a Transfer event from the PoolManager to the wallet
        #     carrying the fee amount.
        #   * Native ETH (currency0): NO Transfer event -- ETH moves as
        #     msg.value-style flow from the PoolManager. The wallet's
        #     native balance delta is the authoritative fee0 measurement.
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        saw_zero_delta_modify_liquidity = False
        usdc_fees_from_transfers = 0
        pool_manager_addr = parser.pool_manager  # lowercased in parser ctor
        wallet_lower = funded_wallet.lower()
        gas_spent_wei = 0

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if not tx_result.receipt:
                continue

            receipt_dict = tx_result.receipt.to_dict()
            parsed = parser.parse_receipt(receipt_dict)

            # Compute gas cost so we can isolate fee0 from gas in the
            # native-ETH balance delta.
            gas_used = receipt_dict.get("gasUsed") or receipt_dict.get("gas_used") or 0
            gas_price = receipt_dict.get("effectiveGasPrice")
            if gas_price is None:
                gas_price = receipt_dict.get("effective_gas_price")
            # Optimism is EIP-1559: ``effectiveGasPrice`` is always present.
            # Fail loudly if it is missing -- it signals an unexpected receipt
            # shape (e.g. a non-1559 tx type or a custom provider transform),
            # which would silently inflate ``eth_fees_received`` and let a
            # COLLECT_FEES no-op slip through the bilateral assertion below.
            assert gas_price is not None, (
                f"effectiveGasPrice missing from Optimism receipt -- tx={tx_result.tx_hash}. "
                f"Optimism is EIP-1559; absence indicates a receipt-shape regression."
            )
            gas_spent_wei += int(gas_used) * int(gas_price)

            for ml in parsed.modify_liquidity_events:
                print(f"  ModifyLiquidity: delta={ml.liquidity_delta}")
                if ml.liquidity_delta == 0:
                    saw_zero_delta_modify_liquidity = True

            for transfer in parsed.transfer_events:
                if (
                    transfer.from_address.lower() == pool_manager_addr
                    and transfer.to_address.lower() == wallet_lower
                ):
                    print(
                        f"  Fee Transfer: token={transfer.token[:10]}... "
                        f"amount={transfer.amount}"
                    )
                    if transfer.token.lower() == usdc_addr.lower():
                        usdc_fees_from_transfers += transfer.amount

        assert saw_zero_delta_modify_liquidity, (
            "V4 LP_COLLECT_FEES must emit a ModifyLiquidity event with "
            "liquidity_delta == 0 (fees-only path, no principal removed)."
        )

        # Layer 4: Balance Deltas + position-liquidity invariant.
        eth_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        # ETH delta excludes gas (gas was deducted from the wallet's ETH).
        eth_fees_received = (eth_after - eth_before) + gas_spent_wei
        usdc_delta = usdc_after - usdc_before

        print("\n--- Balance Deltas ---")
        print(f"ETH  fees (net of gas): {format_token_amount(eth_fees_received, 18)}")
        print(f"USDC delta:             {format_token_amount(usdc_delta, usdc_decimals)}")

        # Wallet MUST NOT lose tokens (net of gas) from a fee collection
        # (fees-only path). The wallet may pay gas in native ETH but the
        # COLLECT step itself can only return value.
        assert eth_fees_received >= 0, (
            f"Native ETH fee0 must be >= 0 after netting out gas. "
            f"eth_delta+gas={eth_fees_received}"
        )
        assert usdc_delta >= 0, "USDC fee1 must not decrease from fee collection"

        # USDC wallet delta MUST equal parsed Transfer amount exactly --
        # COLLECT_FEES routes USDC directly to the wallet (no unwrap), so
        # any USDC the wallet sees came from a PoolManager -> wallet
        # Transfer event the parser surfaced. This is the strict
        # "fees-vs-principal" separation for the ERC-20 leg.
        assert usdc_delta == usdc_fees_from_transfers, (
            f"USDC wallet delta must equal sum of PoolManager Transfer "
            f"amounts. delta={usdc_delta}, transfers="
            f"{usdc_fees_from_transfers}"
        )

        # Counter-swap routed through the SAME pool key as LP_OPEN
        # (asserted via currency0=0x0 above), so the position MUST have
        # accrued fees on at least one side. This is the bilateral
        # no-op guard for COLLECT_FEES: a tx that succeeds but moves no
        # tokens would silently pass without this assertion.
        #
        # The OR (rather than AND) handles a known Optimism quirk: the
        # deployed V4 PositionManager has been observed to leave principal
        # native ETH inside the PoolManager for LP_CLOSE (see VIB-4360 PR
        # body). The same path is used for fee TAKE_PAIR, so the
        # native-ETH fee leg may not reach the wallet on this chain. As
        # long as the USDC (ERC-20) fee leg fires, the COLLECT_FEES flow
        # is exercised end-to-end and the no-op bug class is caught.
        assert eth_fees_received > 0 or usdc_fees_from_transfers > 0, (
            f"Counter-swap was confirmed to route through the LP_OPEN "
            f"pool key, so the LP position MUST have accrued fees. "
            f"Got ETH={eth_fees_received}, USDC={usdc_fees_from_transfers}."
        )

        # Position-liquidity invariant: LP_COLLECT_FEES must NOT touch
        # principal liquidity. Query on-chain liquidity post-collect and
        # verify it matches the pre-LP-open liquidity exactly.
        liquidity_after = v4_sdk.get_position_liquidity(position_id)
        assert liquidity_after == liquidity_before, (
            f"LP_COLLECT_FEES must leave position liquidity unchanged. "
            f"before={liquidity_before}, after={liquidity_after}"
        )

        print(
            f"\nFees collected from position {position_id}: "
            f"ETH={eth_fees_received}, USDC={usdc_delta}"
        )
        print(
            f"Position liquidity invariant: {liquidity_before} == "
            f"{liquidity_after} (unchanged)"
        )
        print("\nALL 4 LAYERS PASSED")

    @pytest.mark.intent(IntentType.LP_COLLECT_FEES)  # noqa: layers
    @pytest.mark.asyncio
    async def test_collect_fees_without_position_id_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
    ):
        """V4 LP_COLLECT_FEES requires ``position_id`` in protocol_params.

        Compilation must fail with a clear error mentioning the missing
        ``position_id`` -- this is a hard precondition of
        ``_compile_collect_fees_uniswap_v4``.

        Intentional layer exception (``# noqa: layers``) -- this test stops at
        Layer 1 by design. The failure-path contract from
        ``.claude/rules/intent-tests.md`` is still honoured by snapshotting
        WETH/USDC around ``compiler.compile(...)`` and asserting both
        balances are unchanged after the failed compilation (matches the
        sibling pattern in ``test_uniswap_v4_lp_close.py``).
        """
        print(f"\n{'=' * 80}")
        print("Test: COLLECT_FEES without position_id (should fail)")
        print(f"{'=' * 80}")

        # Snapshot balances BEFORE compilation so we can assert conservation
        # after the compile-time failure (no transaction should be sent).
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        collect_intent = CollectFeesIntent(
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            # No protocol_params -- missing position_id
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail without position_id"
        )
        assert compilation_result.action_bundle is None, (
            "Failed compilation must not produce an ActionBundle"
        )
        assert compilation_result.error, "Compilation failed without an error message"
        assert "position_id" in compilation_result.error.lower(), (
            f"Error should mention position_id, got: {compilation_result.error}"
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
