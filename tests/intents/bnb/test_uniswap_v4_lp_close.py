"""4-layer intent tests for Uniswap V4 LP_CLOSE on BNB Chain Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
closing V4 LP positions via PositionManager on BNB Chain:
1. Open a BNB/USDT LP position (LP_OPEN as setup -- ``BNB`` symbol so
   currency0 resolves to ``address(0)`` via
   ``UniswapV4Adapter._resolve_token(for_v4_pool=True)``, matching the
   most liquid initialized V4 pool key on BNB Chain)
2. Create LPCloseIntent with position_id and protocol_params
3. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
4. Execute via ExecutionOrchestrator (full production pipeline)
5. Parse receipts using UniswapV4ReceiptParser (liquidity removed,
   tokens returned)
6. Verify bilateral balance deltas: native BNB (net of gas) and USDT
   both strictly positive

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

VIB-4372 / VIB-4343: registry edit (adding "bsc" to uniswap_v4 declared
chains) is OUT OF SCOPE for this ticket. The ``no_zodiac`` marker is required
because uniswap_v4 is not in the synthetic_intents manifest matrix.

Pool selection: ``BNB/USDT/3000``. On-chain probe (verified 2026-05-14
against PoolManager 0x28e2Ea09... on BNB Chain) confirmed that the
``(NATIVE_BNB, USDT, 3000, 60, 0x0)`` pool is initialized with
sqrtPriceX96 ~= 2.054e30 (tick=65102, price ~672 USDT/BNB) and
liquidity ~= 5.59e21 — substantial liquidity, well-suited for a small
two-sided LP position. The Native/USDC pool also exists but with much
lower liquidity (~3e15). USDT is the canonical liquid stablecoin venue
on BNB Chain.

Using ``BNB`` makes ``_resolve_token(for_v4_pool=True)`` substitute
``address(0)`` for the wrapped-native WBNB so the LP routes through
the native-key pool. As on Avalanche, the BNB V4 StateView only exposes
the ``bytes32`` overload of ``getSlot0`` (the SDK calls the tuple
overload), so the adapter degrades to oracle-fallback sqrtPrice
estimation — requiring the "BNB" symbol to be present in the price
oracle (see ``_augment_oracle_with_bnb``).

BNB-mainnet-state quirk (EIP-7702): ``TEST_WALLET = 0xf39F...`` (Anvil
account #0) has signed an EIP-7702 SetCode delegation
(``0xef0100<delegate>``) on BNB mainnet that auto-forwards incoming
native BNB to an external address. Inherited by the Anvil fork, this
swallows the ``TAKE_PAIR`` payout silently — the wallet's
``eth.get_balance`` does not change on close. The test clears that
delegation via ``anvil_setCode`` before LP_OPEN; this is consistent
with production user-wallet behaviour (no delegation set). See the
inline note in ``test_lp_close_bnb_usdt``.

To run:
    uv run pytest tests/intents/bnb/test_uniswap_v4_lp_close.py -v -s
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

# Framework canonical chain name for BNB Chain. The conftest exposes both
# "bsc" and "bnb" aliases in CHAIN_CONFIGS, but the V4 adapter / SDK route
# off the chain name passed to IntentCompiler, and the BNB conftest uses
# "bsc" (matches chain_id=56 lookup order).
CHAIN_NAME = "bsc"

# BNB/USDT pool with 0.3% fee tier. Token-ordering by address resolves the
# native-native pair as:
#   NATIVE  (0x0000000000000000000000000000000000000000)  -- currency0
#   USDT    (0x55d398326f99059fF775485246999027B3197955)  -- currency1
# The ``BNB`` symbol triggers ``_resolve_token(for_v4_pool=True)`` to
# substitute ``address(0)`` for the wrapped-native WBNB address, matching
# the most liquid initialized V4 venue on BNB Chain (~5.59e21 liquidity
# vs ~3e15 for the Native/USDC alternative).
LP_POOL = "BNB/USDT/3000"

# Setup LP_OPEN sizing.
# Choose amounts so that the *native BNB* side is the binding constraint
# (i.e. ``liq0 <= liq1`` in the V4 SDK's ``compute_liquidity_from_amounts``
# min-of formula). This mirrors VIB-4368's avalanche golden, which deposits
# 1 AVAX / 10 USDC with a wide range — the position's binding leg is AVAX,
# and TAKE_PAIR on close returns ~all 1 AVAX (matching the no-op-guard
# bilateral assertion).
#
# Why the native side MUST be binding (debugging 2026-05-14):
# When the binding leg is the ERC-20 side, the V4 PositionManager's
# SETTLE_PAIR consumes the cushioned ``amount0_max`` (= 1.30 × requested
# at the LP-minimum 30% slippage because the on-chain ``getSlot0`` query
# reverts on BNB Chain's StateView — the tuple overload is missing, same
# as Avalanche per VIB-4368). The in-range liquidity computation only
# *needs* a fraction of that, but the difference is not refunded for
# native currency on the SDK's ``[MINT_POSITION, SETTLE_PAIR]`` action
# pair (no SWEEP). The position is then opened with disproportionately
# small native principal, and the closing TAKE_PAIR returns near-zero
# native BNB — failing the bilateral delta check that mirrors the
# avalanche golden's no-op guard.
#
# At fork price ~672 USDT per BNB and range [200, 2000], the natural
# in-range ratio is roughly ~729 USDT per 1 BNB. Picking
# ``LP_AMOUNT_BNB = 1`` and ``LP_AMOUNT_USDT = 800`` keeps BNB as the
# binding constraint with a comfortable USDT cushion — the position
# absorbs ~1 BNB and ~729 USDT, and the close returns measurable
# native BNB.
LP_AMOUNT_BNB = Decimal("1")
LP_AMOUNT_USDT = Decimal("800")

# Wide price range in USDT-per-BNB terms to ensure both tokens are
# deposited at the current ~$672 price.
# range_lower=200 -> BNB at $200
# range_upper=2000 -> BNB at $2000 (matches the bnb V3 LP test's range
# logic of a wide window around the live mid-price).
LP_RANGE_LOWER = Decimal("200")
LP_RANGE_UPPER = Decimal("2000")


# =============================================================================
# Helper: oracle augmentation
# =============================================================================


def _augment_oracle_with_bnb(
    price_oracle: dict[str, Decimal],
) -> dict[str, Decimal]:
    """Return a copy of the session oracle with a ``BNB`` entry.

    The session-scoped oracle is built from
    ``CHAIN_CONFIGS["bsc"]["tokens"]`` which lists ``WBNB`` (the ERC-20
    wrapper) but NOT the bare ``BNB`` symbol. ``LP_POOL =
    "BNB/USDT/3000"`` triggers the V4 adapter to substitute
    ``address(0)`` for the wrapped-native WBNB address at the pool key
    layer, but it still reads ``price_oracle.get(token0_symbol.upper())``
    (= ``"BNB"``) when the on-chain ``StateView.getSlot0`` query reverts
    (BNB's deployed StateView only exposes the ``bytes32`` overload;
    the SDK calls the tuple overload). Without a ``BNB`` price the
    fallback degrades to the tick-range midpoint, which is wildly off
    from the real ~$672 USDT/BNB price and produces a one-sided
    position that fails the bilateral LP_CLOSE delta check.

    BNB and WBNB have the same USD price by construction (1:1 wrap),
    so reusing the WBNB price is correct and preserves the
    session-scoped invariant.
    """
    augmented = dict(price_oracle)
    if "BNB" not in augmented and "WBNB" in augmented:
        augmented["BNB"] = augmented["WBNB"]
    return augmented


# =============================================================================
# Helper: Open a position (setup for close tests)
# =============================================================================


async def _open_v4_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> tuple[int, int, str, str]:
    """Open a V4 LP position and return ``(position_id, liquidity, currency0, currency1)``.

    Self-sufficient setup that mirrors the avalanche / polygon / optimism /
    base / arbitrum / ethereum LP_CLOSE goldens so VIB-4372 can land without
    depending on VIB-4371's parallel LP_OPEN file. Uses the ``BNB/USDT/3000``
    native-key pool (see ``LP_POOL`` comment) which matches the most liquid
    V4 venue on BNB Chain.

    Raises AssertionError if the setup LP_OPEN fails.
    """
    intent = LPOpenIntent(
        pool=LP_POOL,
        amount0=LP_AMOUNT_BNB,
        amount1=LP_AMOUNT_USDT,
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
# LPCloseIntent Tests -- Uniswap V4 on BNB Chain
# =============================================================================


@pytest.mark.bsc
@pytest.mark.lp
class TestUniswapV4LPCloseIntent:
    """Test Uniswap V4 LP_CLOSE using LPCloseIntent on BNB Chain.

    These tests verify the full LP close flow:
    - First open a position (setup) on the native BNB/USDT pool
    - LPCloseIntent creation with position_id and protocol_params
    - IntentCompiler routes to UniswapV4Adapter.compile_lp_close_intent()
    - Transactions execute successfully on-chain via PositionManager
    - UniswapV4ReceiptParser correctly extracts close data
    - Balance changes match expected token returns (native BNB net of gas,
      plus USDT), both strictly positive
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_bnb_usdt(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test full LP_OPEN -> LP_CLOSE lifecycle for BNB/USDT via V4 on BNB Chain.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> lp_close_data extracted
        4. Balance Deltas: native BNB (net of gas) and USDT both returned
           from the pool (principal + fees)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]

        usdt_decimals = get_token_decimals(web3, usdt_addr)
        # Native BNB has 18 decimals (matches WBNB). ``web3.eth.get_balance``
        # returns the raw native balance in wei.
        bnb_decimals = 18

        # Fail-fast funding check: surface infra/fixture funding regressions
        # before LP_OPEN runs and produces a less-actionable error.
        # The wallet pays gas in native BNB, so we need headroom above the
        # raw LP amount. The bnb conftest seeds the EOA with 100 BNB; with
        # ``LP_AMOUNT_BNB = 1`` (BNB-binding) and the 30% LP-slippage
        # cushion ``amount0_max = 1.30 BNB``, the wallet must hold at
        # least 1.5 BNB plus gas before LP_OPEN.
        bnb_available = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_available = get_token_balance(web3, usdt_addr, funded_wallet)
        bnb_required = int(
            (LP_AMOUNT_BNB + Decimal("0.5")) * (Decimal(10) ** bnb_decimals)
        )
        usdt_required = int(LP_AMOUNT_USDT * (Decimal(10) ** usdt_decimals))
        assert bnb_available >= bnb_required, (
            f"Insufficient native BNB funding for setup LP_OPEN: "
            f"have={bnb_available}, need>={bnb_required}"
        )
        assert usdt_available >= usdt_required, (
            f"Insufficient USDT funding for setup LP_OPEN: "
            f"have={usdt_available}, need>={usdt_required}"
        )

        print(f"\n{'=' * 80}")
        print("Test: LP_CLOSE BNB/USDT via Uniswap V4 on BNB Chain")
        print(f"{'=' * 80}")

        # Augment the session oracle with a BNB -> price entry derived
        # from WBNB. See ``_augment_oracle_with_bnb`` for the full
        # rationale (BNB StateView only exposes the bytes32 form
        # of getSlot0, so the V4 adapter degrades to oracle fallback).
        augmented_oracle = _augment_oracle_with_bnb(price_oracle)

        # EIP-7702 delegation cleanup (BNB-mainnet-state artifact):
        # ``TEST_WALLET = 0xf39F...`` is Anvil's first account, which on BNB
        # mainnet has signed an EIP-7702 SetCode delegation
        # (``0xef0100<delegate>``) pointing at a forwarder contract that
        # transfers all incoming native BNB out to an external address.
        # The Anvil fork inherits this code, so any native BNB returned by
        # ``TAKE_PAIR`` lands in the wallet and is immediately forwarded —
        # ``web3.eth.get_balance(funded_wallet)`` shows 0 delta and the
        # bilateral LP_CLOSE assertion silently fails. Clearing the code on
        # the fork is the correct test-time fix: an EOA without delegation
        # is exactly what user wallets look like in production.
        # No effect on Avalanche / Polygon / Optimism / Base / Arbitrum
        # because ``0xf39F...`` has no code on those chains.
        if Web3.to_checksum_address(funded_wallet) == Web3.to_checksum_address(
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        ):
            existing_code = web3.eth.get_code(Web3.to_checksum_address(funded_wallet))
            # Compare the raw byte prefix (``b"\xef\x01\x00"``) rather than
            # ``.hex().startswith("ef0100")``: ``HexBytes.hex()`` returns the
            # unprefixed hex string on hexbytes>=1.0 but the 0x-prefixed form
            # on older releases, so a string-prefix check is version-fragile
            # (codex flagged this on the initial PR). Bytes-level comparison
            # is unambiguous and prefix-independent.
            if bytes(existing_code[:3]) == b"\xef\x01\x00":
                web3.provider.make_request(
                    "anvil_setCode",
                    [funded_wallet, "0x"],
                )
                print(
                    f"Cleared EIP-7702 delegation on {funded_wallet}: "
                    f"0x{existing_code.hex().removeprefix('0x')} -> 0x"
                )

        # Setup: Open a position first
        print("\n--- Setup: Opening LP position ---")
        position_id, liquidity, currency0, currency1 = await _open_v4_position(
            web3, funded_wallet, orchestrator, augmented_oracle,
        )
        print(f"Opened position: id={position_id}, liquidity={liquidity}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # Verify the LP pool key is the native-BNB pool (currency0 must be
        # address(0)). If LP_OPEN ever shifted to a non-native pool key, the
        # bilateral close-side delta check below would silently break.
        assert int(currency0, 16) == 0, (
            f"LP_OPEN must use native BNB as currency0 so the LP routes "
            f"through the most liquid V4 venue on BNB Chain "
            f"(currency0=0x0). Got: {currency0}"
        )
        assert currency1.lower() == usdt_addr.lower(), (
            f"LP_OPEN currency1 must be USDT. Got: {currency1}"
        )

        # Record balances BEFORE the close.
        # Native BNB is currency0 of the LP pool; USDT is currency1.
        # ``web3.eth.get_balance`` returns the native BNB balance; the gas
        # spent on the LP_CLOSE tx itself is accounted for explicitly below.
        bnb_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)

        print("\n--- Closing LP position ---")
        print(f"BNB before close:  {format_token_amount(bnb_before, bnb_decimals)}")
        print(f"USDT before close: {format_token_amount(usdt_before, usdt_decimals)}")

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
            price_oracle=augmented_oracle,
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
        # Track gas spent on the close txs so we can isolate the native BNB
        # principal/fees from the native gas burn in the Layer 4 delta check.
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        lp_close_data = None
        gas_spent_wei = 0

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if not tx_result.receipt:
                continue

            receipt_dict = tx_result.receipt.to_dict()

            # Compute gas cost so we can isolate native BNB returns from
            # the native gas burn in the Layer 4 delta below.
            gas_used = receipt_dict.get("gasUsed") or receipt_dict.get("gas_used") or 0
            gas_price = receipt_dict.get("effectiveGasPrice")
            if gas_price is None:
                gas_price = receipt_dict.get("effective_gas_price")
            # BNB Chain supports EIP-1559 since Pascal hardfork (2024). If
            # ``effectiveGasPrice`` is missing, fail loudly -- it signals an
            # unexpected receipt shape, which would silently inflate the
            # ``bnb_received`` net-of-gas value below and let a no-op
            # LP_CLOSE slip through the bilateral assertion.
            assert gas_price is not None, (
                f"effectiveGasPrice missing from BNB Chain receipt -- "
                f"tx={tx_result.tx_hash}. BNB Chain supports EIP-1559 since "
                f"the Pascal hardfork; absence indicates a receipt-shape regression."
            )
            gas_spent_wei += int(gas_used) * int(gas_price)

            # Exercise parse_receipt() entrypoint -- this is the surface
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
        # Parser MUST report a positive amount on the ERC-20 side (USDT).
        # On a native-key V4 pool (currency0 = address(0)), the native BNB
        # leg flows out of the PoolManager WITHOUT a Transfer event -- the
        # parser sums tokens by walking Transfer events from the
        # PoolManager, so only the USDT transfer surfaces here. Native BNB
        # is therefore measured via the eth.get_balance delta in Layer 4
        # below, not via the parser. The parser assigns the single ERC-20
        # transfer to ``amount0_collected`` because it sorts by token
        # address and USDT is the only key present; this is a parser-naming
        # artefact, not a semantic claim about pool currency0.
        assert lp_close_data.amount0_collected is not None and lp_close_data.amount0_collected > 0, (
            "Parser must extract positive USDT collection from LP_CLOSE receipt "
            "(surfaces as amount0_collected on native-key V4 pools because USDT "
            "is the only ERC-20 transfer the parser walks)"
        )

        # Layer 4: Balance Deltas -- wallet gains native BNB (net of gas)
        # AND USDT (principal + any fees).
        bnb_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)

        # Native BNB delta excludes gas (gas was deducted from the wallet's
        # native balance). Adding ``gas_spent_wei`` back isolates the
        # BNB returned by TAKE_PAIR for currency0.
        bnb_received = (bnb_after - bnb_before) + gas_spent_wei
        usdt_received = usdt_after - usdt_before

        print("\n--- Balance Deltas ---")
        print(f"BNB received (net of gas):  {format_token_amount(bnb_received, bnb_decimals)}")
        print(f"USDT received:              {format_token_amount(usdt_received, usdt_decimals)}")

        # MANDATORY bilateral delta (see .claude/rules/intent-tests.md and
        # #1691): the position was opened with both tokens, so closing it
        # MUST return both. Permitting `or` here would let a V4
        # one-sided-close bug pass.
        assert bnb_received > 0 and usdt_received > 0, (
            f"LP_CLOSE on a two-token position must return BOTH tokens "
            f"(no-op guard). bnb_received={bnb_received} (net of gas), "
            f"usdt_received={usdt_received}"
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

        Intentional layer exception (``# noqa: layers``) -- this test stops at
        Layer 1 by design. The failure-path contract from
        ``.claude/rules/intent-tests.md`` is still honoured by snapshotting
        native BNB and USDT around ``compiler.compile(...)`` and asserting
        both balances are unchanged after the failed compilation.
        """
        print(f"\n{'=' * 80}")
        print("Test: LP_CLOSE without liquidity (should fail compilation)")
        print(f"{'=' * 80}")

        # Snapshot balances BEFORE compilation so we can assert conservation
        # after the compile-time failure (no transaction should be sent).
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        bnb_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)

        # Use a token id well above any minted position on BNB V4 at
        # fork time so the on-chain ``get_position_liquidity`` query returns
        # 0 and the compiler must fall back to the protocol_params-required
        # error path. (Matches the deliberately out-of-range value used in
        # ``tests/intents/avalanche/test_uniswap_v4_lp_close.py`` and the
        # polygon / optimism / base siblings.)
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
            price_oracle=_augment_oracle_with_bnb(price_oracle),
        )

        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail without liquidity in protocol_params"
        )
        assert compilation_result.action_bundle is None, (
            "Failed compilation must not produce an ActionBundle"
        )
        assert compilation_result.error is not None, (
            "FAILED compilation must surface an error message; missing one would "
            "mask the actual failure and trip an obscure AttributeError below."
        )
        assert "liquidity" in compilation_result.error.lower(), (
            f"Error should mention liquidity requirement, got: {compilation_result.error}"
        )

        # Failure-path balance conservation: no on-chain tx fired, balances
        # unchanged. Note: native BNB strictly equality here because no tx
        # was submitted at all -- compile-time failure means no gas was
        # spent.
        bnb_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        assert bnb_after == bnb_before, (
            f"Native BNB balance must be unchanged after compile-time "
            f"failure. before={bnb_before}, after={bnb_after}"
        )
        assert usdt_after == usdt_before, (
            f"USDT balance must be unchanged after compile-time failure. "
            f"before={usdt_before}, after={usdt_after}"
        )

        print(f"Compilation failed as expected: {compilation_result.error}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
