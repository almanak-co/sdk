"""Curve metapool (FRAX/3CRV) intent tests on Ethereum — Tier A + Tier B (VIB-5419).

A Curve METAPOOL is NATIVELY a 2-coin StableSwap pool ``[meta coin, base-pool
LP token]`` — here ``coins(0)=FRAX``, ``coins(1)=3CRV`` (the 3pool LP token).
It exposes two interfaces:

- **Tier A — native 2-coin**: the metapool IS a 2-coin pool. ``add_liquidity(
  [fraxAmt, 3crvAmt], min)`` / ``remove_liquidity`` / ``exchange(0, 1)`` route
  through the SAME flat-pool code paths as any 2-coin StableSwap — the base LP
  token (3CRV) is just ``coins[1]``.
- **Tier B — underlying (combined coin space)**: index 0 = meta coin (FRAX),
  indices 1..N = base-pool coins (DAI/USDC/USDT). ``exchange_underlying(i, j)``
  is on the metapool itself; the combined ``add_liquidity(uint256[N+1])`` /
  ``remove_liquidity`` route through the generic 3CRV DepositZap (whose ABI
  takes the pool as the first arg). Combined-index math verified on-chain:
  ``get_dy_underlying(0=FRAX, 2=USDC, 50e18)`` -> ~49.5e6 USDC.

This file proves all 4 intent-test layers (compile -> execute -> receipt parse
-> exact balance deltas) on a real Ethereum Anvil fork for:

  * Tier A native 2-coin metapool LP open -> close (deposit FRAX + 3CRV),
  * Tier B underlying LP deposit via the zap (FRAX + DAI, combined space),
  * Tier B underlying swap FRAX -> USDC via ``exchange_underlying``.

3CRV cannot be funded via storage-slot manipulation (Vyper HashMap layout), so
the wallet acquires REAL 3CRV by depositing DAI into the 3pool first — the way
users actually obtain it. FRAX is funded via its balanceOf storage slot (0).

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/ethereum/test_curve_metapool_lp.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.adapter import CURVE_POOLS
from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents._lp_setup_helpers import _send_via_orchestrator
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    fund_erc20_token,
    get_token_balance,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"

# Curve FRAX/3CRV factory metapool. Native coins: FRAX (0), 3CRV (1).
POOL = "frax_3crv"
POOL_ADDRESS = "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B"
LP_TOKEN = "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B"  # metapool IS its own LP token

# 3pool (used to acquire real 3CRV) — DAI(0)/USDC(1)/USDT(2)
THREEPOOL_ADDRESS = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"

# Token addresses
FRAX_ADDRESS = "0x853d955aCEf822Db058eb8505911ED77F175b99e"
THREE_CRV_ADDRESS = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
DAI_ADDRESS = "0x6B175474E89094C44Da98b954EedeAC495271d0F"

# balanceOf storage slots on Ethereum mainnet
FRAX_BALANCE_SLOT = 0  # FRAX FraxV2: balances at slot 0 (verified on-chain 2026-06-25)
DAI_BALANCE_SLOT = 2  # MakerDAO Dai.sol: balanceOf at slot 2

# Deposit amounts
LP_AMOUNT_FRAX = Decimal("100")
SETUP_DAI_FOR_3CRV = Decimal("300")  # DAI deposited into 3pool to mint ~288 3CRV
LP_AMOUNT_DAI_UNDERLYING = Decimal("100")
SWAP_AMOUNT_FRAX = Decimal("50")

# Selectors for the 3pool setup (acquire 3CRV)
ERC20_APPROVE_SELECTOR = "0x095ea7b3"
THREEPOOL_ADD_LIQUIDITY_3_SELECTOR = "0x4515cef3"  # add_liquidity(uint256[3],uint256)
MAX_UINT256 = 2**256 - 1


# =============================================================================
# Helpers
# =============================================================================


def _fund_frax(wallet: str, rpc_url: str, amount_frax: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with FRAX via storage slot manipulation (slot 0, 18 dec)."""
    amount_wei = int(amount_frax * Decimal(10**18))
    fund_erc20_token(wallet, FRAX_ADDRESS, amount_wei, FRAX_BALANCE_SLOT, rpc_url)


def _fund_dai(wallet: str, rpc_url: str, amount_dai: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with DAI via storage slot manipulation (slot 2, 18 dec)."""
    amount_wei = int(amount_dai * Decimal(10**18))
    fund_erc20_token(wallet, DAI_ADDRESS, amount_wei, DAI_BALANCE_SLOT, rpc_url)


def _pad_uint256(value: int) -> str:
    return hex(value)[2:].zfill(64)


def _pad_address(addr: str) -> str:
    return addr.lower().replace("0x", "").zfill(64)


async def _acquire_3crv(orchestrator: ExecutionOrchestrator, dai_amount: Decimal) -> None:
    """Deposit ``dai_amount`` DAI into the 3pool to mint real 3CRV to the wallet.

    Routes through the orchestrator (EOA under ``no_zodiac``) so it works under
    the standard execution path. The wallet must already hold the DAI.
    """
    dai_wei = int(dai_amount * Decimal(10**18))
    # approve(3pool, MAX)
    approve_data = bytes.fromhex(
        ERC20_APPROVE_SELECTOR[2:] + _pad_address(THREEPOOL_ADDRESS) + _pad_uint256(MAX_UINT256)
    )
    await _send_via_orchestrator(orchestrator, DAI_ADDRESS, approve_data, intent_type="SWAP")
    # add_liquidity([dai, 0, 0], 0)
    add_data = bytes.fromhex(
        THREEPOOL_ADD_LIQUIDITY_3_SELECTOR[2:]
        + _pad_uint256(dai_wei)
        + _pad_uint256(0)
        + _pad_uint256(0)
        + _pad_uint256(0)
    )
    await _send_via_orchestrator(orchestrator, THREEPOOL_ADDRESS, add_data, intent_type="LP_OPEN")


def _parse_add_liquidity(parser: CurveReceiptParser, execution_result) -> tuple[bool, Decimal | None]:
    """Return (add_liquidity_event_found, lp_tokens_minted_human)."""
    found = False
    lp_minted: Decimal | None = None
    for tx_result in execution_result.transaction_results:
        if not tx_result.receipt:
            continue
        receipt_dict = tx_result.receipt.to_dict()
        parse_result = parser.parse_receipt(receipt_dict)
        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
        for event in parse_result.events:
            if event.event_type == CurveEventType.ADD_LIQUIDITY:
                found = True
                logger.info(
                    "AddLiquidity event: token_amounts=%s supply=%s",
                    event.data.get("token_amounts"),
                    event.data.get("token_supply"),
                )
        minted = parser.extract_lp_tokens_received(receipt_dict)
        if minted is not None and minted > 0:
            lp_minted = minted
    return found, lp_minted


def _parse_remove_liquidity(parser: CurveReceiptParser, execution_result) -> bool:
    found = False
    for tx_result in execution_result.transaction_results:
        if not tx_result.receipt:
            continue
        receipt_dict = tx_result.receipt.to_dict()
        parse_result = parser.parse_receipt(receipt_dict)
        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
        for event in parse_result.events:
            if event.event_type == CurveEventType.REMOVE_LIQUIDITY:
                found = True
                logger.info("RemoveLiquidity event: token_amounts=%s", event.data.get("token_amounts"))
    return found


# =============================================================================
# Layer 1a: Pool configuration (no Anvil)
# =============================================================================


@pytest.mark.intent(IntentType.LP_OPEN)
class TestCurveMetapoolConfig:
    """Verify the FRAX/3CRV metapool is registered as a native 2-coin pool."""

    def test_metapool_registered(self):
        assert POOL in CURVE_POOLS["ethereum"], (
            f"'{POOL}' not in CURVE_POOLS['ethereum']: {list(CURVE_POOLS['ethereum'])}"
        )

    def test_native_two_coin_shape(self):
        pool = CURVE_POOLS["ethereum"][POOL]
        assert pool["n_coins"] == 2
        assert pool["coins"] == ["FRAX", "3CRV"]
        assert pool["coin_addresses"][0].lower() == FRAX_ADDRESS.lower()
        assert pool["coin_addresses"][1].lower() == THREE_CRV_ADDRESS.lower()
        # Metapool IS its own LP token.
        assert pool["lp_token"].lower() == POOL_ADDRESS.lower()

    def test_metapool_metadata(self):
        pool = CURVE_POOLS["ethereum"][POOL]
        assert pool["is_metapool"] is True
        assert pool["base_pool"].lower() == THREEPOOL_ADDRESS.lower()
        # Combined coin space: meta coin (0) + base-pool coins (1..3).
        assert pool["base_pool_coins"] == ["DAI", "USDC", "USDT"]
        assert pool["zap_address"]  # generic deposit zap configured


# =============================================================================
# Tier A — native 2-coin metapool LP open -> close
# =============================================================================


@pytest.mark.no_zodiac(reason="curve LP not in _LP_PROTOCOLS; manifest empty for curve LP")
@pytest.mark.ethereum
@pytest.mark.lp
class TestCurveMetapoolNativeLPLifecycle:
    """Tier A: native 2-coin metapool LP_OPEN (FRAX + 3CRV) then LP_CLOSE."""

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_native_lp_open_then_close(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Deposit FRAX + 3CRV natively, then close proportionally."""
        # --- Setup: fund FRAX + DAI, acquire real 3CRV via the 3pool ---
        _fund_frax(funded_wallet, anvil_rpc_url)
        _fund_dai(funded_wallet, anvil_rpc_url)
        await _acquire_3crv(orchestrator, SETUP_DAI_FOR_3CRV)

        crv3_balance_raw = get_token_balance(web3, THREE_CRV_ADDRESS, funded_wallet)
        assert crv3_balance_raw > 0, "Setup must mint 3CRV via the 3pool deposit"
        crv3_amount = Decimal(crv3_balance_raw) / Decimal(10**18)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # --- Layer 4 BEFORE (OPEN) ---
        frax_before = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        crv3_before = get_token_balance(web3, THREE_CRV_ADDRESS, funded_wallet)
        lp_before = get_token_balance(web3, LP_TOKEN, funded_wallet)

        # --- Layer 1: Compile native 2-coin LP_OPEN (coin_amounts=[FRAX, 3CRV]) ---
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            coin_amounts=[LP_AMOUNT_FRAX, crv3_amount],
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="curve",
            chain=CHAIN_NAME,
        )
        open_result = compiler.compile(open_intent)
        assert open_result.status.value == "SUCCESS", f"Native LP_OPEN compile failed: {open_result.error}"
        assert open_result.action_bundle is not None
        # All transactions target the metapool or token approvals (no zap).
        assert open_result.action_bundle.metadata["pool_address"].lower() == POOL_ADDRESS.lower()

        # --- Layer 2: Execute OPEN ---
        open_exec = await orchestrator.execute(open_result.action_bundle)
        assert open_exec.success, f"Native LP_OPEN execution failed: {open_exec.error}"

        # --- Layer 3: Parse OPEN receipt (AddLiquidity2) ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        add_found, lp_minted = _parse_add_liquidity(parser, open_exec)
        assert add_found, "AddLiquidity event must be found in native metapool LP_OPEN receipt"
        assert lp_minted is not None and lp_minted > 0, "LP tokens minted must be extractable from receipt"

        # --- Layer 4 AFTER (OPEN): exact deltas ---
        frax_after = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        crv3_after = get_token_balance(web3, THREE_CRV_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, LP_TOKEN, funded_wallet)

        frax_spent = frax_before - frax_after
        crv3_spent = crv3_before - crv3_after
        lp_received = lp_after - lp_before

        assert frax_spent == int(LP_AMOUNT_FRAX * Decimal(10**18)), (
            f"FRAX spent must EXACTLY equal deposit. Expected {int(LP_AMOUNT_FRAX * Decimal(10**18))}, got {frax_spent}"
        )
        assert crv3_spent == crv3_balance_raw, (
            f"3CRV spent must EXACTLY equal deposit. Expected {crv3_balance_raw}, got {crv3_spent}"
        )
        assert lp_received > 0, "Must receive metapool LP tokens (no-op guard)"
        assert Decimal(lp_received) / Decimal(10**18) == lp_minted, (
            f"LP delta ({Decimal(lp_received) / Decimal(10**18)}) must match receipt ({lp_minted})"
        )
        logger.info(
            "Native metapool LP_OPEN: spent %s FRAX + %s 3CRV, received %s FRAX3CRV-f",
            LP_AMOUNT_FRAX,
            crv3_amount,
            Decimal(lp_received) / Decimal(10**18),
        )

        # ==================== CLOSE ====================
        frax_before_close = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        crv3_before_close = get_token_balance(web3, THREE_CRV_ADDRESS, funded_wallet)
        lp_amount_str = str(lp_minted)

        close_intent = LPCloseIntent(
            position_id=lp_amount_str,
            pool=POOL,
            collect_fees=True,
            protocol="curve",
            chain=CHAIN_NAME,
        )
        close_result = compiler.compile(close_intent)
        assert close_result.status.value == "SUCCESS", f"Native LP_CLOSE compile failed: {close_result.error}"
        assert close_result.action_bundle is not None

        close_exec = await orchestrator.execute(close_result.action_bundle)
        assert close_exec.success, f"Native LP_CLOSE execution failed: {close_exec.error}"

        remove_found = _parse_remove_liquidity(parser, close_exec)
        assert remove_found, "RemoveLiquidity event must be found in native metapool LP_CLOSE receipt"

        # --- Layer 4 AFTER (CLOSE): native close returns FRAX + 3CRV ---
        lp_after_close = get_token_balance(web3, LP_TOKEN, funded_wallet)
        frax_after_close = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        crv3_after_close = get_token_balance(web3, THREE_CRV_ADDRESS, funded_wallet)

        lp_burned = lp_after - lp_after_close
        frax_returned = frax_after_close - frax_before_close
        crv3_returned = crv3_after_close - crv3_before_close

        assert lp_burned > 0, "Metapool LP tokens must be burned during LP_CLOSE"
        assert frax_returned > 0, "Must receive FRAX back from native metapool LP_CLOSE"
        assert crv3_returned > 0, "Must receive 3CRV (base-LP) back from native metapool LP_CLOSE"
        logger.info(
            "Native metapool LP_CLOSE: burned %s LP, received %s FRAX + %s 3CRV",
            lp_burned / 1e18,
            frax_returned / 1e18,
            crv3_returned / 1e18,
        )


# =============================================================================
# Tier B — underlying LP deposit (zap) + underlying swap
# =============================================================================


@pytest.mark.no_zodiac(reason="curve LP not in _LP_PROTOCOLS; manifest empty for curve LP")
@pytest.mark.ethereum
@pytest.mark.lp
class TestCurveMetapoolUnderlyingLP:
    """Tier B: underlying deposit over the combined coin space via the zap."""

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_underlying_lp_open_then_close(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Deposit FRAX + DAI over the combined space (zap), then close natively.

        coin_amounts is the COMBINED vector [FRAX, DAI, USDC, USDT]; its length
        (4) — not the native 2 — signals the underlying/zap deposit path. The
        zap deposits DAI into the base pool then [FRAX, base-LP] into the
        metapool, so the wallet only spends FRAX + DAI.
        """
        _fund_frax(funded_wallet, anvil_rpc_url)
        _fund_dai(funded_wallet, anvil_rpc_url)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        frax_before = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        dai_before = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        lp_before = get_token_balance(web3, LP_TOKEN, funded_wallet)

        # --- Layer 1: Compile underlying LP_OPEN (combined 4-vector) ---
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            # [FRAX(meta), DAI, USDC, USDT] — combined coin space, length 4.
            coin_amounts=[LP_AMOUNT_FRAX, LP_AMOUNT_DAI_UNDERLYING, Decimal("0"), Decimal("0")],
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="curve",
            chain=CHAIN_NAME,
        )
        open_result = compiler.compile(open_intent)
        assert open_result.status.value == "SUCCESS", f"Underlying LP_OPEN compile failed: {open_result.error}"
        assert open_result.action_bundle is not None
        # The deposit tx must target the ZAP, not the metapool directly.
        zap_addr = CURVE_POOLS["ethereum"][POOL]["zap_address"].lower()
        deposit_txs = [tx for tx in open_result.action_bundle.transactions if tx["to"].lower() == zap_addr]
        assert deposit_txs, (
            f"Underlying deposit must target the zap {zap_addr}; "
            f"targets={[tx['to'] for tx in open_result.action_bundle.transactions]}"
        )

        # --- Layer 2: Execute OPEN ---
        open_exec = await orchestrator.execute(open_result.action_bundle)
        assert open_exec.success, f"Underlying LP_OPEN execution failed: {open_exec.error}"

        # --- Layer 3: Parse OPEN receipt. The metapool emits its native
        # AddLiquidity in [FRAX, base-LP] space (the zap deposits the base coins
        # into the 3pool first, then [FRAX, 3CRV] into the metapool). A zap
        # deposit produces TWO mint-from-zero Transfers (3CRV then the metapool
        # LP), so the authoritative minted-LP amount is the metapool LP-token
        # balance delta below; here we assert the AddLiquidity event is present. ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        add_found, _ = _parse_add_liquidity(parser, open_exec)
        assert add_found, "AddLiquidity event must be found in underlying (zap) LP_OPEN receipt"

        # --- Layer 4 AFTER (OPEN): exact underlying-coin deltas ---
        frax_after = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        dai_after = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, LP_TOKEN, funded_wallet)

        frax_spent = frax_before - frax_after
        dai_spent = dai_before - dai_after
        lp_received = lp_after - lp_before

        assert frax_spent == int(LP_AMOUNT_FRAX * Decimal(10**18)), (
            f"FRAX spent must EXACTLY equal deposit. Expected {int(LP_AMOUNT_FRAX * Decimal(10**18))}, got {frax_spent}"
        )
        assert dai_spent == int(LP_AMOUNT_DAI_UNDERLYING * Decimal(10**18)), (
            f"DAI spent must EXACTLY equal underlying deposit. "
            f"Expected {int(LP_AMOUNT_DAI_UNDERLYING * Decimal(10**18))}, got {dai_spent}"
        )
        assert lp_received > 0, "Must receive metapool LP tokens via the zap (no-op guard)"
        logger.info(
            "Underlying (zap) LP_OPEN: spent %s FRAX + %s DAI, received %s FRAX3CRV-f",
            LP_AMOUNT_FRAX,
            LP_AMOUNT_DAI_UNDERLYING,
            Decimal(lp_received) / Decimal(10**18),
        )

        # ==================== CLOSE (native proportional) ====================
        frax_before_close = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        crv3_before_close = get_token_balance(web3, THREE_CRV_ADDRESS, funded_wallet)

        # Close the full position. The authoritative minted-LP amount is the
        # metapool LP-token balance delta (a zap deposit's receipt carries an
        # intermediate 3CRV mint that the generic first-mint receipt parse would
        # confuse), so close exactly ``lp_received``.
        lp_received_human = Decimal(lp_received) / Decimal(10**18)
        close_intent = LPCloseIntent(
            position_id=str(lp_received_human),
            pool=POOL,
            collect_fees=True,
            protocol="curve",
            chain=CHAIN_NAME,
        )
        close_result = compiler.compile(close_intent)
        assert close_result.status.value == "SUCCESS", f"LP_CLOSE compile failed: {close_result.error}"
        assert close_result.action_bundle is not None

        close_exec = await orchestrator.execute(close_result.action_bundle)
        assert close_exec.success, f"LP_CLOSE execution failed: {close_exec.error}"

        remove_found = _parse_remove_liquidity(parser, close_exec)
        assert remove_found, "RemoveLiquidity event must be found in LP_CLOSE receipt"

        lp_after_close = get_token_balance(web3, LP_TOKEN, funded_wallet)
        frax_after_close = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        crv3_after_close = get_token_balance(web3, THREE_CRV_ADDRESS, funded_wallet)

        lp_burned = lp_after - lp_after_close
        frax_returned = frax_after_close - frax_before_close
        crv3_returned = crv3_after_close - crv3_before_close

        assert lp_burned > 0, "Metapool LP tokens must be burned during LP_CLOSE"
        assert frax_returned > 0, "Must receive FRAX back from metapool LP_CLOSE"
        assert crv3_returned > 0, "Must receive 3CRV (base-LP) back from native LP_CLOSE"
        logger.info(
            "Underlying LP_CLOSE (native proportional): burned %s LP, received %s FRAX + %s 3CRV",
            lp_burned / 1e18,
            frax_returned / 1e18,
            crv3_returned / 1e18,
        )


# =============================================================================
# Tier B — underlying swap FRAX -> USDC via exchange_underlying
# =============================================================================


@pytest.mark.no_zodiac(
    reason="curve metapool exchange_underlying selector (0xd0a9bf58) is not in the "
    "synthetic-intent manifest, which derives only the native exchange selector (VIB-5419)"
)
@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.ethereum
@pytest.mark.swap
class TestCurveMetapoolUnderlyingSwap:
    """Tier B: combined-index swap FRAX -> USDT through the metapool.

    No NATIVE Curve pool carries FRAX/USDT (only ``frax_usdc`` exists), so the
    compiler falls back to the metapool's COMBINED coin space and routes through
    ``exchange_underlying`` (combined indices 0 = FRAX -> 3 = USDT).

    ``no_zodiac``: the synthetic-intent permission matrix derives the NATIVE
    ``exchange`` selector for curve SWAP, not ``exchange_underlying``
    (0xd0a9bf58), so the auto-derived Roles manifest would block this call.
    Wiring the underlying selector into the matrix is follow-up work.
    """

    @pytest.mark.asyncio
    async def test_frax_to_usdt_underlying_swap(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """FRAX -> USDT via exchange_underlying (combined indices 0 -> 3)."""
        _fund_frax(funded_wallet, anvil_rpc_url)

        usdt_addr = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]["USDT"]

        # --- Layer 4 BEFORE (bilateral) ---
        frax_before = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)

        # --- Layer 1: Compile underlying SWAP. No native pool carries FRAX/USDT;
        # the metapool's combined space does -> exchange_underlying. ---
        intent = SwapIntent(
            from_token="FRAX",
            to_token="USDT",
            amount=SWAP_AMOUNT_FRAX,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="curve",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Underlying swap compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None
        # The swap tx must target the metapool (exchange_underlying lives there).
        assert compilation_result.action_bundle.metadata["pool_address"].lower() == POOL_ADDRESS.lower()
        swap_txs = [
            tx for tx in compilation_result.action_bundle.transactions if tx["to"].lower() == POOL_ADDRESS.lower()
        ]
        assert swap_txs, "Underlying swap must target the metapool contract"

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Underlying swap execution failed: {execution_result.error}"

        # --- Layer 3: Parse receipt (TokenExchangeUnderlying) ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        swap_event = None
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
            for event in parse_result.events:
                if event.event_type == CurveEventType.TOKEN_EXCHANGE_UNDERLYING:
                    swap_event = event
                    logger.info(
                        "TokenExchangeUnderlying: sold_id=%s tokens_sold=%s bought_id=%s tokens_bought=%s",
                        event.data.get("sold_id"),
                        event.data.get("tokens_sold"),
                        event.data.get("bought_id"),
                        event.data.get("tokens_bought"),
                    )
        assert swap_event is not None, "TokenExchangeUnderlying event must be found in underlying swap receipt"
        # The event carries the combined indices and amounts directly: FRAX is
        # combined index 0, USDT is combined index 3.
        assert swap_event.data["sold_id"] == 0, "FRAX must be combined index 0 (meta coin)"
        assert swap_event.data["bought_id"] == 3, "USDT must be combined index 3 (3rd base coin)"
        assert swap_event.data["tokens_sold"] == int(SWAP_AMOUNT_FRAX * Decimal(10**18)), (
            "Event tokens_sold must equal the FRAX input"
        )
        assert swap_event.data["tokens_bought"] > 0, "Event tokens_bought must be positive"

        # --- Layer 4 AFTER: exact bilateral deltas ---
        frax_after = get_token_balance(web3, FRAX_ADDRESS, funded_wallet)
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)

        frax_spent = frax_before - frax_after
        usdt_received = usdt_after - usdt_before

        assert frax_spent == int(SWAP_AMOUNT_FRAX * Decimal(10**18)), (
            f"FRAX spent must EXACTLY equal swap amount. "
            f"Expected {int(SWAP_AMOUNT_FRAX * Decimal(10**18))}, got {frax_spent}"
        )
        # USDT out is ~stable-1:1 minus fee; assert positive (no-op guard) and
        # a sane lower bound (>= 90% of input, accounting for fee + slippage).
        assert usdt_received > 0, "Must receive USDT (no-op guard)"
        assert usdt_received >= int(SWAP_AMOUNT_FRAX * Decimal("0.90") * Decimal(10**6)), (
            f"USDT out implausibly low for a stable underlying swap: {usdt_received}"
        )
        logger.info(
            "Underlying swap FRAX -> USDT: spent %s FRAX, received %s USDT",
            SWAP_AMOUNT_FRAX,
            Decimal(usdt_received) / Decimal(10**6),
        )
