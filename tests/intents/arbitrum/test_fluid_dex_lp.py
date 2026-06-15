"""Fluid SmartLending DEX LP intent tests on Arbitrum (VIB-5032, Phase 4).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for Fluid's
fungible ERC-20-share DEX LP surface (``protocol="fluid_dex_lp"``):

1. LPOpenIntent: single-sided USDC deposit into the fSL9 (sUSDai/USDC)
   SmartLending wrapper (approve + ``deposit(token0Amt, token1Amt, minShares,
   to)``). The wrapper IS the whitelisted DEX supplier — direct pool LP is
   gated (``DexT1__UserSupplyInNotOn`` 51013).
2. LPCloseIntent: ``withdraw(token0Amt, token1Amt, maxShares, to)`` sized from
   the live resolver share->token read; burns shares, returns both legs.
3. FluidDexLpReceiptParser decodes the fungible Transfer money-path
   (share mint/burn + token legs); position_id = wrapper, fees = None.
4. EXACT wallet balance deltas + wrapper share ``balanceOf`` movement.

Plus the two no-silent-failure gates from the UAT card:
- **D3.1** deposit-disabled: LP_OPEN on fSL12 (RLP/USDC, supply-off) is REFUSED
  at compile by the live 51013 pre-flight — no transaction is produced.
- **D3.2** slippage: the ``minShares`` floor encoded in the deposit calldata is
  exactly ``floor(quote_shares * (1 - tolerance))`` and tightens with a
  smaller tolerance (non-tautological).

Fungible-LP discipline (curve/aerodrome-classic precedent): no NFT, no tick
range — the wrapper share balance IS the position. LPOpenIntent passes dummy
positive ranges (required by validation for non-tick protocols).

ZODIAC NOTE (VIB-5125): this test runs under the default-on ``ZodiacOrchestrator``
(no ``@pytest.mark.no_zodiac``). The connector is wired into the synthetic
discovery matrix via STATIC permissions in
``almanak/connectors/fluid_dex_lp/permission_hints.py`` (token ``approve`` +
wrapper ``deposit`` / ``withdraw``), because ``fluid_dex_lp``'s compile path is
RPC-bound (the 51013 deposit-enabled pre-flight + the live close-balance read)
and so cannot land selectors through offline compilation — the same reason
TraderJoe V2 pins its LP selectors statically. The two compile-FAILED guard
tests (fSL12 deposit-disabled, slippage) never reach execution, so they don't
exercise Zodiac authorisation; the open / open-then-close lifecycle tests do.

ARCHIVE NOTE: the resolver struct read (``getSmartLendingEntireData``) and the
DEX deposit path touch storage that a non-archive public RPC cannot serve at a
historical fork block ("metadata is not found"). The Arbitrum intent-test fork
uses the configured archive RPC (ALCHEMY_API_KEY) at latest, so depth is
available; fSL9-enabled / fSL12-disabled are persistent across blocks, asserted
dynamically rather than pinned.

NO MOCKING. All tests execute real on-chain transactions on an Anvil fork.

To run:
    uv run pytest tests/intents/arbitrum/test_fluid_dex_lp.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.fluid.dex_lp_receipt_parser import FluidDexLpReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import get_token_balance

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration (verified on-chain — validation report 2026-06-12)
# =============================================================================

CHAIN_NAME = "arbitrum"
# NOTE: intent constructions below use the string literal protocol="fluid_dex_lp"
# (not a module constant) so the AST-based intent-coverage gate
# (scripts/ci/check_intent_coverage.py) can credit the (connector, intent, chain)
# triples — it resolves only literal protocol= kwargs.

# fSL9: sUSDai (token0, 18 dec) / USDC (token1, 6 dec) — the enabled round-trip
# fixture. USDC is in the standard funded_wallet token set; single-sided USDC
# deposit is on-chain-verified to succeed.
WRAPPER_FSL9 = "0x1F0bFd9862ae58208d26db0d80797974434EC013"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
SUSDAI_ADDRESS = "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"

# fSL12: RLP/USDC — supply OFF (the deposit-disabled negative fixture).
WRAPPER_FSL12 = "0xdC1dF9E55f3B7EBD4F19001b294d1e537320BC2E"

USDC_DECIMALS = 6
DEPOSIT_AMOUNT_USDC = Decimal("100")

# deposit(uint256 token0Amt, uint256 token1Amt, uint256 minShares, address to)
# selector(4) + token0Amt(32) + token1Amt(32) + minShares(32) + to(32)
_DEPOSIT_SELECTOR = "0xfad3cc4b"


def _share_balance(web3: Web3, wallet: str) -> int:
    """Wallet's fSL9 wrapper share balance (the position)."""
    return get_token_balance(web3, WRAPPER_FSL9, wallet)


def _decode_deposit_min_shares(deposit_tx: dict) -> int:
    """Decode the ``minShares`` (3rd arg) from a deposit calldata blob."""
    data = deposit_tx["data"]
    if data.startswith("0x"):
        data = data[2:]
    assert "0x" + data[:8] == _DEPOSIT_SELECTOR, f"not a deposit() calldata: 0x{data[:8]}"
    # arg2 (minShares) lives at byte offset 4 + 2*32 = 68 -> hex chars 8 + 128
    word = data[8 + 128 : 8 + 192]
    return int(word, 16)


def _find_deposit_tx(action_bundle) -> dict:
    """Return the deposit transaction (the one carrying the deposit selector)."""
    for tx in action_bundle.transactions:
        data = tx.get("data", "") if isinstance(tx, dict) else getattr(tx, "data", "")
        if data and ("0x" + data[2:10] if data.startswith("0x") else "0x" + data[:8]) == _DEPOSIT_SELECTOR:
            return tx if isinstance(tx, dict) else tx.to_dict()
    raise AssertionError("no deposit() transaction in the LP_OPEN bundle")


# =============================================================================
# LP_OPEN / LP_CLOSE lifecycle
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestFluidDexLpLifecycleArbitrum:
    """Full Fluid SmartLending DEX LP lifecycle on Arbitrum: open then close."""

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_single_sided_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """LP_OPEN: single-sided USDC deposit into fSL9 mints wrapper shares."""
        usdc_before = get_token_balance(web3, USDC_ADDRESS, funded_wallet)
        shares_before = _share_balance(web3, funded_wallet)
        expected_in = int(DEPOSIT_AMOUNT_USDC * Decimal(10**USDC_DECIMALS))
        assert usdc_before >= expected_in, (
            f"funded_wallet must hold >= {DEPOSIT_AMOUNT_USDC} USDC; got {usdc_before / 10**6} "
            "— check the arbitrum conftest funding fixture"
        )

        # Layer 1: compile (token0=sUSDai=0 single-sided; token1=USDC)
        intent = LPOpenIntent(
            pool=WRAPPER_FSL9,
            amount0=Decimal("0"),
            amount1=DEPOSIT_AMOUNT_USDC,
            range_lower=Decimal("1"),  # dummy — fungible, no tick range
            range_upper=Decimal("1000000"),  # dummy — required by LPOpenIntent validation
            protocol="fluid_dex_lp",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"LP_OPEN compile failed: {result.error}"
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["wrapper"].lower() == WRAPPER_FSL9.lower()

        # Layer 2: execute
        exec_result = await orchestrator.execute(result.action_bundle)
        assert exec_result.success, f"LP_OPEN execution failed: {exec_result.error}"

        # Layer 3: receipt parse (fungible Transfer money-path)
        parser = FluidDexLpReceiptParser()
        lp_open = None
        for tx_result in exec_result.transaction_results:
            if not tx_result.receipt:
                continue
            parsed = parser.extract_lp_open_data(tx_result.receipt.to_dict())
            if parsed is not None:
                lp_open = parsed
        assert lp_open is not None, "FluidDexLpReceiptParser must decode the LP_OPEN share mint"
        assert lp_open.position_id == 0, "fungible LP: position_id is 0 (no NFT)"
        assert lp_open.pool_address.lower() == WRAPPER_FSL9.lower()
        assert lp_open.liquidity and lp_open.liquidity > 0, "minted shares must be measured"
        assert lp_open.amount1 == expected_in, (
            f"receipt USDC leg must equal the deposit amount: expected {expected_in}, got {lp_open.amount1}"
        )

        # Layer 4: exact wallet delta + wrapper shares minted
        usdc_after = get_token_balance(web3, USDC_ADDRESS, funded_wallet)
        shares_after = _share_balance(web3, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        assert usdc_spent == expected_in, (
            f"USDC spent must EXACTLY equal the deposit amount: expected {expected_in}, got {usdc_spent}"
        )
        shares_minted = shares_after - shares_before
        assert shares_minted > 0, "wallet must hold fSL9 shares after LP_OPEN"
        assert shares_minted == lp_open.liquidity, "balance-delta shares must match the receipt-decoded shares"
        logger.info("LP_OPEN: USDC spent=%s, shares minted=%s", usdc_spent, shares_minted)

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_open_then_close(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Full round-trip: open single-sided USDC, then close (drain both legs)."""
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # ---------------- OPEN ----------------
        open_intent = LPOpenIntent(
            pool=WRAPPER_FSL9,
            amount0=Decimal("0"),
            amount1=DEPOSIT_AMOUNT_USDC,
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="fluid_dex_lp",
            chain=CHAIN_NAME,
        )
        open_result = compiler.compile(open_intent)
        assert open_result.status.value == "SUCCESS", f"LP_OPEN compile failed: {open_result.error}"
        open_exec = await orchestrator.execute(open_result.action_bundle)
        assert open_exec.success, f"LP_OPEN execution failed: {open_exec.error}"

        shares_after_open = _share_balance(web3, funded_wallet)
        assert shares_after_open > 0, "must hold fSL9 shares before close"

        # ---------------- CLOSE ----------------
        usdc_before_close = get_token_balance(web3, USDC_ADDRESS, funded_wallet)
        susdai_before_close = get_token_balance(web3, SUSDAI_ADDRESS, funded_wallet)

        close_intent = LPCloseIntent(
            position_id=WRAPPER_FSL9,  # fungible: the wrapper IS the position id
            pool=WRAPPER_FSL9,
            collect_fees=True,
            protocol="fluid_dex_lp",
            chain=CHAIN_NAME,
        )
        close_result = compiler.compile(close_intent)
        assert close_result.status.value == "SUCCESS", f"LP_CLOSE compile failed: {close_result.error}"
        assert close_result.action_bundle is not None
        close_exec = await orchestrator.execute(close_result.action_bundle)
        assert close_exec.success, f"LP_CLOSE execution failed: {close_exec.error}"

        # Layer 3: receipt parse the burn + returned legs
        parser = FluidDexLpReceiptParser()
        lp_close = None
        for tx_result in close_exec.transaction_results:
            if not tx_result.receipt:
                continue
            parsed = parser.extract_lp_close_data(tx_result.receipt.to_dict())
            if parsed is not None:
                lp_close = parsed
        assert lp_close is not None, "FluidDexLpReceiptParser must decode the LP_CLOSE share burn"
        assert lp_close.pool_address.lower() == WRAPPER_FSL9.lower()
        assert lp_close.fees0 is None and lp_close.fees1 is None, "fungible LP: fees auto-compound (Empty != Zero)"
        assert lp_close.liquidity_removed and lp_close.liquidity_removed > 0, "shares burned must be measured"

        # Layer 4: shares burned + tokens returned
        shares_after_close = _share_balance(web3, funded_wallet)
        usdc_after_close = get_token_balance(web3, USDC_ADDRESS, funded_wallet)
        susdai_after_close = get_token_balance(web3, SUSDAI_ADDRESS, funded_wallet)

        shares_burned = shares_after_open - shares_after_close
        usdc_returned = usdc_after_close - usdc_before_close
        susdai_returned = susdai_after_close - susdai_before_close

        assert shares_burned > 0, "LP_CLOSE must burn wrapper shares"
        # Single-sided USDC open leaves a proportional claim on BOTH pool legs,
        # so close returns USDC and/or sUSDai — at least one leg must increase.
        assert (usdc_returned + susdai_returned) > 0, "LP_CLOSE must return at least one token leg to the wallet"
        assert usdc_returned == lp_close.amount1_collected, "USDC delta must match the receipt-decoded close leg"
        assert susdai_returned == lp_close.amount0_collected, "sUSDai delta must match the receipt-decoded close leg"
        logger.info(
            "LP_CLOSE: shares burned=%s, USDC returned=%s, sUSDai returned=%s",
            shares_burned,
            usdc_returned,
            susdai_returned,
        )


# =============================================================================
# No-silent-failure gates (UAT card D3)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestFluidDexLpGuards:
    """Compile-time guards: deposit-disabled refusal + slippage floor."""

    @pytest.mark.intent(IntentType.LP_OPEN)
    def test_lp_open_deposit_disabled_refused_at_compile(
        self,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """D3.1 — LP_OPEN on fSL12 (supply OFF) is refused by the live 51013 pre-flight."""
        intent = LPOpenIntent(
            pool=WRAPPER_FSL12,
            amount0=Decimal("0"),
            amount1=DEPOSIT_AMOUNT_USDC,
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="fluid_dex_lp",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        result = compiler.compile(intent)
        assert result.status.value != "SUCCESS", "deposit-disabled wrapper must NOT compile to a transaction"
        assert result.action_bundle is None, "no transaction may be produced for a disabled pool"
        assert "disabled" in (result.error or "").lower(), (
            f"error must name the deposit-disabled condition, got: {result.error!r}"
        )

    @pytest.mark.intent(IntentType.LP_OPEN)
    def test_lp_open_slippage_min_shares_floor(
        self,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """D3.2 — minShares in the deposit calldata == floor(quote * (1 - tol)); tighter tol -> higher floor."""
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        def _compile_with_tol(tol: str):
            intent = LPOpenIntent(
                pool=WRAPPER_FSL9,
                amount0=Decimal("0"),
                amount1=DEPOSIT_AMOUNT_USDC,
                range_lower=Decimal("1"),
                range_upper=Decimal("1000000"),
                protocol="fluid_dex_lp",
                chain=CHAIN_NAME,
                protocol_params={"max_slippage": tol},
            )
            res = compiler.compile(intent)
            assert res.status.value == "SUCCESS", f"compile failed (tol={tol}): {res.error}"
            return res

        # 5% tolerance
        res_loose = _compile_with_tol("0.05")
        quote_loose = int(res_loose.action_bundle.metadata["quote_shares"])
        meta_min_loose = int(res_loose.action_bundle.metadata["min_shares"])
        calldata_min_loose = _decode_deposit_min_shares(_find_deposit_tx(res_loose.action_bundle))
        expected_loose = int(Decimal(quote_loose) * (Decimal(1) - Decimal("0.05")))
        assert meta_min_loose == expected_loose, "metadata min_shares must be floor(quote*(1-tol))"
        assert calldata_min_loose == expected_loose, "calldata minShares must equal the slippage floor (not the quote)"
        assert calldata_min_loose < quote_loose, "minShares must be strictly below the quote (non-tautological)"

        # 0.5% tolerance — a tighter bound must raise the floor
        res_tight = _compile_with_tol("0.005")
        calldata_min_tight = _decode_deposit_min_shares(_find_deposit_tx(res_tight.action_bundle))
        assert calldata_min_tight > calldata_min_loose, "tighter slippage tolerance must raise the minShares floor"
