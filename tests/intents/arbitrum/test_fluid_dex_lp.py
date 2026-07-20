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

from almanak.connectors.fluid.addresses import FLUID_DEX_LP_NATIVE_SENTINEL
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

# fSL5: FLUID (token0, 18 dec) / native ETH (token1, 18 dec) — the native-leg
# fixture (VIB-5121). The native ETH leg rides as msg.value and emits NO ERC-20
# Transfer, so the receipt parser leaves amount1 None and the runner measures it
# from a wallet native-balance bracket.
WRAPPER_FSL5 = "0x82C53239c4CFC89A8E55A691422af24c18A944b1"
FLUID_ADDRESS = "0x61E030A56D33e8260FdD81f03B162A79Fe3449Cd"

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
    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason=(
            "fSL9 (sUSDai/USDC) at governance maxSupplyShares cap on-chain as of 2026-07-20 (#3347) — "
            "Fluid error 51064 DexT1__SupplySharesOverflow, size-independent; strict: when governance "
            "raises the cap this XPASSes and CI forces removal of the marker"
        ),
    )
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
    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason=(
            "fSL9 (sUSDai/USDC) at governance maxSupplyShares cap on-chain as of 2026-07-20 (#3347) — "
            "Fluid error 51064 DexT1__SupplySharesOverflow, size-independent; strict: when governance "
            "raises the cap this XPASSes and CI forces removal of the marker"
        ),
    )
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
# Native-ETH leg accounting (VIB-5121) — fSL5 FLUID/native-ETH
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
@pytest.mark.no_zodiac(
    reason="VIB-5121 re-enables native-leg execution, but VIB-5125 deliberately "
    "EXCLUDES native-leg wrappers from the fluid_dex_lp Zodiac discovery "
    "static_permissions, so the native deposit selector is not in the Roles "
    "manifest. This test proves the native-leg ACCOUNTING mechanism (balance "
    "bracket), not Zodiac discovery; it runs EOA. Aligning native-wrapper "
    "discovery permissions is deferred until native fluid wrappers go "
    "deposit-open (the fSL5 pool currently caps deposits) — tracked in VIB-5135."
)
class TestFluidDexLpNativeLegArbitrum:
    """fSL5 native-ETH leg: 4-layer proof that the native leg is measured from a
    wallet native-balance bracket (the receipt parser leaves it None — Empty ≠
    Zero — and the runner's production capture fills it, gas-separated).

    Marked ``no_zodiac``: native-leg wrappers are excluded from the fluid_dex_lp
    synthetic Zodiac discovery surface (VIB-5125), so this EOA test exercises the
    accounting mechanism without the Roles Modifier blocking the native deposit."""

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_native_leg_balance_bracket(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        """LP_OPEN single-sided native ETH into fSL5: parser leaves amount1 None;
        the runner's native-balance-bracket capture measures the ETH leg."""
        from almanak.framework.intents.vocabulary import LPOpenIntent as _LPOpenIntent
        from almanak.framework.runner.strategy_runner import StrategyRunner

        deposit_eth = Decimal("0.01")
        expected_wei = int(deposit_eth * Decimal(10**18))

        # Layer 1: compile a single-sided native-ETH deposit (amount0=FLUID=0).
        intent = _LPOpenIntent(
            pool=WRAPPER_FSL5,
            amount0=Decimal("0"),
            amount1=deposit_eth,
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
        assert result.status.value == "SUCCESS", f"native LP_OPEN compile failed: {result.error}"
        # The native ETH leg must ride as the deposit tx's msg.value (Layer 1
        # proof of the native-leg encoding — VIB-5121 lifted the compile refusal).
        dep = _find_deposit_tx(result.action_bundle)
        assert int(dep.get("value", 0)) == expected_wei, "native ETH leg must ride as msg.value"

        eth_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        shares_before = get_token_balance(web3, WRAPPER_FSL5, funded_wallet)

        # Layer 2: execute on the fork in EOA mode. This native-leg ACCOUNTING
        # test is marked ``no_zodiac`` (native wrappers are deliberately excluded
        # from the fluid_dex_lp Zodiac discovery surface — VIB-5125), so there is
        # no Roles Modifier in the path to authorize against. EOA is also the mode
        # in which the full on-chain native path can run once the fSL5 pool cap
        # lifts, rather than staying double-blocked by the discovery manifest.
        exec_result = await orchestrator.execute(result.action_bundle)
        if not exec_result.success:
            # fSL5's FLUID/ETH SmartLending pool reverts EVERY deposit at the
            # current Arbitrum fork state with DexT1 error 51064 (selector
            # 0x2fee3e0e, arg 0xc778) — a Liquidity-Layer deposit/supply cap on
            # this pool, independent of amount or ratio (verified on-fork for
            # single-sided native, two-sided, and FLUID-only). This is a pool-
            # liveness / fixture constraint, NOT an accounting defect: the
            # native-leg balance-bracket MECHANISM is proven deterministically by
            # the unit suite (parser leaves the native leg None;
            # StrategyRunner._capture_native_lp_{open,close}_amounts_safe measures
            # it from a block-pinned bracket, gas-separated; the ledger stamp fills
            # only the None leg). Skip the on-chain layers honestly rather than
            # fake a pass. Tracked: the fSL5 deposit cap is a Fluid pool state, not
            # SDK behaviour.
            if "0x2fee3e0e" in (exec_result.error or "") or "c778" in (exec_result.error or ""):
                pytest.skip(
                    "fSL5 FLUID/ETH pool deposit is capped on-chain at this fork "
                    "(DexT1 51064 / 0xc778) — native-leg accounting mechanism is "
                    "covered deterministically by the unit suite; see VIB-5121"
                )
            raise AssertionError(f"native LP_OPEN execution failed: {exec_result.error}")

        # Layer 3: the receipt parser leaves the NATIVE leg None (honest
        # unmeasured — no ERC-20 Transfer for ETH), Empty ≠ Zero.
        parser = FluidDexLpReceiptParser()
        lp_open = None
        for tx_result in exec_result.transaction_results:
            if not tx_result.receipt:
                continue
            parsed = parser.extract_lp_open_data(tx_result.receipt.to_dict())
            if parsed is not None:
                lp_open = parsed
        assert lp_open is not None, "parser must decode the fSL5 share mint"
        assert lp_open.pool_address.lower() == WRAPPER_FSL5.lower()
        assert lp_open.amount1 is None, "native ETH leg must be None (unmeasured) — never a fabricated 0"
        assert lp_open.amount0 == 0, "unfunded ERC-20 (FLUID) leg is a measured 0"
        assert lp_open.currency1.lower() == FLUID_DEX_LP_NATIVE_SENTINEL.lower()
        # Bilateral Layer 4: the wrapper-share OUTPUT delta must match the
        # receipt-decoded minted liquidity (a native deposit is only half the
        # leg balances — the fSL5 shares are the other side).
        assert lp_open.liquidity and lp_open.liquidity > 0, "minted fSL5 shares must be measured"
        shares_after = get_token_balance(web3, WRAPPER_FSL5, funded_wallet)
        assert (shares_after - shares_before) == lp_open.liquidity, "fSL5 share delta must match receipt liquidity"

        # Layer 4: the runner's PRODUCTION capture measures the native leg from a
        # block-pinned balance bracket via the gateway interface (here the Anvil
        # eth_getBalance adapter), gas-separated. This is the exact production path.
        native_amounts = StrategyRunner._capture_native_lp_open_amounts_safe(
            intent=intent,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            result=exec_result,
            gateway_client=anvil_eth_call_adapter,
        )
        assert native_amounts is not None, "native bracket capture must measure the ETH leg"
        _a0, a1 = native_amounts
        # The measured deposit must equal the on-chain ETH deposited (excludes gas).
        assert a1 == expected_wei, f"native leg must equal the deposited ETH (gas-excluded): {a1} != {expected_wei}"

        # Cross-check the bracket against the raw wallet delta minus gas.
        eth_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        gas_wei = sum(
            (tr.receipt.gas_used * tr.receipt.effective_gas_price)
            for tr in exec_result.transaction_results
            if tr.receipt
        )
        assert (eth_before - eth_after - gas_wei) == expected_wei, "raw balance delta minus gas must equal deposit"
        logger.info("native LP_OPEN: measured ETH leg=%s wei (deposit), gas=%s wei", a1, gas_wei)

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_native_bracket_capture_over_real_fork_balances(
        self,
        web3: Web3,
        funded_wallet: str,
        anvil_eth_call_adapter,
    ):
        """Prove the production native-balance-bracket capture against REAL on-chain
        balances (the one piece the unit suite mocks): a real ETH-moving tx creates
        a measurable native delta; the capture reads block-pinned balances via the
        gateway interface and recovers ``deposit = (pre - post) - gas`` exactly.

        fSL5 deposits are pool-capped on-chain (see the test above), so this proves
        the gateway-backed bracket read end-to-end on live fork state independent of
        the undepositable pool — the native leg is keyed off the same currency
        sentinel + None-amount gate the parser emits for a native LP open.
        """
        from types import SimpleNamespace

        from almanak.framework.runner.strategy_runner import StrategyRunner

        send_wei = int(Decimal("0.05") * Decimal(10**18))
        sink = "0x000000000000000000000000000000000000dEaD"
        eth_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))

        # A real on-chain native-ETH transfer (the wallet spends send_wei + gas).
        tx_hash = web3.eth.send_transaction(
            {
                "from": Web3.to_checksum_address(funded_wallet),
                "to": Web3.to_checksum_address(sink),
                "value": send_wei,
            }
        )
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
        gas_wei = receipt["gasUsed"] * receipt["effectiveGasPrice"]
        eth_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        assert (eth_before - eth_after) == send_wei + gas_wei, "real on-chain delta sanity"

        # Build the result envelope a native LP_OPEN would carry: token0 ERC-20
        # (measured), token1 native (currency1 = native sentinel, amount1 None).
        lp_open = SimpleNamespace(
            amount0=0,
            amount1=None,
            currency0=FLUID_ADDRESS.lower(),
            currency1=FLUID_DEX_LP_NATIVE_SENTINEL.lower(),
        )
        result = SimpleNamespace(
            lp_open_data=lp_open,
            extracted_data={"lp_open_data": lp_open},
            transaction_results=[SimpleNamespace(success=True, receipt=SimpleNamespace(block_number=receipt["blockNumber"]))],
            total_gas_cost_wei=gas_wei,
        )

        # The PRODUCTION capture, reading REAL fork balances at the pinned blocks
        # via the gateway interface, must recover send_wei (the "deposit"),
        # gas-excluded.
        native_amounts = StrategyRunner._capture_native_lp_open_amounts_safe(
            intent=SimpleNamespace(),
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            result=result,
            gateway_client=anvil_eth_call_adapter,
        )
        assert native_amounts is not None
        _a0, a1 = native_amounts
        assert a1 == send_wei, f"bracket must recover the deposited ETH, gas-excluded: {a1} != {send_wei}"


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
