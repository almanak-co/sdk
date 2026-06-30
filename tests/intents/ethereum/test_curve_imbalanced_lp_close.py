"""Curve imbalanced LP_CLOSE via ``imbalanced_amounts`` (VIB-5438).

LP_OPEN → imbalanced LP_CLOSE round-trip on a real Ethereum Anvil fork.
``remove_liquidity_imbalance(uint256[N] amounts, uint256 max_burn_amount)`` is the
StableSwap-only exit where you name the EXACT per-coin amounts OUT and the pool
burns however much LP is needed, capped at ``max_burn_amount``. The safety floor
is therefore a MAX-BURN CEILING (the inverse of a min-out), sized fail-closed from
the pool's on-chain ``calc_token_amount(amounts, is_deposit=False)``.

Selectors verified on real mainnet 2026-06-29 against 3pool (0xbEbc44…):
``calc_token_amount(uint256[3],bool)`` = 0x3883e119 (is_deposit=False → LP burned),
``remove_liquidity_imbalance(uint256[3],uint256)`` = 0x9fdaea0c (a too-tight
max_burn reverts "Slippage screwed you"; an adequate one succeeds).

All 4 verification layers:
1. Compile  — compiler emits ``remove_liquidity_imbalance`` with a bounded max_burn.
2. Execute  — the tx lands on the Anvil fork.
3. Parse    — the VIB-5433 ``RemoveLiquidityImbalance`` decode books a proceeds leg
             for EVERY requested coin (no zero-proceeds ghost).
4. Balance  — the wallet receives the EXACT requested per-coin amounts and the LP
             burned is ≤ max_burn.

To run:
    uv run pytest tests/intents/ethereum/test_curve_imbalanced_lp_close.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.receipt_parser import CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import CHAIN_CONFIGS, fund_erc20_token, get_token_balance

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_zodiac(reason="curve LP not in _LP_PROTOCOLS; manifest empty for curve LP")

CHAIN_NAME = "ethereum"

# 3pool = DAI(0) / USDC(1) / USDT(2)
THREEPOOL_LP = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
DAI = "0x6B175474E89094C44Da98b954EedeAC495271d0F"

USDC_SLOT = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDC"]
USDT_SLOT = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDT"]


def _imbalance_max_burn(action_bundle, n_coins: int) -> int:
    """Decode ``max_burn_amount`` from the remove_liquidity_imbalance tx.

    Fixed-array layout: ``amounts[N], max_burn`` → max_burn is the last word.
    """
    tx = next(t for t in action_bundle.transactions if t["tx_type"] == "remove_liquidity_imbalance")
    body = tx["data"][10:] if tx["data"].startswith("0x") else tx["data"][8:]
    return int(body[-64:], 16)


@pytest.mark.ethereum
@pytest.mark.lp
class TestCurveImbalancedLPClose:
    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_stableswap_imbalanced_close_exact_amounts(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """3pool (StableSwap): close a position to EXACT per-coin amounts.

        Open with USDC + USDT, then withdraw an imbalanced vector
        (0 DAI + 30 USDC + 20 USDT) and assert each requested coin's balance
        increases by EXACTLY the requested amount, DAI stays untouched, the LP
        burned clears max_burn, and the parser books a proceeds leg per coin.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr, usdt_addr = tokens["USDC"], tokens["USDT"]
        # Fund generously so LP_OPEN has plenty (10x each).
        fund_erc20_token(funded_wallet, usdc_addr, int(Decimal("100") * Decimal(10**6)) * 10, USDC_SLOT, anvil_rpc_url)
        fund_erc20_token(funded_wallet, usdt_addr, int(Decimal("100") * Decimal(10**6)) * 10, USDT_SLOT, anvil_rpc_url)

        compiler = IntentCompiler(
            chain=CHAIN_NAME, wallet_address=funded_wallet, price_oracle=price_oracle, rpc_url=anvil_rpc_url
        )
        parser = CurveReceiptParser(chain=CHAIN_NAME)

        # --- LP_OPEN: deposit 100 USDC + 100 USDT ---
        open_intent = LPOpenIntent(
            pool="3pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            coin_amounts=[Decimal("0"), Decimal("100"), Decimal("100")],  # USDC + USDT
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="curve",
            chain=CHAIN_NAME,
        )
        open_compiled = compiler.compile(open_intent)
        assert open_compiled.status.value == "SUCCESS", open_compiled.error
        open_exec = await orchestrator.execute(open_compiled.action_bundle)
        assert open_exec.success, open_exec.error
        lp_received = None
        for tr in open_exec.transaction_results:
            if tr.receipt:
                minted = parser.extract_lp_tokens_received(tr.receipt.to_dict())
                if minted and minted > 0:
                    lp_received = minted
        assert lp_received and lp_received > 0, "LP_OPEN must mint LP"

        dai_before = get_token_balance(web3, DAI, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        lp_before = get_token_balance(web3, THREEPOOL_LP, funded_wallet)

        # --- LP_CLOSE imbalanced: exactly 0 DAI + 30 USDC + 20 USDT ---
        want_usdc = 30 * 10**6
        want_usdt = 20 * 10**6
        close_intent = LPCloseIntent(
            position_id=str(lp_received),
            pool="3pool",
            protocol="curve",
            chain=CHAIN_NAME,
            imbalanced_amounts=[Decimal("0"), Decimal("30"), Decimal("20")],
        )

        # Layer 1: compile → remove_liquidity_imbalance with a bounded max_burn.
        close_compiled = compiler.compile(close_intent)
        assert close_compiled.status.value == "SUCCESS", close_compiled.error
        meta = close_compiled.action_bundle.metadata
        assert meta["operation"] == "remove_liquidity_imbalance", meta
        assert meta["imbalanced_amounts"] == ["0", "30", "20"], meta
        max_burn = _imbalance_max_burn(close_compiled.action_bundle, n_coins=3)
        # Bounded, sane ceiling — never unbounded (the core theft-vector invariant).
        assert 0 < max_burn < 2**256 - 1, f"max_burn must be bounded, got {max_burn}"
        assert max_burn <= lp_before, f"max_burn {max_burn} must not exceed held LP {lp_before}"

        # Layer 2: execute.
        close_exec = await orchestrator.execute(close_compiled.action_bundle)
        assert close_exec.success, close_exec.error

        # Layer 3: parse — every requested coin books a proceeds leg (no ghost).
        close_data = None
        for tr in close_exec.transaction_results:
            if tr.receipt:
                cd = parser.extract_lp_close_data(tr.receipt.to_dict())
                if cd is not None:
                    close_data = cd
        assert close_data is not None, "imbalanced close must decode to LPCloseData (no ghost)"
        # DAI (coin 0) is a measured zero (not None); USDC (coin 1) + USDT (coin 2) book proceeds.
        assert close_data.amount0_collected == 0, close_data
        assert close_data.amount1_collected == want_usdc, close_data
        assert close_data.additional_amounts == {2: want_usdt}, close_data

        # Layer 4: balance deltas — EXACT per-coin amounts; DAI untouched.
        lp_burned = lp_before - get_token_balance(web3, THREEPOOL_LP, funded_wallet)
        dai_back = get_token_balance(web3, DAI, funded_wallet) - dai_before
        usdc_back = get_token_balance(web3, usdc_addr, funded_wallet) - usdc_before
        usdt_back = get_token_balance(web3, usdt_addr, funded_wallet) - usdt_before
        assert usdc_back == want_usdc, f"USDC back {usdc_back} must EXACTLY equal requested {want_usdc}"
        assert usdt_back == want_usdt, f"USDT back {usdt_back} must EXACTLY equal requested {want_usdt}"
        assert dai_back == 0, f"DAI must be untouched on this imbalanced exit, got {dai_back}"
        # LP burned is positive and within the ceiling.
        assert 0 < lp_burned <= max_burn, f"LP burned {lp_burned} must be >0 and <= max_burn {max_burn}"
        # Imbalanced (not full) close: residual LP remains (we only pulled 50 of ~200).
        assert lp_burned < lp_before, "imbalanced partial withdrawal must leave residual LP"
        logger.info(
            "3pool imbalanced close: burned %d LP (max_burn %d), DAI=%d USDC=%d USDT=%d",
            lp_burned,
            max_burn,
            dai_back,
            usdc_back,
            usdt_back,
        )
