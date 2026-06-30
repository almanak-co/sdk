"""Curve single-sided LP_CLOSE via ``coin_index`` (VIB-5437).

LP_OPEN → single-sided LP_CLOSE round-trips on a real Ethereum Anvil fork, for
BOTH Curve pool families (their single-sided selectors differ):

* StableSwap (3pool): ``remove_liquidity_one_coin(uint256,int128,uint256)`` 0x1a4d01d2,
  min-out from ``calc_withdraw_one_coin(uint256,int128)`` 0xcc2b27d7.
* CryptoSwap (tricrypto2): ``remove_liquidity_one_coin(uint256,uint256,uint256)`` 0xf1dc3cc9,
  min-out from ``calc_withdraw_one_coin(uint256,uint256)`` 0x4fb08c5e.

Proof scope (per the VIB-5437 ↔ VIB-5433 split): compile → execute →
balance-delta. The single-sided distinguishing property is asserted directly on
chain — ONLY the target coin's balance increases (the other pool coins are
untouched), the LP is fully burned, and the received amount clears the
``calc_withdraw_one_coin`` min-out floor encoded into the calldata. The typed
accounting decode of the ``RemoveLiquidityOne`` event is sibling ticket VIB-5433,
so this test deliberately does NOT assert a layer-3 typed event / layer-5
accounting leg (the parser does not decode that event yet).

To run:
    uv run pytest tests/intents/ethereum/test_curve_single_sided_lp_close.py -v -s
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

# tricrypto2 = USDT(0) / WBTC(1) / WETH(2)
TRICRYPTO_LP = "0xc4AD29ba4B3c580e6D59105FFf484999997675Ff"
WBTC = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"

# 3pool = DAI(0) / USDC(1) / USDT(2)
THREEPOOL_LP = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
DAI = "0x6B175474E89094C44Da98b954EedeAC495271d0F"

USDT_SLOT = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDT"]
USDC_SLOT = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDC"]
WETH_SLOT = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["WETH"]


def _remove_one_min_out(action_bundle) -> int:
    """Decode the ``_min_amount`` (3rd arg) from the remove_liquidity_one_coin tx."""
    tx = next(t for t in action_bundle.transactions if t["tx_type"] == "remove_liquidity")
    body = tx["data"][10:] if tx["data"].startswith("0x") else tx["data"][8:]
    return int(body[128:192], 16)


@pytest.mark.ethereum
@pytest.mark.lp
class TestCurveSingleSidedLPClose:
    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_cryptoswap_single_sided_close_to_usdt(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """tricrypto2 (CryptoSwap): close the whole position into USDT only."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr, weth_addr = tokens["USDT"], tokens["WETH"]
        fund_erc20_token(funded_wallet, usdt_addr, int(Decimal("100") * Decimal(10**6)) * 10, USDT_SLOT, anvil_rpc_url)
        fund_erc20_token(
            funded_wallet, weth_addr, int(Decimal("0.05") * Decimal(10**18)) * 10, WETH_SLOT, anvil_rpc_url
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME, wallet_address=funded_wallet, price_oracle=price_oracle, rpc_url=anvil_rpc_url
        )
        parser = CurveReceiptParser(chain=CHAIN_NAME)

        open_intent = LPOpenIntent(
            pool="tricrypto2",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            coin_amounts=[Decimal("100"), Decimal("0"), Decimal("0.05")],
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

        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        wbtc_before = get_token_balance(web3, WBTC, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        lp_before = get_token_balance(web3, TRICRYPTO_LP, funded_wallet)

        # Single-sided exit: coin_index=0 (USDT).
        close_intent = LPCloseIntent(
            position_id=str(lp_received), pool="tricrypto2", protocol="curve", chain=CHAIN_NAME, coin_index=0
        )
        close_compiled = compiler.compile(close_intent)
        assert close_compiled.status.value == "SUCCESS", close_compiled.error
        meta = close_compiled.action_bundle.metadata
        assert meta["operation"] == "remove_liquidity_one_coin", meta
        assert meta["coin_index"] == 0
        min_out = _remove_one_min_out(close_compiled.action_bundle)
        assert min_out > 0, "min-out floor must be a real non-zero calc_withdraw_one_coin quote"

        close_exec = await orchestrator.execute(close_compiled.action_bundle)
        assert close_exec.success, close_exec.error

        lp_burned = lp_before - get_token_balance(web3, TRICRYPTO_LP, funded_wallet)
        usdt_back = get_token_balance(web3, usdt_addr, funded_wallet) - usdt_before
        wbtc_back = get_token_balance(web3, WBTC, funded_wallet) - wbtc_before
        weth_back = get_token_balance(web3, weth_addr, funded_wallet) - weth_before
        # Full position burns: lp_burned (wei) equals the pre-close balance, i.e.
        # nothing is left. (lp_received from the parser is in human units; compare
        # against the on-chain wei balance instead to avoid a unit mismatch.)
        assert lp_burned == lp_before and lp_burned > 0, f"full position must burn: burned={lp_burned} before={lp_before}"
        # Single-sided: ONLY USDT comes back; WBTC and WETH are untouched.
        assert usdt_back >= min_out > 0, f"USDT back {usdt_back} must clear floor {min_out}"
        assert wbtc_back == 0, f"WBTC must be untouched on a single-sided USDT exit, got {wbtc_back}"
        assert weth_back == 0, f"WETH must be untouched on a single-sided USDT exit, got {weth_back}"
        logger.info("tricrypto2 single-sided→USDT: burned %d LP, USDT=%d (floor %d)", lp_burned, usdt_back, min_out)

    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_stableswap_single_sided_close_to_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """3pool (StableSwap): close the whole position into USDC only."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr, usdt_addr = tokens["USDC"], tokens["USDT"]
        fund_erc20_token(funded_wallet, usdc_addr, int(Decimal("100") * Decimal(10**6)) * 10, USDC_SLOT, anvil_rpc_url)
        fund_erc20_token(funded_wallet, usdt_addr, int(Decimal("100") * Decimal(10**6)) * 10, USDT_SLOT, anvil_rpc_url)

        compiler = IntentCompiler(
            chain=CHAIN_NAME, wallet_address=funded_wallet, price_oracle=price_oracle, rpc_url=anvil_rpc_url
        )
        parser = CurveReceiptParser(chain=CHAIN_NAME)

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

        # Single-sided exit: coin_index=1 (USDC).
        close_intent = LPCloseIntent(
            position_id=str(lp_received), pool="3pool", protocol="curve", chain=CHAIN_NAME, coin_index=1
        )
        close_compiled = compiler.compile(close_intent)
        assert close_compiled.status.value == "SUCCESS", close_compiled.error
        meta = close_compiled.action_bundle.metadata
        assert meta["operation"] == "remove_liquidity_one_coin", meta
        assert meta["coin_index"] == 1
        min_out = _remove_one_min_out(close_compiled.action_bundle)
        assert min_out > 0, "min-out floor must be a real non-zero calc_withdraw_one_coin quote"

        close_exec = await orchestrator.execute(close_compiled.action_bundle)
        assert close_exec.success, close_exec.error

        lp_burned = lp_before - get_token_balance(web3, THREEPOOL_LP, funded_wallet)
        dai_back = get_token_balance(web3, DAI, funded_wallet) - dai_before
        usdc_back = get_token_balance(web3, usdc_addr, funded_wallet) - usdc_before
        usdt_back = get_token_balance(web3, usdt_addr, funded_wallet) - usdt_before
        # Full position burns: lp_burned (wei) equals the pre-close balance, i.e.
        # nothing is left. (lp_received from the parser is in human units; compare
        # against the on-chain wei balance instead to avoid a unit mismatch.)
        assert lp_burned == lp_before and lp_burned > 0, f"full position must burn: burned={lp_burned} before={lp_before}"
        # Single-sided: ONLY USDC comes back; DAI and USDT are untouched.
        assert usdc_back >= min_out > 0, f"USDC back {usdc_back} must clear floor {min_out}"
        assert dai_back == 0, f"DAI must be untouched on a single-sided USDC exit, got {dai_back}"
        assert usdt_back == 0, f"USDT must be untouched on a single-sided USDC exit, got {usdt_back}"
        logger.info("3pool single-sided→USDC: burned %d LP, USDC=%d (floor %d)", lp_burned, usdc_back, min_out)
