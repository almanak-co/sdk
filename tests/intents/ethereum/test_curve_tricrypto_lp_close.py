"""Curve Tricrypto2 (CryptoSwap) LP_CLOSE RemoveLiquidity decode (VIB-5491).

A proportional CryptoSwap LP_CLOSE emits the old-style 3-coin
``RemoveLiquidity(address,uint256[3],uint256)`` event, which the parser did not
decode → a teardown accounting ghost (LP burned on-chain, zero typed events).
This LP_OPEN → LP_CLOSE round-trip on a real Ethereum Anvil fork proves the
event now decodes and the LP_CLOSE books a typed event through the real
accounting pipeline.

Tricrypto2 = USDT(0) / WBTC(1) / WETH(2). The fork funds USDT + WETH.

To run:
    uv run pytest tests/intents/ethereum/test_curve_tricrypto_lp_close.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._curve_lp_layer5_helpers import assert_curve_lp_layer5, enrich_for_accounting
from tests.intents.conftest import CHAIN_CONFIGS, fund_erc20_token, get_token_balance

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_zodiac(reason="curve LP not in _LP_PROTOCOLS; manifest empty for curve LP")

CHAIN_NAME = "ethereum"
POOL = "tricrypto2"
POOL_ADDRESS = "0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"
LP_TOKEN = "0xc4AD29ba4B3c580e6D59105FFf484999997675Ff"  # crv3crypto
WBTC = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"  # tricrypto2 middle leg (coin 1)

LP_AMOUNT_USDT = Decimal("100")
LP_AMOUNT_WETH = Decimal("0.05")

USDT_SLOT = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDT"]
WETH_SLOT = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["WETH"]


@pytest.mark.ethereum
@pytest.mark.lp
class TestCurveTricryptoLPClose:
    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_open_then_close_decodes_removeliquidity(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr, weth_addr = tokens["USDT"], tokens["WETH"]
        fund_erc20_token(funded_wallet, usdt_addr, int(LP_AMOUNT_USDT * Decimal(10**6)) * 10, USDT_SLOT, anvil_rpc_url)
        fund_erc20_token(funded_wallet, weth_addr, int(LP_AMOUNT_WETH * Decimal(10**18)) * 10, WETH_SLOT, anvil_rpc_url)

        compiler = IntentCompiler(
            chain=CHAIN_NAME, wallet_address=funded_wallet, price_oracle=price_oracle, rpc_url=anvil_rpc_url
        )
        parser = CurveReceiptParser(chain=CHAIN_NAME)

        # --- LP_OPEN (to get a real LP position to close) ---
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            coin_amounts=[LP_AMOUNT_USDT, Decimal("0"), LP_AMOUNT_WETH],
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="curve",
            chain=CHAIN_NAME,
        )
        open_compiled = compiler.compile(open_intent)
        assert open_compiled.status.value == "SUCCESS", open_compiled.error
        assert open_compiled.action_bundle is not None, "LP_OPEN must produce an ActionBundle"
        open_exec = await orchestrator.execute(open_compiled.action_bundle)
        assert open_exec.success, open_exec.error

        lp_received: Decimal | None = None
        for tr in open_exec.transaction_results:
            if tr.receipt:
                minted = parser.extract_lp_tokens_received(tr.receipt.to_dict())
                if minted and minted > 0:
                    lp_received = minted
        assert lp_received is not None and lp_received > 0, "LP_OPEN must mint LP"

        # --- LP_CLOSE (proportional remove → RemoveLiquidity 3-coin crypto event) ---
        # Snapshot ALL three coins (incl. WBTC, the middle leg the decode must carry),
        # so the decoded amounts can be checked against exact on-chain deltas below.
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        wbtc_before = get_token_balance(web3, WBTC, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        lp_before = get_token_balance(web3, LP_TOKEN, funded_wallet)

        close_intent = LPCloseIntent(
            position_id=str(lp_received), pool=POOL, collect_fees=True, protocol="curve", chain=CHAIN_NAME
        )
        close_compiled = compiler.compile(close_intent)
        assert close_compiled.status.value == "SUCCESS", f"LP_CLOSE compile failed: {close_compiled.error}"
        assert close_compiled.action_bundle is not None, "LP_CLOSE must produce an ActionBundle"
        close_exec = await orchestrator.execute(close_compiled.action_bundle)
        assert close_exec.success, f"LP_CLOSE execution failed: {close_exec.error}"
        close_exec = enrich_for_accounting(
            close_exec,
            close_intent,
            funded_wallet,
            chain=CHAIN_NAME,
            bundle_metadata=close_compiled.action_bundle.metadata,
        )

        # --- Layer 3: the RemoveLiquidity event now decodes (was a ghost) ---
        remove_seen = False
        remove_amounts = None
        for tr in close_exec.transaction_results:
            if not tr.receipt:
                continue
            pr = parser.parse_receipt(tr.receipt.to_dict())
            assert pr.success, pr.error
            for ev in pr.events:
                if ev.event_type == CurveEventType.REMOVE_LIQUIDITY:
                    remove_seen = True
                    remove_amounts = ev.data.get("token_amounts")
                    assert ev.event_name == "RemoveLiquidityV2Crypto3", ev.event_name
        assert remove_seen, "RemoveLiquidity event must be decoded from the Tricrypto2 LP_CLOSE receipt (VIB-5491)"
        assert remove_amounts and len(remove_amounts) == 3, f"3-coin proceeds expected, got {remove_amounts}"

        # --- Layer 4: LP burned; EXACT per-coin deltas equal the DECODED amounts ---
        lp_burned = lp_before - get_token_balance(web3, LP_TOKEN, funded_wallet)
        usdt_back = get_token_balance(web3, usdt_addr, funded_wallet) - usdt_before
        wbtc_back = get_token_balance(web3, WBTC, funded_wallet) - wbtc_before
        weth_back = get_token_balance(web3, weth_addr, funded_wallet) - weth_before
        assert lp_burned > 0, "LP must be burned on close"
        # Proportional close returns all three coins; each on-chain balance delta
        # must EXACTLY equal the decoded RemoveLiquidity leg (proves the decode is
        # correct, incl. the WBTC middle leg, and nothing is dropped or mis-ordered).
        assert usdt_back == remove_amounts[0], f"USDT delta {usdt_back} != decoded {remove_amounts[0]}"
        assert wbtc_back == remove_amounts[1], f"WBTC delta {wbtc_back} != decoded {remove_amounts[1]}"
        assert weth_back == remove_amounts[2], f"WETH delta {weth_back} != decoded {remove_amounts[2]}"
        assert usdt_back > 0 and wbtc_back > 0 and weth_back > 0, (
            f"all three legs positive: USDT={usdt_back} WBTC={wbtc_back} WETH={weth_back}"
        )
        logger.info(
            "Tricrypto2 LP_CLOSE: burned %d LP; deltas == decoded legs USDT=%d WBTC=%d WETH=%d",
            lp_burned,
            usdt_back,
            wbtc_back,
            weth_back,
        )

        # --- Layer 5: LP_CLOSE books a typed event through the real pipeline ---
        await assert_curve_lp_layer5(
            layer5_accounting_harness,
            intent=close_intent,
            result=close_exec,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            event_type="LP_CLOSE",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            expected_pool_address=POOL_ADDRESS,
        )
