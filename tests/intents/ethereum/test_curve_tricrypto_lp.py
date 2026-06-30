"""Curve Tricrypto2 (CryptoSwap) LP_OPEN min-LP protection (VIB-5441 / audit P1-7).

Volatile-pool LP deposits previously shipped ``min_lp=0`` (no slippage protection,
an MEV theft vector). This 4(+1)-layer test proves on a real Ethereum Anvil fork
that a Tricrypto2 LP_OPEN now:

- compiles with a **non-zero** ``min_mint`` derived from an on-chain
  ``calc_token_amount`` quote (Layer 1),
- executes on-chain (Layer 2),
- parses the AddLiquidity event + mints LP (Layer 3),
- moves the exact deposited balances and receives LP (Layer 4),
- books a typed LP_OPEN through the real accounting pipeline (Layer 5).

Tricrypto2 = USDT(0) / WBTC(1) / WETH(2). The fork funds USDT + WETH (not WBTC),
so we deposit USDT + WETH with WBTC=0 (an imbalanced CryptoSwap deposit is fine).

To run:
    uv run pytest tests/intents/ethereum/test_curve_tricrypto_lp.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._curve_lp_layer5_helpers import (
    assert_curve_lp_layer5,
    enrich_for_accounting,
)
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    fund_erc20_token,
    get_token_balance,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_zodiac(reason="curve LP not in _LP_PROTOCOLS; manifest empty for curve LP")

CHAIN_NAME = "ethereum"

# Curve Tricrypto2 (USDT/WBTC/WETH) — CryptoSwap, 3-coin
POOL = "tricrypto2"
POOL_ADDRESS = "0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"
LP_TOKEN = "0xc4AD29ba4B3c580e6D59105FFf484999997675Ff"  # crv3crypto

# Deposit USDT + WETH (WBTC = 0). Both funded on the ethereum fork.
LP_AMOUNT_USDT = Decimal("100")
LP_AMOUNT_WETH = Decimal("0.05")

USDT_BALANCE_SLOT = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDT"]
WETH_BALANCE_SLOT = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["WETH"]


def _decode_min_mint(add_liq_tx) -> int:
    """Decode the trailing ``min_mint`` word from a CryptoSwap add_liquidity tx.

    ``add_liquidity(uint256[3] amounts, uint256 min_mint)`` encodes min_mint as
    the last 32-byte word of the calldata.
    """
    data = add_liq_tx.data
    if data.startswith("0x"):
        data = data[2:]
    return int(data[-64:], 16)


@pytest.mark.ethereum
@pytest.mark.lp
class TestCurveTricryptoLPOpen:
    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_usdt_weth_min_lp_nonzero(
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
        usdt_addr = tokens["USDT"]
        weth_addr = tokens["WETH"]

        # Fund deposits (storage-slot manipulation).
        fund_erc20_token(
            funded_wallet, usdt_addr, int(LP_AMOUNT_USDT * Decimal(10**6)) * 10, USDT_BALANCE_SLOT, anvil_rpc_url
        )
        fund_erc20_token(
            funded_wallet, weth_addr, int(LP_AMOUNT_WETH * Decimal(10**18)) * 10, WETH_BALANCE_SLOT, anvil_rpc_url
        )

        # --- Layer 4 BEFORE ---
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        lp_before = get_token_balance(web3, LP_TOKEN, funded_wallet)
        assert usdt_before > 0 and weth_before > 0, "funding failed"

        # --- Layer 1: Compile (CryptoSwap min_lp from on-chain calc_token_amount) ---
        intent = LPOpenIntent(
            pool=POOL,
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            coin_amounts=[LP_AMOUNT_USDT, Decimal("0"), LP_AMOUNT_WETH],  # USDT, WBTC=0, WETH
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
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
            f"Tricrypto2 LP_OPEN compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # VIB-5441 core assertion: the volatile-pool add_liquidity ships min_mint > 0.
        add_liq_tx = next(tx for tx in compilation_result.transactions if tx.tx_type == "add_liquidity")
        min_mint = _decode_min_mint(add_liq_tx)
        assert min_mint > 0, "CryptoSwap LP_OPEN must NOT ship min_lp=0 (theft vector)"
        logger.info("Tricrypto2 LP_OPEN min_mint=%d (non-zero — protected)", min_mint)

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Tricrypto2 LP_OPEN execution failed: {execution_result.error}"
        execution_result = enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            chain=CHAIN_NAME,
            bundle_metadata=compilation_result.action_bundle.metadata,
        )

        # --- Layer 3: Receipt parsing ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        add_liquidity_seen = False
        decoded_amounts: list[int] | None = None
        decoded_supply: int | None = None
        lp_from_receipt: Decimal | None = None
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
            for event in parse_result.events:
                if event.event_type == CurveEventType.ADD_LIQUIDITY:
                    add_liquidity_seen = True
                    # Exercise the legacy AddLiquidity(address,uint256[3],...) decoder
                    # this PR fixes — a regression in _decode_add_liquidity_data must
                    # fail the test, not slip through on the mint-Transfer alone.
                    assert event.event_name == "AddLiquidityV2Crypto3", event.event_name
                    decoded_amounts = event.data.get("token_amounts")
                    decoded_supply = event.data.get("token_supply")
            minted = parser.extract_lp_tokens_received(receipt_dict)
            if minted is not None and minted > 0:
                lp_from_receipt = minted
        assert add_liquidity_seen, "AddLiquidity event must be parsed from the LP_OPEN receipt"
        # Layer 3: the decoded AddLiquidity payload itself must be present and positive
        # (USDT + WETH deposited → coins 0 and 2 non-zero; supply > 0).
        assert decoded_amounts is not None and len(decoded_amounts) == 3, (
            f"3-coin AddLiquidity amounts expected, got {decoded_amounts}"
        )
        assert decoded_amounts[0] > 0 and decoded_amounts[2] > 0, (
            f"deposited legs must decode positive: {decoded_amounts}"
        )
        assert decoded_supply is not None and decoded_supply > 0, f"token_supply must decode positive: {decoded_supply}"
        assert lp_from_receipt is not None and lp_from_receipt > 0, "LP tokens minted must be extractable"

        # --- Layer 4 AFTER ---
        usdt_spent = usdt_before - get_token_balance(web3, usdt_addr, funded_wallet)
        weth_spent = weth_before - get_token_balance(web3, weth_addr, funded_wallet)
        lp_received = get_token_balance(web3, LP_TOKEN, funded_wallet) - lp_before
        assert usdt_spent == int(LP_AMOUNT_USDT * Decimal(10**6)), f"USDT spent {usdt_spent}"
        assert weth_spent == int(LP_AMOUNT_WETH * Decimal(10**18)), f"WETH spent {weth_spent}"
        assert lp_received > 0, "Must receive LP tokens"
        # min_mint must have been a real floor below the actual mint, never above it.
        assert lp_received >= min_mint, f"actual LP mint {lp_received} must be >= the min_mint floor {min_mint}"
        logger.info(
            "Tricrypto2 LP_OPEN: spent %s USDT + %s WETH, received %d LP (floor %d)",
            LP_AMOUNT_USDT,
            LP_AMOUNT_WETH,
            lp_received,
            min_mint,
        )

        # --- Layer 5: real accounting pipeline ---
        await assert_curve_lp_layer5(
            layer5_accounting_harness,
            intent=intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            event_type="LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            expected_pool_address=POOL_ADDRESS,
        )
