"""Four-layer exact-address TraderJoe V2 LB pair LP lifecycle on Arbitrum Anvil.

The symbolic ``TOKEN_X/TOKEN_Y/BIN_STEP`` lifecycle lives in
``test_traderjoe_v2_lp.py``. This cell proves the OTHER admission lane: a
strategy names the LB pair by its bare address, the compiler reverses the
address into the pair's own ``getTokenX/getTokenY/getBinStep`` through the
gateway-shaped adapter, authenticates it against the registered LB factory,
and both LP_OPEN and LP_CLOSE bind to THAT address (bin ids travel from the
open receipt to the close intent, exactly as a strategy would carry them).
"""

import os
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config
from almanak.connectors.traderjoe_v2.addresses import TRADERJOE_V2_LBPAIRS
from almanak.connectors.traderjoe_v2.receipt_parser import TraderJoeV2EventType, TraderJoeV2ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals

CHAIN_NAME = "arbitrum"
# WETH/USDC bin_step=15 — the same registered LB pair the symbolic sibling
# opens as "WETH/USDC/15", named here by address so the two cells certify the
# same venue through both lanes.
_REGISTERED = next(
    row for row in TRADERJOE_V2_LBPAIRS[CHAIN_NAME] if row["tokenX"] == "WETH" and row["tokenY"] == "USDC"
)
EXACT_POOL = str(_REGISTERED["address"])
EXPECTED_BIN_STEP = int(_REGISTERED["bin_step"])
# A real, well-formed contract that is NOT an LB pair (Uniswap V3 WETH/USDC
# 0.05% on Arbitrum): the exact lane must refuse it before any approval.
NOT_AN_LB_PAIR = "0xC6962004f452bE9203591991D15f6b388e09E8D0"
LP_AMOUNT_WETH = Decimal("0.05")  # amount0 (token X = WETH)
LP_AMOUNT_USDC = Decimal("150")  # amount1 (token Y = USDC)
# Required by the intent model; TraderJoe V2 places liquidity around the
# active bin via ``protocol_params.bin_range`` rather than mapping this range.
RANGE_LOWER = Decimal("200")
RANGE_UPPER = Decimal("20000")


def _position(rpc_url: str, wallet: str, token_x: str, token_y: str):
    adapter = TraderJoeV2Adapter(TraderJoeV2Config(chain=CHAIN_NAME, wallet_address=wallet, rpc_url=rpc_url))
    return adapter.get_position(token_x, token_y, EXPECTED_BIN_STEP, wallet=wallet)


@pytest.mark.arbitrum
@pytest.mark.lp
class TestTraderJoeV2ExactPoolLP:
    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_exact_pool_address_open_close_roundtrip(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        """Compile, execute, parse, and balance-check one address-bound LB lifecycle."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)
        # CI pins the fork block; a local managed fork may leave it unset.
        fork_block = int(os.environ.get(f"ANVIL_FORK_BLOCK_{CHAIN_NAME.upper()}") or 1)

        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_before > 0 and weth_before > 0, "funded_wallet seeding failed"

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle={"USDC": Decimal("1"), "WETH": Decimal("3000")},
            rpc_url=anvil_rpc_url,
            gateway_client=anvil_eth_call_adapter,
        )

        # LP_OPEN by exact address
        open_intent = LPOpenIntent(
            pool=EXACT_POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        # Layer 1 — the bare address clears the format gate and is bound exactly.
        open_compilation = compiler.compile(open_intent)
        assert open_compilation.status.value == "SUCCESS", (
            f"Exact-pool LP_OPEN compilation failed: {open_compilation.error}"
        )
        assert open_compilation.action_bundle is not None
        open_meta = open_compilation.action_bundle.metadata
        assert open_meta["pool"].lower() == EXACT_POOL.lower()
        assert open_meta["bin_step"] == EXPECTED_BIN_STEP, "bin step must come from the pair contract"
        assert open_meta["token_x"]["address"].lower() == weth_addr.lower()
        assert open_meta["token_y"]["address"].lower() == usdc_addr.lower()

        # Layer 2 — execute on the managed fork.
        open_execution = await orchestrator.execute(open_compilation.action_bundle)
        assert open_execution.success, f"Exact-pool LP_OPEN execution failed: {open_execution.error}"
        assert open_execution.transaction_results
        for tx_result in open_execution.transaction_results:
            assert tx_result.tx_hash.startswith("0x") and len(tx_result.tx_hash) == 66
            assert tx_result.receipt is not None
            assert tx_result.receipt.status == 1
            assert tx_result.receipt.block_number >= fork_block

        # Layer 3 — the production parser recovers the deposit on THIS pair.
        parser = TraderJoeV2ReceiptParser()
        bin_ids: list[int] | None = None
        deposit_pool: str | None = None
        for tx_result in open_execution.transaction_results:
            receipt_dict = tx_result.receipt.to_dict()
            parsed = parser.parse_receipt(receipt_dict)
            assert parsed.success, f"LP_OPEN receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
            if any(event.event_type == TraderJoeV2EventType.DEPOSITED_TO_BINS for event in parsed.events):
                assert parsed.liquidity_result is not None and parsed.liquidity_result.is_add
                deposit_pool = parsed.liquidity_result.pool_address
                # The production open extraction must recover both deposited amounts.
                open_data = parser.extract_lp_open_data(receipt_dict)
                assert open_data is not None and open_data.amount0 > 0 and open_data.amount1 > 0, open_data
                bin_ids = parser.extract_bin_ids(receipt_dict)
        assert bin_ids, "Exact-pool LP_OPEN receipt must yield the deposited bin ids"
        assert deposit_pool is not None and deposit_pool.lower() == EXACT_POOL.lower()

        position = _position(anvil_rpc_url, funded_wallet, weth_addr, usdc_addr)
        assert position is not None and position.bin_ids, "Position must exist after exact-pool LP_OPEN"
        assert position.pool_address.lower() == EXACT_POOL.lower()
        assert sum(position.balances.values()) > 0

        # Layer 4 — deposited assets leave the wallet without exceeding the
        # requested amounts (bilateral: something was spent, nothing over-spent).
        usdc_after_open = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_open = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_spent = usdc_before - usdc_after_open
        weth_spent = weth_before - weth_after_open
        assert usdc_spent > 0 and weth_spent > 0, "a straddling LB deposit must take both tokens"
        assert usdc_spent <= int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        assert weth_spent <= int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))

        # LP_CLOSE by the same exact address
        close_intent = LPCloseIntent(
            position_id="0",  # TraderJoe V2 positions are pair + bin ids, not NFT ids
            pool=EXACT_POOL,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
            protocol_params={"bin_ids": bin_ids},
        )

        # Layer 1 — same authentication, bin ids from the open receipt.
        close_compilation = compiler.compile(close_intent)
        assert close_compilation.status.value == "SUCCESS", (
            f"Exact-pool LP_CLOSE compilation failed: {close_compilation.error}"
        )
        assert close_compilation.action_bundle is not None
        close_meta = close_compilation.action_bundle.metadata
        assert close_meta["pool"].lower() == EXACT_POOL.lower()
        assert len(close_compilation.action_bundle.transactions) == 2, "approveForAll + removeLiquidity"
        approve_tx = close_compilation.action_bundle.transactions[0]
        approve_target = approve_tx["to"] if isinstance(approve_tx, dict) else approve_tx.to
        assert approve_target.lower() == EXACT_POOL.lower(), "approveForAll must target the exact pair"

        # Layer 2 — execute the close.
        close_execution = await orchestrator.execute(close_compilation.action_bundle)
        assert close_execution.success, f"Exact-pool LP_CLOSE execution failed: {close_execution.error}"
        for tx_result in close_execution.transaction_results:
            assert tx_result.receipt is not None
            assert tx_result.receipt.status == 1

        # Layer 3 — the parser sees the withdrawal from THIS pair.
        withdrawal_pool: str | None = None
        for tx_result in close_execution.transaction_results:
            receipt_dict = tx_result.receipt.to_dict()
            parsed = parser.parse_receipt(receipt_dict)
            assert parsed.success, f"LP_CLOSE receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
            if any(event.event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS for event in parsed.events):
                assert parsed.liquidity_result is not None and not parsed.liquidity_result.is_add
                withdrawal_pool = parsed.liquidity_result.pool_address
                # The production close extraction must recover both withdrawn amounts.
                close_data = parser.extract_lp_close_data(receipt_dict)
                assert close_data is not None, "LP_CLOSE receipt must yield lp_close_data"
                assert close_data.amount0_collected is not None and close_data.amount0_collected > 0
                assert close_data.amount1_collected is not None and close_data.amount1_collected > 0
        assert withdrawal_pool is not None and withdrawal_pool.lower() == EXACT_POOL.lower()

        # Layer 4 — tokens return to the wallet and the position is gone.
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_after_close > usdc_after_open and weth_after_close > weth_after_open, (
            "withdrawing a straddling LB position must return both tokens"
        )
        position_after = _position(anvil_rpc_url, funded_wallet, weth_addr, usdc_addr)
        assert position_after is None or sum(position_after.balances.values()) == 0, (
            f"Exact-pool LP_CLOSE left LBPair shares behind: {position_after}"
        )

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    # Compile-time refusal: no bundle exists to execute, so Layers 2-4 cannot
    # apply; bilateral balance conservation is asserted instead.
    async def test_wrong_exact_address_is_refused_and_moves_nothing(  # noqa: layers
        self,
        web3: Web3,
        funded_wallet: str,
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        """A well-formed address the LB factory does not own must fail closed with balances untouched."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_before = get_token_balance(web3, tokens["USDC"], funded_wallet)
        weth_before = get_token_balance(web3, tokens["WETH"], funded_wallet)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle={"USDC": Decimal("1"), "WETH": Decimal("3000")},
            rpc_url=anvil_rpc_url,
            gateway_client=anvil_eth_call_adapter,
        )
        refused = compiler.compile(
            LPOpenIntent(
                pool=NOT_AN_LB_PAIR,
                amount0=LP_AMOUNT_WETH,
                amount1=LP_AMOUNT_USDC,
                range_lower=RANGE_LOWER,
                range_upper=RANGE_UPPER,
                protocol="traderjoe_v2",
                chain=CHAIN_NAME,
            )
        )
        assert refused.status.value == "FAILED"
        assert refused.action_bundle is None
        assert "Invalid pool format" not in (refused.error or ""), "the bare address must clear the format gate"
        assert get_token_balance(web3, tokens["USDC"], funded_wallet) == usdc_before
        assert get_token_balance(web3, tokens["WETH"], funded_wallet) == weth_before
