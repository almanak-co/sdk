"""Curve raw-ETH/stETH swap accounting on an Ethereum Anvil fork (ALM-3229).

Proves both directions through the four required intent-test layers. The stETH
pool holds raw ETH, so that receipt leg has no ERC-20 Transfer and must be
identified from the pool coin vector plus the TokenExchange indices.

Run with:
    uv run pytest tests/intents/ethereum/test_curve_steth_native_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors._strategy_base.base import stamp_trading_wallet
from almanak.connectors.curve.receipt_parser import (
    CURVE_NATIVE_ETH_PLACEHOLDER,
    CurveEventType,
    CurveReceiptParser,
)
from almanak.framework.execution.extract_result import ExtractOk
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import SWAP_MAX_SLIPPAGE, get_token_balance

CHAIN_NAME = "ethereum"
STETH_POOL = "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"
STETH_ADDRESS = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
SWAP_AMOUNT = Decimal("0.1")


def _receipt_dict(receipt) -> dict:
    return receipt if isinstance(receipt, dict) else receipt.to_dict()


def _assert_native_swap_receipt(
    execution_result,
    *,
    trading_wallet: str,
    expected_sold_id: int,
    expected_bought_id: int,
    expected_token_in: str,
    expected_token_out: str,
    expected_amount_in: int,
) -> None:
    """Assert Layer 3, including the fail-closed accounting extraction seam."""
    parser = CurveReceiptParser(chain=CHAIN_NAME)
    exchange_receipts = 0

    for tx_result in execution_result.transaction_results:
        if not tx_result.receipt:
            continue
        receipt = _receipt_dict(tx_result.receipt)
        parsed = parser.parse_receipt(receipt)
        assert parsed.success, f"Curve receipt parsing failed: {parsed.error}"

        exchanges = [event for event in parsed.events if event.event_type is CurveEventType.TOKEN_EXCHANGE]
        if not exchanges:
            continue

        exchange_receipts += 1
        exchange = exchanges[0]
        assert exchange.data["sold_id"] == expected_sold_id
        assert exchange.data["bought_id"] == expected_bought_id
        assert exchange.data["tokens_sold"] > 0
        assert exchange.data["tokens_bought"] > 0

        # ResultEnricher authoritatively stamps the configured Safe/EOA before
        # invoking any parser; reproduce that production boundary here rather
        # than letting the raw receipt's signer EOA masquerade as the Safe.
        accounting_receipt = stamp_trading_wallet(receipt, trading_wallet)
        extracted = parser.extract_swap_amounts_result(accounting_receipt)
        assert isinstance(extracted, ExtractOk), (
            f"A successful native Curve swap must produce accounting-ready swap_amounts; got {extracted!r}"
        )
        assert extracted.value.amount_in == expected_amount_in
        assert extracted.value.amount_in_decimal > 0
        assert extracted.value.amount_out_decimal > 0
        assert extracted.value.token_in.lower() == expected_token_in.lower()
        assert extracted.value.token_out.lower() == expected_token_out.lower()

    assert exchange_receipts == 1, f"Expected one TokenExchange receipt, found {exchange_receipts}"


@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.ethereum
@pytest.mark.swap
class TestCurveStethNativeSwap:
    """Full ETH -> stETH -> ETH lifecycle through the pinned Curve pool."""

    @pytest.mark.asyncio
    async def test_native_eth_swap_round_trip_is_accounting_ready(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        # Layer 4 setup for the entry. Under the default Zodiac harness the Safe
        # owns the assets while the member EOA pays gas, so its native delta is
        # exactly the Curve call value.
        native_before_entry = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        steth_before_entry = get_token_balance(web3, STETH_ADDRESS, funded_wallet)

        # Layer 1: compile the exact raw-ETH route reported in ALM-3229.
        entry_intent = SwapIntent(
            from_token=CURVE_NATIVE_ETH_PLACEHOLDER,
            to_token=STETH_ADDRESS,
            amount=SWAP_AMOUNT,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="curve",
            chain=CHAIN_NAME,
            swap_params={"pool": STETH_POOL},
        )
        entry_compilation = compiler.compile(entry_intent)
        assert entry_compilation.status is CompilationStatus.SUCCESS, entry_compilation.error
        assert entry_compilation.action_bundle is not None
        assert entry_compilation.action_bundle.metadata["pool_address"].lower() == STETH_POOL.lower()

        # Layer 2: execute ETH -> stETH.
        entry_execution = await orchestrator.execute(entry_compilation.action_bundle)
        assert entry_execution.success, f"ETH -> stETH execution failed: {entry_execution.error}"

        # Layer 3: the native input has no Transfer but must still enrich.
        expected_native_spent = int(SWAP_AMOUNT * Decimal(10**18))
        _assert_native_swap_receipt(
            entry_execution,
            trading_wallet=funded_wallet,
            expected_sold_id=0,
            expected_bought_id=1,
            expected_token_in=CURVE_NATIVE_ETH_PLACEHOLDER,
            expected_token_out=STETH_ADDRESS,
            expected_amount_in=expected_native_spent,
        )

        # Layer 4: bilateral entry deltas.
        native_after_entry = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        steth_after_entry = get_token_balance(web3, STETH_ADDRESS, funded_wallet)
        native_spent = native_before_entry - native_after_entry
        steth_received = steth_after_entry - steth_before_entry
        assert native_spent == expected_native_spent
        assert steth_received > 0

        # Layer 1: compile the reverse direction with the exact stETH received.
        exit_amount = Decimal(steth_received) / Decimal(10**18)
        exit_intent = SwapIntent(
            from_token=STETH_ADDRESS,
            to_token=CURVE_NATIVE_ETH_PLACEHOLDER,
            amount=exit_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="curve",
            chain=CHAIN_NAME,
            swap_params={"pool": STETH_POOL},
        )
        exit_compilation = compiler.compile(exit_intent)
        assert exit_compilation.status is CompilationStatus.SUCCESS, exit_compilation.error
        assert exit_compilation.action_bundle is not None
        assert exit_compilation.action_bundle.metadata["pool_address"].lower() == STETH_POOL.lower()

        # Layer 2: execute stETH -> ETH.
        native_before_exit = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        steth_before_exit = get_token_balance(web3, STETH_ADDRESS, funded_wallet)
        exit_execution = await orchestrator.execute(exit_compilation.action_bundle)
        assert exit_execution.success, f"stETH -> ETH execution failed: {exit_execution.error}"

        # Layer 3: the native output has no Transfer but must still enrich.
        _assert_native_swap_receipt(
            exit_execution,
            trading_wallet=funded_wallet,
            expected_sold_id=1,
            expected_bought_id=0,
            expected_token_in=STETH_ADDRESS,
            expected_token_out=CURVE_NATIVE_ETH_PLACEHOLDER,
            expected_amount_in=steth_received,
        )

        # Layer 4: bilateral exit deltas.
        native_after_exit = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        steth_after_exit = get_token_balance(web3, STETH_ADDRESS, funded_wallet)
        steth_spent = steth_before_exit - steth_after_exit
        # stETH balanceOf derives token units from shares; a transfer can leave
        # 1-2 wei of display dust even though TokenExchange records the exact
        # requested input (asserted above). Match the Lido intent-test contract.
        assert abs(steth_spent - steth_received) <= 10
        assert native_after_exit - native_before_exit > 0
