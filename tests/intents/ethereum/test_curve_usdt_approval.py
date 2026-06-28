"""USDT-class approval hardening — real-fork proof (VIB-5442 / audit P1-8).

USDT reverts on a non-zero → non-zero ``approve`` (``require(value == 0 ||
allowance == 0)``). Before this fix the Curve adapter, with a cache never seeded
from on-chain ``allowance()``, would emit ``approve(MAX)`` on a USDT that already
carried a non-zero allowance → the approve reverts and the whole bundle dies.

This test pre-sets a non-zero USDT allowance to the 3pool (via storage), then
compiles + executes a USDT→USDC swap on an Ethereum Anvil fork. With the fix the
adapter seeds the on-chain allowance, sees it is non-zero-but-insufficient, and
emits ``approve(0)`` + ``approve(MAX)`` (reset-to-zero) so the swap executes
without the USDT revert.

To run:
    uv run pytest tests/intents/ethereum/test_curve_usdt_approval.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import CHAIN_CONFIGS, fund_erc20_token, get_token_balance

logger = logging.getLogger(__name__)

CHAIN_NAME = "ethereum"
POOL_3POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"


def _read_allowance(web3: Web3, owner: str, spender: str) -> int:
    res = web3.eth.call(
        {
            "to": Web3.to_checksum_address(USDT),
            "data": "0xdd62ed3e" + owner[2:].rjust(64, "0") + spender[2:].rjust(64, "0"),
        }
    )
    return int(res.hex(), 16)


def _set_allowance_via_tx(web3: Web3, owner: str, spender: str, value: int) -> None:
    """Set USDT ``allowed[owner][spender]`` with a real approve, impersonated on Anvil."""
    web3.provider.make_request("anvil_impersonateAccount", [owner])
    try:
        data = "0x095ea7b3" + spender[2:].rjust(64, "0") + value.to_bytes(32, "big").hex()
        tx_hash = web3.eth.send_transaction(
            {
                "from": Web3.to_checksum_address(owner),
                "to": Web3.to_checksum_address(USDT),
                "data": data,
                "gas": 100_000,
            }
        )
        web3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    finally:
        # Always stop impersonating, even if the send/wait raises, so the node
        # doesn't leak an impersonated account into the rest of the session.
        web3.provider.make_request("anvil_stopImpersonatingAccount", [owner])


@pytest.mark.ethereum
@pytest.mark.swap
class TestCurveUSDTApproval:
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_usdt_swap_with_preexisting_allowance_no_revert(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        usdc_addr = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]["USDC"]
        fund_erc20_token(funded_wallet, USDT, int(Decimal("1000") * Decimal(10**6)), 2, anvil_rpc_url)

        # Pre-set a NON-ZERO, insufficient USDT allowance to the 3pool — the exact
        # state that makes a naive approve(MAX) revert on USDT.
        _set_allowance_via_tx(web3, funded_wallet, POOL_3POOL, 1)
        assert _read_allowance(web3, funded_wallet, POOL_3POOL) == 1, "pre-set allowance failed"

        usdt_before = get_token_balance(web3, USDT, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        compiler = IntentCompiler(
            chain=CHAIN_NAME, wallet_address=funded_wallet, price_oracle=price_oracle, rpc_url=anvil_rpc_url
        )
        intent = SwapIntent(
            from_token="USDT",
            to_token="USDC",
            amount=Decimal("100"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )
        compiled = compiler.compile(intent)
        assert compiled.status.value == "SUCCESS", compiled.error

        # The bundle must reset USDT to zero before re-approving (two approve txs).
        approve_txs = [tx for tx in compiled.transactions if tx.tx_type == "approve"]
        assert len(approve_txs) == 2, f"expected reset+approve, got {len(approve_txs)} approves"

        exec_result = await orchestrator.execute(compiled.action_bundle)
        assert exec_result.success, f"USDT swap with pre-existing allowance reverted: {exec_result.error}"

        # Layer 3: the swap receipt parses a TokenExchange event (the swap actually
        # executed past the approve sequence, not just succeeded structurally).
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        swap_event_seen = False
        for tx_result in exec_result.transaction_results:
            if not tx_result.receipt:
                continue
            parsed = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parsed.success, parsed.error
            if any(e.event_type == CurveEventType.TOKEN_EXCHANGE for e in parsed.events):
                swap_event_seen = True
        assert swap_event_seen, "TokenExchange event must be parsed from the USDT->USDC swap receipt"

        usdt_spent = usdt_before - get_token_balance(web3, USDT, funded_wallet)
        usdc_received = get_token_balance(web3, usdc_addr, funded_wallet) - usdc_before
        assert usdt_spent == int(Decimal("100") * Decimal(10**6)), f"USDT spent {usdt_spent}"
        assert usdc_received > 0, "must receive USDC"
        logger.info("USDT->USDC swap with pre-existing allowance succeeded (reset-to-zero, no revert)")
