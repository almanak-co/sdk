"""Intent tests for Gimo Finance STAKE and UNSTAKE on 0G Chain (VIB-4307).

Gimo is the liquid staking primitive on 0G Chain (StaFi EVM LSD Stack):

* STAKE -- send native A0GI to the StakePool contract, receive st0G.
* UNSTAKE -- burn st0G via the StakePool to initiate the 22-day
  unbonding period.

This file covers the (gimo, STAKE, zerog) and (gimo, UNSTAKE, zerog)
triples from ConnectorRegistry.

Gimo is **NOT in the synthetic-intents matrix** (zerog is non-EVM-mainnet,
and the StaFi LSD pattern is not yet represented in
``almanak/framework/permissions/synthetic_intents.py``). The module opts
out of Zodiac wrapping via ``pytestmark = pytest.mark.no_zodiac(...)``.

Four-layer coverage status:

* **Layer 1 (Compilation)** — fully covered. The compiler routes
  ``StakeIntent / UnstakeIntent`` with ``protocol="gimo"`` to
  ``GimoAdapter`` and produces correctly-shaped calldata.
* **Layers 2–4 (Execution / Receipt / Balance)** — exercised on the
  0G Anvil fork. The Gimo StakePool (0xAc06...) has on-chain code on
  0G mainnet, but the test does **not** depend on a specific st0G
  exchange rate or unbonding state; it asserts the standard
  invariants (native balance decreased by stake_amount + gas, st0G
  balance increased > 0). See per-test docstrings for the funding
  pattern (native A0GI from the zerog conftest seed).

To run::

    uv run pytest tests/intents/zerog/test_gimo_stake.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.gimo.adapter import (
    GIMO_ADDRESSES,
    GIMO_STAKE_SELECTOR,
    GIMO_UNSTAKE_SELECTOR,
)
from almanak.framework.connectors.gimo.receipt_parser import GimoReceiptParser

# Pinned independently of DEFAULT_GAS_ESTIMATES so a regression in the adapter
# constant surfaces here instead of silently passing.
EXPECTED_GIMO_UNSTAKE_GAS = 300_000
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import (
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import (
    IntentType,
    StakeIntent,
    UnstakeIntent,
)
from tests.intents.conftest import get_token_balance

pytestmark = [
    pytest.mark.no_zodiac(
        reason="VIB-4307: gimo not in synthetic-intents matrix"
    ),
    pytest.mark.intent(IntentType.STAKE, IntentType.UNSTAKE),
]


# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "zerog"
ST0G_ADDRESS = GIMO_ADDRESSES["zerog"]["st0g"]
STAKE_POOL_ADDRESS = GIMO_ADDRESSES["zerog"]["stake_pool"]


# =============================================================================
# Layer 1: Compilation tests (no Anvil required)
# =============================================================================


class TestGimoStakeCompilation:
    """Layer 1 compilation for Gimo STAKE intent on 0G."""

    def test_compile_stake_intent_single_transaction(self) -> None:
        """``StakeIntent(protocol='gimo', A0GI)`` → 1 transaction (stake)."""
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address="0x1234567890123456789012345678901234567890",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = StakeIntent(
            protocol="gimo",
            token_in="A0GI",
            amount=Decimal("10.0"),
            chain=CHAIN_NAME,
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", (
            f"Compilation failed: {result.error}"
        )
        assert result.action_bundle is not None
        assert len(result.transactions) == 1, (
            f"Gimo stake must produce 1 tx; got {len(result.transactions)}"
        )
        tx = result.transactions[0]
        assert tx.to.lower() == STAKE_POOL_ADDRESS.lower(), (
            "Stake transaction must target the Gimo StakePool contract"
        )
        assert tx.data.startswith(GIMO_STAKE_SELECTOR), (
            f"Calldata must use Gimo stake(string) selector "
            f"({GIMO_STAKE_SELECTOR}); got {tx.data[:10]}"
        )
        assert tx.value == int(Decimal("10.0") * Decimal(10**18)), (
            "msg.value must equal stake amount (native A0GI is sent as value)"
        )


class TestGimoUnstakeCompilation:
    """Layer 1 compilation for Gimo UNSTAKE intent on 0G."""

    def test_compile_unstake_intent_approve_plus_unstake(self) -> None:
        """``UnstakeIntent(protocol='gimo', st0G)`` → approve + unstake (2 txs).

        st0G is an ERC-20 token; the StakePool needs an approval before
        the burn-and-initiate-unbonding call.
        """
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address="0x1234567890123456789012345678901234567890",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = UnstakeIntent(
            protocol="gimo",
            token_in="st0G",
            amount=Decimal("5.0"),
            chain=CHAIN_NAME,
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", (
            f"Compilation failed: {result.error}"
        )
        assert result.action_bundle is not None
        assert len(result.transactions) == 2, (
            f"Gimo unstake must produce approve + unstake (2 txs); "
            f"got {len(result.transactions)}"
        )

        approve_tx = result.transactions[0]
        unstake_tx = result.transactions[1]

        assert approve_tx.to.lower() == ST0G_ADDRESS.lower(), (
            "Approve transaction must target the st0G token contract"
        )
        assert unstake_tx.to.lower() == STAKE_POOL_ADDRESS.lower(), (
            "Unstake transaction must target the Gimo StakePool contract"
        )
        assert unstake_tx.data.startswith(GIMO_UNSTAKE_SELECTOR), (
            f"Calldata must use Gimo unstake(uint256) selector "
            f"({GIMO_UNSTAKE_SELECTOR}); got {unstake_tx.data[:10]}"
        )
        assert unstake_tx.gas_estimate == EXPECTED_GIMO_UNSTAKE_GAS


# =============================================================================
# Layers 2–4: On-chain integration tests (Anvil 0G fork)
# =============================================================================


@pytest.mark.zerog
class TestGimoStakeOnChain:
    """Layers 2–4 for Gimo STAKE on a 0G Anvil fork.

    The zerog conftest seeds 100 native A0GI on the funded wallet (no
    ERC-20 funding, since storage slots for 0G tokens are not mapped).
    We stake a small amount and verify the native delta + st0G mint.
    """

    @pytest.mark.asyncio
    async def test_stake_a0gi_to_st0g_full_4_layer(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Stake 1 A0GI → receive st0G, verify all 4 layers.

        Notes:
            * The Gimo StakePool emits a ``Staked`` event but its receipt
              parser is not yet hardened against fork-block edge cases —
              we assert on the underlying invariants (native delta + st0G
              mint) rather than a specific log shape.
            * The native A0GI side is the ``no-op guard`` here: the test
              fails loudly if msg.value handling regresses.
        """
        stake_amount = Decimal("1.0")
        stake_amount_wei = int(stake_amount * Decimal(10**18))

        # Layer 4 setup
        native_before = web3.eth.get_balance(
            Web3.to_checksum_address(funded_wallet)
        )
        st0g_before = get_token_balance(web3, ST0G_ADDRESS, funded_wallet)

        assert native_before >= stake_amount_wei + 10**17, (
            f"Need at least {stake_amount + Decimal('0.1')} A0GI for "
            f"stake + gas; have {Decimal(native_before) / Decimal(10**18)}"
        )

        # Layer 1: Compile
        intent = StakeIntent(
            protocol="gimo",
            token_in="A0GI",
            amount=stake_amount,
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 2: Execute
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle
        )
        assert execution_result.success, (
            f"Execution failed: {execution_result.error}"
        )

        # Layer 3: Receipt — parse with GimoReceiptParser and assert the
        # st0G mint amount; also confirm a log was emitted from the StakePool.
        parser = GimoReceiptParser(chain=CHAIN_NAME)
        stake_pool_logs_found = False
        total_gas_cost = 0
        parsed_stake_amount_wei = 0
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            total_gas_cost += tx_result.gas_cost_wei or 0
            parsed = parser.parse_receipt(tx_result.receipt.to_dict())
            if parsed.success and parsed.stakes:
                parsed_stake_amount_wei = int(parsed.stakes[0].amount * Decimal(10**18))
            for log in tx_result.receipt.logs or []:
                # ``logs`` is a list[dict] per TransactionReceipt; getattr would
                # silently return None on a dict, so use dict access.
                log_addr = log.get("address") if isinstance(log, dict) else getattr(log, "address", None)
                if log_addr and log_addr.lower() == STAKE_POOL_ADDRESS.lower():
                    stake_pool_logs_found = True
                    break
        assert stake_pool_logs_found, (
            "Expected at least one log from the Gimo StakePool after staking"
        )
        assert parsed_stake_amount_wei > 0, (
            "GimoReceiptParser must report a st0G mint amount > 0 after staking"
        )

        # Layer 4: Balance deltas
        native_after = web3.eth.get_balance(
            Web3.to_checksum_address(funded_wallet)
        )
        st0g_after = get_token_balance(web3, ST0G_ADDRESS, funded_wallet)

        native_spent = native_before - native_after
        st0g_received = st0g_after - st0g_before

        assert native_spent == stake_amount_wei + total_gas_cost, (
            f"Native A0GI spent must equal stake_amount + gas. "
            f"Expected: {stake_amount_wei + total_gas_cost}, "
            f"Got: {native_spent} "
            f"(amount={stake_amount_wei}, gas={total_gas_cost})"
        )
        assert st0g_received > 0, (
            "st0G balance must increase after staking A0GI (no-op guard)"
        )


@pytest.mark.zerog
class TestGimoUnstakeOnChain:
    """Layers 2–4 for Gimo UNSTAKE on a 0G Anvil fork.

    Setup: stake A0GI to acquire st0G (the only reliable way to seed a
    non-funded ERC-20 balance on zerog — storage slots are not mapped).
    Then unstake half of the resulting st0G balance.
    """

    @pytest.mark.asyncio
    async def test_unstake_st0g_initiates_unbonding(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Stake A0GI to acquire st0G, then unstake half and verify burn."""
        # ── Step 1: Acquire st0G by staking ────────────────────────────────
        seed_amount = Decimal("5.0")
        seed_intent = StakeIntent(
            protocol="gimo",
            token_in="A0GI",
            amount=seed_amount,
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        seed_result = compiler.compile(seed_intent)
        assert seed_result.status.value == "SUCCESS"
        seed_exec = await orchestrator.execute(seed_result.action_bundle)
        assert seed_exec.success, (
            f"Seed stake execution failed: {seed_exec.error}"
        )

        st0g_balance = get_token_balance(web3, ST0G_ADDRESS, funded_wallet)
        assert st0g_balance > 0, "Seed stake produced no st0G balance"
        st0g_balance_decimal = Decimal(st0g_balance) / Decimal(10**18)

        # ── Step 2: Unstake half ──────────────────────────────────────────
        unstake_amount = (st0g_balance_decimal / Decimal("2")).quantize(
            Decimal("0.0001")
        )
        unstake_amount_wei = int(unstake_amount * Decimal(10**18))

        st0g_before_unstake = get_token_balance(
            web3, ST0G_ADDRESS, funded_wallet
        )

        # Layer 1: Compile
        intent = UnstakeIntent(
            protocol="gimo",
            token_in="st0G",
            amount=unstake_amount,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None
        assert len(compilation_result.action_bundle.transactions) == 2

        # Layer 2: Execute
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle
        )
        assert execution_result.success, (
            f"Unstake execution failed: {execution_result.error}"
        )

        # Layer 3: Receipt — parse with GimoReceiptParser and assert the
        # st0G burn amount; also confirm a log was emitted from the StakePool.
        parser = GimoReceiptParser(chain=CHAIN_NAME)
        stake_pool_logs_found = False
        parsed_unstake_amount_wei = 0
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            parsed = parser.parse_receipt(tx_result.receipt.to_dict())
            if parsed.success and parsed.unstakes:
                parsed_unstake_amount_wei = int(parsed.unstakes[0].amount * Decimal(10**18))
            for log in tx_result.receipt.logs or []:
                # ``logs`` is a list[dict] per TransactionReceipt; getattr would
                # silently return None on a dict, so use dict access.
                log_addr = log.get("address") if isinstance(log, dict) else getattr(log, "address", None)
                if log_addr and log_addr.lower() == STAKE_POOL_ADDRESS.lower():
                    stake_pool_logs_found = True
                    break
        assert stake_pool_logs_found, (
            "Expected at least one log from the Gimo StakePool after unstaking"
        )
        assert parsed_unstake_amount_wei == unstake_amount_wei, (
            f"GimoReceiptParser must report a st0G burn equal to the unstake "
            f"amount. Expected: {unstake_amount_wei}, Got: {parsed_unstake_amount_wei}"
        )

        # Layer 4: Balance deltas — st0G balance must have decreased.
        st0g_after = get_token_balance(web3, ST0G_ADDRESS, funded_wallet)
        st0g_spent = st0g_before_unstake - st0g_after

        assert st0g_spent == unstake_amount_wei, (
            f"st0G spent must equal unstake amount. "
            f"Expected: {unstake_amount_wei}, Got: {st0g_spent}"
        )
