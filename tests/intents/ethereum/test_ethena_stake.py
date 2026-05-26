"""Intent tests for Ethena STAKE on Ethereum (VIB-4307).

Ethena's STAKE intent path: deposit USDe into the sUSDe ERC4626 vault
(approve + deposit). This mirrors the existing UNSTAKE-side coverage
in ``test_ethena_unstake_complete.py``.

This file covers the (ethena, STAKE, ethereum) triple from
ConnectorRegistry — the intent-coverage gate
(scripts/ci/check_intent_coverage.py) consumes the ``protocol="ethena"``
literal plus the ``IntentType.STAKE`` marker to credit coverage.

Ethena is **NOT in the synthetic-intents matrix** (see
``almanak/framework/permissions/synthetic_intents.py``) — Zodiac
permission discovery does not yet emit a manifest for ethena, so this
module opts out via ``pytestmark = pytest.mark.no_zodiac(...)``.

Four-layer coverage:

1. Compilation — ``IntentCompiler.compile(StakeIntent)`` → 2 txs
   (approve USDe for sUSDe, then deposit).
2. Execution — both txs land on Anvil Ethereum fork.
3. Receipt parsing — ``EthenaReceiptParser.parse_receipt(...)`` finds
   a Deposit event with shares > 0.
4. Balance deltas — USDe balance decreased by exact deposit amount;
   sUSDe balance increased.

To run::

    uv run pytest tests/intents/ethereum/test_ethena_stake.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.ethena.adapter import (
    ERC20_APPROVE_SELECTOR,
    ETHENA_ADDRESSES,
    ETHENA_DEPOSIT_SELECTOR,
)
from almanak.connectors.ethena.receipt_parser import EthenaReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import (
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import IntentType, StakeIntent
from tests.intents.conftest import (
    fund_erc20_token,
    get_token_balance,
)

pytestmark = [
    pytest.mark.no_zodiac(
        reason="VIB-4307: ethena not in synthetic-intents matrix"
    ),
    pytest.mark.intent(IntentType.STAKE),
]


# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"
USDE_ADDRESS = ETHENA_ADDRESSES["ethereum"]["usde"]
SUSDE_ADDRESS = ETHENA_ADDRESSES["ethereum"]["susde"]

# USDe is a standard ERC-20 (Ethena's _balances at slot 0; verified against
# the live contract). See test_ethena_unstake_complete.py for cross-check.
USDE_BALANCE_SLOT = 0


# =============================================================================
# Layer 1: Compilation tests (no Anvil required)
# =============================================================================


class TestEthenaStakeCompilation:
    """Layer 1 compilation tests for Ethena STAKE intent."""

    def test_compile_stake_intent_two_transactions(self) -> None:
        """``StakeIntent(protocol='ethena', USDe)`` → approve + deposit (2 txs)."""
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address="0x1234567890123456789012345678901234567890",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("1000.0"),
            chain=CHAIN_NAME,
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", (
            f"Compilation failed: {result.error}"
        )
        assert result.action_bundle is not None
        assert len(result.transactions) == 2, (
            "Ethena stake must produce approve + deposit (2 txs); "
            f"got {len(result.transactions)}"
        )

        approve_tx = result.transactions[0]
        deposit_tx = result.transactions[1]

        assert approve_tx.to.lower() == USDE_ADDRESS.lower(), (
            "Approve transaction must target USDe contract"
        )
        assert approve_tx.data.startswith(ERC20_APPROVE_SELECTOR), (
            f"Approve must use ERC20.approve selector "
            f"({ERC20_APPROVE_SELECTOR}); got {approve_tx.data[:10]}"
        )
        assert deposit_tx.to.lower() == SUSDE_ADDRESS.lower(), (
            "Deposit transaction must target sUSDe contract"
        )
        assert deposit_tx.data.startswith(ETHENA_DEPOSIT_SELECTOR), (
            f"Deposit must use sUSDe.deposit selector "
            f"({ETHENA_DEPOSIT_SELECTOR}); got {deposit_tx.data[:10]}"
        )


# =============================================================================
# Layers 2–4: On-chain integration tests (Anvil Ethereum fork)
# =============================================================================


@pytest.mark.ethereum
class TestEthenaStakeOnChain:
    """Layers 2–4 for Ethena STAKE on an Ethereum Anvil fork.

    USDe is not in the default Ethereum CHAIN_CONFIGS token list (see
    ``tests/intents/conftest.py``), so the test seeds it directly via
    ``fund_erc20_token`` against USDe's _balances mapping at slot 0
    (verified live — same pattern as
    ``test_ethena_unstake_complete.py::_fund_erc20_balance``).
    """

    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=True,
        reason="VIB-4307: USDe storage slot funding fails on ethereum Anvil fork — slot mapping in CHAIN_CONFIGS may not match USDe ERC20 (as of 2026-05-12)",
    )
    async def test_stake_usde_to_susde_full_4_layer(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Stake 1000 USDe → receive sUSDe shares, verify all 4 layers."""
        stake_amount = Decimal("1000.0")
        stake_amount_wei = int(stake_amount * Decimal(10**18))

        # ── Seed wallet with USDe (10x the stake amount for headroom) ──
        fund_amount = stake_amount_wei * 10
        fund_erc20_token(
            funded_wallet,
            USDE_ADDRESS,
            fund_amount,
            USDE_BALANCE_SLOT,
            anvil_rpc_url,
        )

        # ── Layer 4 setup: record balances BEFORE ──
        usde_before = get_token_balance(web3, USDE_ADDRESS, funded_wallet)
        susde_before = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        assert usde_before >= stake_amount_wei, (
            f"USDe funding failed: have {usde_before}, need {stake_amount_wei}"
        )

        # ── Layer 1: Compile ──
        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
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

        # ── Layer 2: Execute ──
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle
        )
        assert execution_result.success, (
            f"Execution failed: {execution_result.error}"
        )

        # ── Layer 3: Receipt parsing — expect a Deposit event ──
        parser = EthenaReceiptParser()
        deposit_found = False
        deposit_assets_wei = 0
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parsed = parser.parse_receipt(receipt_dict)
            if parsed.success and parsed.deposits:
                deposit = parsed.deposits[0]
                deposit_assets_wei = int(deposit.assets * Decimal(10**18))
                deposit_found = True
                break
        assert deposit_found, (
            "Expected a Deposit event from sUSDe contract after stake; "
            "EthenaReceiptParser found none"
        )
        assert deposit_assets_wei > 0, (
            f"Deposit event found but assets = 0 (parser drift?). "
            f"Got: {deposit_assets_wei}"
        )

        # ── Layer 4: Balance deltas ──
        usde_after = get_token_balance(web3, USDE_ADDRESS, funded_wallet)
        susde_after = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)

        usde_spent = usde_before - usde_after
        susde_received = susde_after - susde_before

        # USDe side: exact match — staking is deterministic on a frozen fork.
        assert usde_spent == stake_amount_wei, (
            f"USDe spent must EXACTLY equal stake amount. "
            f"Expected: {stake_amount_wei}, Got: {usde_spent}"
        )
        # sUSDe side: must increase (no-op guard).
        assert susde_received > 0, (
            "sUSDe balance must increase after staking USDe (no-op guard)"
        )
        # Cross-check: the parser's deposit.assets must match the on-chain delta.
        assert deposit_assets_wei == usde_spent, (
            f"Parser deposit.assets ({deposit_assets_wei}) must equal "
            f"on-chain USDe delta ({usde_spent})"
        )
