"""Kamino Finance lending intent tests for Solana.

Layer 1 (Compilation): Verifies the full lending intent compilation pipeline
against the REAL Kamino API. Always runs — no solana-test-validator required.

Layers 2-4 (Execution + Receipt + Balance Deltas): Require solana-test-validator.
These execute supply/withdraw operations and verify exact balance changes.

Notes:
- Borrow, repay, and withdraw require an existing Kamino obligation (deposit first).
  Compilation tests verify the pipeline reaches the API correctly;
  API errors like KLEND_OBLIGATION_NOT_FOUND are expected for a fresh wallet.
- Supply is the only operation that works without prior state.

Run compilation tests:
    uv run pytest tests/intents/solana/test_kamino_lending.py -v -s -k Compilation

Run all tests (needs solana-test-validator):
    uv run pytest tests/intents/solana/test_kamino_lending.py -v -s
"""

import base64
from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from tests.intents.solana.conftest import (
    CHAIN_NAME,
    SOLANA_TOKEN_DECIMALS,
    SOLANA_TOKENS,
    get_spl_token_balance,
    requires_solana_validator,
)


# =============================================================================
# Layer 1: Compilation Tests (always run — hit real Kamino API)
# =============================================================================


class TestKaminoSupplyCompilation:
    """Kamino supply: SupplyIntent -> Compile -> ActionBundle (real API)."""

    @pytest.mark.asyncio
    async def test_compile_usdc_supply(self, solana_compiler):
        """SupplyIntent for USDC compiles via Kamino API and returns a valid tx."""
        intent = SupplyIntent(
            protocol="kamino",
            token="USDC",
            amount=Decimal("1.0"),  # 1 USDC
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        # Layer 1: Compilation succeeds
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None, "ActionBundle must be created"

        bundle = result.action_bundle

        # Has transactions
        assert bundle.transactions, "Bundle must contain transactions"
        tx_data = bundle.transactions[0]
        serialized_tx = tx_data.get("serialized_transaction", "")
        assert serialized_tx, "Must have serialized_transaction"

        # Valid base64
        decoded = base64.b64decode(serialized_tx)
        assert len(decoded) > 50, f"Decoded tx too small ({len(decoded)} bytes)"

        # Metadata
        metadata = bundle.metadata
        assert metadata.get("chain_family") == "SOLANA"
        assert metadata.get("protocol") == "kamino"
        # Kamino adapter uses "action" key for the operation type
        assert metadata.get("action") == "deposit"

    @pytest.mark.asyncio
    async def test_compile_sol_supply(self, solana_compiler):
        """SupplyIntent for SOL compiles via Kamino API."""
        intent = SupplyIntent(
            protocol="kamino",
            token="SOL",
            amount=Decimal("0.01"),  # 0.01 SOL
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        assert result.action_bundle.transactions

    @pytest.mark.asyncio
    async def test_kamino_supply_route_from_generic_intent(self, solana_compiler):
        """SupplyIntent with protocol='kamino' routes correctly on Solana."""
        intent = SupplyIntent(
            protocol="kamino",
            token="USDC",
            amount=Decimal("1.0"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        assert result.action_bundle.metadata.get("protocol") == "kamino"

    @pytest.mark.asyncio
    async def test_kamino_supply_tx_is_deserializable(self, solana_compiler):
        """Kamino supply transaction is a valid Solana VersionedTransaction."""
        intent = SupplyIntent(
            protocol="kamino",
            token="USDC",
            amount=Decimal("1.0"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "SUCCESS"

        serialized_tx = result.action_bundle.transactions[0]["serialized_transaction"]
        decoded = base64.b64decode(serialized_tx)

        # Verify deserialization with solders
        from solders.transaction import VersionedTransaction

        tx = VersionedTransaction.from_bytes(decoded)
        assert tx is not None, "Must deserialize as a valid VersionedTransaction"


class TestKaminoBorrowCompilation:
    """Kamino borrow: BorrowIntent -> Compile -> ActionBundle (real API).

    BorrowIntent requires collateral_token, collateral_amount, borrow_token,
    borrow_amount. The Kamino API returns a transaction even though the
    wallet may not have an obligation yet (the tx would fail on-chain).
    """

    @pytest.mark.asyncio
    async def test_compile_usdc_borrow_fails_without_obligation(self, solana_compiler):
        """BorrowIntent for USDC fails gracefully when wallet has no obligation.

        Like repay and withdraw, borrow requires an existing Kamino obligation
        (the wallet must have deposited collateral first).
        """
        intent = BorrowIntent(
            protocol="kamino",
            collateral_token="SOL",
            collateral_amount=Decimal("0.01"),
            borrow_token="USDC",
            borrow_amount=Decimal("0.50"),  # Low LTV
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        # Expected: FAILED because the test wallet has no Kamino obligation
        assert result.status.value == "FAILED", (
            "Expected FAILED for wallet without Kamino obligation"
        )
        assert "KLEND_OBLIGATION_NOT_FOUND" in (result.error or ""), (
            f"Expected KLEND_OBLIGATION_NOT_FOUND error, got: {result.error}"
        )


class TestKaminoRepayCompilation:
    """Kamino repay: RepayIntent -> Compile -> ActionBundle (real API).

    Repay requires an existing obligation. The API will return
    KLEND_OBLIGATION_NOT_FOUND for a fresh wallet — we test that
    the compilation pipeline handles this gracefully.
    """

    @pytest.mark.asyncio
    async def test_compile_usdc_repay_fails_without_obligation(self, solana_compiler):
        """RepayIntent for USDC fails gracefully when wallet has no obligation."""
        intent = RepayIntent(
            protocol="kamino",
            token="USDC",
            amount=Decimal("0.50"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        # Expected: FAILED because the test wallet has no Kamino obligation
        # This proves the pipeline reaches the API and handles the error
        assert result.status.value == "FAILED", (
            "Expected FAILED for wallet without Kamino obligation"
        )
        assert "KLEND_OBLIGATION_NOT_FOUND" in (result.error or ""), (
            f"Expected KLEND_OBLIGATION_NOT_FOUND error, got: {result.error}"
        )


class TestKaminoWithdrawCompilation:
    """Kamino withdraw: WithdrawIntent -> Compile -> ActionBundle (real API).

    Withdraw requires an existing obligation. Same as repay — expected to fail
    for a fresh wallet.
    """

    @pytest.mark.asyncio
    async def test_compile_usdc_withdraw_fails_without_obligation(self, solana_compiler):
        """WithdrawIntent for USDC fails gracefully when wallet has no obligation."""
        intent = WithdrawIntent(
            protocol="kamino",
            token="USDC",
            amount=Decimal("0.50"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        # Expected: FAILED because the test wallet has no Kamino deposit
        assert result.status.value == "FAILED", (
            "Expected FAILED for wallet without Kamino obligation"
        )
        assert "KLEND_OBLIGATION_NOT_FOUND" in (result.error or ""), (
            f"Expected KLEND_OBLIGATION_NOT_FOUND error, got: {result.error}"
        )


# =============================================================================
# Layers 2-4: Execution Tests (require solana-test-validator)
# =============================================================================


@requires_solana_validator
class TestKaminoSupplyExecution:
    """Kamino supply: full 4-layer verification on local test-validator.

    Layer 1: Compilation success (SupplyIntent -> ActionBundle)
    Layer 2: Execution success (ActionBundle -> on-chain tx)
    Layer 3: Receipt parser integration (parse deposit amounts)
    Layer 4: Exact balance deltas (USDC decreases by supply amount)
    """

    @pytest.mark.asyncio
    async def test_supply_usdc(
        self, solana_fork, funded_solana_wallet, solana_orchestrator, execution_compiler,
    ):
        """Supply 10 USDC to Kamino: full 4-layer verification."""
        wallet_address, _ = funded_solana_wallet
        supply_amount = Decimal("10")

        # Layer 4 setup: Record balances BEFORE
        usdc_before = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        # Layer 1: Compile
        intent = SupplyIntent(
            protocol="kamino",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )
        compilation_result = execution_compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 2: Execute
        execution_result = await solana_orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parser
        from almanak.framework.connectors.kamino import KaminoReceiptParser

        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parser = KaminoReceiptParser(chain=CHAIN_NAME)
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.get("success") is True, "Receipt parser must succeed"

        # Layer 4: Exact balance delta
        usdc_after = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )
        usdc_spent = usdc_before - usdc_after
        expected_spent = int(supply_amount * Decimal(10 ** SOLANA_TOKEN_DECIMALS["USDC"]))
        assert usdc_spent == expected_spent, (
            f"USDC spent must EXACTLY equal supply amount. "
            f"Expected: {expected_spent}, Got: {usdc_spent}"
        )

    @pytest.mark.asyncio
    async def test_supply_insufficient_balance_fails(
        self, solana_fork, funded_solana_wallet, solana_orchestrator, execution_compiler,
    ):
        """Supply with 100x balance should fail. Balances must be conserved."""
        wallet_address, _ = funded_solana_wallet

        # Record balances BEFORE
        usdc_before = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        excessive_amount = Decimal("1000000")  # 1M USDC
        intent = SupplyIntent(
            protocol="kamino",
            token="USDC",
            amount=excessive_amount,
            chain=CHAIN_NAME,
        )

        compilation_result = execution_compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            "Compilation should succeed (insufficient balance is a runtime error)"
        )

        execution_result = await solana_orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail with insufficient balance"

        # Conservation check — MANDATORY
        usdc_after = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )
        assert usdc_after == usdc_before, (
            f"USDC balance must be unchanged after failed supply. "
            f"Before: {usdc_before}, After: {usdc_after}"
        )


@requires_solana_validator
class TestKaminoSupplyWithdrawRoundtrip:
    """Kamino supply -> withdraw roundtrip: verify principal recovery.

    Supply USDC, then withdraw. The withdrawn amount should equal the
    supplied amount (+ small interest allowed for the brief time held).
    """

    @pytest.mark.asyncio
    async def test_supply_then_withdraw(
        self, solana_fork, funded_solana_wallet, solana_orchestrator, execution_compiler,
    ):
        """Supply 10 USDC, then withdraw — verify roundtrip conservation."""
        wallet_address, _ = funded_solana_wallet
        supply_amount = Decimal("10")

        # Record USDC balance before roundtrip
        usdc_before = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        # Step 1: Supply
        supply_intent = SupplyIntent(
            protocol="kamino",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )
        supply_result = execution_compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS", (
            f"Supply compilation failed: {supply_result.error}"
        )

        supply_exec = await solana_orchestrator.execute(supply_result.action_bundle)
        assert supply_exec.success, f"Supply execution failed: {supply_exec.error}"

        # Step 2: Withdraw (same amount)
        withdraw_intent = WithdrawIntent(
            protocol="kamino",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )
        withdraw_result = execution_compiler.compile(withdraw_intent)
        assert withdraw_result.status.value == "SUCCESS", (
            f"Withdraw compilation failed: {withdraw_result.error}"
        )

        withdraw_exec = await solana_orchestrator.execute(withdraw_result.action_bundle)
        assert withdraw_exec.success, f"Withdraw execution failed: {withdraw_exec.error}"

        # Verify roundtrip conservation
        usdc_after = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        # Should recover principal (+ small interest allowed)
        usdc_diff = usdc_after - usdc_before
        # diff should be >= 0 (got back at least what we put in)
        assert usdc_diff >= 0, (
            f"Roundtrip should recover at least principal. "
            f"Before: {usdc_before}, After: {usdc_after}, Diff: {usdc_diff}"
        )
        # Interest earned should be tiny for the brief hold period
        max_interest_raw = int(Decimal("0.01") * Decimal(10 ** SOLANA_TOKEN_DECIMALS["USDC"]))
        assert usdc_diff <= max_interest_raw, (
            f"Interest earned seems too large: {usdc_diff} raw units"
        )
