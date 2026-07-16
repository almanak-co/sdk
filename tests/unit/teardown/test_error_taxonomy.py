"""Unit tests for the teardown error taxonomy.

Covers VIB-4532 (contract-arg / liquidity reverts), VIB-4664 (insufficient
balance pre-flight) and VIB-4258 (transport/RPC transient) — the three rows of
``classify_teardown_failure`` — plus a parity guard that the shared keyword core
(`error_keywords.categorize_error`, lifted out of
``IntentStateMachine._categorize_error``) still returns its legacy categories.
"""

import pytest

from almanak.framework.intents.error_keywords import categorize_error
from almanak.framework.teardown.error_taxonomy import (
    Disposition,
    RevertClass,
    classify_teardown_failure,
)


class TestInsufficientBalance:
    """VIB-4664 — pre-flight balance/collateral shortfalls are deterministic."""

    @pytest.mark.parametrize(
        "msg",
        [
            "Pre-flight balance check failed: Insufficient USDT: have 0.000000, need 0.300000.",
            "Insufficient WETH: need 1.0, have 0.5 (deficit: 0.5)",
            "Simulation failed: insufficient collateral",
            "Insufficient balance for transfer",
        ],
    )
    def test_non_retryable(self, msg: str) -> None:
        rc, disp = classify_teardown_failure(msg)
        assert disp is Disposition.NON_RETRYABLE
        assert rc is RevertClass.INSUFFICIENT_BALANCE


class TestContractArgInvalid:
    """VIB-4532 — contract-arg / approval reverts repeat at every slippage level."""

    @pytest.mark.parametrize(
        "msg",
        [
            "Simulation failed: inconsistent input",
            "Error: inconsistent inputs",
            "reverted: Error: Not approved",
            "execution reverted: InvalidParam",
            # ERC-20 allowance / transfer family (gemini review on PR #2507) —
            # deterministic, no slippage level can fix them.
            "ERC20: transfer amount exceeds allowance",
            "Dai/insufficient-allowance",
            "execution reverted: TRANSFER_FROM_FAILED",
            "TransferFrom failed",
        ],
    )
    def test_non_retryable(self, msg: str) -> None:
        rc, disp = classify_teardown_failure(msg)
        assert disp is Disposition.NON_RETRYABLE
        assert rc is RevertClass.CONTRACT_ARG_INVALID


class TestLiquidityAndPermanentDelegated:
    """Rich permanent reverts are delegated to the shared intent classifier."""

    @pytest.mark.parametrize(
        "msg",
        [
            "enso router rejected route with selector 0xef3dcb2f",
            "market not found",
            "pool not found",
            "Comptroller: insufficient_liquidity",
        ],
    )
    def test_non_retryable(self, msg: str) -> None:
        _rc, disp = classify_teardown_failure(msg)
        assert disp is Disposition.NON_RETRYABLE


class TestTransportTransient:
    """VIB-4258 — transport/RPC failures retry the SAME level, never escalate."""

    @pytest.mark.parametrize(
        "msg",
        [
            "Fork Error: Transport(Custom(reqwest dns error failed to lookup address information))",
            "Fork Error: host unreachable",
            "connection reset by peer",
            "broken pipe",
            "unexpected EOF",
            "nonce too low",
            "request timed out",
            "rate limit exceeded",
        ],
    )
    def test_retry_same_level(self, msg: str) -> None:
        rc, disp = classify_teardown_failure(msg)
        assert disp is Disposition.RETRY_SAME_LEVEL
        assert rc is RevertClass.TRANSPORT_TRANSIENT


class TestGasUnderestimate:
    """Gas underestimate is classified but NOT fixed here (VIB-4533 owns that)."""

    @pytest.mark.parametrize("msg", ["out of gas", "gas estimation failed", "gas limit too low"])
    def test_non_retryable_no_escalation(self, msg: str) -> None:
        rc, disp = classify_teardown_failure(msg)
        assert disp is Disposition.NON_RETRYABLE
        assert rc is RevertClass.GAS_UNDERESTIMATE


class TestSlippageStillEscalates:
    """The genuine slippage path must keep walking the ladder (no regression)."""

    @pytest.mark.parametrize(
        "msg",
        [
            "InsufficientOutputAmount",
            "Simulation failed: Too little received",
            "TOO_LITTLE_RECEIVED",
            "min_amount_out not met",
            "slippage too high",
            "price impact exceeds limit",
        ],
    )
    def test_escalate(self, msg: str) -> None:
        rc, disp = classify_teardown_failure(msg)
        assert disp is Disposition.ESCALATE
        assert rc is RevertClass.SLIPPAGE_MINIMUM_VIOLATED


class TestUnknownPreservesEscalation:
    """Unknown / bare reverts preserve the historical escalate behaviour."""

    @pytest.mark.parametrize("msg", ["", None, "some totally novel revert reason", "execution reverted"])
    def test_escalate(self, msg: str | None) -> None:
        rc, disp = classify_teardown_failure(msg)
        assert disp is Disposition.ESCALATE
        assert rc is RevertClass.UNKNOWN


class TestSharedKeywordParity:
    """Guard the extraction: ``categorize_error`` keeps the legacy contract.

    This is the parity check for lifting ``IntentStateMachine._categorize_error``
    into ``error_keywords`` — VIB-2866 / VIB-1215 categories must be unchanged.
    """

    @pytest.mark.parametrize(
        "msg,expected",
        [
            ("enso router rejected route with selector 0x", "COMPILATION_PERMANENT"),
            ("market not found", "COMPILATION_PERMANENT"),
            ("collateral_cannot_cover_new_borrow", "COMPILATION_PERMANENT"),
            ("Comptroller: insufficient_liquidity", "COMPILATION_PERMANENT"),
            ("cannot connect to host", "COMPILATION_PERMANENT"),
            ("Insufficient funds", "INSUFFICIENT_FUNDS"),
            ("nonce too low", "NONCE_ERROR"),
            ("request timed out", "TIMEOUT"),
            ("slippage too high", "SLIPPAGE"),
            ("rate limit exceeded", "RATE_LIMIT"),
            ("connection dropped", "NETWORK_ERROR"),
            ("totally novel error string", None),
        ],
    )
    def test_legacy_categories_unchanged(self, msg: str, expected: str | None) -> None:
        assert categorize_error(msg) == expected


class TestVaultCashShortage:
    """VIB-5801 — a lending vault that cannot settle a redeem right now.

    Distinct from the CALLER being short (INSUFFICIENT_BALANCE / NON_RETRYABLE): the
    caller owns the shares; the vault has lent the underlying out. Liquidity returns as
    borrowers repay, so the correct reaction is to retry at the SAME level. Escalating
    slippage is meaningless for a cash shortage and just re-broadcasts a reverting redeem
    at each rung.
    """

    @pytest.mark.parametrize(
        "message",
        [
            "execution reverted: E_InsufficientCash()",  # EVK / euler_v2, decoded
            'execution reverted, data: "0xf077d877"',  # EVK, bare selector
            "execution reverted: NotEnoughLiquidity()",  # Silo V2, decoded
            'execution reverted, data: "0x4323a555"',  # Silo V2, bare selector
        ],
    )
    def test_vault_cash_shortage_is_liquidity_unavailable_and_retryable(self, message):
        revert_class, disposition = classify_teardown_failure(message)
        assert revert_class == RevertClass.LIQUIDITY_UNAVAILABLE
        assert disposition == Disposition.RETRY_SAME_LEVEL
        # Never terminal: the funds are recoverable once the vault has cash again.
        assert disposition != Disposition.NON_RETRYABLE
        # Never a slippage bump: slippage cannot conjure cash into a vault.
        assert disposition != Disposition.ESCALATE

    def test_caller_short_still_classifies_as_insufficient_balance(self):
        # Guard the boundary: the new branch must not swallow the caller-is-short case,
        # which is deterministic and correctly NON_RETRYABLE.
        revert_class, disposition = classify_teardown_failure("Insufficient USDC: need 100, have 5 (deficit: 95)")
        assert revert_class == RevertClass.INSUFFICIENT_BALANCE
        assert disposition == Disposition.NON_RETRYABLE
