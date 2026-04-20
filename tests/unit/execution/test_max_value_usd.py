"""Tests for the USD-denominated per-transaction value cap (VIB-3133).

Background: ``TransactionRiskConfig.max_value_eth`` was the only per-tx value
cap. It was enforced in raw wei against ``tx.value`` regardless of chain, so
26 POL on Polygon (~$5.50) tripped the same wei threshold as 26 ETH on
Ethereum (~$70K). The fix introduces ``max_value_usd`` which converts the
native amount via ``native_token_price_usd`` before comparing.

These tests pin the new behavior, the legacy opt-in, and the fail-closed
contract when the oracle price is missing.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.interfaces import TransactionType, UnsignedTransaction
from almanak.framework.execution.orchestrator import (
    ExecutionOrchestrator,
    TransactionRiskConfig,
)


def _make_orchestrator(
    chain: str,
    *,
    max_value_usd: Decimal = Decimal("0"),
    max_value_eth: Decimal = Decimal("0"),
    native_token_price_usd: float = 0.0,
) -> ExecutionOrchestrator:
    """Build an orchestrator wired for value-cap tests only."""
    signer = MagicMock()
    signer.address = "0x1234567890abcdef1234567890abcdef12345678"
    submitter = MagicMock()
    simulator = MagicMock()

    tx_risk_config = TransactionRiskConfig.permissive()  # disables every other guard
    tx_risk_config.max_value_usd = max_value_usd
    tx_risk_config.max_value_eth = max_value_eth
    tx_risk_config.native_token_price_usd = native_token_price_usd

    return ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain=chain,
        tx_risk_config=tx_risk_config,
    )


def _native_tx(amount_native: Decimal, chain_id: int) -> UnsignedTransaction:
    """Build a tx that transfers ``amount_native`` of the chain's native token."""
    return UnsignedTransaction(
        to="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        value=int(amount_native * Decimal(10**18)),
        data="0x",
        chain_id=chain_id,
        gas_limit=21_000,
        tx_type=TransactionType.LEGACY,
        gas_price=1_000_000_000,
    )


# =============================================================================
# Reproducer from the ticket: POL on Polygon
# =============================================================================


class TestPolygonReproducer:
    """26 POL on Polygon must NOT be blocked by the cross-chain USD cap."""

    @pytest.mark.asyncio
    async def test_26_pol_passes_default_cap(self):
        """26 POL @ $0.21 ~= $5.50 — well under the $50K default cap."""
        orchestrator = _make_orchestrator(
            chain="polygon",
            max_value_usd=Decimal("50000"),
            native_token_price_usd=0.21,
        )
        result = await orchestrator._validate_transactions(
            [_native_tx(Decimal("26"), chain_id=137)],
            context=MagicMock(),
        )
        assert result.passed is True, result.violations

    @pytest.mark.asyncio
    async def test_26_pol_fails_tight_cap(self):
        """26 POL @ $0.21 ~= $5.50 — exceeds a $5 cap."""
        orchestrator = _make_orchestrator(
            chain="polygon",
            max_value_usd=Decimal("5"),
            native_token_price_usd=0.21,
        )
        result = await orchestrator._validate_transactions(
            [_native_tx(Decimal("26"), chain_id=137)],
            context=MagicMock(),
        )
        assert result.passed is False
        assert "exceeds" in result.violations[0]
        assert "$5" in result.violations[0]


# =============================================================================
# Symmetry: ETH on Ethereum should still trip a sensible default
# =============================================================================


class TestEthereumStillBlocks:
    """26 ETH on Ethereum must FAIL the USD cap — that was always the intent."""

    @pytest.mark.asyncio
    async def test_26_eth_at_2700_exceeds_50k(self):
        """26 ETH @ $2700 = $70,200 — over the $50K default cap."""
        orchestrator = _make_orchestrator(
            chain="ethereum",
            max_value_usd=Decimal("50000"),
            native_token_price_usd=2700.0,
        )
        result = await orchestrator._validate_transactions(
            [_native_tx(Decimal("26"), chain_id=1)],
            context=MagicMock(),
        )
        assert result.passed is False
        # Message should include both USD and native amounts for forensics.
        msg = result.violations[0]
        assert "$70200" in msg.replace(",", "") or "70200" in msg.replace(",", "")
        assert "native=26" in msg


# =============================================================================
# Fail-closed when the oracle price is unavailable
# =============================================================================


class TestFailClosedOracle:
    """If max_value_usd > 0 but native_token_price_usd <= 0, block."""

    @pytest.mark.asyncio
    async def test_no_price_blocks_with_clear_message(self):
        orchestrator = _make_orchestrator(
            chain="polygon",
            max_value_usd=Decimal("50000"),
            native_token_price_usd=0.0,
        )
        result = await orchestrator._validate_transactions(
            [_native_tx(Decimal("1"), chain_id=137)],
            context=MagicMock(),
        )
        assert result.passed is False
        assert "native_token_price_usd" in result.violations[0]
        assert "fail-closed" in result.violations[0]

    @pytest.mark.asyncio
    async def test_zero_value_tx_does_not_trigger_oracle_check(self):
        """Most non-native-transfer txs (swaps, approvals) carry value=0 and
        must not be blocked solely because the oracle price wasn't fetched."""
        orchestrator = _make_orchestrator(
            chain="polygon",
            max_value_usd=Decimal("50000"),
            native_token_price_usd=0.0,
        )
        zero_value_tx = UnsignedTransaction(
            to="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            value=0,
            data="0xdeadbeef",
            chain_id=137,
            gas_limit=200_000,
            tx_type=TransactionType.LEGACY,
            gas_price=1_000_000_000,
        )
        result = await orchestrator._validate_transactions(
            [zero_value_tx], context=MagicMock()
        )
        assert result.passed is True, result.violations


# =============================================================================
# Legacy max_value_eth — still honored when explicitly set
# =============================================================================


class TestLegacyWeiCap:
    """max_value_eth=0 by default; still works when a user explicitly opts in."""

    @pytest.mark.asyncio
    async def test_default_does_not_enforce_wei_cap(self):
        """Default for_chain config has max_value_eth=0 — wei cap is off."""
        config = TransactionRiskConfig.for_chain("polygon")
        assert config.max_value_eth == Decimal("0")

    @pytest.mark.asyncio
    async def test_explicit_wei_cap_blocks_when_exceeded(self):
        """Setting max_value_eth=10 still works for the legacy path."""
        orchestrator = _make_orchestrator(
            chain="polygon",
            max_value_eth=Decimal("10"),
            # No max_value_usd, no oracle price — pure legacy path.
        )
        result = await orchestrator._validate_transactions(
            [_native_tx(Decimal("26"), chain_id=137)],
            context=MagicMock(),
        )
        assert result.passed is False
        assert "max_value_eth" in result.violations[0]


# =============================================================================
# Default factory pins the new behavior
# =============================================================================


class TestFactoryDefaults:
    """USD cap is opt-in (off by default). The CLI opts in via env var; other
    callers (gateway, paper trading) leave it off so they don't need to
    hydrate native_token_price_usd. Codex flagged that defaulting it on
    would fail-close every native-value tx on those paths."""

    def test_for_chain_does_not_enable_usd_cap_by_default(self):
        config = TransactionRiskConfig.for_chain("polygon")
        assert config.max_value_usd == Decimal("0")
        assert config.max_value_eth == Decimal("0")

    def test_default_does_not_enable_usd_cap_by_default(self):
        config = TransactionRiskConfig.default()
        assert config.max_value_usd == Decimal("0")
        assert config.max_value_eth == Decimal("0")

    def test_permissive_disables_both_caps(self):
        config = TransactionRiskConfig.permissive()
        assert config.max_value_usd == Decimal("0")
        assert config.max_value_eth == Decimal("0")


# =============================================================================
# Boundary + multi-tx coverage (Claude pr-auditor #5)
# =============================================================================


class TestEdgeCases:
    """Pin behavior at exact boundary, across multi-tx bundles, and when both
    caps are configured (independent enforcement)."""

    @pytest.mark.asyncio
    async def test_value_exactly_at_cap_passes(self):
        """Validator uses strict ``>``: a tx exactly at the cap must PASS."""
        orchestrator = _make_orchestrator(
            chain="polygon",
            max_value_usd=Decimal("100"),
            native_token_price_usd=1.0,  # 100 native units * $1 = $100 == cap
        )
        result = await orchestrator._validate_transactions(
            [_native_tx(Decimal("100"), chain_id=137)],
            context=MagicMock(),
        )
        assert result.passed is True, result.violations

    @pytest.mark.asyncio
    async def test_multi_tx_bundle_reports_all_violations(self):
        """In a 3-tx bundle where #0 passes and #1, #2 exceed, both
        violations should appear so the operator sees the full picture."""
        orchestrator = _make_orchestrator(
            chain="polygon",
            max_value_usd=Decimal("50"),
            native_token_price_usd=1.0,
        )
        txs = [
            _native_tx(Decimal("10"), chain_id=137),  # $10 — pass
            _native_tx(Decimal("75"), chain_id=137),  # $75 — fail
            _native_tx(Decimal("100"), chain_id=137),  # $100 — fail
        ]
        result = await orchestrator._validate_transactions(txs, context=MagicMock())
        assert result.passed is False
        # Two violation lines, one for each over-cap tx.
        over_cap_violations = [v for v in result.violations if "exceeds" in v]
        assert len(over_cap_violations) == 2
        assert any("Transaction 1" in v for v in over_cap_violations)
        assert any("Transaction 2" in v for v in over_cap_violations)

    @pytest.mark.asyncio
    async def test_both_caps_set_each_enforced_independently(self):
        """If both ``max_value_usd`` and ``max_value_eth`` are configured,
        a tx that violates either should generate a violation per cap."""
        orchestrator = _make_orchestrator(
            chain="polygon",
            max_value_usd=Decimal("5"),
            max_value_eth=Decimal("1"),  # legacy wei cap also active
            native_token_price_usd=1.0,  # 26 POL = $26 USD, > $5
        )
        result = await orchestrator._validate_transactions(
            [_native_tx(Decimal("26"), chain_id=137)],
            context=MagicMock(),
        )
        assert result.passed is False
        # Two distinct violations from the two distinct caps.
        assert any("max_value_eth" in v for v in result.violations)
        assert any("$26" in v.replace(",", "") for v in result.violations)
        assert len(result.violations) >= 2

    @pytest.mark.asyncio
    async def test_negative_cap_rejects_at_validation_time(self):
        """A negative cap value must be reported as misconfiguration, not
        silently treated as 'disabled' (CodeRabbit catch on PR #1568)."""
        orchestrator = _make_orchestrator(
            chain="polygon",
            max_value_usd=Decimal("-1"),
            native_token_price_usd=1.0,
        )
        result = await orchestrator._validate_transactions(
            [_native_tx(Decimal("1"), chain_id=137)],
            context=MagicMock(),
        )
        assert result.passed is False
        assert "misconfigured" in result.violations[0]
        assert "max_value_usd" in result.violations[0]

    @pytest.mark.asyncio
    async def test_violation_message_uses_thousands_separator(self):
        """Audit nit: message should print ``$50,000.00`` not ``$50000``."""
        orchestrator = _make_orchestrator(
            chain="ethereum",
            max_value_usd=Decimal("50000"),
            native_token_price_usd=2700.0,
        )
        result = await orchestrator._validate_transactions(
            [_native_tx(Decimal("26"), chain_id=1)],
            context=MagicMock(),
        )
        assert result.passed is False
        assert "$50,000.00" in result.violations[0]
        assert "$70,200.00" in result.violations[0]
