"""Tests for empty ActionBundle handling in ExecutionOrchestrator.

HOLD intents legitimately produce 0 transactions (SUCCESS).
All other intent types with 0 transactions should fail (not false-positive SUCCESS).

Fixes VIB-234.
"""

import pytest

from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.models.reproduction_bundle import ActionBundle
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_orchestrator():
    """Create an ExecutionOrchestrator with mocked dependencies."""
    signer = MagicMock()
    signer.address = "0x1234567890abcdef1234567890abcdef12345678"
    submitter = MagicMock()
    simulator = MagicMock()

    orch = ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain="arbitrum",
    )
    return orch


class TestEmptyBundleHandling:
    """Test that empty bundles are handled correctly per intent type."""

    @pytest.mark.asyncio
    async def test_hold_intent_empty_bundle_is_success(self, mock_orchestrator):
        """HOLD intents with 0 transactions should succeed."""
        bundle = ActionBundle(intent_type="HOLD", transactions=[])
        result = await mock_orchestrator.execute(bundle)

        assert result.success is True
        assert result.phase == ExecutionPhase.COMPLETE
        assert result.error is None

    @pytest.mark.asyncio
    async def test_lp_close_empty_bundle_is_failure(self, mock_orchestrator):
        """LP_CLOSE with 0 transactions should fail (no position found)."""
        bundle = ActionBundle(intent_type="LP_CLOSE", transactions=[])
        result = await mock_orchestrator.execute(bundle)

        assert result.success is False
        assert result.phase == ExecutionPhase.COMPLETE
        assert result.error is not None
        assert "Empty ActionBundle" in result.error
        assert "LP_CLOSE" in result.error

    @pytest.mark.asyncio
    async def test_swap_empty_bundle_is_failure(self, mock_orchestrator):
        """SWAP with 0 transactions should fail."""
        bundle = ActionBundle(intent_type="SWAP", transactions=[])
        result = await mock_orchestrator.execute(bundle)

        assert result.success is False
        assert "Empty ActionBundle" in result.error
        assert "SWAP" in result.error

    @pytest.mark.asyncio
    async def test_lp_open_empty_bundle_is_failure(self, mock_orchestrator):
        """LP_OPEN with 0 transactions should fail."""
        bundle = ActionBundle(intent_type="LP_OPEN", transactions=[])
        result = await mock_orchestrator.execute(bundle)

        assert result.success is False
        assert "Empty ActionBundle" in result.error

    @pytest.mark.asyncio
    async def test_supply_empty_bundle_is_failure(self, mock_orchestrator):
        """SUPPLY with 0 transactions should fail."""
        bundle = ActionBundle(intent_type="SUPPLY", transactions=[])
        result = await mock_orchestrator.execute(bundle)

        assert result.success is False
        assert "Empty ActionBundle" in result.error

    @pytest.mark.asyncio
    async def test_hold_lowercase_still_succeeds(self, mock_orchestrator):
        """HOLD intent type comparison should be case-insensitive."""
        bundle = ActionBundle(intent_type="hold", transactions=[])
        result = await mock_orchestrator.execute(bundle)

        assert result.success is True
        assert result.error is None

    @pytest.mark.asyncio
    async def test_unknown_intent_type_empty_bundle_is_failure(self, mock_orchestrator):
        """Unknown intent types with 0 transactions should also fail."""
        bundle = ActionBundle(intent_type="UNKNOWN", transactions=[])
        result = await mock_orchestrator.execute(bundle)

        assert result.success is False
        assert "Empty ActionBundle" in result.error
