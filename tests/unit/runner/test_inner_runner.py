"""Tests for IntentExecutionService -- the shared execution pipeline.

Verifies:
- Retry logic with exponential backoff
- Result enrichment via ResultEnricher
- Sadflow hook invocation on failures
- Non-retryable error detection
"""

import asyncio
import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.runner.inner_runner import (
    EnrichedExecutionResult,
    IntentExecutionService,
    RetryPolicy,
    SadflowEvent,
    _is_retryable,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_gateway():
    """Create a mock GatewayClient with execution stubs."""
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def service(mock_gateway):
    """Create an IntentExecutionService with fast retry for testing."""
    return IntentExecutionService(
        mock_gateway,
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        strategy_id="test-strategy",
        retry_policy=RetryPolicy(
            max_retries=2,
            initial_delay_seconds=0.01,  # Fast for tests
            max_delay_seconds=0.05,
            backoff_multiplier=2.0,
        ),
    )


def _make_compile_resp(success=True, error="", bundle_data=None):
    """Helper to create a mock CompileIntent response."""
    resp = MagicMock()
    resp.success = success
    resp.error = error
    if bundle_data is None:
        bundle_data = {"actions": [{"type": "SWAP"}]}
    resp.action_bundle = json.dumps(bundle_data).encode()
    return resp


def _make_exec_resp(success=True, error="", tx_hashes=None, receipts=None):
    """Helper to create a mock Execute response."""
    resp = MagicMock()
    resp.success = success
    resp.error = error
    resp.tx_hashes = tx_hashes or (["0xabc123"] if success else [])
    if receipts is not None:
        resp.receipts = json.dumps(receipts).encode()
    else:
        resp.receipts = b"[]"
    return resp


# =============================================================================
# RetryPolicy tests
# =============================================================================


class TestRetryPolicy:
    def test_default_values(self):
        policy = RetryPolicy()
        assert policy.max_retries == 3
        assert policy.initial_delay_seconds == 1.0
        assert policy.max_delay_seconds == 60.0
        assert policy.backoff_multiplier == 2.0

    def test_delay_for_attempt_exponential(self):
        policy = RetryPolicy(initial_delay_seconds=1.0, backoff_multiplier=2.0, max_delay_seconds=100.0)
        assert policy.delay_for_attempt(0) == 1.0
        assert policy.delay_for_attempt(1) == 2.0
        assert policy.delay_for_attempt(2) == 4.0
        assert policy.delay_for_attempt(3) == 8.0

    def test_delay_capped_at_max(self):
        policy = RetryPolicy(initial_delay_seconds=1.0, backoff_multiplier=2.0, max_delay_seconds=5.0)
        assert policy.delay_for_attempt(10) == 5.0


# =============================================================================
# Non-retryable error detection
# =============================================================================


class TestRetryability:
    def test_insufficient_funds_not_retryable(self):
        assert not _is_retryable("Transaction failed: insufficient funds for gas")

    def test_execution_reverted_not_retryable(self):
        assert not _is_retryable("execution reverted: AMOUNT_TOO_LOW")

    def test_nonce_too_low_not_retryable(self):
        assert not _is_retryable("nonce too low: next nonce 42, got 41")

    def test_unauthenticated_not_retryable(self):
        assert not _is_retryable("UNAUTHENTICATED: invalid token")

    def test_no_authentication_token_not_retryable(self):
        assert not _is_retryable("No authentication token provided")

    def test_permission_denied_not_retryable(self):
        assert not _is_retryable("PERMISSION_DENIED: insufficient scope")

    def test_timeout_is_retryable(self):
        assert _is_retryable("Transaction confirmation timeout after 60s")

    def test_rpc_error_is_retryable(self):
        assert _is_retryable("gRPC connection reset")

    def test_empty_error_is_retryable(self):
        assert _is_retryable("")


# =============================================================================
# IntentExecutionService - success path
# =============================================================================


class TestIntentExecutionServiceSuccess:
    @pytest.mark.asyncio
    async def test_successful_execution(self, service, mock_gateway):
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp()

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH", "amount": "1000"},
        )

        assert result.success
        assert result.tx_hashes == ["0xabc123"]
        assert result.attempts == 1
        assert result.error is None

    @pytest.mark.asyncio
    async def test_dry_run_returns_success(self, service, mock_gateway):
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(success=True, tx_hashes=[])

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH", "amount": "1000"},
            dry_run=True,
        )

        assert result.success
        assert result.dry_run

    @pytest.mark.asyncio
    async def test_chain_override(self, service, mock_gateway):
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp()

        await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH"},
            chain="base",
        )

        # Verify the compile call used the overridden chain
        call_args = mock_gateway.execution.CompileIntent.call_args
        assert call_args[0][0].chain == "base"


# =============================================================================
# IntentExecutionService - retry logic
# =============================================================================


class TestIntentExecutionServiceRetry:
    @pytest.mark.asyncio
    async def test_retries_on_execution_failure(self, service, mock_gateway):
        """Execution fails twice then succeeds on third attempt."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.side_effect = [
            _make_exec_resp(success=False, error="timeout waiting for confirmation"),
            _make_exec_resp(success=False, error="timeout waiting for confirmation"),
            _make_exec_resp(success=True),
        ]

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH"},
        )

        assert result.success
        assert result.attempts == 3

    @pytest.mark.asyncio
    async def test_retries_exhausted_returns_failure(self, service, mock_gateway):
        """All retries exhausted returns failure."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=False, error="timeout waiting for confirmation"
        )

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH"},
        )

        assert not result.success
        assert result.attempts == 3  # initial + 2 retries
        assert "timeout" in result.error.lower()

    @pytest.mark.asyncio
    async def test_non_retryable_error_fails_immediately(self, service, mock_gateway):
        """Non-retryable errors skip retry attempts."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=False, error="execution reverted: AMOUNT_TOO_LOW"
        )

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH"},
        )

        assert not result.success
        assert result.attempts == 1  # No retries for non-retryable errors
        assert "execution reverted" in result.error.lower()

    @pytest.mark.asyncio
    async def test_compilation_failure_retries(self, service, mock_gateway):
        """Compilation failures are also retried (if retryable)."""
        mock_gateway.execution.CompileIntent.side_effect = [
            _make_compile_resp(success=False, error="gateway timeout"),
            _make_compile_resp(success=True),
        ]
        mock_gateway.execution.Execute.return_value = _make_exec_resp()

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH"},
        )

        assert result.success
        assert result.attempts == 2

    @pytest.mark.asyncio
    async def test_compilation_non_retryable_fails_immediately(self, service, mock_gateway):
        """Non-retryable compilation errors don't retry."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp(
            success=False, error="execution reverted: invalid selector"
        )

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH"},
        )

        assert not result.success
        assert result.attempts == 1


# =============================================================================
# IntentExecutionService - sadflow hooks
# =============================================================================


class TestIntentExecutionServiceSadflow:
    @pytest.mark.asyncio
    async def test_sadflow_callback_invoked_on_failure(self, mock_gateway):
        """Sadflow callback fires on execution failure."""
        sadflow_events = []

        service = IntentExecutionService(
            mock_gateway,
            chain="arbitrum",
            wallet_address="0x1234",
            strategy_id="test",
            retry_policy=RetryPolicy(max_retries=0, initial_delay_seconds=0.01),
            on_sadflow=sadflow_events.append,
        )

        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=False, error="some timeout error"
        )

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH"},
            tool_name="swap_tokens",
        )

        assert not result.success
        assert len(sadflow_events) == 1
        event = sadflow_events[0]
        assert isinstance(event, SadflowEvent)
        assert event.intent_type == "swap"
        assert event.tool_name == "swap_tokens"
        assert event.is_final
        assert "timeout" in event.error.lower()

    @pytest.mark.asyncio
    async def test_sadflow_fires_for_each_retry(self, mock_gateway):
        """Sadflow fires on each failed attempt, with is_final=True on last."""
        sadflow_events = []

        service = IntentExecutionService(
            mock_gateway,
            chain="arbitrum",
            wallet_address="0x1234",
            strategy_id="test",
            retry_policy=RetryPolicy(max_retries=1, initial_delay_seconds=0.01),
            on_sadflow=sadflow_events.append,
        )

        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=False, error="gateway timeout"
        )

        await service.execute_intent(
            intent_type="lp_open",
            intent_params={"pool": "ETH/USDC"},
        )

        # Should fire twice: attempt 0 (not final), attempt 1 (final)
        assert len(sadflow_events) == 2
        assert not sadflow_events[0].is_final
        assert sadflow_events[1].is_final

    @pytest.mark.asyncio
    async def test_sadflow_callback_error_swallowed(self, mock_gateway):
        """Sadflow callback errors are swallowed (non-fatal)."""

        def bad_callback(event):
            raise RuntimeError("callback exploded")

        service = IntentExecutionService(
            mock_gateway,
            chain="arbitrum",
            wallet_address="0x1234",
            strategy_id="test",
            retry_policy=RetryPolicy(max_retries=0),
            on_sadflow=bad_callback,
        )

        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=False, error="timeout"
        )

        # Should not raise despite callback error
        result = await service.execute_intent(
            intent_type="swap",
            intent_params={},
        )
        assert not result.success


# =============================================================================
# IntentExecutionService - result enrichment
# =============================================================================


class TestIntentExecutionServiceEnrichment:
    @pytest.mark.asyncio
    async def test_enrichment_extracts_position_id(self, service, mock_gateway):
        """ResultEnricher extracts position_id from LP_OPEN receipts.

        The Uniswap V3 receipt parser looks for ERC-721 Transfer events from
        the NonfungiblePositionManager where from=0x0 (mint). The tokenId
        is in topics[3] (4th topic).
        """
        # ERC-721 Transfer(address from, address to, uint256 tokenId)
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        zero_address_padded = "0x" + "0" * 64
        token_id = 42
        # tokenId in topics[3] as 32-byte hex
        token_id_padded = "0x" + hex(token_id)[2:].zfill(64)
        # recipient address (topics[2])
        recipient_padded = "0x" + "1234567890abcdef1234567890abcdef12345678".zfill(64)

        receipt = {
            "status": "0x1",
            "transactionHash": "0xabc123",
            "gasUsed": "0x5208",
            "logs": [
                {
                    "topics": [transfer_topic, zero_address_padded, recipient_padded, token_id_padded],
                    "data": "0x",
                    "address": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
                }
            ],
        }

        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=True,
            tx_hashes=["0xabc123"],
            receipts=[receipt],
        )

        result = await service.execute_intent(
            intent_type="lp_open",
            intent_params={"pool": "ETH/USDC", "protocol": "uniswap_v3"},
            protocol="uniswap_v3",
        )

        assert result.success
        assert result.position_id == token_id

    @pytest.mark.asyncio
    async def test_enrichment_does_not_crash_on_missing_receipts(self, service, mock_gateway):
        """Enrichment gracefully handles missing receipts."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=True, tx_hashes=["0xabc"], receipts=[],
        )

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={"from_token": "USDC", "to_token": "ETH"},
        )

        assert result.success
        # No enrichment but no crash
        assert result.position_id is None

    @pytest.mark.asyncio
    async def test_enrichment_skipped_on_dry_run(self, service, mock_gateway):
        """Enrichment is not run for dry_run executions."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(success=True)

        result = await service.execute_intent(
            intent_type="swap",
            intent_params={},
            dry_run=True,
        )

        assert result.success
        assert result.dry_run
        # Enrichment should not have been attempted
        assert result.position_id is None
        assert not result.extracted_data


# =============================================================================
# EnrichedExecutionResult
# =============================================================================


class TestEnrichedExecutionResult:
    def test_tx_hash_property(self):
        result = EnrichedExecutionResult(success=True, tx_hashes=["0xabc", "0xdef"])
        assert result.tx_hash == "0xabc"

    def test_tx_hash_property_empty(self):
        result = EnrichedExecutionResult(success=True, tx_hashes=[])
        assert result.tx_hash is None

    def test_default_values(self):
        result = EnrichedExecutionResult(success=False, error="test")
        assert result.attempts == 1
        assert not result.dry_run
        assert result.position_id is None
        assert result.swap_amounts is None
        assert result.lp_close_data is None
        assert result.extracted_data == {}
        assert result.extraction_warnings == []


# =============================================================================
# IntentExecutionService - retry log-level matrix (VIB-1147)
# =============================================================================


class TestRetryLogLevelMatrix:
    """Verify intermediate retry failures log at DEBUG, final failures at WARNING."""

    @pytest.mark.asyncio
    async def test_intermediate_compile_rpc_error_logs_debug(self, service, mock_gateway, caplog):
        """Retryable compile RPC error on non-final attempt -> DEBUG."""
        mock_gateway.execution.CompileIntent.side_effect = [
            Exception("gateway timeout"),
            _make_compile_resp(success=True),
        ]
        mock_gateway.execution.Execute.return_value = _make_exec_resp()

        with caplog.at_level(logging.DEBUG):
            result = await service.execute_intent("swap", {"from_token": "USDC", "to_token": "ETH"})

        assert result.success
        compile_fail_records = [
            r for r in caplog.records if "Intent compilation failed" in r.message and "attempt 1/" in r.message
        ]
        assert len(compile_fail_records) == 1
        assert compile_fail_records[0].levelno == logging.DEBUG

    @pytest.mark.asyncio
    async def test_final_compile_rpc_error_logs_warning(self, mock_gateway, caplog):
        """Retryable compile RPC error on final attempt -> WARNING."""
        service = IntentExecutionService(
            mock_gateway, chain="arbitrum", wallet_address="0x1234",
            strategy_id="test", retry_policy=RetryPolicy(max_retries=0, initial_delay_seconds=0.01),
        )
        mock_gateway.execution.CompileIntent.side_effect = Exception("gateway timeout")

        with caplog.at_level(logging.DEBUG):
            result = await service.execute_intent("swap", {"from_token": "USDC", "to_token": "ETH"})

        assert not result.success
        compile_fail_records = [r for r in caplog.records if "Intent compilation failed" in r.message]
        assert len(compile_fail_records) == 1
        assert compile_fail_records[0].levelno == logging.WARNING

    @pytest.mark.asyncio
    async def test_non_retryable_compile_error_logs_warning_immediately(self, service, mock_gateway, caplog):
        """Non-retryable compile error -> WARNING on first attempt."""
        mock_gateway.execution.CompileIntent.side_effect = Exception("execution reverted: bad selector")

        with caplog.at_level(logging.DEBUG):
            result = await service.execute_intent("swap", {"from_token": "USDC", "to_token": "ETH"})

        assert not result.success
        assert result.attempts == 1
        compile_fail_records = [r for r in caplog.records if "Intent compilation failed" in r.message]
        assert len(compile_fail_records) == 1
        assert compile_fail_records[0].levelno == logging.WARNING

    @pytest.mark.asyncio
    async def test_intermediate_exec_failure_logs_debug(self, service, mock_gateway, caplog):
        """Retryable execution failure on non-final attempt -> DEBUG."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.side_effect = [
            _make_exec_resp(success=False, error="timeout waiting for confirmation"),
            _make_exec_resp(success=True),
        ]

        with caplog.at_level(logging.DEBUG):
            result = await service.execute_intent("swap", {"from_token": "USDC", "to_token": "ETH"})

        assert result.success
        exec_fail_records = [
            r for r in caplog.records if "Intent execution failed" in r.message and "attempt 1/" in r.message
        ]
        assert len(exec_fail_records) == 1
        assert exec_fail_records[0].levelno == logging.DEBUG

    @pytest.mark.asyncio
    async def test_final_exec_failure_logs_warning(self, mock_gateway, caplog):
        """Retryable execution failure on final attempt -> WARNING."""
        service = IntentExecutionService(
            mock_gateway, chain="arbitrum", wallet_address="0x1234",
            strategy_id="test", retry_policy=RetryPolicy(max_retries=0, initial_delay_seconds=0.01),
        )
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=False, error="timeout waiting for confirmation"
        )

        with caplog.at_level(logging.DEBUG):
            result = await service.execute_intent("swap", {"from_token": "USDC", "to_token": "ETH"})

        assert not result.success
        exec_fail_records = [r for r in caplog.records if "Intent execution failed" in r.message]
        assert len(exec_fail_records) == 1
        assert exec_fail_records[0].levelno == logging.WARNING

    @pytest.mark.asyncio
    async def test_exec_with_tx_hashes_logs_warning(self, service, mock_gateway, caplog):
        """Execution failure with tx_hashes (broadcast) -> WARNING regardless of attempt."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=False, error="timeout waiting for confirmation", tx_hashes=["0xbroadcast"]
        )

        with caplog.at_level(logging.DEBUG):
            result = await service.execute_intent("swap", {"from_token": "USDC", "to_token": "ETH"})

        assert not result.success
        exec_fail_records = [r for r in caplog.records if "Intent execution failed" in r.message]
        assert len(exec_fail_records) == 1
        assert exec_fail_records[0].levelno == logging.WARNING
