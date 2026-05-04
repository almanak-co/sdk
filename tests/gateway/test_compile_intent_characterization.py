"""Characterization tests for ``ExecutionService.CompileIntent`` (Phase 8.3a).

These tests capture the current observable behaviour of the RPC as documented
checkpoints. They do not change production code - if any assertion fails, the
refactor broke a behaviour the tests pinned.

Focus areas (complementing the existing ``test_compile_intent_*.py`` suites):

- Pre-compile validation gates (missing intent_type, invalid chain, invalid
  wallet_address, malformed JSON body).
- Default chain fallback (empty ``chain`` -> ``"arbitrum"``).
- Post-compile branches: ``CompilationStatus.FAILED`` and the
  ``action_bundle is None`` path.
- Unexpected ``Exception`` during ``compiler.compile`` -> ``INTERNAL``.
- Success response shape and the ``_sensitive_data`` roundtrip contract for
  the Execute step.
- Compiler state restoration when no ``price_map`` is supplied on non-mainnet
  networks (placeholder oracle preserved).

All tests use the shared harness in ``grpc_harness``.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.intents.compiler import CompilationResult, CompilationStatus
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer
from tests.gateway.grpc_harness import (
    assert_grpc_error,
    assert_set_code_not_called,
    make_grpc_context,
)

VALID_WALLET = "0x1234567890abcdef1234567890abcdef12345678"
VALID_SWAP_INTENT_DATA = {
    "from_token": "USDC",
    "to_token": "WETH",
    "amount": "100",
    "chain": "arbitrum",
    "max_slippage": "0.005",
}


@pytest.fixture
def service() -> ExecutionServiceServicer:
    """Fresh ExecutionService with a no-op _ensure_initialized.

    Uses ``network="anvil"`` so the mainnet price-sensitive gate does not
    short-circuit tests that exercise later compile-layer branches. Dedicated
    mainnet price-gate coverage lives in ``test_execution_service_prices.py``.
    """
    svc = ExecutionServiceServicer(GatewaySettings(network="anvil"))
    svc._ensure_initialized = AsyncMock()
    return svc


@pytest.fixture
def context() -> MagicMock:
    """Shared gRPC mock context."""
    return make_grpc_context()


def _make_request(
    *,
    intent_type: str = "swap",
    intent_data: dict | bytes | None = None,
    chain: str = "arbitrum",
    wallet_address: str = VALID_WALLET,
    price_map: dict[str, str] | None = None,
) -> gateway_pb2.CompileIntentRequest:
    """Build a CompileIntentRequest with sensible defaults."""
    if intent_data is None:
        intent_data = VALID_SWAP_INTENT_DATA
    if isinstance(intent_data, dict):
        intent_data_bytes = json.dumps(intent_data).encode("utf-8")
    else:
        intent_data_bytes = intent_data
    return gateway_pb2.CompileIntentRequest(
        intent_type=intent_type,
        intent_data=intent_data_bytes,
        chain=chain,
        wallet_address=wallet_address,
        price_map=price_map or {},
    )


def _install_success_compiler(service: ExecutionServiceServicer, bundle: ActionBundle) -> MagicMock:
    """Wire a compiler that returns a given action bundle via compile()."""
    compiler = MagicMock()
    compiler.compile.return_value = CompilationResult(
        status=CompilationStatus.SUCCESS,
        action_bundle=bundle,
        intent_id="i-test",
    )
    service._get_compiler = MagicMock(return_value=compiler)
    return compiler


# ---------------------------------------------------------------------------
# 1. Pre-compile validation: missing intent_type
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_intent_type_returns_invalid_argument(service, context):
    """Empty ``intent_type`` trips the earliest guard - never touches compiler."""
    service._get_compiler = MagicMock(side_effect=AssertionError("compiler must not be reached"))
    request = _make_request(intent_type="")

    result = await service.CompileIntent(request, context)

    assert_grpc_error(
        context,
        result,
        expected_status=grpc.StatusCode.INVALID_ARGUMENT,
        error_substring="intent_type",
    )
    assert result.error == "intent_type required"


# ---------------------------------------------------------------------------
# 2. Pre-compile validation: invalid chain string
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_chain_returns_invalid_argument(service, context):
    """Unknown chain trips ``validate_chain`` - never touches compiler or init."""
    service._get_compiler = MagicMock(side_effect=AssertionError("compiler must not be reached"))
    request = _make_request(chain="fakechain")

    result = await service.CompileIntent(request, context)

    assert_grpc_error(
        context,
        result,
        expected_status=grpc.StatusCode.INVALID_ARGUMENT,
    )
    # Validation runs BEFORE _ensure_initialized (guard-at-the-door pattern).
    service._ensure_initialized.assert_not_called()


# ---------------------------------------------------------------------------
# 3. Pre-compile validation: invalid wallet address
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_wallet_address_returns_invalid_argument(service, context):
    """Malformed wallet address for EVM chain trips ``validate_address_for_chain``."""
    service._get_compiler = MagicMock(side_effect=AssertionError("compiler must not be reached"))
    # Not hex, wrong length - fails checksum/shape validation.
    request = _make_request(wallet_address="not-a-real-wallet")

    result = await service.CompileIntent(request, context)

    assert_grpc_error(
        context,
        result,
        expected_status=grpc.StatusCode.INVALID_ARGUMENT,
    )
    service._ensure_initialized.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Malformed JSON body -> INTERNAL (current behaviour - characterization only)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_malformed_intent_data_json_returns_internal(service, context):
    """Non-JSON ``intent_data`` bytes currently surface as ``INTERNAL``.

    This is the observed behaviour today: ``json.loads`` is inside the outer
    ``try`` block and falls through to the generic ``Exception`` handler.
    Pinned as characterization: if a refactor promotes this to
    INVALID_ARGUMENT, update the assertion.
    """
    service._get_compiler = MagicMock(side_effect=AssertionError("compiler must not be reached"))
    request = _make_request(intent_data=b"not-json-at-all{")

    result = await service.CompileIntent(request, context)

    assert_grpc_error(
        context,
        result,
        expected_status=grpc.StatusCode.INTERNAL,
        expected_error_code="COMPILATION_FAILED",
    )


# ---------------------------------------------------------------------------
# 5. Default chain fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_chain_defaults_to_arbitrum(service, context):
    """Empty ``chain`` in the request defaults to ``"arbitrum"``.

    Observed behaviour: ``validate_chain(request.chain or "arbitrum")``.
    """
    bundle = ActionBundle(
        intent_type="SWAP",
        transactions=[{"to": "0x1", "value": "0", "data": "0x", "gas_estimate": 1, "tx_type": "swap"}],
    )
    compiler = _install_success_compiler(service, bundle)
    service._create_intent = MagicMock(return_value=MagicMock())

    request = _make_request(chain="")
    result = await service.CompileIntent(request, context)

    assert result.success is True
    # Compiler was obtained for the default chain.
    service._get_compiler.assert_called_once()
    assert service._get_compiler.call_args[0][0] == "arbitrum"
    compiler.compile.assert_called_once()
    assert_set_code_not_called(context)


# ---------------------------------------------------------------------------
# 6. Compiler returns FAILED -> COMPILATION_FAILED (no grpc code set)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compiler_status_failed_returns_compilation_failed(service, context):
    """``CompilationStatus.FAILED`` surfaces the compiler's error unchanged.

    Note: the current code path returns a failure response but does NOT call
    ``context.set_code``. This pins that contract - gRPC status stays OK while
    the structured response carries ``success=False`` + ``COMPILATION_FAILED``.
    """
    compiler = MagicMock()
    compiler.compile.return_value = CompilationResult(
        status=CompilationStatus.FAILED,
        action_bundle=None,
        intent_id="i-fail",
        error="insufficient liquidity",
    )
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    result = await service.CompileIntent(_make_request(), context)

    assert result.success is False
    assert result.error_code == "COMPILATION_FAILED"
    assert "insufficient liquidity" in result.error
    # Compilation-layer failures do NOT set a gRPC error code.
    context.set_code.assert_not_called()


# ---------------------------------------------------------------------------
# 7. SUCCESS but action_bundle is None -> NO_ACTION_BUNDLE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_success_without_action_bundle_returns_no_action_bundle(service, context):
    """Defensive branch: SUCCESS status with ``None`` bundle returns a
    dedicated ``NO_ACTION_BUNDLE`` error code."""
    compiler = MagicMock()
    compiler.compile.return_value = CompilationResult(
        status=CompilationStatus.SUCCESS,
        action_bundle=None,
        intent_id="i-empty",
    )
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    result = await service.CompileIntent(_make_request(), context)

    assert result.success is False
    assert result.error_code == "NO_ACTION_BUNDLE"
    context.set_code.assert_not_called()


# ---------------------------------------------------------------------------
# 8. Unexpected exception during compile -> INTERNAL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unexpected_exception_in_compile_returns_internal(service, context):
    """A non-pydantic/non-ValueError in ``compiler.compile`` surfaces as INTERNAL."""
    compiler = MagicMock()
    compiler.compile.side_effect = RuntimeError("orchestrator unavailable")
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    result = await service.CompileIntent(_make_request(), context)

    assert_grpc_error(
        context,
        result,
        expected_status=grpc.StatusCode.INTERNAL,
        expected_error_code="COMPILATION_FAILED",
        error_substring="orchestrator unavailable",
    )


# ---------------------------------------------------------------------------
# 9. Success response shape - action_bundle bytes round-trip cleanly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_success_response_serializes_action_bundle(service, context):
    """Happy path: response carries JSON-encoded bundle bytes with no error fields."""
    bundle = ActionBundle(
        intent_type="SWAP",
        transactions=[
            {
                "to": "0xaaa",
                "value": "0",
                "data": "0xdeadbeef",
                "gas_estimate": 21000,
                "tx_type": "swap",
            }
        ],
        metadata={"route": "v3"},
    )
    _install_success_compiler(service, bundle)
    service._create_intent = MagicMock(return_value=MagicMock())

    result = await service.CompileIntent(_make_request(), context)

    assert result.success is True
    assert result.error == ""
    assert result.error_code == ""
    decoded = json.loads(result.action_bundle.decode("utf-8"))
    assert decoded["intent_type"] == "SWAP"
    assert decoded["transactions"][0]["data"] == "0xdeadbeef"
    assert decoded["metadata"]["route"] == "v3"
    # No _sensitive_data key when bundle.sensitive_data is falsy.
    assert "_sensitive_data" not in decoded


# ---------------------------------------------------------------------------
# 10. sensitive_data is included in the roundtrip bundle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_success_response_includes_sensitive_data_roundtrip(service, context):
    """When the compiler returns ``sensitive_data`` (e.g. Raydium mint keypair)
    it is serialized under the ``_sensitive_data`` key so Execute can recover it."""
    bundle = ActionBundle(
        intent_type="LP_OPEN",
        transactions=[{"to": "0xbbb", "value": "0", "data": "0x", "gas_estimate": 1, "tx_type": "lp"}],
        sensitive_data={"mint_secret": "base58_payload"},
    )
    _install_success_compiler(service, bundle)
    service._create_intent = MagicMock(return_value=MagicMock())

    result = await service.CompileIntent(_make_request(), context)

    assert result.success is True
    decoded = json.loads(result.action_bundle.decode("utf-8"))
    assert decoded["_sensitive_data"] == {"mint_secret": "base58_payload"}


# ---------------------------------------------------------------------------
# 11. No price_map on non-mainnet preserves compiler oracle state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_price_map_non_mainnet_preserves_compiler_state(context):
    """On non-mainnet, compilation without price_map must not touch the oracle.

    Pins the ``original_oracle`` / ``original_placeholders`` capture+restore
    contract: even on the success path, the cached compiler's oracle reference
    is returned to its pre-call state AND ``restore_prices`` is invoked with
    the captured originals.

    Robustness: we have ``compile`` actively mutate oracle state, so the final
    assertions only hold if the servicer calls ``restore_prices``. Without this
    mutation the end-state assertions would pass even if ``restore_prices`` is
    silently skipped - which would defeat the characterization.
    """
    service = ExecutionServiceServicer(GatewaySettings(network="anvil"))
    service._ensure_initialized = AsyncMock()

    sentinel_oracle = object()
    mutated_oracle = object()
    compiler = MagicMock()
    compiler.price_oracle = sentinel_oracle
    compiler._using_placeholders = True

    bundle = ActionBundle(
        intent_type="SWAP",
        transactions=[{"to": "0x1", "value": "0", "data": "0x", "gas_estimate": 1, "tx_type": "swap"}],
    )

    def _mutating_compile(intent):
        # Simulate the compiler writing transient state onto itself during
        # compile - the servicer's restore step is what must undo this.
        compiler.price_oracle = mutated_oracle
        compiler._using_placeholders = False
        return CompilationResult(
            status=CompilationStatus.SUCCESS,
            action_bundle=bundle,
            intent_id="i-ok",
        )

    compiler.compile.side_effect = _mutating_compile

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    # Spy on restore_prices so we can assert it was called with the captured
    # originals (CodeRabbit / Codex both flagged this gap).
    compiler.restore_prices = MagicMock(side_effect=_restore_prices)
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    result = await service.CompileIntent(_make_request(), context)

    assert result.success is True
    # The compiler oracle is returned to exactly the same object reference -
    # which requires restore_prices to have actually been invoked.
    assert compiler.price_oracle is sentinel_oracle
    assert compiler._using_placeholders is True
    compiler.restore_prices.assert_called_once_with(sentinel_oracle, True)
    # update_prices must NOT have been invoked (no price_map, non-mainnet).
    compiler.update_prices.assert_not_called()
