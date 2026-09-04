from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.framework.intents.compiler import CompilationResult, CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import Intent, IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle

POOL_ADDRESS = "0x" + "ab" * 20
FETCH_SQRT_PRICE = "almanak.connectors._strategy_base.pool_validation_registry.PoolValidationRegistry.fetch_sqrt_price"


def _compiler(*, gateway_client: object | None = None) -> IntentCompiler:
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = "arbitrum"
    compiler.wallet_address = "0x" + "11" * 20
    compiler.default_protocol = "uniswap_v3"
    compiler._gateway_client = gateway_client
    return compiler


def _pool_check(pool_address: str | None = POOL_ADDRESS) -> PoolValidationResult:
    return PoolValidationResult(
        exists=True,
        reason=PoolValidationReason.CONFIRMED,
        pool_address=pool_address,
    )


def test_fetch_lp_pool_slot0_skips_lookup_without_pool_address() -> None:
    compiler = _compiler()

    with (
        patch.object(compiler, "_get_chain_rpc_url") as get_rpc_url,
        patch(FETCH_SQRT_PRICE) as fetch_sqrt_price,
    ):
        result = compiler._fetch_lp_pool_slot0(_pool_check(None))

    assert result is None
    get_rpc_url.assert_not_called()
    fetch_sqrt_price.assert_not_called()


@pytest.mark.parametrize("gateway_client", [None, SimpleNamespace(is_connected=False)])
def test_fetch_lp_pool_slot0_skips_lookup_without_transport(gateway_client: object | None) -> None:
    compiler = _compiler(gateway_client=gateway_client)

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value=None),
        patch(FETCH_SQRT_PRICE) as fetch_sqrt_price,
    ):
        result = compiler._fetch_lp_pool_slot0(_pool_check())

    assert result is None
    fetch_sqrt_price.assert_not_called()


@pytest.mark.parametrize("slot0_result", [(2**96, -1), [2**96, -1]])
def test_fetch_lp_pool_slot0_uses_connected_gateway_and_normalizes_result_shape(
    slot0_result: tuple[int, int] | list[int],
) -> None:
    gateway_client = SimpleNamespace(is_connected=True)
    compiler = _compiler(gateway_client=gateway_client)

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value=None),
        patch(FETCH_SQRT_PRICE, return_value=slot0_result) as fetch_sqrt_price,
    ):
        result = compiler._fetch_lp_pool_slot0(_pool_check())

    assert result == (2**96, -1)
    fetch_sqrt_price.assert_called_once_with(
        "uniswap_v3",
        POOL_ADDRESS,
        "arbitrum",
        None,
        gateway_client=gateway_client,
    )


def test_fetch_lp_pool_slot0_uses_rpc_transport() -> None:
    compiler = _compiler()

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch(FETCH_SQRT_PRICE, return_value=(2**96, 0)) as fetch_sqrt_price,
    ):
        result = compiler._fetch_lp_pool_slot0(_pool_check())

    assert result == (2**96, 0)
    fetch_sqrt_price.assert_called_once_with(
        "uniswap_v3",
        POOL_ADDRESS,
        "arbitrum",
        "http://localhost:8545",
        gateway_client=None,
    )


@pytest.mark.parametrize(
    "slot0_result",
    [None, (None, 0), (0, 0), (-1, 0), (2**96, None)],
)
def test_fetch_lp_pool_slot0_rejects_unusable_results(slot0_result: object) -> None:
    compiler = _compiler()

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch(FETCH_SQRT_PRICE, return_value=slot0_result),
    ):
        result = compiler._fetch_lp_pool_slot0(_pool_check())

    assert result is None


@pytest.mark.parametrize("slot0_result", [(), (2**96,), (2**96, 0, 1), object()])
def test_fetch_lp_pool_slot0_malformed_result_fails_open(
    slot0_result: object,
    caplog: pytest.LogCaptureFixture,
) -> None:
    compiler = _compiler()

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch(FETCH_SQRT_PRICE, return_value=slot0_result),
    ):
        result = compiler._fetch_lp_pool_slot0(_pool_check())

    assert result is None
    assert "LP slot0 lookup failed" in caplog.text


def test_fetch_lp_pool_slot0_lookup_error_fails_open(caplog: pytest.LogCaptureFixture) -> None:
    compiler = _compiler()

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch(FETCH_SQRT_PRICE, side_effect=RuntimeError("gateway unavailable")),
    ):
        result = compiler._fetch_lp_pool_slot0(_pool_check())

    assert result is None
    assert "gateway unavailable" in caplog.text


def _collect_intent():
    return Intent.collect_fees(
        pool="USDC/WETH/3000",
        protocol="uniswap_v3",
        chain="arbitrum",
        protocol_params={"position_id": "42"},
        registry_handle="lp-position-42",
    )


def test_compile_collect_fees_rejects_wrong_intent_type() -> None:
    intent = SimpleNamespace(intent_id="wrong-intent")

    result = _compiler()._compile_collect_fees(intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == "Expected CollectFeesIntent"
    assert result.intent_id == "wrong-intent"


def test_compile_collect_fees_preserves_connector_result_and_bundle_identity() -> None:
    compiler = _compiler()
    intent = _collect_intent()
    context = object()
    metadata = {
        "protocol": "uniswap_v3",
        "pool": intent.pool,
        "position_id": "42",
        "registry_handle": intent.registry_handle,
        "currency0": "0x" + "01" * 20,
        "currency1": "0x" + "02" * 20,
        "tick_lower": -120,
        "tick_upper": 120,
    }
    bundle = ActionBundle(
        intent_type=IntentType.LP_COLLECT_FEES.value,
        transactions=[{"to": POOL_ADDRESS, "data": "0x1234"}],
        metadata=metadata,
    )
    expected = CompilationResult(
        status=CompilationStatus.SUCCESS,
        action_bundle=bundle,
        total_gas_estimate=120_000,
        warnings=["connector warning"],
        intent_id=intent.intent_id,
    )
    connector_compiler = MagicMock()
    connector_compiler.intents = {IntentType.LP_COLLECT_FEES}
    connector_compiler.compile.return_value = expected

    with (
        patch("almanak.framework.intents.compiler.get_connector_compiler", return_value=connector_compiler),
        patch.object(compiler, "_build_compiler_context", return_value=context) as build_context,
    ):
        result = compiler._compile_collect_fees(intent)

    assert result is expected
    assert result.action_bundle is bundle
    assert result.action_bundle.metadata is metadata
    assert result.action_bundle.metadata["currency0"] == "0x" + "01" * 20
    assert result.action_bundle.metadata["currency1"] == "0x" + "02" * 20
    assert result.action_bundle.metadata["tick_lower"] == -120
    assert result.action_bundle.metadata["tick_upper"] == 120
    build_context.assert_called_once_with("uniswap_v3", connector_compiler)
    connector_compiler.compile.assert_called_once_with(context, intent)


@pytest.mark.parametrize("connector_registered", [False, True])
def test_compile_collect_fees_returns_canonical_failure_when_connector_does_not_support_intent(
    connector_registered: bool,
) -> None:
    compiler = _compiler()
    intent = _collect_intent()
    connector_compiler = MagicMock()
    connector_compiler.intents = {IntentType.SWAP}

    with (
        patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler if connector_registered else None,
        ),
        patch(
            "almanak.connectors._strategy_base.compiler_registry.CompilerRegistry.protocols_for_intent",
            return_value=["aerodrome_slipstream", "uniswap_v3"],
        ) as protocols_for_intent,
    ):
        result = compiler._compile_collect_fees(intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == (
        "Protocol 'uniswap_v3' does not support LP_COLLECT_FEES. Supported: aerodrome_slipstream, uniswap_v3"
    )
    assert result.intent_id == intent.intent_id
    connector_compiler.compile.assert_not_called()
    protocols_for_intent.assert_called_once_with(IntentType.LP_COLLECT_FEES)
