from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import ZERO_ADDRESS
from almanak.connectors.aerodrome import compiler as aerodrome_compiler
from almanak.connectors.aerodrome.addresses import AERODROME, slipstream_lp_deployments
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import LPCloseIntent

POOL = "0x" + "ab" * 20
WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
CURRENT = slipstream_lp_deployments("base")[0]


def _close_compiler(*, chain: str = "base", permission_discovery: bool = False) -> MagicMock:
    compiler = MagicMock()
    compiler.chain = chain
    compiler.wallet_address = "0x" + "11" * 20
    compiler.default_deadline_seconds = 300
    compiler.price_oracle = {}
    compiler._gateway_client = None
    compiler._config = SimpleNamespace(permission_discovery=permission_discovery)
    compiler._get_chain_rpc_url.return_value = "http://localhost:8545"
    return compiler


def _close_intent(position_id: str = "7") -> LPCloseIntent:
    values = {
        "position_id": position_id,
        "pool": "WETH/USDC/50",
        "collect_fees": True,
        "protocol": "aerodrome_slipstream",
        "chain": "base",
        "intent_id": "close-1",
    }
    if not position_id:
        return LPCloseIntent.model_construct(**values)
    return LPCloseIntent(
        **values,
    )


def _resolved_position() -> aerodrome_compiler._ResolvedSlipstreamPosition:
    return aerodrome_compiler._ResolvedSlipstreamPosition(CURRENT, None)


@pytest.mark.parametrize(
    ("position_id", "error"),
    [
        ("", "position_id is required"),
        ("not-a-token", "requires a numeric tokenId"),
        ("-1", "requires a positive NFT tokenId"),
        ("0", "requires a positive NFT tokenId"),
    ],
)
def test_slipstream_close_rejects_invalid_nft_identity_before_adapter(position_id: str, error: str) -> None:
    with patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls:
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(_close_compiler(), _close_intent(position_id))

    assert result.status is CompilationStatus.FAILED
    assert error in (result.error or "")
    adapter_cls.assert_not_called()


def test_slipstream_close_rejects_chain_without_reviewed_deployments() -> None:
    compiler = _close_compiler(chain="optimism")
    with patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls:
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(compiler, _close_intent())

    assert result.status is CompilationStatus.FAILED
    assert result.error == "Aerodrome Slipstream CL not supported on chain 'optimism'. Only 'base' is supported."
    adapter_cls.assert_not_called()


def test_slipstream_close_permission_discovery_substitutes_token_id_without_changing_position_metadata() -> None:
    compiler = _close_compiler(permission_discovery=True)
    tx = MagicMock(gas_estimate=120_000, tx_type="remove_liquidity")
    tx.to_dict.return_value = {"tx_type": "remove_liquidity", "sequence": 1}
    intent = _close_intent("0")

    with (
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
    ):
        adapter_cls.return_value.remove_cl_liquidity.return_value = SimpleNamespace(
            success=True, transactions=[tx], error=None
        )
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(compiler, intent)

    assert result.status is CompilationStatus.SUCCESS
    # Permission discovery never resolves a real, owned position -- it enumerates
    # every reviewed generation's calldata directly via
    # _compile_discovery_position_bundle, so _resolve_slipstream_position (which
    # requires an actual on-chain-owned NFT) is not on this path at all.
    assert adapter_cls.return_value.remove_cl_liquidity.call_args.kwargs["token_id"] == 1
    assert result.action_bundle.metadata["position_id"] == "0"
    assert result.action_bundle.metadata["token_id"] == 1
    assert result.action_bundle.metadata["slipstream_deployment"] == "all-reviewed"


def test_slipstream_close_returns_position_resolution_failure_without_building_transactions() -> None:
    refusal = CompilationResult(
        status=CompilationStatus.FAILED,
        error="position authority mismatch",
        is_safety_refusal=True,
        intent_id="close-1",
    )
    with (
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
        patch.object(aerodrome_compiler, "_resolve_slipstream_position", return_value=refusal),
    ):
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(_close_compiler(), _close_intent())

    assert result is refusal
    adapter_cls.return_value.remove_cl_liquidity.assert_not_called()


def test_slipstream_close_preserves_adapter_failure() -> None:
    with (
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
        patch.object(aerodrome_compiler, "_resolve_slipstream_position", return_value=_resolved_position()),
    ):
        adapter_cls.return_value.remove_cl_liquidity.return_value = SimpleNamespace(
            success=False, transactions=[], error="decrease encoding failed"
        )
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(_close_compiler(), _close_intent())

    assert result.status is CompilationStatus.FAILED
    assert result.error == "Failed to build CL decreaseLiquidity TX: decrease encoding failed"


def test_slipstream_close_zero_liquidity_preserves_noop_metadata_order() -> None:
    with (
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
        patch.object(aerodrome_compiler, "_resolve_slipstream_position", return_value=_resolved_position()),
    ):
        adapter_cls.return_value.remove_cl_liquidity.return_value = SimpleNamespace(
            success=True, transactions=[], error=None
        )
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(_close_compiler(), _close_intent())

    assert result.status is CompilationStatus.SUCCESS
    assert result.action_bundle.transactions == []
    assert result.total_gas_estimate == 0
    assert list(result.action_bundle.metadata) == [
        "position_id",
        "token_id",
        "protocol",
        "collect_fees",
        "no_op",
        "reason",
        "nft_manager",
        "slipstream_deployment",
    ]
    assert len(result.warnings) == 1
    assert "CL position tokenId=7 has zero liquidity" in result.warnings[0]


def test_slipstream_close_preserves_transaction_and_metadata_order() -> None:
    decrease = MagicMock(gas_estimate=170_000, tx_type="decrease_liquidity")
    decrease.to_dict.return_value = {"tx_type": "decrease_liquidity", "sequence": 1}
    collect = MagicMock(gas_estimate=120_000, tx_type="collect")
    collect.to_dict.return_value = {"tx_type": "collect", "sequence": 2}
    with (
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
        patch.object(aerodrome_compiler, "_resolve_slipstream_position", return_value=_resolved_position()),
    ):
        adapter_cls.return_value.remove_cl_liquidity.return_value = SimpleNamespace(
            success=True, transactions=[decrease, collect], error=None
        )
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(_close_compiler(), _close_intent())

    assert result.status is CompilationStatus.SUCCESS
    assert result.transactions == [decrease, collect]
    assert result.action_bundle.transactions == [
        {"tx_type": "decrease_liquidity", "sequence": 1},
        {"tx_type": "collect", "sequence": 2},
    ]
    assert result.total_gas_estimate == 290_000
    assert list(result.action_bundle.metadata) == [
        "position_id",
        "token_id",
        "protocol",
        "collect_fees",
        "nft_manager",
        "slipstream_deployment",
    ]


def test_slipstream_close_converts_unexpected_exception_to_failed_compilation() -> None:
    with patch("almanak.connectors.aerodrome.AerodromeConfig", side_effect=RuntimeError("config exploded")):
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(_close_compiler(), _close_intent())

    assert result.status is CompilationStatus.FAILED
    assert result.error == "config exploded"
    assert result.transactions == []


def _pool_compiler(*, gateway: object | None = None, rpc_url: str | None = "http://localhost:8545") -> SimpleNamespace:
    return SimpleNamespace(
        chain="base",
        _gateway_client=gateway,
        _get_chain_rpc_url=lambda: rpc_url,
    )


def _address_payload(address: str) -> bytes:
    return bytes(12) + bytes.fromhex(address[2:])


def _metadata_payload(*, token0: str = WETH, token1: str = USDC, stable: bool = False) -> bytes:
    words = [
        (18).to_bytes(32, "big"),
        (6).to_bytes(32, "big"),
        (10**18).to_bytes(32, "big"),
        (2500 * 10**6).to_bytes(32, "big"),
        int(stable).to_bytes(32, "big"),
        _address_payload(token0),
        _address_payload(token1),
    ]
    return b"".join(words)


def test_pool_address_rejects_chain_without_factory_before_encoding() -> None:
    compiler = _pool_compiler()
    compiler.chain = "arbitrum"
    with patch("almanak.connectors.aerodrome.pool_validation._encode_get_pool_aerodrome") as encode:
        result = aerodrome_compiler.get_aerodrome_pool_address(compiler, WETH, USDC, False)

    assert result is None
    encode.assert_not_called()


@pytest.mark.parametrize(
    ("gateway_result", "expected"),
    [
        ("0x" + _address_payload(POOL).hex(), POOL),
        ("0x" + _address_payload(ZERO_ADDRESS).hex(), None),
        ("0x", None),
    ],
)
def test_pool_address_gateway_result_wins_without_direct_fallback(gateway_result: str, expected: str | None) -> None:
    gateway = MagicMock()
    gateway.eth_call.return_value = gateway_result
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation._encode_get_pool_aerodrome", return_value="0xfeed"
        ) as encode,
        patch("almanak.connectors._strategy_base.pool_validation_base.eth_call") as direct_call,
    ):
        result = aerodrome_compiler.get_aerodrome_pool_address(_pool_compiler(gateway=gateway), WETH, USDC, True)

    assert result == expected
    encode.assert_called_once_with(WETH, USDC, True)
    gateway.eth_call.assert_called_once_with(chain="base", to=AERODROME["base"]["factory"], data="0xfeed")
    direct_call.assert_not_called()


@pytest.mark.parametrize("gateway_result", [RuntimeError("gateway down"), "0xnot-hex"])
def test_pool_address_gateway_exception_falls_back_to_direct_rpc(gateway_result: object) -> None:
    gateway = MagicMock()
    if isinstance(gateway_result, Exception):
        gateway.eth_call.side_effect = gateway_result
    else:
        gateway.eth_call.return_value = gateway_result
    with (
        patch("almanak.connectors.aerodrome.pool_validation._encode_get_pool_aerodrome", return_value="0xfeed"),
        patch(
            "almanak.connectors._strategy_base.pool_validation_base.eth_call",
            return_value=_address_payload(POOL),
        ) as direct_call,
    ):
        result = aerodrome_compiler.get_aerodrome_pool_address(_pool_compiler(gateway=gateway), WETH, USDC, False)

    assert result == POOL
    direct_call.assert_called_once_with("http://localhost:8545", AERODROME["base"]["factory"], "0xfeed")


def test_pool_address_without_transport_returns_none() -> None:
    with patch("almanak.connectors.aerodrome.pool_validation._encode_get_pool_aerodrome", return_value="0xfeed"):
        result = aerodrome_compiler.get_aerodrome_pool_address(
            _pool_compiler(gateway=None, rpc_url=None), WETH, USDC, False
        )

    assert result is None


@pytest.mark.parametrize(
    ("rpc_result", "expected"),
    [(_address_payload(POOL), POOL), (None, None)],
)
def test_pool_address_direct_rpc_result(rpc_result: bytes | None, expected: str | None) -> None:
    with (
        patch("almanak.connectors.aerodrome.pool_validation._encode_get_pool_aerodrome", return_value="0xfeed"),
        patch("almanak.connectors._strategy_base.pool_validation_base.eth_call", return_value=rpc_result),
    ):
        result = aerodrome_compiler.get_aerodrome_pool_address(_pool_compiler(), WETH, USDC, False)

    assert result == expected


@pytest.mark.parametrize("stable", [False, True])
def test_pool_metadata_gateway_decodes_ordered_tokens_and_stable_flag(stable: bool) -> None:
    gateway = MagicMock()
    gateway.eth_call.return_value = "0x" + _metadata_payload(stable=stable).hex()
    with patch("almanak.connectors._strategy_base.pool_validation_base.eth_call") as direct_call:
        result = aerodrome_compiler.get_aerodrome_pool_metadata(_pool_compiler(gateway=gateway), POOL)

    assert result == (WETH, USDC, stable)
    gateway.eth_call.assert_called_once_with(
        chain="base", to=POOL, data=aerodrome_compiler._AERODROME_POOL_METADATA_SELECTOR
    )
    direct_call.assert_not_called()


@pytest.mark.parametrize("gateway_result", ["0x", "0x" + bytes(32).hex()])
def test_pool_metadata_invalid_gateway_result_does_not_change_fallback_precedence(gateway_result: str) -> None:
    gateway = MagicMock()
    gateway.eth_call.return_value = gateway_result
    with patch("almanak.connectors._strategy_base.pool_validation_base.eth_call") as direct_call:
        result = aerodrome_compiler.get_aerodrome_pool_metadata(_pool_compiler(gateway=gateway), POOL)

    assert result is None
    direct_call.assert_not_called()


def test_pool_metadata_gateway_exception_falls_back_to_direct_rpc() -> None:
    gateway = MagicMock()
    gateway.eth_call.side_effect = RuntimeError("gateway down")
    with patch(
        "almanak.connectors._strategy_base.pool_validation_base.eth_call",
        return_value=_metadata_payload(stable=True),
    ) as direct_call:
        result = aerodrome_compiler.get_aerodrome_pool_metadata(_pool_compiler(gateway=gateway), POOL)

    assert result == (WETH, USDC, True)
    direct_call.assert_called_once_with(
        "http://localhost:8545", POOL, aerodrome_compiler._AERODROME_POOL_METADATA_SELECTOR
    )


def test_pool_metadata_without_transport_returns_none() -> None:
    assert aerodrome_compiler.get_aerodrome_pool_metadata(_pool_compiler(rpc_url=None), POOL) is None


@pytest.mark.parametrize(
    "rpc_result",
    [None, _metadata_payload(token0=ZERO_ADDRESS), _metadata_payload(token1=ZERO_ADDRESS)],
)
def test_pool_metadata_direct_rpc_rejects_missing_or_zero_token_identity(rpc_result: bytes | None) -> None:
    with patch("almanak.connectors._strategy_base.pool_validation_base.eth_call", return_value=rpc_result):
        result = aerodrome_compiler.get_aerodrome_pool_metadata(_pool_compiler(), POOL)

    assert result is None


@pytest.mark.parametrize(
    "reader",
    [aerodrome_compiler.get_aerodrome_pool_address, aerodrome_compiler.get_aerodrome_pool_metadata],
)
def test_direct_pool_reads_preserve_transport_exceptions(reader) -> None:
    with (
        patch("almanak.connectors.aerodrome.pool_validation._encode_get_pool_aerodrome", return_value="0xfeed"),
        patch("almanak.connectors._strategy_base.pool_validation_base.eth_call", side_effect=RuntimeError("rpc bug")),
        pytest.raises(RuntimeError, match="rpc bug"),
    ):
        if reader is aerodrome_compiler.get_aerodrome_pool_address:
            reader(_pool_compiler(), WETH, USDC, False)
        else:
            reader(_pool_compiler(), POOL)
