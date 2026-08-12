"""Fail-closed exact-address Uniswap V3 LP compilation (ALM-3222)."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.base.compiler import CLCompilerContext
from almanak.connectors._strategy_base.pool_validation_base import (
    PoolValidationReason,
    PoolValidationResult,
    eth_call,
)
from almanak.connectors._strategy_base.v3_pool_validation import (
    V3PoolBinding,
    V3PositionBinding,
    V3PositionBindingReadError,
    read_v3_position_binding,
)
from almanak.connectors.uniswap_v3.compiler import UniswapV3Compiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

POOL = "0xC6962004f452bE9203591991D15f6b388e09E8D0"
OTHER_POOL = "0xc473e2aEE3441BF9240Be85eb122aBB059A3B57c"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


class _Gateway:
    is_connected = True


class _DisconnectedGateway:
    is_connected = False


def _ctx(*, chain: str = "arbitrum", gateway: object | None = None) -> CLCompilerContext:
    services = MagicMock()
    services.resolve_token.side_effect = lambda address: {
        USDC: TokenInfo("USDC", USDC, 6),
        WETH: TokenInfo("WETH", WETH, 18),
    }.get(address.lower())
    return CLCompilerContext(
        chain=chain,
        wallet_address="0x2222222222222222222222222222222222222222",
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        gateway_internal_preflight=False,
        managed_fork=False,
        token_resolver=None,
        gateway_client=gateway,
        price_oracle={},
        cache={},
        services=services,
        protocol="uniswap_v3",
        default_swap_adapter_factory=MagicMock(),
        lp_adapter_factory=MagicMock(),
        swap_pool_selection_mode="auto",
        fixed_swap_fee_tier=None,
        default_lp_slippage=Decimal("0.01"),
        max_price_impact_pct=Decimal("0.10"),
    )


def _confirmed(pool: str = POOL) -> PoolValidationResult:
    return PoolValidationResult(
        exists=True,
        reason=PoolValidationReason.CONFIRMED,
        pool_address=pool,
    )


def test_exact_pool_requires_gateway_boundary() -> None:
    result = UniswapV3Compiler._resolve_exact_lp_pool(
        ctx=_ctx(), protocol="uniswap_v3", pool_address=POOL, intent_id="open-1"
    )
    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert "connected gateway is required" in (result.error or "")


def test_exact_pool_rejects_disconnected_gateway_before_reads() -> None:
    result = UniswapV3Compiler._resolve_exact_lp_pool(
        ctx=_ctx(gateway=_DisconnectedGateway()),
        protocol="uniswap_v3",
        pool_address=POOL,
        intent_id="open-1",
    )
    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert "connected gateway is required" in (result.error or "")


def test_exact_pool_rejects_malformed_address_before_reads() -> None:
    result = UniswapV3Compiler._resolve_exact_lp_pool(
        ctx=_ctx(gateway=_Gateway()),
        protocol="uniswap_v3",
        pool_address="0x1234",
        intent_id="open-1",
    )
    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert result.error == "Invalid exact LP pool address: 0x1234"


def test_exact_pool_resolves_contract_binding_and_factory_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.read_v3_pool_binding",
        lambda *_args, **_kwargs: V3PoolBinding(token0=USDC, token1=WETH, fee_tier=500),
    )
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.validate_v3_pool",
        lambda *_args, **_kwargs: _confirmed(),
    )

    resolved = UniswapV3Compiler._resolve_exact_lp_pool(
        ctx=_ctx(gateway=_Gateway()), protocol="uniswap_v3", pool_address=POOL, intent_id="open-1"
    )

    assert not isinstance(resolved, CompilationResult)
    token0, token1, fee = resolved
    assert (token0.address, token1.address, fee) == (USDC, WETH, 500)


def test_lp_open_compiles_mint_for_exact_address_without_substitution(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx(gateway=_Gateway())
    adapter = MagicMock()
    adapter.get_position_manager_address.return_value = "0x3333333333333333333333333333333333333333"
    adapter.get_mint_calldata.return_value = b"\xaa\xbb"
    adapter.estimate_mint_gas.return_value = 250_000
    object.__setattr__(ctx, "lp_adapter_factory", lambda _protocol: adapter)
    ctx.services.build_approve_tx.return_value = []
    ctx.services.validate_pool.return_value = None
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.read_v3_pool_binding",
        lambda *_args, **_kwargs: V3PoolBinding(token0=USDC, token1=WETH, fee_tier=500),
    )
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.validate_v3_pool",
        lambda *_args, **_kwargs: _confirmed(),
    )
    monkeypatch.setattr(UniswapV3Compiler, "_compute_lp_ticks", staticmethod(lambda **_kwargs: (-10, 10, 10)))
    monkeypatch.setattr(UniswapV3Compiler, "_fetch_lp_pool_slot0", lambda *_args: (2**96, 0))
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.compiler.maybe_recompute_lp_amounts_from_slot0",
        lambda **kwargs: (kwargs["amount0_desired"], kwargs["amount1_desired"]),
    )
    monkeypatch.setattr(UniswapV3Compiler, "_preflight_lp_liquidity", staticmethod(lambda **_kwargs: None))
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.compiler.compute_lp_slippage_mins",
        lambda **_kwargs: (900_000, 9 * 10**17),
    )
    intent = LPOpenIntent(
        pool=POOL,
        amount0=Decimal("1"),
        amount1=Decimal("1"),
        range_lower=Decimal("0.9"),
        range_upper=Decimal("1.1"),
        protocol="uniswap_v3",
    )

    result = UniswapV3Compiler().compile_lp_open(ctx, intent)

    assert result.status is CompilationStatus.SUCCESS
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["pool"] == POOL
    mint = adapter.get_mint_calldata.call_args.kwargs
    assert (mint["token0"], mint["token1"], mint["fee"]) == (USDC, WETH, 500)


def test_exact_pool_refuses_different_factory_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.read_v3_pool_binding",
        lambda *_args, **_kwargs: V3PoolBinding(token0=USDC, token1=WETH, fee_tier=500),
    )
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.validate_v3_pool",
        lambda *_args, **_kwargs: _confirmed(OTHER_POOL),
    )

    result = UniswapV3Compiler._resolve_exact_lp_pool(
        ctx=_ctx(gateway=_Gateway()), protocol="uniswap_v3", pool_address=POOL, intent_id="open-1"
    )

    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert "Refusing alternate-pool substitution" in (result.error or "")


def test_exact_pool_refuses_unsupported_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.read_v3_pool_binding",
        lambda *_args, **_kwargs: V3PoolBinding(token0=USDC, token1=WETH, fee_tier=500),
    )
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.validate_v3_pool",
        lambda *_args, **_kwargs: PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning="No factory registered",
        ),
    )

    result = UniswapV3Compiler._resolve_exact_lp_pool(
        ctx=_ctx(chain="solana", gateway=_Gateway()),
        protocol="uniswap_v3",
        pool_address=POOL,
        intent_id="open-1",
    )

    assert isinstance(result, CompilationResult)
    assert "registered uniswap_v3 factory" in (result.error or "")


def test_exact_close_refuses_position_from_wrong_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx(gateway=_Gateway())
    monkeypatch.setattr(
        UniswapV3Compiler,
        "_resolve_exact_lp_pool",
        lambda **_kwargs: (TokenInfo("USDC", USDC, 6), TokenInfo("WETH", WETH, 18), 500),
    )
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.read_v3_position_binding",
        lambda *_args, **_kwargs: V3PositionBinding(
            token0="0x1111111111111111111111111111111111111111",
            token1=WETH,
            fee_tier=500,
        ),
    )

    result = UniswapV3Compiler._validate_exact_lp_close_identity(
        ctx=ctx,
        protocol="uniswap_v3",
        pool_address=POOL,
        position_manager="0x3333333333333333333333333333333333333333",
        token_id=42,
        intent_id="close-1",
    )

    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert "Position #42 belongs to" in (result.error or "")
    assert "not exact LP pool" in (result.error or "")


@pytest.mark.parametrize(("position_fee", "matches"), [(500, True), (3000, False)])
def test_exact_close_requires_position_fee_to_match_pool(
    monkeypatch: pytest.MonkeyPatch, position_fee: int, matches: bool
) -> None:
    ctx = _ctx(gateway=_Gateway())
    monkeypatch.setattr(
        UniswapV3Compiler,
        "_resolve_exact_lp_pool",
        lambda **_kwargs: (TokenInfo("USDC", USDC, 6), TokenInfo("WETH", WETH, 18), 500),
    )
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.read_v3_position_binding",
        lambda *_args, **_kwargs: V3PositionBinding(token0=USDC, token1=WETH, fee_tier=position_fee),
    )

    result = UniswapV3Compiler._validate_exact_lp_close_identity(
        ctx=ctx,
        protocol="uniswap_v3",
        pool_address=POOL,
        position_manager="0x3333333333333333333333333333333333333333",
        token_id=42,
        intent_id="close-1",
    )

    if matches:
        assert result is None
    else:
        assert isinstance(result, CompilationResult)
        assert "fee 3000" in (result.error or "")
        assert "not exact LP pool" in (result.error or "")


def test_exact_close_rejects_empty_or_malformed_active_position(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx(gateway=_Gateway())
    monkeypatch.setattr(
        UniswapV3Compiler,
        "_resolve_exact_lp_pool",
        lambda **_kwargs: (TokenInfo("USDC", USDC, 6), TokenInfo("WETH", WETH, 18), 500),
    )
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.read_v3_position_binding",
        lambda *_args, **_kwargs: None,
    )
    kwargs = {
        "ctx": ctx,
        "protocol": "uniswap_v3",
        "pool_address": POOL,
        "position_manager": "0x3333333333333333333333333333333333333333",
        "token_id": 42,
        "intent_id": "close-1",
    }

    active_result = UniswapV3Compiler._validate_exact_lp_close_identity(**kwargs)

    assert isinstance(active_result, CompilationResult)
    assert "Cannot verify position #42" in (active_result.error or "")
    assert "empty or malformed" in (active_result.error or "")


def test_exact_close_reports_position_read_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx(gateway=_Gateway())
    monkeypatch.setattr(
        UniswapV3Compiler,
        "_resolve_exact_lp_pool",
        lambda **_kwargs: (TokenInfo("USDC", USDC, 6), TokenInfo("WETH", WETH, 18), 500),
    )

    def _unavailable(*_args, **_kwargs):
        raise V3PositionBindingReadError("transient gateway failure")

    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.pool_validation.read_v3_position_binding",
        _unavailable,
    )

    result = UniswapV3Compiler._validate_exact_lp_close_identity(
        ctx=ctx,
        protocol="uniswap_v3",
        pool_address=POOL,
        position_manager="0x3333333333333333333333333333333333333333",
        token_id=42,
        intent_id="close-1",
    )

    assert isinstance(result, CompilationResult)
    assert "read is unavailable" in (result.error or "")


def test_compile_exact_close_skips_all_identity_reads_for_measured_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx(gateway=_Gateway())
    adapter = MagicMock()
    adapter.get_position_manager_address.return_value = "0x3333333333333333333333333333333333333333"
    object.__setattr__(ctx, "lp_adapter_factory", lambda _protocol: adapter)
    ctx.services.query_position_liquidity.return_value = 0
    ctx.services.query_position_tokens_owed.return_value = (0, 0)

    def _unexpected_identity_read(**_kwargs):
        pytest.fail("a measured no-op close must not perform pool or NFT identity reads")

    monkeypatch.setattr(UniswapV3Compiler, "_validate_exact_lp_close_identity", _unexpected_identity_read)

    result = UniswapV3Compiler().compile_lp_close(
        ctx,
        LPCloseIntent(position_id="42", pool=POOL, protocol="uniswap_v3"),
    )

    assert result.status is CompilationStatus.SUCCESS
    assert result.transactions == []
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["no_op"] is True


@pytest.mark.parametrize("pool", [POOL, "USDC/WETH/500"])
def test_compile_lp_close_wires_identity_only_for_active_exact_address(
    monkeypatch: pytest.MonkeyPatch, pool: str
) -> None:
    ctx = _ctx(gateway=_Gateway())
    adapter = MagicMock()
    adapter.get_position_manager_address.return_value = "0x3333333333333333333333333333333333333333"
    object.__setattr__(ctx, "lp_adapter_factory", lambda _protocol: adapter)
    ctx.services.query_position_liquidity.return_value = 1
    ctx.services.query_position_tokens_owed.return_value = (0, 0)
    identity = MagicMock(return_value=CompilationResult(status=CompilationStatus.FAILED, error="identity gate"))
    monkeypatch.setattr(UniswapV3Compiler, "_validate_exact_lp_close_identity", identity)
    monkeypatch.setattr(UniswapV3Compiler, "_extend_lp_close_transactions", MagicMock())

    result = UniswapV3Compiler().compile_lp_close(
        ctx,
        LPCloseIntent(position_id="42", pool=pool, protocol="uniswap_v3"),
    )

    if pool == POOL:
        assert result.status is CompilationStatus.FAILED
        assert result.error == "identity gate"
        identity.assert_called_once()
    else:
        assert result.status is CompilationStatus.SUCCESS
        identity.assert_not_called()


def test_position_binding_decoder_reads_positions_tuple(monkeypatch: pytest.MonkeyPatch) -> None:
    words = [bytes(32) for _ in range(12)]
    words[2] = bytes.fromhex("00" * 12 + USDC[2:])
    words[3] = bytes.fromhex("00" * 12 + WETH[2:])
    words[4] = (500).to_bytes(32, "big")
    monkeypatch.setattr(
        "almanak.connectors._strategy_base.v3_pool_validation.eth_call",
        lambda *_args, **_kwargs: b"".join(words),
    )

    binding = read_v3_position_binding(
        "0x3333333333333333333333333333333333333333",
        42,
        None,
        chain="arbitrum",
        gateway_client=_Gateway(),
    )

    assert binding == V3PositionBinding(token0=USDC, token1=WETH, fee_tier=500)


def test_position_binding_transport_failure_is_not_reported_as_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    def _failed_read(*_args, **_kwargs):
        raise ValueError("gateway unavailable")

    monkeypatch.setattr(
        "almanak.connectors._strategy_base.v3_pool_validation.eth_call",
        _failed_read,
    )

    with pytest.raises(V3PositionBindingReadError, match="read unavailable"):
        read_v3_position_binding(
            "0x3333333333333333333333333333333333333333",
            42,
            None,
            chain="arbitrum",
            gateway_client=_Gateway(),
        )


def test_position_binding_requests_strict_gateway_error_propagation() -> None:
    gateway = MagicMock()
    gateway.is_connected = True
    gateway.eth_call.side_effect = ValueError("gateway unavailable")

    with pytest.raises(V3PositionBindingReadError):
        read_v3_position_binding(
            "0x3333333333333333333333333333333333333333",
            42,
            None,
            chain="arbitrum",
            gateway_client=gateway,
        )

    assert gateway.eth_call.call_args.kwargs["raise_on_error"] is True


def test_local_error_propagation_does_not_widen_legacy_gateway_call_shape() -> None:
    class NarrowGateway:
        is_connected = True

        def eth_call(self, *, chain: str, to: str, data: str) -> str:
            assert chain == "arbitrum"
            assert to == POOL
            assert data == "0x12345678"
            return "0x" + (1).to_bytes(32, "big").hex()

    result = eth_call(
        "",
        POOL,
        "0x12345678",
        chain="arbitrum",
        gateway_client=NarrowGateway(),
        raise_errors=True,
    )

    assert result == (1).to_bytes(32, "big")
