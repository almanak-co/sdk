"""Exact bare-address Aerodrome Classic (Solidly) LP_OPEN and factory-authenticated LP_CLOSE.

Classic LP_OPEN used to reject a bare address at its format gate; LP_CLOSE
accepted one but only reversed ``metadata()`` and never asked the factory
whether it deployed that address. Both lanes now share one contract: the
pool's own ``(token0, token1, stable)`` must round-trip through the registered
factory to the same address, and nothing is substituted.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors.aerodrome import compiler as aerodrome_compiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

# Real Base addresses: the WETH/USDC volatile Classic pool (factory.getPool(WETH, USDC, false)).
POOL = "0xcdac0d6c6c59727a65f871236188350531885c43"
OTHER_POOL = "0x7f670f78b17dec44d5ef68a48740b6f8849cc2e6"
WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"

_TOKENS = {WETH: TokenInfo("WETH", WETH, 18), USDC: TokenInfo("USDC", USDC, 6)}


class _Gateway:
    is_connected = True


def _compiler(*, gateway: object | None = _Gateway(), internal_preflight: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        chain="base",
        _gateway_client=gateway,
        _gateway_internal_preflight=internal_preflight,
        _get_chain_rpc_url=lambda: "http://gateway-internal:8545",
        _resolve_token=lambda token: _TOKENS.get(token.lower()),
        _validate_pool=lambda result, intent_id: None,
    )


def _confirmed(pool: str = POOL) -> PoolValidationResult:
    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool)


def _metadata_patch(value=(WETH, USDC, False)):
    return patch.object(aerodrome_compiler, "get_aerodrome_pool_metadata", return_value=value)


def _factory_patch(value: PoolValidationResult):
    return patch("almanak.connectors.aerodrome.pool_validation.validate_aerodrome_pool", return_value=value)


def _resolve(compiler: SimpleNamespace, pool: str = POOL):
    return aerodrome_compiler._resolve_exact_aerodrome_classic_pool(compiler, pool, "open-1")


def _failed(result: object) -> CompilationResult:
    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    return result


def test_exact_classic_pool_rejects_malformed_address_before_reads() -> None:
    with _metadata_patch() as read:
        result = _failed(_resolve(_compiler(), "0xabc"))
    assert result.error == "Invalid exact Aerodrome pool address: 0xabc"
    read.assert_not_called()


def test_exact_classic_pool_requires_gateway_boundary() -> None:
    with _metadata_patch() as read:
        result = _failed(_resolve(_compiler(gateway=None)))
    assert "connected gateway is required" in (result.error or "")
    read.assert_not_called()


def test_exact_classic_pool_rejects_unsupported_chain() -> None:
    compiler = _compiler()
    compiler.chain = "arbitrum"
    result = _failed(_resolve(compiler))
    assert "Aerodrome not supported on arbitrum" in (result.error or "")


def test_exact_classic_pool_fails_closed_when_metadata_unreadable() -> None:
    with _metadata_patch(None), _factory_patch(_confirmed()) as factory:
        result = _failed(_resolve(_compiler()))
    assert "Could not resolve Aerodrome pool metadata" in (result.error or "")
    factory.assert_not_called()


def test_exact_classic_pool_fails_closed_on_missing_token_metadata() -> None:
    compiler = _compiler()
    compiler._resolve_token = lambda token: _TOKENS.get(token.lower()) if token.lower() == WETH else None
    with _metadata_patch(), _factory_patch(_confirmed()) as factory:
        result = _failed(_resolve(compiler))
    assert "Could not resolve tokens for Aerodrome pool" in (result.error or "")
    factory.assert_not_called()


def test_exact_classic_pool_round_trips_through_the_factory() -> None:
    with _metadata_patch(), _factory_patch(_confirmed()) as factory:
        resolved = _resolve(_compiler())
    assert not isinstance(resolved, CompilationResult)
    assert (resolved.token0.address, resolved.token1.address, resolved.stable) == (WETH, USDC, False)
    assert resolved.pool_check.pool_address == POOL
    assert factory.call_args.args[:4] == ("base", WETH, USDC, False)
    # Strategy-side: no direct RPC url crosses the boundary.
    assert factory.call_args.args[4] is None


@pytest.mark.parametrize(
    "factory_result",
    [
        PoolValidationResult(exists=False, reason=PoolValidationReason.NOT_FOUND, error="no pool"),
        PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_FAILED, warning="rpc down"),
    ],
)
def test_exact_classic_pool_requires_a_positive_factory_answer(factory_result: PoolValidationResult) -> None:
    with _metadata_patch(), _factory_patch(factory_result):
        result = _failed(_resolve(_compiler()))
    assert "Cannot authenticate exact Aerodrome pool" in (result.error or "")


def test_exact_classic_pool_refuses_alternate_pool_substitution() -> None:
    with _metadata_patch(), _factory_patch(_confirmed(OTHER_POOL)):
        result = _failed(_resolve(_compiler()))
    assert "Refusing alternate-pool substitution" in (result.error or "")


# LP_OPEN dispatch
def _open_intent(pool: str) -> LPOpenIntent:
    return LPOpenIntent(
        pool=pool,
        amount0=Decimal("0.1"),
        amount1=Decimal("250"),
        range_lower=Decimal("0.5"),  # full-range Classic ignores the band; the intent still requires one
        range_upper=Decimal("2"),
        protocol="aerodrome",
        chain="base",
    )


def _full_compiler() -> MagicMock:
    compiler = MagicMock()
    compiler.chain = "base"
    compiler._gateway_client = _Gateway()
    compiler._gateway_internal_preflight = False
    compiler._resolve_token = lambda token: _TOKENS.get(token.lower())
    compiler.wallet_address = "0x" + "33" * 20
    compiler.price_oracle = {}
    return compiler


def test_classic_lp_open_bare_address_clears_the_format_gate() -> None:
    compiler = _full_compiler()
    compiler._gateway_client = None
    result = aerodrome_compiler.compile_lp_open_aerodrome(compiler, _open_intent(POOL))
    assert result.status is CompilationStatus.FAILED
    assert "Invalid pool format" not in (result.error or "")
    assert "connected gateway is required" in (result.error or "")


def test_classic_lp_open_symbolic_gate_error_advertises_exact_form() -> None:
    result = aerodrome_compiler.compile_lp_open_aerodrome(_full_compiler(), _open_intent("WETH"))
    assert result.status is CompilationStatus.FAILED
    assert "Invalid pool format" in (result.error or "")
    assert "exact 0x pool address" in (result.error or "")


def test_classic_lp_open_bare_address_mints_with_contract_identity() -> None:
    compiler = _full_compiler()
    tx = MagicMock(gas_estimate=200_000, tx_type="add_liquidity")
    tx.to_dict.return_value = {"tx_type": "add_liquidity"}
    with (
        _metadata_patch(),
        _factory_patch(_confirmed()),
        patch.object(aerodrome_compiler, "_lp_slippage_bps", return_value=50),
        patch.dict(aerodrome_compiler.LP_POSITION_MANAGERS, {"base": {"aerodrome": "0x" + "ee" * 20}}),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
    ):
        adapter_cls.return_value.add_liquidity.return_value = MagicMock(success=True, transactions=[tx], error=None)
        result = aerodrome_compiler.compile_lp_open_aerodrome(compiler, _open_intent(POOL))
    assert result.status is CompilationStatus.SUCCESS, result.error
    kwargs = adapter_cls.return_value.add_liquidity.call_args.kwargs
    assert (kwargs["token_a"], kwargs["token_b"], kwargs["stable"]) == (WETH, USDC, False)
    assert result.action_bundle.metadata["pool"] == POOL
    assert result.action_bundle.metadata["stable"] is False


# LP_CLOSE: the bare address is now factory-authenticated
def _close_compiler() -> MagicMock:
    compiler = MagicMock()
    compiler.chain = "base"
    compiler._gateway_client = None
    compiler._get_chain_rpc_url.return_value = "http://localhost:8545"
    compiler._resolve_token = lambda token: _TOKENS.get(token.lower())
    compiler.wallet_address = "0x" + "33" * 20
    return compiler


def test_classic_lp_close_refuses_bare_address_the_factory_does_not_own() -> None:
    intent = LPCloseIntent(position_id=POOL, pool=POOL, protocol="aerodrome", chain="base")
    with (
        _metadata_patch(),
        _factory_patch(_confirmed(OTHER_POOL)),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
    ):
        result = aerodrome_compiler.compile_lp_close_aerodrome(_close_compiler(), intent)
    assert result.status is CompilationStatus.FAILED
    assert "Refusing alternate-pool substitution" in (result.error or "")
    adapter_cls.return_value.remove_liquidity.assert_not_called()


@pytest.mark.parametrize(
    "factory_result",
    [
        PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_FAILED, warning="rpc down"),
        PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_UNAVAILABLE, warning="no rpc"),
    ],
)
def test_classic_lp_close_proceeds_when_factory_read_is_unavailable(factory_result: PoolValidationResult) -> None:
    """A close is risk-reducing: an unconfirmable factory read must not strand the position."""
    intent = LPCloseIntent(position_id=POOL, pool=POOL, protocol="aerodrome", chain="base")
    compiler = _close_compiler()
    compiler._query_erc20_balance.return_value = 123456789
    tx = MagicMock(gas_estimate=100_000, tx_type="remove_liquidity")
    tx.to_dict.return_value = {"tx_type": "remove_liquidity"}
    with (
        _metadata_patch(),
        _factory_patch(factory_result),
        patch.object(aerodrome_compiler, "_lp_slippage_bps", return_value=50),
        patch.dict(aerodrome_compiler.LP_POSITION_MANAGERS, {"base": {"aerodrome": "0x" + "ee" * 20}}),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
    ):
        adapter_cls.return_value.remove_liquidity.return_value = MagicMock(success=True, transactions=[tx], error=None)
        result = aerodrome_compiler.compile_lp_close_aerodrome(compiler, intent)
    assert result.status is CompilationStatus.SUCCESS, result.error
    adapter_cls.return_value.remove_liquidity.assert_called_once()


def test_classic_lp_close_refuses_measured_factory_denial() -> None:
    intent = LPCloseIntent(position_id=POOL, pool=POOL, protocol="aerodrome", chain="base")
    denied = PoolValidationResult(exists=False, reason=PoolValidationReason.NOT_FOUND, error="no pool")
    with (
        _metadata_patch(),
        _factory_patch(denied),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
    ):
        result = aerodrome_compiler.compile_lp_close_aerodrome(_close_compiler(), intent)
    assert result.status is CompilationStatus.FAILED
    adapter_cls.return_value.remove_liquidity.assert_not_called()


def test_classic_lp_close_refuses_pool_that_disagrees_with_position_id() -> None:
    intent = LPCloseIntent(position_id=POOL, pool=OTHER_POOL, protocol="aerodrome", chain="base")
    with _metadata_patch() as read, patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls:
        result = aerodrome_compiler.compile_lp_close_aerodrome(_close_compiler(), intent)
    assert result.status is CompilationStatus.FAILED
    assert "mismatched pool" in (result.error or "")
    read.assert_not_called()
    adapter_cls.return_value.remove_liquidity.assert_not_called()


def test_classic_lp_open_never_tolerates_an_unavailable_factory_read() -> None:
    """The asymmetry is close-only: a new position needs a confirmed factory answer."""
    unavailable = PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_FAILED, warning="rpc down")
    with _metadata_patch(), _factory_patch(unavailable):
        result = _failed(_resolve(_compiler()))
    assert "Cannot authenticate exact Aerodrome pool" in (result.error or "")
