"""Regression tests for Aerodrome LP_CLOSE compilation behavior."""

from unittest.mock import MagicMock, patch

from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import Intent


def _make_token_info(symbol: str, address: str, decimals: int = 18) -> MagicMock:
    token = MagicMock()
    token.symbol = symbol
    token.address = address
    token.decimals = decimals
    token.is_native = False
    token.to_dict.return_value = {
        "symbol": symbol,
        "address": address,
        "decimals": decimals,
        "is_native": False,
    }
    return token


def _make_compiler() -> IntentCompiler:
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = "base"
    compiler.wallet_address = "0x" + "11" * 20
    compiler.price_oracle = {}
    return compiler


def _make_lp_close_intent() -> Intent:
    return Intent.lp_close(
        position_id="WETH/USDC/volatile",
        pool="WETH/USDC/volatile",
        collect_fees=True,
        protocol="aerodrome",
    )


def test_aerodrome_lp_close_zero_lp_balance_is_noop_success() -> None:
    """LP_CLOSE should be a no-op success when wallet has no Aerodrome LP tokens."""
    compiler = _make_compiler()
    token0 = _make_token_info("WETH", "0x" + "aa" * 20)
    token1 = _make_token_info("USDC", "0x" + "bb" * 20)
    intent = _make_lp_close_intent()

    with (
        patch.object(compiler, "_resolve_token", side_effect=[token0, token1]),
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch.object(compiler, "_query_erc20_balance", return_value=0),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.sdk.get_pool_address.return_value = "0x" + "cc" * 20

        result = compiler._compile_lp_close_aerodrome(intent)

    assert result.status == CompilationStatus.SUCCESS
    assert result.error is None
    assert result.action_bundle is not None
    assert result.action_bundle.transactions == []
    assert result.total_gas_estimate == 0
    assert any("LP_CLOSE as no-op" in warning for warning in result.warnings)


def test_aerodrome_lp_close_nonzero_lp_balance_builds_transactions() -> None:
    """LP_CLOSE should compile when remove_liquidity tx build succeeds."""
    compiler = _make_compiler()
    token0 = _make_token_info("WETH", "0x" + "aa" * 20)
    token1 = _make_token_info("USDC", "0x" + "bb" * 20)
    intent = _make_lp_close_intent()

    approve_tx = MagicMock()
    approve_tx.gas_estimate = 45_000
    approve_tx.to_dict.return_value = {"tx_type": "approve", "to": "0x" + "cc" * 20}
    remove_tx = MagicMock()
    remove_tx.gas_estimate = 180_000
    remove_tx.to_dict.return_value = {"tx_type": "remove_liquidity", "to": "0x" + "dd" * 20}

    liquidity_result = MagicMock(success=True, transactions=[approve_tx, remove_tx], error=None)

    with (
        patch.object(compiler, "_resolve_token", side_effect=[token0, token1]),
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch.object(compiler, "_query_erc20_balance", return_value=123456789),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.sdk.get_pool_address.return_value = "0x" + "cc" * 20
        mock_adapter.remove_liquidity.return_value = liquidity_result

        result = compiler._compile_lp_close_aerodrome(intent)

    assert result.status == CompilationStatus.SUCCESS
    assert result.error is None
    assert result.action_bundle is not None
    assert len(result.transactions) == 2
    assert result.total_gas_estimate == 225_000


def test_aerodrome_lp_close_propagates_remove_liquidity_build_error() -> None:
    """LP_CLOSE should fail with adapter error message when tx build fails."""
    compiler = _make_compiler()
    token0 = _make_token_info("WETH", "0x" + "aa" * 20)
    token1 = _make_token_info("USDC", "0x" + "bb" * 20)
    intent = _make_lp_close_intent()

    with (
        patch.object(compiler, "_resolve_token", side_effect=[token0, token1]),
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch.object(compiler, "_query_erc20_balance", return_value=1000000000000000000),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.sdk.get_pool_address.return_value = "0x" + "cc" * 20
        mock_adapter.remove_liquidity.return_value = MagicMock(
            success=False,
            transactions=[],
            error="lp approval metadata resolution failed",
        )

        result = compiler._compile_lp_close_aerodrome(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "Failed to build removeLiquidity TX: lp approval metadata resolution failed" in result.error
