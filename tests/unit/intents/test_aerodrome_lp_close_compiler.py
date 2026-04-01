"""Regression tests for Aerodrome LP_CLOSE compilation behavior."""

from unittest.mock import MagicMock, patch

from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
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
        patch.object(compiler, "_get_aerodrome_pool_address", return_value="0x" + "cc" * 20),
        patch.object(compiler, "_query_erc20_balance", return_value=0),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
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
        patch.object(compiler, "_get_aerodrome_pool_address", return_value="0x" + "cc" * 20),
        patch.object(compiler, "_query_erc20_balance", return_value=123456789),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
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
        patch.object(compiler, "_get_aerodrome_pool_address", return_value="0x" + "cc" * 20),
        patch.object(compiler, "_query_erc20_balance", return_value=1000000000000000000),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.remove_liquidity.return_value = MagicMock(
            success=False,
            transactions=[],
            error="lp approval metadata resolution failed",
        )

        result = compiler._compile_lp_close_aerodrome(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "Failed to build removeLiquidity TX: lp approval metadata resolution failed" in result.error


def test_aerodrome_lp_close_permission_discovery_uses_synthetic_balance() -> None:
    """permission_discovery=True should produce approve + removeLiquidity even with zero LP balance."""
    compiler = _make_compiler()
    compiler._config = IntentCompilerConfig(allow_placeholder_prices=True, permission_discovery=True)

    token0 = _make_token_info("WETH", "0x" + "aa" * 20)
    token1 = _make_token_info("USDC", "0x" + "bb" * 20)
    pool_address = "0x" + "cc" * 20
    intent = _make_lp_close_intent()

    approve_tx = MagicMock()
    approve_tx.gas_estimate = 45_000
    approve_tx.to = pool_address
    approve_tx.data = "0x095ea7b3" + "00" * 60  # approve selector
    approve_tx.tx_type = "approve"
    approve_tx.to_dict.return_value = {"tx_type": "approve", "to": pool_address}
    remove_tx = MagicMock()
    remove_tx.gas_estimate = 180_000
    remove_tx.to = "0x" + "dd" * 20
    remove_tx.data = "0x0dede6c4" + "00" * 60  # removeLiquidity selector
    remove_tx.tx_type = "remove_liquidity"
    remove_tx.to_dict.return_value = {"tx_type": "remove_liquidity", "to": "0x" + "dd" * 20}

    liquidity_result = MagicMock(success=True, transactions=[approve_tx, remove_tx], error=None)

    with (
        patch.object(compiler, "_resolve_token", side_effect=[token0, token1]),
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch.object(compiler, "_get_aerodrome_pool_address", return_value=pool_address),
        # Return 0 — permission_discovery should substitute a synthetic balance
        patch.object(compiler, "_query_erc20_balance", return_value=0),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.remove_liquidity.return_value = liquidity_result

        result = compiler._compile_lp_close_aerodrome(intent)

    # Should succeed (NOT a no-op) — synthetic balance bypasses the zero-balance early return
    assert result.status == CompilationStatus.SUCCESS
    assert result.error is None
    assert len(result.transactions) == 2
    assert result.total_gas_estimate == 225_000
    # Verify the bundle includes both the LP token approve and removeLiquidity
    tx_types = [tx.tx_type for tx in result.transactions]
    assert "approve" in tx_types
    assert "remove_liquidity" in tx_types
    # The approve MUST target the pool address (LP token) — this is the
    # permission that was missing and caused the staging ConditionViolation.
    approve_txs = [tx for tx in result.transactions if tx.tx_type == "approve"]
    assert len(approve_txs) == 1
    assert approve_txs[0].to == pool_address


def test_aerodrome_lp_close_permission_discovery_none_balance() -> None:
    """permission_discovery=True should use synthetic balance even when RPC returns None."""
    compiler = _make_compiler()
    compiler._config = IntentCompilerConfig(allow_placeholder_prices=True, permission_discovery=True)

    token0 = _make_token_info("WETH", "0x" + "aa" * 20)
    token1 = _make_token_info("USDC", "0x" + "bb" * 20)
    pool_address = "0x" + "cc" * 20
    intent = _make_lp_close_intent()

    approve_tx = MagicMock()
    approve_tx.gas_estimate = 45_000
    approve_tx.to = pool_address
    approve_tx.data = "0x095ea7b3" + "00" * 60
    approve_tx.tx_type = "approve"
    approve_tx.to_dict.return_value = {"tx_type": "approve", "to": pool_address}
    remove_tx = MagicMock()
    remove_tx.gas_estimate = 180_000
    remove_tx.to = "0x" + "dd" * 20
    remove_tx.data = "0x0dede6c4" + "00" * 60
    remove_tx.tx_type = "remove_liquidity"
    remove_tx.to_dict.return_value = {"tx_type": "remove_liquidity", "to": "0x" + "dd" * 20}

    liquidity_result = MagicMock(success=True, transactions=[approve_tx, remove_tx], error=None)

    with (
        patch.object(compiler, "_resolve_token", side_effect=[token0, token1]),
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch.object(compiler, "_get_aerodrome_pool_address", return_value=pool_address),
        # Return None (RPC unavailable) — permission_discovery should still work
        patch.object(compiler, "_query_erc20_balance", return_value=None),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.remove_liquidity.return_value = liquidity_result

        result = compiler._compile_lp_close_aerodrome(intent)

    assert result.status == CompilationStatus.SUCCESS
    assert len(result.transactions) == 2
    # Verify approve targets the pool address (LP token)
    tx_types = [tx.tx_type for tx in result.transactions]
    assert "approve" in tx_types
    assert "remove_liquidity" in tx_types
    approve_txs = [tx for tx in result.transactions if tx.tx_type == "approve"]
    assert len(approve_txs) == 1
    assert approve_txs[0].to == pool_address
