"""Unit tests for TraderJoe V2 LP close compilation."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import Intent


def test_traderjoe_lp_close_uses_known_bin_ids_without_position_rediscovery() -> None:
    compiler = IntentCompiler(
        chain="avalanche",
        wallet_address="0x" + "1" * 40,
        rpc_url="http://localhost:8545",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )
    compiler._resolve_token = MagicMock(
        side_effect=[
            SimpleNamespace(address="0x" + "2" * 40),
            SimpleNamespace(address="0x" + "3" * 40),
        ]
    )

    intent = Intent.lp_close(
        position_id="WAVAX/USDC/20",
        pool="WAVAX/USDC/20",
        collect_fees=True,
        protocol="traderjoe_v2",
        protocol_params={"bin_ids": [8388600, 8388601]},
    )

    mock_adapter = MagicMock()
    mock_adapter.sdk.router_address = "0x" + "4" * 40
    mock_adapter.sdk.get_pool_address.return_value = "0x" + "5" * 40
    mock_adapter.sdk.get_position_balances_for_ids.return_value = {
        8388600: 111,
        8388601: 222,
    }
    # The targeted path computes amount_x/amount_y so the adapter can derive
    # slippage-protected minimums (VIB-3741). Mock both helpers it uses.
    mock_adapter.sdk.get_total_position_value.return_value = (1000, 2000)
    mock_adapter.sdk.get_pool_info.return_value = SimpleNamespace(active_id=8388600)
    mock_adapter.sdk.build_approve_for_all_transaction.return_value = (
        {"to": "0x" + "6" * 40, "data": "0xaaaa", "value": 0},
        12345,
    )
    mock_adapter.build_remove_liquidity_transaction.return_value = SimpleNamespace(
        to="0x" + "7" * 40,
        data="0xbbbb",
        value=0,
        gas=54321,
    )

    with patch("almanak.framework.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=mock_adapter):
        result = compiler._compile_lp_close_traderjoe_v2(intent)

    assert result.status.value == "SUCCESS", result.error
    assert result.action_bundle is not None
    assert len(result.action_bundle.transactions) == 2
    mock_adapter.sdk.get_position_balances_for_ids.assert_called_once_with(
        "0x" + "5" * 40,
        "0x" + "1" * 40,
        [8388600, 8388601],
    )
    mock_adapter.get_position.assert_not_called()
    # Pin the slippage-valuation call: same pool, same wallet, and the exact
    # balances from the targeted lookup. get_total_position_value is best-
    # effort by design (matches the heuristic fallback path's tolerance);
    # see VIB-3757 for follow-up to harden once we understand which fork-only
    # reverts trigger per-bin skips.
    mock_adapter.sdk.get_total_position_value.assert_called_once_with(
        "0x" + "5" * 40,
        "0x" + "1" * 40,
        precomputed_balances={8388600: 111, 8388601: 222},
    )
    mock_adapter.build_remove_liquidity_transaction.assert_called_once()
    _, kwargs = mock_adapter.build_remove_liquidity_transaction.call_args
    # VIB-3741: targeted path must NOT bypass slippage protection. Either it omits
    # amount_x_min/amount_y_min entirely (adapter computes from position), or it
    # passes them as None — both leave the adapter in charge of slippage.
    assert kwargs.get("amount_x_min") is None
    assert kwargs.get("amount_y_min") is None
    # And the position passed to the adapter must carry non-zero token amounts so
    # the adapter can actually compute slippage-protected minimums.
    position_passed = kwargs["position"]
    assert position_passed.amount_x == 1000
    assert position_passed.amount_y == 2000


def test_traderjoe_lp_close_falls_back_preserves_slippage_when_targeted_lookup_empty() -> None:
    """When known_bin_ids are stale and targeted lookup returns empty, the
    compiler must fall back to full discovery AND pass amount_x_min=None /
    amount_y_min=None so the adapter derives slippage-protected minimums.
    Passing 0 here would disable slippage protection on the fallback path.
    """
    compiler = IntentCompiler(
        chain="avalanche",
        wallet_address="0x" + "1" * 40,
        rpc_url="http://localhost:8545",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )
    compiler._resolve_token = MagicMock(
        side_effect=[
            SimpleNamespace(address="0x" + "2" * 40),
            SimpleNamespace(address="0x" + "3" * 40),
        ]
    )

    intent = Intent.lp_close(
        position_id="WAVAX/USDC/20",
        pool="WAVAX/USDC/20",
        collect_fees=True,
        protocol="traderjoe_v2",
        protocol_params={"bin_ids": [8388600, 8388601]},  # stale
    )

    mock_adapter = MagicMock()
    mock_adapter.sdk.router_address = "0x" + "4" * 40
    mock_adapter.sdk.get_pool_address.return_value = "0x" + "5" * 40
    # Targeted lookup returns empty (stale bin_ids)
    mock_adapter.sdk.get_position_balances_for_ids.return_value = {}
    # Full discovery finds a real position
    mock_adapter.get_position.return_value = SimpleNamespace(
        pool_address="0x" + "5" * 40,
        bin_ids=[8388602, 8388603],
        balances={8388602: 333, 8388603: 444},
        amount_x=1000,
        amount_y=2000,
    )
    mock_adapter.sdk.build_approve_for_all_transaction.return_value = (
        {"to": "0x" + "6" * 40, "data": "0xaaaa", "value": 0},
        12345,
    )
    mock_adapter.build_remove_liquidity_transaction.return_value = SimpleNamespace(
        to="0x" + "7" * 40,
        data="0xbbbb",
        value=0,
        gas=54321,
    )

    with patch("almanak.framework.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=mock_adapter):
        result = compiler._compile_lp_close_traderjoe_v2(intent)

    assert result.status.value == "SUCCESS", result.error
    mock_adapter.sdk.get_position_balances_for_ids.assert_called_once_with(
        "0x" + "5" * 40,
        "0x" + "1" * 40,
        [8388600, 8388601],
    )
    mock_adapter.get_position.assert_called_once_with(
        "0x" + "2" * 40,
        "0x" + "3" * 40,
        20,
    )
    # Targeted-lookup short-circuit: empty balances skip the strict valuation.
    mock_adapter.sdk.get_total_position_value.assert_not_called()
    mock_adapter.build_remove_liquidity_transaction.assert_called_once()
    _, kwargs = mock_adapter.build_remove_liquidity_transaction.call_args
    # Fallback path: amount_x_min/amount_y_min are not passed, so the adapter
    # derives them from position.amount_x / position.amount_y (slippage-protected).
    assert kwargs.get("amount_x_min") is None
    assert kwargs.get("amount_y_min") is None
    position_passed = kwargs["position"]
    assert position_passed.amount_x == 1000
    assert position_passed.amount_y == 2000


def test_traderjoe_lp_close_pool_info_failure_does_not_block_compilation() -> None:
    """get_pool_info() is informational (active_bin); build_remove_liquidity_transaction
    derives slippage from amount_x/amount_y and doesn't need active_bin. A
    get_pool_info revert must therefore not block a close when we already have
    enough data (balances + valuation) to build it.
    """
    compiler = IntentCompiler(
        chain="avalanche",
        wallet_address="0x" + "1" * 40,
        rpc_url="http://localhost:8545",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )
    compiler._resolve_token = MagicMock(
        side_effect=[
            SimpleNamespace(address="0x" + "2" * 40),
            SimpleNamespace(address="0x" + "3" * 40),
        ]
    )

    intent = Intent.lp_close(
        position_id="WAVAX/USDC/20",
        pool="WAVAX/USDC/20",
        collect_fees=True,
        protocol="traderjoe_v2",
        protocol_params={"bin_ids": [8388600, 8388601]},
    )

    mock_adapter = MagicMock()
    mock_adapter.sdk.router_address = "0x" + "4" * 40
    mock_adapter.sdk.get_pool_address.return_value = "0x" + "5" * 40
    mock_adapter.sdk.get_position_balances_for_ids.return_value = {
        8388600: 111,
        8388601: 222,
    }
    mock_adapter.sdk.get_total_position_value.return_value = (1000, 2000)
    mock_adapter.sdk.get_pool_info.side_effect = RuntimeError("RPC blip on getPoolInfo")
    mock_adapter.sdk.build_approve_for_all_transaction.return_value = (
        {"to": "0x" + "6" * 40, "data": "0xaaaa", "value": 0},
        12345,
    )
    mock_adapter.build_remove_liquidity_transaction.return_value = SimpleNamespace(
        to="0x" + "7" * 40,
        data="0xbbbb",
        value=0,
        gas=54321,
    )

    with patch("almanak.framework.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=mock_adapter):
        result = compiler._compile_lp_close_traderjoe_v2(intent)

    assert result.status.value == "SUCCESS", (
        "Compilation must NOT fail when get_pool_info reverts; got: "
        f"{result.status.value} -- {result.error}"
    )
    mock_adapter.build_remove_liquidity_transaction.assert_called_once()
    _, kwargs = mock_adapter.build_remove_liquidity_transaction.call_args
    position_passed = kwargs["position"]
    # active_bin defaults to 0 when get_pool_info fails; amount_x/amount_y still
    # carry the slippage-protection values.
    assert position_passed.active_bin == 0
    assert position_passed.amount_x == 1000
    assert position_passed.amount_y == 2000


