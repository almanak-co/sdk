"""Teardown tests for TraderJoe V2 LP demo strategy."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.demo_strategies.traderjoe_lp import TraderJoeLPStrategy

# A real-shaped 42-char LBPair address (WAVAX/USDC/20 on Avalanche).
_LB_PAIR_ADDRESS = "0xB5352A39C11a81FE6748993D586EC448A01f08b5"


def _create_strategy() -> TraderJoeLPStrategy:
    """Create TraderJoeLPStrategy with minimal test attributes."""
    with patch.object(TraderJoeLPStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = TraderJoeLPStrategy.__new__(TraderJoeLPStrategy)

    strategy._deployment_id = "test-traderjoe-lp"
    strategy._chain = "avalanche"
    strategy._wallet_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    strategy._gateway_client = None
    strategy.pool = "WAVAX/USDC/20"
    strategy.bin_step = 20
    strategy.token_x_symbol = "WAVAX"
    strategy.token_y_symbol = "USDC"
    strategy.amount_x = Decimal("1.0")
    strategy.amount_y = Decimal("30")
    strategy._position_bin_ids = []

    return strategy


def test_get_open_positions_returns_valid_summary_without_position() -> None:
    """Method should return a valid summary object even with no position."""
    strategy = _create_strategy()

    summary = strategy.get_open_positions()

    assert summary.deployment_id == "test-traderjoe-lp"
    assert len(summary.positions) == 0
    assert summary.total_value_usd == Decimal("0")


def test_get_open_positions_returns_valid_summary_with_position() -> None:
    """Method should return LP position summary without constructor field errors."""
    strategy = _create_strategy()
    strategy._position_bin_ids = [8388608, 8388609, 8388610]

    snapshot = SimpleNamespace(price=lambda symbol: Decimal("30") if symbol == "WAVAX" else Decimal("1"))
    with patch.object(TraderJoeLPStrategy, "create_market_snapshot", return_value=snapshot):
        summary = strategy.get_open_positions()

    assert summary.deployment_id == "test-traderjoe-lp"
    assert len(summary.positions) == 1
    pos = summary.positions[0]
    assert pos.protocol == "traderjoe_v2"
    assert pos.position_id == "traderjoe-lp-WAVAX/USDC/20-avalanche"
    assert pos.chain == "avalanche"
    assert pos.details["num_bins"] == 3
    assert pos.details["bin_ids"] == [8388608, 8388609, 8388610]
    # WAVAX 1.0 @ 30 + USDC 30 @ 1 == 60 USD; deterministic snapshot pricing.
    assert summary.total_value_usd == Decimal("60")
    assert pos.value_usd == Decimal("60")


def test_get_open_positions_populates_lb_pair_address_vib4877() -> None:
    """VIB-4877: details must carry the 42-char LBPair address, not the symbol.

    The teardown post-condition verifier resolves the closed position by its
    LBPair contract address. The pool symbol ("WAVAX/USDC/20") must NOT leak
    into details["pool_address"] — that flips a successfully-closed teardown to
    FAILED. The producer must populate the real LBPair address.
    """
    strategy = _create_strategy()
    strategy._position_bin_ids = [8388608, 8388609, 8388610]

    snapshot = SimpleNamespace(price=lambda symbol: Decimal("30") if symbol == "WAVAX" else Decimal("1"))

    mock_adapter = MagicMock()
    mock_adapter.resolve_token_address.side_effect = lambda sym: (
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7" if sym == "WAVAX" else "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"
    )
    mock_adapter.sdk.get_pool_address.return_value = _LB_PAIR_ADDRESS

    with (
        patch.object(TraderJoeLPStrategy, "create_market_snapshot", return_value=snapshot),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=mock_adapter),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config"),
    ):
        summary = strategy.get_open_positions()

    pos = summary.positions[0]
    pool_address = pos.details["pool_address"]
    # Must be the resolved LBPair address, not the symbol triple.
    assert pool_address == _LB_PAIR_ADDRESS
    assert pool_address != strategy.pool
    # And it must satisfy the post-condition verifier's 42-char hex contract.
    assert isinstance(pool_address, str) and pool_address.startswith("0x") and len(pool_address) == 42
    # Symbol stays available for humans under "pool".
    assert pos.details["pool"] == "WAVAX/USDC/20"
    # Resolved with the configured bin_step.
    mock_adapter.sdk.get_pool_address.assert_called_once()
    assert mock_adapter.sdk.get_pool_address.call_args.args[2] == 20


def test_get_open_positions_failsoft_when_address_unresolvable_vib4877() -> None:
    """If the LBPair address cannot be resolved, preview must not crash.

    pool_address is simply omitted (the verifier then reports a precise
    missing-address error) — teardown preview still returns the position so
    on-chain risk is still surfaced.
    """
    strategy = _create_strategy()
    strategy._position_bin_ids = [8388608]

    snapshot = SimpleNamespace(price=lambda symbol: Decimal("30") if symbol == "WAVAX" else Decimal("1"))

    mock_adapter = MagicMock()
    mock_adapter.resolve_token_address.side_effect = lambda sym: "0x" + "1" * 40
    mock_adapter.sdk.get_pool_address.side_effect = RuntimeError("RPC down")

    with (
        patch.object(TraderJoeLPStrategy, "create_market_snapshot", return_value=snapshot),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=mock_adapter),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config"),
    ):
        summary = strategy.get_open_positions()

    assert len(summary.positions) == 1
    assert "pool_address" not in summary.positions[0].details


def test_get_open_positions_failsoft_when_gateway_client_none_vib4877() -> None:
    """Degraded path: no gateway client at preview time → real TraderJoeV2Config
    raises in __post_init__ (needs rpc_url or gateway_client). The producer must
    catch it and omit pool_address rather than crash teardown preview.

    This intentionally does NOT patch TraderJoeV2Config / TraderJoeV2Adapter, so
    the real construction path (the most likely production failure mode) runs.
    """
    strategy = _create_strategy()
    strategy._position_bin_ids = [8388608, 8388609]
    strategy._gateway_client = None  # no gateway at preview time

    snapshot = SimpleNamespace(price=lambda symbol: Decimal("30") if symbol == "WAVAX" else Decimal("1"))

    with patch.object(TraderJoeLPStrategy, "create_market_snapshot", return_value=snapshot):
        # Sanity: the real config genuinely raises for this input, so we know the
        # except-branch — not a silent no-op — is what omits pool_address.
        from almanak.connectors.traderjoe_v2 import TraderJoeV2Config
        from almanak.connectors.traderjoe_v2.sdk import TraderJoeV2SDKError

        with pytest.raises(TraderJoeV2SDKError):
            TraderJoeV2Config(chain="avalanche", wallet_address=strategy.wallet_address, gateway_client=None)

        summary = strategy.get_open_positions()

    # Preview still returns the position (on-chain risk surfaced); pool_address
    # is omitted because resolution degraded.
    assert len(summary.positions) == 1
    pos = summary.positions[0]
    assert "pool_address" not in pos.details
    # Symbol + bins still present so teardown can still act on the position.
    assert pos.details["pool"] == "WAVAX/USDC/20"
    assert pos.details["bin_ids"] == [8388608, 8388609]
