"""Tests for the LP dashboard data contract.

These tests enforce that:
1. prepare_lp_session_state() produces all keys the template needs
2. LP_CRITICAL_KEYS uses the same key names strategies write (not legacy aliases)
3. Graceful degradation when API calls fail
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.dashboard.templates.lp_dashboard import (
    LP_CRITICAL_KEYS,
    LPDashboardConfig,
    prepare_lp_session_state,
)


@pytest.fixture
def mock_api_client():
    """API client returning realistic LP strategy state."""
    client = MagicMock()
    client.get_state.return_value = {
        "position_id": "4977387",
        "range_lower": "2310.8008",
        "range_upper": "2405.1192",
        "total_value_usd": "99.93",
    }
    client.get_price.return_value = 2358.76
    client.get_position.return_value = {
        "token_balances": [
            {"symbol": "WETH", "balance": "0.0176", "value_usd": "41.52"},
            {"symbol": "USDC", "balance": "42.49", "value_usd": "42.49"},
        ],
        "lp_positions": [],
        "total_lp_value_usd": "84.01",
    }
    return client


@pytest.fixture
def config():
    return LPDashboardConfig(token0="WETH", token1="USDC", chain="base")


# ---------------------------------------------------------------------------
# Contract: prepare must produce every critical key
# ---------------------------------------------------------------------------


class TestPrepareProducesAllCriticalKeys:
    """prepare_lp_session_state must produce every key in LP_CRITICAL_KEYS."""

    def test_all_critical_keys_present(self, mock_api_client, config):
        result = prepare_lp_session_state(mock_api_client, config=config)
        missing = [k for k in LP_CRITICAL_KEYS if k not in result]
        assert not missing, f"prepare_lp_session_state missing keys: {missing}"

    def test_state_keys_pass_through(self, mock_api_client, config):
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["position_id"] == "4977387"
        assert result["range_lower"] == "2310.8008"
        assert result["range_upper"] == "2405.1192"
        assert result["total_value_usd"] == "99.93"

    def test_is_active_true_when_position_exists(self, mock_api_client, config):
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["is_active"] is True

    def test_is_active_false_when_no_position(self, mock_api_client, config):
        mock_api_client.get_state.return_value = {"position_id": None}
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["is_active"] is False

    def test_in_range_true(self, mock_api_client, config):
        # price 2358.76 is in [2310.80, 2405.12]
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["in_range"] is True

    def test_in_range_false_when_price_above(self, mock_api_client, config):
        mock_api_client.get_price.return_value = 2500.0
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["in_range"] is False

    def test_in_range_false_when_price_below(self, mock_api_client, config):
        mock_api_client.get_price.return_value = 2000.0
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["in_range"] is False

    def test_in_range_none_when_no_price(self, mock_api_client, config):
        mock_api_client.get_state.return_value = {}
        mock_api_client.get_price.return_value = None
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["in_range"] is None

    def test_token_amounts_matched_by_symbol(self, mock_api_client, config):
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["token0_amount"] == 0.0176
        assert result["token1_amount"] == 42.49

    def test_token_amounts_symbol_match_is_case_insensitive(self, mock_api_client, config):
        mock_api_client.get_position.return_value = {
            "token_balances": [
                {"symbol": "weth", "balance": "1.0", "value_usd": "2000"},
                {"symbol": "usdc", "balance": "500", "value_usd": "500"},
            ],
        }
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["token0_amount"] == 1.0
        assert result["token1_amount"] == 500.0


# ---------------------------------------------------------------------------
# Contract: key names must match what strategies actually write
# ---------------------------------------------------------------------------


class TestKeyNamesMatchStrategyState:
    """LP_CRITICAL_KEYS must use the key names strategies write, not legacy aliases."""

    def test_uses_range_lower_not_lower_price(self):
        assert "range_lower" in LP_CRITICAL_KEYS
        assert "lower_price" not in LP_CRITICAL_KEYS

    def test_uses_range_upper_not_upper_price(self):
        assert "range_upper" in LP_CRITICAL_KEYS
        assert "upper_price" not in LP_CRITICAL_KEYS

    def test_uses_total_value_usd_not_position_value_usd(self):
        assert "total_value_usd" in LP_CRITICAL_KEYS
        assert "position_value_usd" not in LP_CRITICAL_KEYS

    def test_uses_position_id(self):
        assert "position_id" in LP_CRITICAL_KEYS


# ---------------------------------------------------------------------------
# Contract: prepare must not crash on API failures
# ---------------------------------------------------------------------------


class TestPrepareGracefulDegradation:
    """prepare_lp_session_state should degrade gracefully, never crash."""

    def test_position_api_failure(self, mock_api_client, config):
        mock_api_client.get_position.side_effect = Exception("network error")
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["token0_amount"] == 0
        assert result["token1_amount"] == 0

    def test_empty_state(self, mock_api_client, config):
        mock_api_client.get_state.return_value = {}
        mock_api_client.get_price.return_value = None
        mock_api_client.get_position.return_value = {"token_balances": []}
        result = prepare_lp_session_state(mock_api_client, config=config)
        # Should still have all critical keys with sensible defaults
        for k in LP_CRITICAL_KEYS:
            assert k in result, f"Missing key after empty state: {k}"

    def test_no_config_skips_price_fetch(self, mock_api_client):
        result = prepare_lp_session_state(mock_api_client, config=None)
        mock_api_client.get_price.assert_not_called()
        assert result["current_price"] is None

    def test_live_state_wins_over_stale_caller_state(self, mock_api_client, config):
        """VIB-5025: a stale, caller-preserved ``session_state`` must NOT mask
        fresh gateway state. Live-owned keys refresh; custom keys pass through.
        """
        stale = {
            "position_id": "stale-id",
            "range_lower": "1.0",
            "range_upper": "2.0",
            "custom_key": "preserved",
        }
        result = prepare_lp_session_state(mock_api_client, session_state=stale, config=config)
        # Live gateway reads win over the stale caller values.
        assert result["position_id"] == "4977387"
        assert result["range_lower"] == "2310.8008"
        assert result["range_upper"] == "2405.1192"
        # Non-live custom keys still pass through untouched.
        assert result["custom_key"] == "preserved"

    def test_preserve_keys_pins_caller_value_over_live_read(self, mock_api_client, config):
        """The explicit ``preserve_keys`` opt-out keeps a caller value over the
        live read (e.g. a replay / snapshot dashboard)."""
        pinned = {"position_id": "snapshot-id", "range_lower": "1.0"}
        result = prepare_lp_session_state(
            mock_api_client,
            session_state=pinned,
            config=config,
            preserve_keys=["position_id"],
        )
        # position_id is pinned to the caller value...
        assert result["position_id"] == "snapshot-id"
        # ...but range_lower (not pinned) still refreshes from the live read.
        assert result["range_lower"] == "2310.8008"

    def test_live_none_not_resurrected_by_stale_caller(self, mock_api_client, config):
        """A live ``None`` is a measured value (position closed) and must NOT be
        overwritten by a stale caller value (VIB-5025 resurrection guard)."""
        mock_api_client.get_state.return_value = {"position_id": None}
        stale = {"position_id": "closed-stale-id"}
        result = prepare_lp_session_state(mock_api_client, session_state=stale, config=config)
        assert result["position_id"] is None
        assert result["is_active"] is False

    def test_caller_value_is_fallback_when_live_read_missing(self, mock_api_client, config):
        """Empty != Zero: a caller value fills a live key the gateway omits,
        rather than being blanked."""
        mock_api_client.get_state.return_value = {"position_id": "4977387"}
        caller = {"range_lower": "1234.5", "range_upper": "2345.6"}
        result = prepare_lp_session_state(mock_api_client, session_state=caller, config=config)
        # The gateway omitted the range; the caller's last-known value fills in.
        assert result["range_lower"] == "1234.5"
        assert result["range_upper"] == "2345.6"

    def test_no_token_balances_defaults_to_zero(self, mock_api_client, config):
        mock_api_client.get_position.return_value = {"token_balances": []}
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["token0_amount"] == 0.0
        assert result["token1_amount"] == 0.0

    def test_get_state_failure_still_produces_all_keys(self, mock_api_client, config):
        mock_api_client.get_state.side_effect = Exception("gateway unavailable")
        mock_api_client.get_position.return_value = {"token_balances": []}
        result = prepare_lp_session_state(mock_api_client, config=config)
        for k in LP_CRITICAL_KEYS:
            assert k in result, f"Missing key after get_state failure: {k}"

    def test_in_range_handles_non_numeric_bounds(self, mock_api_client, config):
        mock_api_client.get_state.return_value = {
            "position_id": "123",
            "range_lower": "N/A",
            "range_upper": "N/A",
        }
        result = prepare_lp_session_state(mock_api_client, config=config)
        assert result["in_range"] is None
