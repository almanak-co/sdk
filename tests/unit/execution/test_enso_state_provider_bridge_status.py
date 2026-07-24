"""Branch-coverage tests for ``EnsoStateProvider.get_bridge_transfer_status``.

Enso cross-chain completion is detected by polling the destination-chain
balance, so the method multiplexes:

  * deposit_id parsing (wrong part count, wrong prefix, non-integer amounts),
  * registered vs unregistered transfers (started_at source, pending cleanup),
  * balance-read failure -> "unknown",
  * completion thresholds — slippage-adjusted when an expected amount is set,
    dust threshold when it is not, and the dust guard on tiny increases.

The balance seam (``_get_token_balance``) is replaced with a Mock on the
instance; no Web3 providers or gateway clients are constructed.
"""

import asyncio
from unittest.mock import Mock

import pytest

from almanak.framework.execution.enso_state_provider import EnsoStateProvider

WALLET = "0x" + "11" * 20
TX_HASH = "0xabc"


def _make_provider(balance: int | Exception = 0) -> EnsoStateProvider:
    provider = EnsoStateProvider(rpc_urls={}, wallet_address=WALLET)
    if isinstance(balance, Exception):
        provider._get_token_balance = Mock(side_effect=balance)
    else:
        provider._get_token_balance = Mock(return_value=balance)
    return provider


def _deposit_id(expected: int, initial: int, prefix: str = "enso") -> str:
    return f"{prefix}:base:arbitrum:{TX_HASH}:WETH:{expected}:{initial}"


class TestDepositIdParsing:
    @pytest.mark.parametrize(
        "deposit_id",
        [
            pytest.param("enso:base:arbitrum", id="too-few-parts"),
            pytest.param(_deposit_id(1, 0) + ":extra", id="too-many-parts"),
            pytest.param(_deposit_id(1, 0, prefix="stargate"), id="wrong-prefix"),
        ],
    )
    def test_malformed_deposit_id_returns_unknown(self, deposit_id):
        provider = _make_provider()

        status = asyncio.run(provider.get_bridge_transfer_status("enso", deposit_id))

        assert status["status"] == "unknown"
        assert status["error"] == f"Invalid deposit_id format: {deposit_id}"
        provider._get_token_balance.assert_not_called()

    @pytest.mark.parametrize(
        "deposit_id",
        [
            pytest.param(f"enso:base:arbitrum:{TX_HASH}:WETH:not-an-int:0", id="bad-expected"),
            pytest.param(f"enso:base:arbitrum:{TX_HASH}:WETH:100:not-an-int", id="bad-initial"),
        ],
    )
    def test_non_integer_amounts_return_parse_error(self, deposit_id):
        provider = _make_provider()

        status = asyncio.run(provider.get_bridge_transfer_status("enso", deposit_id))

        assert status["status"] == "unknown"
        assert status["error"].startswith("Failed to parse deposit_id: ")


class TestBalanceReadFailure:
    def test_balance_read_exception_returns_unknown(self):
        provider = _make_provider(balance=RuntimeError("rpc down"))

        status = asyncio.run(
            provider.get_bridge_transfer_status("enso", _deposit_id(expected=100, initial=0))
        )

        assert status == {"status": "unknown", "error": "rpc down"}


class TestCompletionDetection:
    def test_unregistered_transfer_completes_on_full_arrival(self):
        expected = 10**18
        provider = _make_provider(balance=expected)

        status = asyncio.run(
            provider.get_bridge_transfer_status("enso", _deposit_id(expected=expected, initial=0))
        )

        assert status["status"] == "completed"
        assert status["destination_balance"] == expected
        assert status["balance_increase"] == expected
        assert status["expected_amount"] == expected
        assert status["destination_tx"] is None
        assert status["elapsed_seconds"] >= 0
        # Balance is read on the destination chain for the bridged token.
        provider._get_token_balance.assert_called_once_with("arbitrum", "WETH")

    def test_slippage_tolerance_accepts_slightly_short_arrival(self):
        expected = 1_000_000
        provider = _make_provider(balance=960_000)  # 4% short, tolerance is 5%

        status = asyncio.run(
            provider.get_bridge_transfer_status("enso", _deposit_id(expected=expected, initial=0))
        )

        assert status["status"] == "completed"
        assert status["balance_increase"] == 960_000

    def test_registered_transfer_is_cleaned_up_on_completion(self):
        expected = 10**18
        provider = _make_provider(balance=500)
        deposit_id = provider.register_bridge_transfer(
            source_chain="base",
            destination_chain="arbitrum",
            source_tx_hash=TX_HASH,
            token_symbol="WETH",
            expected_amount=expected,
        )
        assert deposit_id in provider._pending_transfers
        provider._get_token_balance = Mock(return_value=500 + expected)

        status = asyncio.run(provider.get_bridge_transfer_status("enso", deposit_id))

        assert status["status"] == "completed"
        assert status["balance_increase"] == expected
        assert deposit_id not in provider._pending_transfers

    def test_zero_expected_amount_completes_on_any_increase_above_dust(self):
        provider = _make_provider(balance=2_000)

        status = asyncio.run(
            provider.get_bridge_transfer_status("enso", _deposit_id(expected=0, initial=0))
        )

        assert status["status"] == "completed"
        assert status["balance_increase"] == 2_000


class TestPendingDetection:
    def test_insufficient_increase_stays_pending(self):
        expected = 1_000_000
        provider = _make_provider(balance=100_000)  # far below 95% threshold

        status = asyncio.run(
            provider.get_bridge_transfer_status("enso", _deposit_id(expected=expected, initial=0))
        )

        assert status["status"] == "pending"
        assert status["destination_balance"] == 100_000
        assert status["balance_increase"] == 100_000
        assert status["expected_amount"] == expected
        assert status["initial_balance"] == 0
        assert status["elapsed_seconds"] >= 0

    def test_increase_meeting_expectation_but_below_dust_stays_pending(self):
        """A tiny expected amount is met, but the dust guard still holds it."""
        provider = _make_provider(balance=200)  # >= 95% of 200 but <= 1000 wei dust

        status = asyncio.run(
            provider.get_bridge_transfer_status("enso", _deposit_id(expected=200, initial=0))
        )

        assert status["status"] == "pending"
        assert status["balance_increase"] == 200

    def test_zero_expected_amount_below_dust_stays_pending(self):
        provider = _make_provider(balance=500)  # below the 1000-wei dust threshold

        status = asyncio.run(
            provider.get_bridge_transfer_status("enso", _deposit_id(expected=0, initial=0))
        )

        assert status["status"] == "pending"
        assert status["balance_increase"] == 500

    def test_pending_uses_initial_balance_from_deposit_id(self):
        provider = _make_provider(balance=5_000)

        status = asyncio.run(
            provider.get_bridge_transfer_status(
                "enso", _deposit_id(expected=10**18, initial=4_000)
            )
        )

        assert status["status"] == "pending"
        assert status["balance_increase"] == 1_000
        assert status["initial_balance"] == 4_000
