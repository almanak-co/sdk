"""Unit tests for teardown post-conditions (VIB-3742).

Verifies the on-chain closure verification hooks. The TJ V2 default is
exercised via a mocked SDK so we can assert behaviour for both the
"closed" and "residual liquidity" paths without a live fork.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.teardown.post_conditions import (
    ClosureCheckResult,
    _traderjoe_v2_post_condition,
    get_teardown_post_condition,
    has_teardown_post_condition,
    register_teardown_post_condition,
)

WALLET = "0x1111111111111111111111111111111111111111"
POOL = "0x2222222222222222222222222222222222222222"


def _make_position(
    pool_address: str = POOL,
    bin_ids: list[int] | None = None,
    chain: str = "avalanche",
    position_id: str = "tj-v2-test",
) -> SimpleNamespace:
    details = {"pool_address": pool_address}
    if bin_ids is not None:
        details["bin_ids"] = bin_ids
    return SimpleNamespace(
        protocol="traderjoe_v2",
        position_id=position_id,
        chain=chain,
        details=details,
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_traderjoe_v2_registered_by_default(self) -> None:
        # Importing the module registers the default; that contract is
        # important because TeardownManager looks it up by name.
        assert has_teardown_post_condition("traderjoe_v2")
        hook = get_teardown_post_condition("TraderJoe_V2")  # case-insensitive
        assert hook is not None

    def test_register_replace_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        original = get_teardown_post_condition("traderjoe_v2")
        try:
            with caplog.at_level("WARNING"):
                replacement = lambda **_: ClosureCheckResult(closed=True)  # noqa: E731
                register_teardown_post_condition("traderjoe_v2", replacement)
            assert any("Replacing existing teardown post-condition" in r.message for r in caplog.records)
        finally:
            # Restore the default so other tests aren't affected.
            if original is not None:
                register_teardown_post_condition("traderjoe_v2", original)


# ---------------------------------------------------------------------------
# TraderJoe V2 default post-condition
# ---------------------------------------------------------------------------


class TestTraderJoeV2PostCondition:
    def test_closed_when_known_bin_ids_have_zero_balance(self) -> None:
        """Strong-mode: bin_ids in details, balanceOfBatch returns empty."""
        sdk = MagicMock()
        sdk.get_position_balances_for_ids.return_value = {}

        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            adapter_cls.return_value.sdk = sdk

            position = _make_position(bin_ids=[100, 101, 102])
            result = _traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is True
        assert result.residual == {}
        sdk.get_position_balances_for_ids.assert_called_once_with(POOL, WALLET, [100, 101, 102])
        # Heuristic must NOT fire when bin_ids are present.
        sdk.get_position_balances.assert_not_called()

    def test_failed_when_known_bin_ids_have_residual(self) -> None:
        """Strong-mode: residual liquidity detected -> closed=False."""
        sdk = MagicMock()
        sdk.get_position_balances_for_ids.return_value = {100: 4567, 101: 1234}

        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            adapter_cls.return_value.sdk = sdk

            position = _make_position(bin_ids=[100, 101, 102])
            result = _traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is False
        assert result.residual["bin_balances"] == {100: 4567, 101: 1234}
        assert result.residual["total_lb_tokens"] == 5801
        assert result.residual["pool_address"] == POOL
        # Strong-mode result must not carry the weak fallback notice.
        assert "fallback_scan" not in result.residual

    def test_fallback_scan_when_bin_ids_absent(self) -> None:
        """Weak-mode: no bin_ids in details, falls back to active-id scan."""
        sdk = MagicMock()
        sdk.get_position_balances.return_value = {}

        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            adapter_cls.return_value.sdk = sdk

            position = _make_position(bin_ids=None)
            result = _traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is True
        assert "fallback_scan" in result.residual
        sdk.get_position_balances.assert_called_once_with(POOL, WALLET)
        sdk.get_position_balances_for_ids.assert_not_called()

    def test_fallback_scan_residual_marks_incomplete(self) -> None:
        """Weak-mode + residual: closed=False AND fallback note attached."""
        sdk = MagicMock()
        sdk.get_position_balances.return_value = {500: 999}

        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            adapter_cls.return_value.sdk = sdk

            position = _make_position(bin_ids=None)
            result = _traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is False
        assert result.residual["fallback_scan"]
        assert result.residual["bin_balances"] == {500: 999}

    def test_missing_pool_address_returns_error(self) -> None:
        position = SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="x",
            chain="avalanche",
            details={},
        )
        result = _traderjoe_v2_post_condition(position=position, wallet_address=WALLET)
        assert result.closed is False
        assert "pool_address" in (result.error or "")

    def test_pool_symbol_string_rejected_as_non_hex(self) -> None:
        """VIB-3943: a symbol like ``WAVAX/USDC/20`` must NOT be fed to balanceOf.

        Before the fix the symbol slipped through the ``details["pool"]`` fallback,
        web3.py raised ``ValueError: when sending a str, it must be a hex string``,
        and the runner marked an already-closed teardown as failed.
        """
        position = SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="tj-symbol",
            chain="avalanche",
            details={"pool": "WAVAX/USDC/20"},
        )
        result = _traderjoe_v2_post_condition(position=position, wallet_address=WALLET)
        assert result.closed is False
        assert "42-char hex address" in (result.error or "")
        assert "WAVAX/USDC/20" in (result.error or "")

    def test_sdk_init_failure_returns_error(self) -> None:
        with patch(
            "almanak.connectors.traderjoe_v2.TraderJoeV2Adapter",
            side_effect=RuntimeError("boom"),
        ):
            position = _make_position()
            result = _traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )
        assert result.closed is False
        assert "boom" in (result.error or "")

    def test_skips_token_position_type_without_failing_closed(self) -> None:
        """VIB-3974: A ``PositionType.TOKEN`` position with
        ``protocol='traderjoe_v2'`` (e.g. S-008 RSI flipper on Avalanche
        reporting a residual base-token balance like WAVAX with no
        ``pool_address``) must NOT be routed through the LB-pair closure
        check. Pre-fix, the hook fell through to the missing-pool_address
        branch and fail-closed every swap-only TraderJoe V2 teardown.
        Mirrors the Uniswap V3 non-LP gate.
        """
        # Pre-empt any SDK init by ensuring the adapter is never constructed.
        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            position = SimpleNamespace(
                protocol="traderjoe_v2",
                position_id="s008_rsi_token_0",
                chain="avalanche",
                position_type=SimpleNamespace(value="TOKEN"),
                details={"asset": "WAVAX", "balance": "1000000000000000000"},
            )
            result = _traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
            )

        # Hook reports closed=True with a residual note explaining it
        # deferred — verifier moves on to the next position.
        assert result.closed is True
        assert result.error is None
        assert "skipped_reason" in result.residual
        assert "TOKEN" in result.residual["skipped_reason"]
        # Adapter MUST NOT have been constructed — that's the whole point
        # of gating before SDK init.
        adapter_cls.assert_not_called()

    def test_lp_position_still_gated_through_strong_mode(self) -> None:
        """Regression guard: a ``PositionType.LP`` position still flows
        through the existing LB-pair balance check. The non-LP gate must
        not short-circuit real LP teardowns.
        """
        sdk = MagicMock()
        sdk.get_position_balances_for_ids.return_value = {}
        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            adapter_cls.return_value.sdk = sdk

            position = SimpleNamespace(
                protocol="traderjoe_v2",
                position_id="tj-v2-lp",
                chain="avalanche",
                position_type=SimpleNamespace(value="LP"),
                details={"pool_address": POOL, "bin_ids": [10, 11, 12]},
            )
            result = _traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is True
        assert "skipped_reason" not in result.residual
        sdk.get_position_balances_for_ids.assert_called_once_with(POOL, WALLET, [10, 11, 12])

    def test_lp_position_with_residual_still_fails_closure(self) -> None:
        """Regression guard: an LP position with residual liquidity must
        still report closed=False. The non-LP gate must not weaken the
        LP path.
        """
        sdk = MagicMock()
        sdk.get_position_balances_for_ids.return_value = {10: 7777}
        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            adapter_cls.return_value.sdk = sdk

            position = SimpleNamespace(
                protocol="traderjoe_v2",
                position_id="tj-v2-lp",
                chain="avalanche",
                position_type=SimpleNamespace(value="LP"),
                details={"pool_address": POOL, "bin_ids": [10]},
            )
            result = _traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is False
        assert result.residual["bin_balances"] == {10: 7777}

    def test_balance_query_failure_returns_error(self) -> None:
        sdk = MagicMock()
        sdk.get_position_balances_for_ids.side_effect = RuntimeError("rpc-down")
        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            adapter_cls.return_value.sdk = sdk

            position = _make_position(bin_ids=[1, 2])
            result = _traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )
        assert result.closed is False
        assert "rpc-down" in (result.error or "")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
