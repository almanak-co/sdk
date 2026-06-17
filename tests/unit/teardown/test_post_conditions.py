"""Unit tests for teardown post-conditions (VIB-3742).

Verifies the on-chain closure verification hooks. The TJ V2 default is
exercised via a mocked SDK so we can assert behaviour for both the
"closed" and "residual liquidity" paths without a live fork.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.teardown_post_condition import (
    # Registration is framework-internal (manifest-driven via
    # CONNECTOR.teardown_post_condition); tests reach the private seam to
    # swap/restore hooks without building a whole connector manifest.
    _register_teardown_post_condition,
)
from almanak.connectors.traderjoe_v2.teardown_post_condition import (
    traderjoe_v2_post_condition,
)
from almanak.framework.teardown.post_conditions import (
    ClosureCheckResult,
    get_teardown_post_condition,
    has_teardown_post_condition,
)

WALLET = "0x1111111111111111111111111111111111111111"
POOL = "0x2222222222222222222222222222222222222222"
TOKEN_X = "0x3333333333333333333333333333333333333333"
TOKEN_Y = "0x4444444444444444444444444444444444444444"


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
                _register_teardown_post_condition("traderjoe_v2", replacement)
            assert any("Replacing existing teardown post-condition" in r.message for r in caplog.records)
        finally:
            # Restore the default so other tests aren't affected.
            if original is not None:
                _register_teardown_post_condition("traderjoe_v2", original)

    def test_v3_default_does_not_clobber_connector_owned_hook(self) -> None:
        """The V3 NPM default registration must not overwrite a connector that
        already owns its teardown post-condition (manifest-published). Connector
        hooks win; the framework default is a fallback only.
        """
        from almanak.framework.teardown import post_conditions as pc

        v3_slugs = sorted(pc._V3_NPM_PROTOCOLS)
        if not v3_slugs:
            pytest.skip("no V3 NPM protocols registered in this build")
        slug = v3_slugs[0]
        original = get_teardown_post_condition(slug)
        sentinel = lambda **_: ClosureCheckResult(closed=True)  # noqa: E731
        try:
            _register_teardown_post_condition(slug, sentinel)
            pc._register_default_v3_post_conditions()
            # Re-running default registration must leave the connector-owned hook
            # in place rather than swapping in the generic V3 default.
            assert get_teardown_post_condition(slug) is sentinel
        finally:
            if original is not None:
                _register_teardown_post_condition(slug, original)


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
            result = traderjoe_v2_post_condition(
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
            result = traderjoe_v2_post_condition(
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
            result = traderjoe_v2_post_condition(
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
            result = traderjoe_v2_post_condition(
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
        result = traderjoe_v2_post_condition(position=position, wallet_address=WALLET)
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
        result = traderjoe_v2_post_condition(position=position, wallet_address=WALLET)
        assert result.closed is False
        assert "42-char hex address" in (result.error or "")
        assert "WAVAX/USDC/20" in (result.error or "")

    # -- ALM-2807 L3: derive the LBPair from a token_x/token_y/bin_step
    #    descriptor when no explicit pool_address is supplied. ----------------
    @staticmethod
    def _patch_adapter_for_derive(adapter_cls: MagicMock, sdk: MagicMock) -> None:
        """Wire a mocked adapter so token symbols resolve and sdk is swapped in."""
        adapter = adapter_cls.return_value
        adapter.sdk = sdk
        adapter.resolve_token_address.side_effect = lambda sym: {
            "WAVAX": TOKEN_X,
            "USDC": TOKEN_Y,
        }[sym]

    def test_derives_pool_from_token_descriptor_strong_mode(self) -> None:
        """A position reporting only token_x/token_y/bin_step (+ bin_ids) and NO
        pool_address must derive the LBPair on-chain and verify per-bin, not
        fail closed. This is the demo_traderjoe_crisis_lp / pnl_lp shape.
        """
        sdk = MagicMock()
        sdk.get_pool_address.return_value = POOL
        sdk.get_position_balances_for_ids.return_value = {}

        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            self._patch_adapter_for_derive(adapter_cls, sdk)
            position = SimpleNamespace(
                protocol="traderjoe_v2",
                position_id="tj-derive",
                chain="avalanche",
                details={"token_x": "WAVAX", "token_y": "USDC", "bin_step": 20, "bin_ids": [100, 101]},
            )
            result = traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is True
        assert result.residual == {}
        sdk.get_pool_address.assert_called_once_with(TOKEN_X, TOKEN_Y, 20)
        sdk.get_position_balances_for_ids.assert_called_once_with(POOL, WALLET, [100, 101])

    def test_derives_pool_then_detects_residual(self) -> None:
        """Derivation path still catches real residual liquidity (closed=False)."""
        sdk = MagicMock()
        sdk.get_pool_address.return_value = POOL
        sdk.get_position_balances_for_ids.return_value = {100: 42}

        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            self._patch_adapter_for_derive(adapter_cls, sdk)
            position = SimpleNamespace(
                protocol="traderjoe_v2",
                position_id="tj-derive-residual",
                chain="avalanche",
                details={"token_x": "WAVAX", "token_y": "USDC", "bin_step": 20, "bin_ids": [100]},
            )
            result = traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is False
        assert result.residual["bin_balances"] == {100: 42}
        assert result.residual["pool_address"] == POOL

    def test_derives_pool_fallback_scan_when_no_bin_ids(self) -> None:
        """Derive the pool from tokens, then fall back to the active-id +/-50
        scan (weak mode) when bin_ids are absent — still verifies, never
        fail-closes on a token-only descriptor.
        """
        sdk = MagicMock()
        sdk.get_pool_address.return_value = POOL
        sdk.get_position_balances.return_value = {}

        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            self._patch_adapter_for_derive(adapter_cls, sdk)
            position = SimpleNamespace(
                protocol="traderjoe_v2",
                position_id="tj-derive-fallback",
                chain="avalanche",
                details={"token_x": "WAVAX", "token_y": "USDC", "bin_step": 20},
            )
            result = traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is True
        assert "fallback_scan" in result.residual
        sdk.get_pool_address.assert_called_once_with(TOKEN_X, TOKEN_Y, 20)
        sdk.get_position_balances.assert_called_once_with(POOL, WALLET)

    def test_derive_failure_fails_closed(self) -> None:
        """An unresolvable token pair must fail closed (never report closed)."""
        sdk = MagicMock()
        sdk.get_pool_address.side_effect = RuntimeError("pool not found")

        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            self._patch_adapter_for_derive(adapter_cls, sdk)
            position = SimpleNamespace(
                protocol="traderjoe_v2",
                position_id="tj-derive-fail",
                chain="avalanche",
                details={"token_x": "WAVAX", "token_y": "USDC", "bin_step": 20},
            )
            result = traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )

        assert result.closed is False
        assert "could not derive" in (result.error or "")

    def test_sdk_init_failure_returns_error(self) -> None:
        with patch(
            "almanak.connectors.traderjoe_v2.TraderJoeV2Adapter",
            side_effect=RuntimeError("boom"),
        ):
            position = _make_position()
            result = traderjoe_v2_post_condition(
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
            result = traderjoe_v2_post_condition(
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
            result = traderjoe_v2_post_condition(
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
            result = traderjoe_v2_post_condition(
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
            result = traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )
        assert result.closed is False
        assert "rpc-down" in (result.error or "")

    def test_missing_chain_fails_closed(self) -> None:
        """A position without a chain must fail closed, not default to
        "avalanche". The LB pair address is chain-scoped, so guessing would
        verify against the wrong network. Mirrors the Uniswap V3 sibling.
        """
        with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
            position = SimpleNamespace(
                protocol="traderjoe_v2",
                position_id="tj-no-chain",
                chain=None,
                details={"pool_address": POOL},
            )
            result = traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                rpc_url="http://localhost:8545",
            )
        assert result.closed is False
        assert "position.chain" in (result.error or "")
        # The chain check fires before SDK construction.
        adapter_cls.assert_not_called()

    def test_hosted_mode_without_gateway_client_fails_closed(self) -> None:
        """Gateway-boundary: in hosted mode a missing gateway_client must fail
        closed rather than silently fall back to direct rpc_url egress.
        """
        with (
            patch("almanak.framework.deployment.is_hosted", return_value=True),
            patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls,
        ):
            position = _make_position(bin_ids=[1, 2])
            result = traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                gateway_client=None,
                rpc_url="http://localhost:8545",
            )
        assert result.closed is False
        assert "hosted" in (result.error or "")
        # No direct-RPC SDK is built when the gateway path is unavailable in hosted.
        adapter_cls.assert_not_called()

    def test_local_mode_without_gateway_client_uses_rpc(self) -> None:
        """Local/test mode keeps the rpc_url dual path: a missing gateway_client
        is allowed and the SDK is driven over the supplied rpc_url.
        """
        sdk = MagicMock()
        sdk.get_position_balances_for_ids.return_value = {}
        with (
            patch("almanak.framework.deployment.is_hosted", return_value=False),
            patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls,
        ):
            adapter_cls.return_value.sdk = sdk
            position = _make_position(bin_ids=[1, 2])
            result = traderjoe_v2_post_condition(
                position=position,
                wallet_address=WALLET,
                gateway_client=None,
                rpc_url="http://localhost:8545",
            )
        assert result.closed is True
        adapter_cls.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
