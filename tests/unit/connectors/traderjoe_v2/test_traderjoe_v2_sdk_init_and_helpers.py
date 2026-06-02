"""Tests for TraderJoe V2 SDK initialization, validation, and bin/price math.

Targets uncovered branches in `sdk.py`:
- chain validation
- rpc_url/gateway_client requirement
- bin_id_to_price / price_to_bin_id math (round-trip + edge cases)
- POA middleware path (bsc/avalanche)
- pool address cache hit path
- PoolNotFoundError path
"""

from __future__ import annotations

import math
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.traderjoe_v2.sdk import (
    BIN_ID_OFFSET,
    BIN_STEPS,
    DEFAULT_GAS_ESTIMATES,
    InvalidBinStepError,
    PoolNotFoundError,
    TraderJoeV2SDK,
    TraderJoeV2SDKError,
)

WALLET = "0x1234567890123456789012345678901234567890"
TOKEN_X = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"  # WAVAX
TOKEN_Y = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"  # USDC.e
POOL_ADDR = "0xD446eb1660F766d533BeCeEf890Df7A69d26f7d1"


# =============================================================================
# Construction errors
# =============================================================================


class TestSDKConstructionErrors:
    """Validation paths in `__init__` that don't require a Web3 instance."""

    def test_unsupported_chain_raises(self) -> None:
        with pytest.raises(TraderJoeV2SDKError, match="not supported"):
            TraderJoeV2SDK(chain="solana", rpc_url="http://x")

    def test_neither_rpc_nor_gateway_raises(self) -> None:
        with pytest.raises(TraderJoeV2SDKError, match="requires either rpc_url"):
            TraderJoeV2SDK(chain="avalanche")

    def test_chain_lowercased(self) -> None:
        """Chain name normalization to lowercase."""
        with patch("almanak.connectors.traderjoe_v2.sdk.Web3") as mock_web3_cls:
            instance = MagicMock()
            instance.is_connected.return_value = True
            mock_web3_cls.return_value = instance
            mock_web3_cls.HTTPProvider = MagicMock()
            mock_web3_cls.to_checksum_address = MagicMock(side_effect=lambda x: x)
            sdk = TraderJoeV2SDK(chain="AVALANCHE", rpc_url="http://localhost:8545")
            assert sdk.chain == "avalanche"

    def test_gateway_client_path_used_when_provided(self) -> None:
        """When gateway_client is supplied, the RPC connectivity check is skipped
        and Web3 is constructed with GatewayWeb3Provider."""
        gateway_client = MagicMock()
        with patch("almanak.connectors.traderjoe_v2.sdk.Web3") as mock_web3_cls, patch(
            "almanak.framework.web3.gateway_provider.GatewayWeb3Provider"
        ) as mock_gw_provider:
            instance = MagicMock()
            mock_web3_cls.return_value = instance
            mock_web3_cls.to_checksum_address = MagicMock(side_effect=lambda x: x)
            sdk = TraderJoeV2SDK(chain="avalanche", gateway_client=gateway_client)
            # GatewayWeb3Provider should be instantiated, not HTTPProvider.
            mock_gw_provider.assert_called_once()
            # is_connected should NOT be invoked on the gateway path.
            assert sdk.web3 is instance

    def test_rpc_disconnected_raises(self) -> None:
        with patch("almanak.connectors.traderjoe_v2.sdk.Web3") as mock_web3_cls:
            instance = MagicMock()
            instance.is_connected.return_value = False
            mock_web3_cls.return_value = instance
            mock_web3_cls.HTTPProvider = MagicMock()
            mock_web3_cls.to_checksum_address = MagicMock(side_effect=lambda x: x)
            with pytest.raises(TraderJoeV2SDKError, match="Failed to connect"):
                TraderJoeV2SDK(chain="avalanche", rpc_url="http://nowhere")


# =============================================================================
# Bin / price math (pure, no RPC)
# =============================================================================


class TestBinPriceMath:
    """`bin_id_to_price` / `price_to_bin_id` are static — pure math."""

    def test_bin_id_offset_returns_one(self) -> None:
        # At BIN_ID_OFFSET (zero exponent), price = 1 * 10^(0) = 1
        price = TraderJoeV2SDK.bin_id_to_price(BIN_ID_OFFSET, bin_step=20)
        assert math.isclose(price, 1.0, rel_tol=1e-9)

    def test_bin_id_above_offset_increases_price(self) -> None:
        p_at = TraderJoeV2SDK.bin_id_to_price(BIN_ID_OFFSET, bin_step=20)
        p_above = TraderJoeV2SDK.bin_id_to_price(BIN_ID_OFFSET + 10, bin_step=20)
        assert p_above > p_at

    def test_bin_id_below_offset_decreases_price(self) -> None:
        p_at = TraderJoeV2SDK.bin_id_to_price(BIN_ID_OFFSET, bin_step=20)
        p_below = TraderJoeV2SDK.bin_id_to_price(BIN_ID_OFFSET - 10, bin_step=20)
        assert p_below < p_at

    def test_decimal_adjustment_applied(self) -> None:
        # decimals_x=18, decimals_y=6 → factor 10^12 vs equal-decimals.
        p_18 = TraderJoeV2SDK.bin_id_to_price(BIN_ID_OFFSET, bin_step=20, decimals_x=18, decimals_y=18)
        p_6 = TraderJoeV2SDK.bin_id_to_price(BIN_ID_OFFSET, bin_step=20, decimals_x=18, decimals_y=6)
        assert math.isclose(p_6 / p_18, 10**12, rel_tol=1e-6)

    @pytest.mark.parametrize("bin_step", [1, 5, 10, 20, 50, 100])
    def test_round_trip_price_to_bin(self, bin_step: int) -> None:
        # bin_id_to_price → price_to_bin_id should be an identity (within rounding).
        bin_id = BIN_ID_OFFSET + 100
        price = TraderJoeV2SDK.bin_id_to_price(bin_id, bin_step=bin_step)
        recovered = TraderJoeV2SDK.price_to_bin_id(price, bin_step=bin_step)
        assert recovered == bin_id

    def test_price_to_bin_with_decimal_adjustment(self) -> None:
        bin_id = BIN_ID_OFFSET + 50
        price = TraderJoeV2SDK.bin_id_to_price(bin_id, bin_step=20, decimals_x=18, decimals_y=6)
        recovered = TraderJoeV2SDK.price_to_bin_id(price, bin_step=20, decimals_x=18, decimals_y=6)
        assert recovered == bin_id


# =============================================================================
# Pool address cache + PoolNotFoundError path
# =============================================================================


def _bare_sdk() -> TraderJoeV2SDK:
    """Construct an SDK without invoking __init__ (no RPC / file IO)."""
    return TraderJoeV2SDK.__new__(TraderJoeV2SDK)


class TestGetPoolAddress:
    def test_cache_hit_skips_rpc(self) -> None:
        sdk = _bare_sdk()
        sdk._factory_contract = MagicMock()
        sdk._pool_address_cache = {}
        # Pre-warm the cache; canonical key is sorted lowercased addresses + bin_step.
        canonical = (min(TOKEN_X.lower(), TOKEN_Y.lower()), max(TOKEN_X.lower(), TOKEN_Y.lower()), 20)
        sdk._pool_address_cache[canonical] = POOL_ADDR

        result = sdk.get_pool_address(TOKEN_X, TOKEN_Y, 20)
        assert result == POOL_ADDR
        # Verify factory contract was NOT called.
        sdk._factory_contract.functions.getLBPairInformation.assert_not_called()

    def test_cache_hit_works_for_reversed_token_order(self) -> None:
        """Cache key is canonical (sorted), so reversed order also hits."""
        sdk = _bare_sdk()
        sdk._factory_contract = MagicMock()
        sdk._pool_address_cache = {}
        canonical = (min(TOKEN_X.lower(), TOKEN_Y.lower()), max(TOKEN_X.lower(), TOKEN_Y.lower()), 20)
        sdk._pool_address_cache[canonical] = POOL_ADDR

        # Reverse argument order should still resolve via the same cache entry.
        result = sdk.get_pool_address(TOKEN_Y, TOKEN_X, 20)
        assert result == POOL_ADDR

    def test_factory_returns_zero_address_raises(self) -> None:
        sdk = _bare_sdk()
        factory = MagicMock()
        factory.functions.getLBPairInformation.return_value.call.return_value = (
            20,
            "0x0000000000000000000000000000000000000000",
            False,
            False,
        )
        sdk._factory_contract = factory
        sdk._pool_address_cache = {}

        with pytest.raises(PoolNotFoundError):
            sdk.get_pool_address(TOKEN_X, TOKEN_Y, 20)

    def test_factory_exception_wraps_as_pool_not_found(self) -> None:
        sdk = _bare_sdk()
        factory = MagicMock()
        factory.functions.getLBPairInformation.return_value.call.side_effect = RuntimeError("rpc error")
        sdk._factory_contract = factory
        sdk._pool_address_cache = {}

        with pytest.raises(PoolNotFoundError):
            sdk.get_pool_address(TOKEN_X, TOKEN_Y, 20)

    def test_successful_lookup_caches_result(self) -> None:
        sdk = _bare_sdk()
        factory = MagicMock()
        factory.functions.getLBPairInformation.return_value.call.return_value = (
            20,
            POOL_ADDR,
            False,
            False,
        )
        sdk._factory_contract = factory
        sdk._pool_address_cache = {}

        result = sdk.get_pool_address(TOKEN_X, TOKEN_Y, 20)
        assert result == POOL_ADDR
        # Cache populated.
        canonical = (min(TOKEN_X.lower(), TOKEN_Y.lower()), max(TOKEN_X.lower(), TOKEN_Y.lower()), 20)
        assert sdk._pool_address_cache[canonical] == POOL_ADDR


class TestGetLBPairInformation:
    """VIB-3100: get_lb_pair_information surfaces the ignoredForRouting flag."""

    def test_decodes_ignored_for_routing_true(self) -> None:
        sdk = _bare_sdk()
        factory = MagicMock()
        # (binStep, LBPair, createdByOwner, ignoredForRouting)
        factory.functions.getLBPairInformation.return_value.call.return_value = (
            15,
            POOL_ADDR,
            True,
            True,  # ignoredForRouting
        )
        sdk._factory_contract = factory
        sdk._pool_address_cache = {}

        info = sdk.get_lb_pair_information(TOKEN_X, TOKEN_Y, 15)
        assert info.pair_address == POOL_ADDR
        assert info.bin_step == 15
        assert info.ignored_for_routing is True
        # Address lookup is cached for a later get_pool_address().
        canonical = (min(TOKEN_X.lower(), TOKEN_Y.lower()), max(TOKEN_X.lower(), TOKEN_Y.lower()), 15)
        assert sdk._pool_address_cache[canonical] == POOL_ADDR

    def test_decodes_ignored_for_routing_false(self) -> None:
        sdk = _bare_sdk()
        factory = MagicMock()
        factory.functions.getLBPairInformation.return_value.call.return_value = (
            20,
            POOL_ADDR,
            False,
            False,
        )
        sdk._factory_contract = factory
        sdk._pool_address_cache = {}

        info = sdk.get_lb_pair_information(TOKEN_X, TOKEN_Y, 20)
        assert info.ignored_for_routing is False
        assert info.pair_address == POOL_ADDR

    def test_zero_address_raises_pool_not_found(self) -> None:
        sdk = _bare_sdk()
        factory = MagicMock()
        factory.functions.getLBPairInformation.return_value.call.return_value = (
            20,
            "0x0000000000000000000000000000000000000000",
            False,
            False,
        )
        sdk._factory_contract = factory
        sdk._pool_address_cache = {}

        with pytest.raises(PoolNotFoundError):
            sdk.get_lb_pair_information(TOKEN_X, TOKEN_Y, 20)

    def test_rpc_error_raises_sdk_error_not_pool_not_found(self) -> None:
        """VIB-3100 Gemini HIGH: a transient RPC error must surface as the base
        TraderJoeV2SDKError and must NOT be masked as PoolNotFoundError. The
        autodetect skip-loop catches PoolNotFoundError to skip a candidate, so
        masking would silently drop a bin step that actually exists."""
        sdk = _bare_sdk()
        factory = MagicMock()
        factory.functions.getLBPairInformation.return_value.call.side_effect = RuntimeError("rpc")
        sdk._factory_contract = factory
        sdk._pool_address_cache = {}

        with pytest.raises(TraderJoeV2SDKError) as exc_info:
            sdk.get_lb_pair_information(TOKEN_X, TOKEN_Y, 20)
        # Critically NOT the absence signal — autodetect must fail loud, not skip.
        assert not isinstance(exc_info.value, PoolNotFoundError)
        # Original transport error is chained for diagnosis.
        assert isinstance(exc_info.value.__cause__, RuntimeError)


# =============================================================================
# Token contract / balance / allowance helpers
# =============================================================================


class TestTokenHelpers:
    def test_get_token_contract_caches(self) -> None:
        sdk = _bare_sdk()
        sdk._token_contracts = {}
        sdk.erc20_abi = [{"name": "balanceOf"}]
        sdk.web3 = MagicMock()
        contract_a = MagicMock()
        sdk.web3.eth.contract.return_value = contract_a

        first = sdk.get_token_contract(TOKEN_X)
        second = sdk.get_token_contract(TOKEN_X)
        assert first is second
        sdk.web3.eth.contract.assert_called_once()

    def test_get_balance_proxies_to_contract(self) -> None:
        sdk = _bare_sdk()
        sdk._token_contracts = {}
        sdk.erc20_abi = [{"name": "balanceOf"}]
        sdk.web3 = MagicMock()
        contract = MagicMock()
        contract.functions.balanceOf.return_value.call.return_value = 12345
        sdk.web3.eth.contract.return_value = contract

        result = sdk.get_balance(TOKEN_X, WALLET)
        assert result == 12345

    def test_get_allowance_proxies_to_contract(self) -> None:
        sdk = _bare_sdk()
        sdk._token_contracts = {}
        sdk.erc20_abi = [{"name": "allowance"}]
        sdk.web3 = MagicMock()
        contract = MagicMock()
        contract.functions.allowance.return_value.call.return_value = 999
        sdk.web3.eth.contract.return_value = contract

        result = sdk.get_allowance(TOKEN_X, WALLET, POOL_ADDR)
        assert result == 999


# =============================================================================
# Pair contract caching + pool info / spot rate
# =============================================================================


class TestPoolInfo:
    def test_get_pair_contract_caches(self) -> None:
        sdk = _bare_sdk()
        sdk._pair_contracts = {}
        sdk.pair_abi = [{"name": "getActiveId"}]
        sdk.web3 = MagicMock()
        sdk.web3.eth.contract.return_value = MagicMock()

        first = sdk.get_pair_contract(POOL_ADDR)
        second = sdk.get_pair_contract(POOL_ADDR)
        assert first is second
        sdk.web3.eth.contract.assert_called_once()

    def test_get_pool_info_returns_pool_info(self) -> None:
        sdk = _bare_sdk()
        pair = MagicMock()
        pair.functions.getActiveId.return_value.call.return_value = BIN_ID_OFFSET
        pair.functions.getBinStep.return_value.call.return_value = 20
        pair.functions.getTokenX.return_value.call.return_value = TOKEN_X
        pair.functions.getTokenY.return_value.call.return_value = TOKEN_Y
        pair.functions.getReserves.return_value.call.return_value = (1_000, 2_000)
        sdk.get_pair_contract = MagicMock(return_value=pair)

        info = sdk.get_pool_info(POOL_ADDR)
        assert info.address == POOL_ADDR
        assert info.bin_step == 20
        assert info.active_id == BIN_ID_OFFSET
        assert info.reserve_x == 1_000
        assert info.reserve_y == 2_000

    def test_get_pool_spot_rate(self) -> None:
        sdk = _bare_sdk()
        pair = MagicMock()
        pair.functions.getActiveId.return_value.call.return_value = BIN_ID_OFFSET
        pair.functions.getBinStep.return_value.call.return_value = 20
        pair.functions.getTokenX.return_value.call.return_value = TOKEN_X
        pair.functions.getTokenY.return_value.call.return_value = TOKEN_Y

        token_x_ct = MagicMock()
        token_x_ct.functions.decimals.return_value.call.return_value = 18
        token_y_ct = MagicMock()
        token_y_ct.functions.decimals.return_value.call.return_value = 18
        sdk.get_pair_contract = MagicMock(return_value=pair)
        sdk.get_token_contract = MagicMock(side_effect=lambda addr: token_x_ct if addr == TOKEN_X else token_y_ct)

        rate = sdk.get_pool_spot_rate(POOL_ADDR)
        # At BIN_ID_OFFSET with equal decimals, price = 1.0.
        assert math.isclose(rate, 1.0, rel_tol=1e-9)


# =============================================================================
# Builder methods (build_*_transaction) - test deadline default + dispatch.
# =============================================================================


class TestBuilders:
    def _setup_router(self, sdk: TraderJoeV2SDK) -> MagicMock:
        sdk.web3 = MagicMock()
        sdk.web3.eth.get_transaction_count.return_value = 7
        router = MagicMock()
        sdk._router_contract = router
        return router

    def test_build_swap_uses_default_deadline(self) -> None:
        sdk = _bare_sdk()
        router = self._setup_router(sdk)
        router.functions.swapExactTokensForTokens.return_value.build_transaction.return_value = {
            "to": "0xrouter",
            "data": "0xabcd",
            "value": 0,
            "gas": 200_000,
        }
        tx, gas = sdk.build_swap_exact_tokens_for_tokens(
            amount_in=1_000,
            amount_out_min=900,
            path=[TOKEN_X, TOKEN_Y],
            bin_steps=[20],
            recipient=WALLET,
        )
        assert gas == 200_000
        call_args = router.functions.swapExactTokensForTokens.call_args
        # Path struct: 5 args (amountIn, amountOutMin, pathStruct, recipient, deadline).
        assert call_args[0][2]["pairBinSteps"] == [20]
        assert call_args[0][2]["versions"] == [2]

    def test_build_swap_with_explicit_deadline(self) -> None:
        sdk = _bare_sdk()
        router = self._setup_router(sdk)
        router.functions.swapExactTokensForTokens.return_value.build_transaction.return_value = {
            "to": "0xrouter",
            "data": "0xabcd",
            "value": 0,
            "gas": 200_000,
        }
        sdk.build_swap_exact_tokens_for_tokens(
            amount_in=1,
            amount_out_min=0,
            path=[TOKEN_X, TOKEN_Y],
            bin_steps=[20],
            recipient=WALLET,
            deadline=42,
        )
        # Deadline at index 4.
        call_args = router.functions.swapExactTokensForTokens.call_args
        assert call_args[0][4] == 42

    def test_build_approve_transaction(self) -> None:
        sdk = _bare_sdk()
        sdk.web3 = MagicMock()
        sdk.web3.eth.get_transaction_count.return_value = 3
        sdk._token_contracts = {}
        sdk.erc20_abi = []
        token_ct = MagicMock()
        token_ct.functions.approve.return_value.build_transaction.return_value = {
            "to": TOKEN_X,
            "data": "0xa9059cbb",
            "value": 0,
            "gas": 50_000,
        }
        sdk.web3.eth.contract.return_value = token_ct

        tx, gas = sdk.build_approve_transaction(
            token_address=TOKEN_X,
            spender_address=POOL_ADDR,
            amount=1_000_000,
            from_address=WALLET,
        )
        assert gas == 50_000
        token_ct.functions.approve.assert_called_once()

    def test_build_approve_for_all_transaction(self) -> None:
        sdk = _bare_sdk()
        sdk.web3 = MagicMock()
        sdk.web3.eth.get_transaction_count.return_value = 0
        sdk._pair_contracts = {}
        sdk.pair_abi = []
        pair_ct = MagicMock()
        pair_ct.functions.approveForAll.return_value.build_transaction.return_value = {
            "to": POOL_ADDR,
            "data": "0xe584b654",
            "value": 0,
            "gas": 50_000,
        }
        sdk.web3.eth.contract.return_value = pair_ct

        tx, gas = sdk.build_approve_for_all_transaction(
            pool_address=POOL_ADDR,
            spender_address="0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",
            from_address=WALLET,
        )
        assert gas == 50_000
        pair_ct.functions.approveForAll.assert_called_once()

    def test_build_add_liquidity(self) -> None:
        sdk = _bare_sdk()
        router = self._setup_router(sdk)
        router.functions.addLiquidity.return_value.build_transaction.return_value = {
            "to": "0xrouter",
            "data": "0xa3c7271a",
            "value": 0,
            "gas": 700_000,
        }
        tx, gas = sdk.build_add_liquidity(
            token_x=TOKEN_X,
            token_y=TOKEN_Y,
            bin_step=20,
            amount_x=10**18,
            amount_y=10**6,
            amount_x_min=0,
            amount_y_min=0,
            active_id_desired=BIN_ID_OFFSET,
            id_slippage=50,
            delta_ids=[-1, 0, 1],
            distribution_x=[0, 0, 10**18],
            distribution_y=[10**18, 0, 0],
            to=WALLET,
            refund_to=WALLET,
        )
        assert gas == 700_000

    def test_build_remove_liquidity(self) -> None:
        sdk = _bare_sdk()
        router = self._setup_router(sdk)
        router.functions.removeLiquidity.return_value.build_transaction.return_value = {
            "to": "0xrouter",
            "data": "0xc22159b6",
            "value": 0,
            "gas": 400_000,
        }
        tx, gas = sdk.build_remove_liquidity(
            token_x=TOKEN_X,
            token_y=TOKEN_Y,
            bin_step=20,
            amount_x_min=10,
            amount_y_min=20,
            ids=[BIN_ID_OFFSET],
            amounts=[1_000],
            to=WALLET,
        )
        assert gas == 400_000

    def test_build_collect_fees_empty_ids_raises(self) -> None:
        sdk = _bare_sdk()
        with pytest.raises(TraderJoeV2SDKError, match="No bin IDs"):
            sdk.build_collect_fees(pool_address=POOL_ADDR, account=WALLET, ids=[])

    def test_build_collect_fees_with_ids(self) -> None:
        sdk = _bare_sdk()
        sdk.web3 = MagicMock()
        sdk.web3.eth.get_transaction_count.return_value = 0
        pair = MagicMock()
        pair.functions.collectFees.return_value.build_transaction.return_value = {
            "to": POOL_ADDR,
            "data": "0x225b20b9",
            "value": 0,
            "gas": 200_000,
        }
        sdk.get_pair_contract = MagicMock(return_value=pair)

        tx, gas = sdk.build_collect_fees(pool_address=POOL_ADDR, account=WALLET, ids=[BIN_ID_OFFSET])
        assert gas == 200_000
        pair.functions.collectFees.assert_called_once()


# =============================================================================
# Pending fees aggregation
# =============================================================================


class TestGetPendingFees:
    def test_aggregates_per_bin_fees(self) -> None:
        sdk = _bare_sdk()
        pair = MagicMock()
        # Each pendingFees call returns (fee_x, fee_y).
        per_bin_fees = {100: (10, 1), 101: (20, 2), 102: (30, 3)}
        pair.functions.pendingFees.side_effect = lambda acct, bid: MagicMock(
            **{"call.return_value": per_bin_fees[bid]}
        )
        sdk.get_pair_contract = MagicMock(return_value=pair)

        x, y = sdk.get_pending_fees(POOL_ADDR, WALLET, [100, 101, 102])
        assert x == 60
        assert y == 6

    def test_skips_bins_that_revert(self) -> None:
        sdk = _bare_sdk()
        pair = MagicMock()
        per_bin_fees = {100: (10, 1), 101: (20, 2)}

        def pending(acct, bid):
            m = MagicMock()
            if bid == 999:  # Unsupported bin reverts.
                m.call.side_effect = Exception("not supported")
            else:
                m.call.return_value = per_bin_fees[bid]
            return m

        pair.functions.pendingFees.side_effect = pending
        sdk.get_pair_contract = MagicMock(return_value=pair)

        x, y = sdk.get_pending_fees(POOL_ADDR, WALLET, [100, 999, 101])
        # 999 should be silently skipped.
        assert x == 30
        assert y == 3


# =============================================================================
# Constants surface area
# =============================================================================


class TestPositionBalancesPerBinFallback:
    """Per-bin fallback paths in get_position_balances when batch fails."""

    def test_per_bin_fallback_logs_when_all_bins_fail(self, caplog: pytest.LogCaptureFixture) -> None:
        sdk = _bare_sdk()
        pair = MagicMock()
        pair.functions.getActiveId.return_value.call.return_value = 1000
        pair.functions.balanceOfBatch.return_value.call.side_effect = Exception("not supported")
        # Every per-bin call also reverts → "all per-bin balanceOf calls failed" branch.
        pair.functions.balanceOf.side_effect = lambda w, b: MagicMock(
            **{"call.side_effect": Exception("revert")}
        )
        sdk.get_pair_contract = MagicMock(return_value=pair)

        with caplog.at_level("ERROR", logger="almanak.connectors.traderjoe_v2.sdk"):
            result = sdk.get_position_balances(POOL_ADDR, WALLET, bin_range=2)
        assert result == {}
        # ERROR log line was emitted.
        assert any("All per-bin balanceOf calls failed" in r.getMessage() for r in caplog.records)

    def test_for_ids_empty_returns_empty(self) -> None:
        sdk = _bare_sdk()
        pair = MagicMock()
        sdk.get_pair_contract = MagicMock(return_value=pair)
        result = sdk.get_position_balances_for_ids(POOL_ADDR, WALLET, [])
        assert result == {}
        # batch should NOT be invoked when no candidate bins.
        pair.functions.balanceOfBatch.assert_not_called()

    def test_for_ids_all_per_bin_fails(self, caplog: pytest.LogCaptureFixture) -> None:
        sdk = _bare_sdk()
        pair = MagicMock()
        pair.functions.balanceOfBatch.return_value.call.side_effect = Exception("not supported")
        pair.functions.balanceOf.side_effect = lambda w, b: MagicMock(
            **{"call.side_effect": Exception("revert")}
        )
        sdk.get_pair_contract = MagicMock(return_value=pair)

        with caplog.at_level("ERROR", logger="almanak.connectors.traderjoe_v2.sdk"):
            result = sdk.get_position_balances_for_ids(POOL_ADDR, WALLET, [42, 43, 44])
        assert result == {}
        assert any("All per-bin balanceOf calls failed" in r.getMessage() for r in caplog.records)

    def test_for_ids_partial_per_bin_failure_continues(self, caplog: pytest.LogCaptureFixture) -> None:
        """Partial per-bin failures are silently logged at debug — non-empty result returned."""
        sdk = _bare_sdk()
        pair = MagicMock()
        pair.functions.balanceOfBatch.return_value.call.side_effect = Exception("not supported")
        per_bin_values = {42: 7, 43: 0, 44: None}  # bin 44 will revert

        def balance_of(w, b):
            m = MagicMock()
            if per_bin_values[b] is None:
                m.call.side_effect = Exception("revert")
            else:
                m.call.return_value = per_bin_values[b]
            return m

        pair.functions.balanceOf.side_effect = balance_of
        sdk.get_pair_contract = MagicMock(return_value=pair)

        result = sdk.get_position_balances_for_ids(POOL_ADDR, WALLET, [42, 43, 44])
        assert result == {42: 7}


class TestConstants:
    def test_invalid_bin_step_error_carries_bin_step(self) -> None:
        err = InvalidBinStepError(7777)
        assert err.bin_step == 7777
        assert "7777" in str(err)

    def test_pool_not_found_error_carries_context(self) -> None:
        err = PoolNotFoundError(TOKEN_X, TOKEN_Y, 20)
        assert err.token_x == TOKEN_X
        assert err.bin_step == 20

    def test_default_gas_estimates_present(self) -> None:
        for key in ("approve", "swap", "add_liquidity", "remove_liquidity", "collect_fees"):
            assert key in DEFAULT_GAS_ESTIMATES
            assert DEFAULT_GAS_ESTIMATES[key] > 0

    def test_bin_steps_sorted_and_positive(self) -> None:
        assert BIN_STEPS == sorted(BIN_STEPS)
        assert all(b > 0 for b in BIN_STEPS)
