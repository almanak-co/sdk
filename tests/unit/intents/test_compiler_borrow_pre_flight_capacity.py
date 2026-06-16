"""Regression guards for the BORROW capacity pre-flight (defense in depth).

The Aave V3 and BENQI (Compound V2 fork) borrow compilers add a compile-time
borrow-capacity check that mirrors the VIB-3825 ``borrowingEnabled`` blueprint:

1. Aave V3 — ``Pool.getUserAccountData(wallet)`` returns ``availableBorrowsBase``
   (8-decimal USD); ``IAaveOracle.getAssetPrice(asset)`` returns the asset's
   USD price (8-decimal). Capacity in underlying = base_capacity / asset_price.
2. BENQI (Compound V2 fork) — ``Comptroller.getAccountLiquidity(wallet)`` returns
   ``(error, liquidity, shortfall)`` (18-decimal USD); ``oracle().getUnderlyingPrice(qiToken)``
   returns the underlying price scaled by ``1e(36 - decimals)`` (Compound V2
   convention). Capacity = liquidity / underlying_price (in wei of underlying).

When the requested borrow exceeds capacity (after a 1% safety margin), the
compiler raises :class:`LendingBorrowExceedsCapacityError`. The ERROR_PREFIX
is added to ``IntentStateMachine._categorize_error`` so retrying with the same
amount never enters the retry-storm.

Mainnet protocol enforcement is presumed correct — this pre-flight is defense
in depth (see GitHub issue tracking the empirical mainnet probe).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.lending.aave_helpers import (
    _check_lending_borrow_capacity_aave_v3,
    _check_lending_borrow_capacity_benqi,
    _gateway_eth_call_raw,
)
from almanak.framework.intents.intent_errors import LendingBorrowExceedsCapacityError
from almanak.framework.intents.state_machine import IntentStateMachine

# Real addresses are arbitrary for the mocked plumbing; just keep them
# checksum-shaped so .lower() / padding behave normally.
_USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
_QI_USDC_BENQI = "0xB715808a78F6041E46d61Cb123C9B4A27056AE9C"
_WALLET = "0x" + "1" * 40


def _make_compiler(chain: str = "arbitrum") -> MagicMock:
    """Minimal compiler stub for the borrow-capacity check."""
    compiler = MagicMock()
    compiler.chain = chain
    compiler.rpc_timeout = 5.0
    compiler.wallet_address = _WALLET
    compiler._gateway_client = MagicMock()
    compiler._gateway_client.is_connected = True
    # Return a real dict so the helper's isinstance(cache, dict) check succeeds.
    compiler._lending_borrow_capacity_cache = {}
    return compiler


# ─── _gateway_eth_call_raw direct coverage ────────────────────────────────────


class TestGatewayEthCallRaw:
    """Direct coverage of ``_gateway_eth_call_raw`` so its defensive
    branches (no gateway, exception, unsuccessful response, malformed
    payload) are exercised. The capacity-helper tests above patch this
    function entirely, so without these direct tests the function would
    show ~19% coverage and trip the CRAP gate (CRAP = 11 + 11² × (1-cov)³)."""

    def test_returns_none_when_no_gateway_client(self) -> None:
        compiler = MagicMock()
        compiler.chain = "arbitrum"
        compiler.rpc_timeout = 5.0
        compiler._gateway_client = None
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") is None

    def test_returns_none_when_gateway_disconnected(self) -> None:
        compiler = MagicMock()
        compiler.chain = "arbitrum"
        compiler.rpc_timeout = 5.0
        compiler._gateway_client = MagicMock()
        compiler._gateway_client.is_connected = False
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") is None

    def test_returns_none_on_rpc_exception(self) -> None:
        compiler = _make_compiler()
        compiler._gateway_client.rpc.Call.side_effect = RuntimeError("boom")
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") is None

    def test_returns_none_when_response_unsuccessful(self) -> None:
        compiler = _make_compiler()
        response = MagicMock()
        response.success = False
        response.result = ""
        compiler._gateway_client.rpc.Call.return_value = response
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") is None

    def test_returns_none_when_response_result_empty(self) -> None:
        compiler = _make_compiler()
        response = MagicMock()
        response.success = True
        response.result = ""
        compiler._gateway_client.rpc.Call.return_value = response
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") is None

    def test_returns_none_when_result_is_zero_x(self) -> None:
        compiler = _make_compiler()
        response = MagicMock()
        response.success = True
        response.result = "0x"
        compiler._gateway_client.rpc.Call.return_value = response
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") is None

    def test_returns_none_on_malformed_json_quoted_result(self) -> None:
        """A result that starts with ``"`` is JSON-decoded; a malformed
        quoted string returns None rather than raising."""
        compiler = _make_compiler()
        response = MagicMock()
        response.success = True
        response.result = '"invalid'  # opening quote, no close
        compiler._gateway_client.rpc.Call.return_value = response
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") is None

    def test_returns_decoded_hex_for_plain_result(self) -> None:
        compiler = _make_compiler()
        response = MagicMock()
        response.success = True
        response.result = "0xabcd"
        compiler._gateway_client.rpc.Call.return_value = response
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") == "0xabcd"

    def test_decodes_json_quoted_hex_result(self) -> None:
        """Some gateway transports return the result as a JSON string;
        decode the quoting so callers get the bare hex payload."""
        compiler = _make_compiler()
        response = MagicMock()
        response.success = True
        response.result = '"0xdeadbeef"'
        compiler._gateway_client.rpc.Call.return_value = response
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") == "0xdeadbeef"

    def test_quoted_string_payload_round_trips(self) -> None:
        """A JSON-quoted string payload is decoded and returned as the
        unquoted hex; a non-hex quoted string is also returned (the helper
        leaves hex-validation to its callers)."""
        compiler = _make_compiler()
        response = MagicMock()
        response.success = True
        response.result = '"0xfeed"'
        compiler._gateway_client.rpc.Call.return_value = response
        assert _gateway_eth_call_raw(compiler, "0xtoken", "0xdata", "test") == "0xfeed"


# ─── Aave V3 capacity pre-flight ──────────────────────────────────────────────


class TestAaveV3BorrowCapacityCheck:
    """The Aave V3 capacity check is the user-facing analog of VIB-3825 — a
    typed compile-time gate on a condition the on-chain protocol enforces but
    expensively (gas + retry iteration)."""

    # Aave V3 arbitrum pool + oracle addresses from
    # ``almanak/core/contracts.py:AAVE_V3["arbitrum"]``. Hardcoded here so a
    # regression that points the pre-flight at a different (or stale)
    # contract fails inside the fake — without this guard the selector-only
    # branch would let miswired calls slip through green tests.
    _AAVE_V3_ARB_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD".lower()
    _AAVE_V3_ARB_ORACLE = "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7".lower()

    def _patch_eth_call(self, available_borrows_base: int, asset_price_base: int):
        """Patch the gateway eth_call helper to return ABI-encoded uints.

        Aave V3 ``getUserAccountData`` returns 6 words; we only set word 2
        (availableBorrowsBase). Oracle ``getAssetPrice`` returns 1 word.

        The fake also asserts ``to`` matches the expected contract for each
        selector — selector-only matching would let a miswired call (right
        ABI, wrong address) silently pass the test.
        """
        expected_pool = self._AAVE_V3_ARB_POOL
        expected_oracle = self._AAVE_V3_ARB_ORACLE

        def fake_call(_compiler, to, data, _label):
            to_lower = (to or "").lower()
            # availableBorrowsBase lives at word 2 of getUserAccountData; pad
            # to 6 words.
            if data.startswith("0xbf92857c"):  # getUserAccountData(address)
                assert to_lower == expected_pool, (
                    f"getUserAccountData must target the Aave V3 pool "
                    f"({expected_pool}); got {to_lower}"
                )
                words = ["0" * 64] * 6
                words[2] = format(available_borrows_base, "064x")
                return "0x" + "".join(words)
            if data.startswith("0xb3596f07"):  # getAssetPrice(address)
                assert to_lower == expected_oracle, (
                    f"getAssetPrice must target the Aave V3 oracle "
                    f"({expected_oracle}); got {to_lower}"
                )
                return "0x" + format(asset_price_base, "064x")
            return None

        return patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._gateway_eth_call_raw",
            side_effect=fake_call,
        )

    def test_borrow_within_capacity_returns_none(self) -> None:
        """Available USD = $1000; USDC oracle = $1.00 → ~990 USDC after 1% margin.
        Requesting 500 USDC → cleanly under cap, no reason."""
        compiler = _make_compiler()
        # availableBorrowsBase = 1000 * 1e8 = 100_000_000_000
        # asset_price_base = 1.00 * 1e8 = 100_000_000
        with self._patch_eth_call(
            available_borrows_base=100_000_000_000, asset_price_base=100_000_000
        ):
            reason, available = _check_lending_borrow_capacity_aave_v3(
                compiler,
                _WALLET,
                _USDC_ARBITRUM,
                "USDC",
                Decimal("500"),
                6,
            )
        assert reason is None
        assert available is not None
        # 1000 / 1.00 * (1 - 0.01) = 990
        assert Decimal("989") < available < Decimal("991")

    def test_borrow_exceeds_capacity_returns_reason(self) -> None:
        """Available USD = $1000; requesting 5000 USDC → exceeds cap, reason set."""
        compiler = _make_compiler()
        with self._patch_eth_call(
            available_borrows_base=100_000_000_000, asset_price_base=100_000_000
        ):
            reason, available = _check_lending_borrow_capacity_aave_v3(
                compiler,
                _WALLET,
                _USDC_ARBITRUM,
                "USDC",
                Decimal("5000"),
                6,
            )
        assert reason is not None
        assert "USDC" in reason
        assert "aave_v3" in reason
        assert "5000" in reason
        assert "exceeds wallet capacity" in reason
        assert available is not None

    def test_borrow_exactly_at_capacity_after_margin_passes(self) -> None:
        """A request matching the post-margin cap exactly should pass (≤, not <)."""
        compiler = _make_compiler()
        # available_in_underlying = 1000.0; cap = 1000 * 0.99 = 990.0
        with self._patch_eth_call(
            available_borrows_base=100_000_000_000, asset_price_base=100_000_000
        ):
            reason, available = _check_lending_borrow_capacity_aave_v3(
                compiler,
                _WALLET,
                _USDC_ARBITRUM,
                "USDC",
                Decimal("990"),
                6,
            )
        # Exactly at cap should not raise.
        assert reason is None
        assert available is not None

    def test_unknown_chain_skips(self) -> None:
        compiler = _make_compiler(chain="solana")
        with patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._gateway_eth_call_raw"
        ) as mock_call:
            reason, available = _check_lending_borrow_capacity_aave_v3(
                compiler,
                _WALLET,
                _USDC_ARBITRUM,
                "USDC",
                Decimal("100"),
                6,
            )
        assert reason is None
        assert available is None
        mock_call.assert_not_called()

    def test_rpc_failure_fails_open(self) -> None:
        compiler = _make_compiler()
        with patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._gateway_eth_call_raw",
            return_value=None,
        ):
            reason, available = _check_lending_borrow_capacity_aave_v3(
                compiler,
                _WALLET,
                _USDC_ARBITRUM,
                "USDC",
                Decimal("100000"),
                6,
            )
        # Fail-open: no reason, no cap.
        assert reason is None
        assert available is None

    def test_zero_oracle_price_fails_open(self) -> None:
        """A zero oracle price would otherwise divide-by-zero — fail-open."""
        compiler = _make_compiler()
        with self._patch_eth_call(
            available_borrows_base=100_000_000_000, asset_price_base=0
        ):
            reason, available = _check_lending_borrow_capacity_aave_v3(
                compiler,
                _WALLET,
                _USDC_ARBITRUM,
                "USDC",
                Decimal("100"),
                6,
            )
        assert reason is None
        assert available is None


# ─── Capacity cache: hit + key-isolation ──────────────────────────────────────


class TestBorrowCapacityCacheBehavior:
    """Cover ``_lending_borrow_capacity_cache`` hit semantics and key
    isolation. A bad cache key or stale reuse across
    ``(chain, protocol, wallet, asset)`` would silently miscompile later
    borrows; cold-read tests alone cannot catch that.
    """

    # Same Aave V3 contract addresses as TestAaveV3BorrowCapacityCheck — kept
    # in sync so the cache-behavior tests catch routing regressions too.
    _AAVE_V3_ARB_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD".lower()
    _AAVE_V3_ARB_ORACLE = "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7".lower()

    def _patch_eth_call_aave(self, available_borrows_base: int, asset_price_base: int):
        """Same fake_call as TestAaveV3BorrowCapacityCheck; duplicated here
        to keep the cache-behavior tests self-contained. Asserts ``to``
        matches the expected contract for each selector."""
        expected_pool = self._AAVE_V3_ARB_POOL
        expected_oracle = self._AAVE_V3_ARB_ORACLE

        def fake_call(_compiler, to, data, _label):
            to_lower = (to or "").lower()
            if data.startswith("0xbf92857c"):  # getUserAccountData(address)
                assert to_lower == expected_pool, (
                    f"getUserAccountData must target the Aave V3 pool "
                    f"({expected_pool}); got {to_lower}"
                )
                words = ["0" * 64] * 6
                words[2] = format(available_borrows_base, "064x")
                return "0x" + "".join(words)
            if data.startswith("0xb3596f07"):  # getAssetPrice(address)
                assert to_lower == expected_oracle, (
                    f"getAssetPrice must target the Aave V3 oracle "
                    f"({expected_oracle}); got {to_lower}"
                )
                return "0x" + format(asset_price_base, "064x")
            return None

        return patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._gateway_eth_call_raw",
            side_effect=fake_call,
        )

    # Aave V3 cold-read cost: 2 eth_calls (getUserAccountData + getAssetPrice).
    # Pin exact deltas so a regression that adds a redundant RPC fails here.
    _AAVE_COLD_READ_CALLS = 2

    def test_cache_hit_skips_rpc_when_request_fits_prior_capacity(self) -> None:
        """Two calls with the same (chain, protocol, wallet, asset) and a
        request that fits within the prior available capacity must hit the
        cache and skip the RPC roundtrip on the second call.

        Pin exact deltas: cold read = exactly 2 calls; cache hit = +0.
        """
        compiler = _make_compiler()
        # available_in_underlying = 1000 / 1.00 * (1 - 0.01) = 990 USDC
        with self._patch_eth_call_aave(
            available_borrows_base=100_000_000_000, asset_price_base=100_000_000
        ) as mocked_call:
            # First call: cold read.
            _check_lending_borrow_capacity_aave_v3(
                compiler, _WALLET, _USDC_ARBITRUM, "USDC", Decimal("100"), 6,
            )
            first_call_count = mocked_call.call_count
            assert first_call_count == self._AAVE_COLD_READ_CALLS, (
                f"Cold read must invoke the gateway eth_call helper exactly "
                f"{self._AAVE_COLD_READ_CALLS} times "
                f"(getUserAccountData + getAssetPrice); got {first_call_count}"
            )

            # Second call with a request that fits within the prior cap (100
            # < 990): MUST be served from cache, NO new RPC.
            reason, available = _check_lending_borrow_capacity_aave_v3(
                compiler, _WALLET, _USDC_ARBITRUM, "USDC", Decimal("100"), 6,
            )
            assert reason is None
            assert available is not None
            assert mocked_call.call_count == first_call_count, (
                "Second call with the same key + smaller request must hit "
                "the cache and NOT invoke the gateway again. Got an extra "
                f"{mocked_call.call_count - first_call_count} call(s)."
            )

    def test_cache_isolation_by_asset(self) -> None:
        """Cache key must include the borrow asset — a second call for a
        different asset under the same wallet/chain MUST trigger a fresh
        RPC, not return the prior asset's verdict.

        Pin exact deltas: each unique key = exactly 2 calls.
        """
        compiler = _make_compiler()
        with self._patch_eth_call_aave(
            available_borrows_base=100_000_000_000, asset_price_base=100_000_000
        ) as mocked_call:
            # Call A: USDC.
            _check_lending_borrow_capacity_aave_v3(
                compiler, _WALLET, _USDC_ARBITRUM, "USDC", Decimal("100"), 6,
            )
            after_a = mocked_call.call_count
            assert after_a == self._AAVE_COLD_READ_CALLS

            # Call B: a different asset (DAI-shaped fake address).
            _DAI = "0xDA1000000000000000000000000000000000DA1A"
            _check_lending_borrow_capacity_aave_v3(
                compiler, _WALLET, _DAI, "DAI", Decimal("100"), 18,
            )
            after_b = mocked_call.call_count
            assert after_b - after_a == self._AAVE_COLD_READ_CALLS, (
                f"Different asset key MUST force a fresh RPC of exactly "
                f"{self._AAVE_COLD_READ_CALLS} calls; got {after_b - after_a}."
            )

    def test_cache_isolation_by_wallet(self) -> None:
        """Cache key must include wallet — a second call from a different
        wallet MUST trigger a fresh RPC.

        Pin exact deltas: each unique wallet = exactly 2 calls.
        """
        compiler = _make_compiler()
        with self._patch_eth_call_aave(
            available_borrows_base=100_000_000_000, asset_price_base=100_000_000
        ) as mocked_call:
            # Wallet A.
            _check_lending_borrow_capacity_aave_v3(
                compiler, _WALLET, _USDC_ARBITRUM, "USDC", Decimal("100"), 6,
            )
            after_a = mocked_call.call_count
            assert after_a == self._AAVE_COLD_READ_CALLS

            # Wallet B (same asset, same chain, different wallet).
            _OTHER_WALLET = "0x" + "2" * 40
            _check_lending_borrow_capacity_aave_v3(
                compiler, _OTHER_WALLET, _USDC_ARBITRUM, "USDC", Decimal("100"), 6,
            )
            after_b = mocked_call.call_count
            assert after_b - after_a == self._AAVE_COLD_READ_CALLS, (
                f"Different wallet key MUST force a fresh RPC of exactly "
                f"{self._AAVE_COLD_READ_CALLS} calls; got {after_b - after_a}."
            )

    def test_cache_recomputes_when_request_exceeds_prior_capacity(self) -> None:
        """A tighter request that exceeds the previously-cached available
        capacity must recompute (the previous "OK at 100 USDC" verdict
        cannot rule on a new "OK at 5000 USDC" request).

        Pin exact deltas: recompute = exactly 2 calls.
        """
        compiler = _make_compiler()
        with self._patch_eth_call_aave(
            available_borrows_base=100_000_000_000, asset_price_base=100_000_000
        ) as mocked_call:
            # First: 100 USDC (well under 990 cap).
            _check_lending_borrow_capacity_aave_v3(
                compiler, _WALLET, _USDC_ARBITRUM, "USDC", Decimal("100"), 6,
            )
            after_first = mocked_call.call_count
            assert after_first == self._AAVE_COLD_READ_CALLS

            # Second: 5000 USDC (exceeds the 990 cap stored in the cache).
            # Must recompute, not blindly return "OK".
            _check_lending_borrow_capacity_aave_v3(
                compiler, _WALLET, _USDC_ARBITRUM, "USDC", Decimal("5000"), 6,
            )
            after_second = mocked_call.call_count
            assert after_second - after_first == self._AAVE_COLD_READ_CALLS, (
                f"Request exceeding cached capacity MUST recompute with "
                f"exactly {self._AAVE_COLD_READ_CALLS} fresh calls; got "
                f"{after_second - after_first}."
            )


# ─── BENQI / Compound V2 capacity pre-flight ──────────────────────────────────


class TestBenqiBorrowCapacityCheck:
    """BENQI's Comptroller is a Compound V2 fork — these tests cover the
    Compound V2 borrow-capacity convention (18-decimal liquidity, oracle
    price scaled by 1e(36 - underlying_decimals))."""

    # BENQI Comptroller address from
    # ``almanak/connectors/benqi/adapter.py:BENQI_COMPTROLLER_ADDRESS``.
    _BENQI_COMPTROLLER = "0x486Af39519B4Dc9a7fCcd318217352830E8AD9b4".lower()

    def _patch_eth_call(
        self,
        liquidity_18: int,
        underlying_price: int,
        *,
        err_code: int = 0,
        shortfall_18: int = 0,
        oracle_address: int = 0xABCD,
    ):
        """Patch the gateway eth_call helper for the 3 BENQI reads.

        Asserts ``to`` matches the expected contract for each selector:
        - ``getAccountLiquidity`` and ``oracle()`` go to the Comptroller.
        - ``getUnderlyingPrice`` goes to the resolved oracle address.

        Selector-only matching would let a miswired call slip past green
        tests; pinning ``to`` catches a routing regression at the unit
        layer.
        """
        comptroller = self._BENQI_COMPTROLLER
        expected_oracle_address = "0x" + format(oracle_address, "040x").lower()

        def fake_call(_compiler, to, data, _label):
            to_lower = (to or "").lower()
            if data.startswith("0x5ec88c79"):  # getAccountLiquidity(address)
                assert to_lower == comptroller, (
                    f"getAccountLiquidity must target the BENQI Comptroller "
                    f"({comptroller}); got {to_lower}"
                )
                # Returns (error, liquidity, shortfall) — three 32-byte words.
                return (
                    "0x"
                    + format(err_code, "064x")
                    + format(liquidity_18, "064x")
                    + format(shortfall_18, "064x")
                )
            if data == "0x7dc0d1d0":  # oracle()
                assert to_lower == comptroller, (
                    f"oracle() must target the BENQI Comptroller "
                    f"({comptroller}); got {to_lower}"
                )
                return "0x" + format(oracle_address, "064x")
            if data.startswith("0xfc57d4df"):  # getUnderlyingPrice(address)
                assert to_lower == expected_oracle_address, (
                    f"getUnderlyingPrice must target the resolved oracle "
                    f"({expected_oracle_address}); got {to_lower}"
                )
                return "0x" + format(underlying_price, "064x")
            return None

        return patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._gateway_eth_call_raw",
            side_effect=fake_call,
        )

    def test_borrow_within_capacity_returns_none(self) -> None:
        """Liquidity = $1000 (18-dec USD); USDC underlying price scales to $1
        for 6-decimal USDC → cap ≈ 1000 USDC * 0.99 = 990 USDC.
        Requesting 500 USDC → cleanly under."""
        compiler = _make_compiler(chain="avalanche")
        # liquidity = 1000 * 1e18
        # USDC has 6 decimals; Compound V2 price = USD_per_unit * 1e(36 - 6) = 1e30
        with self._patch_eth_call(
            liquidity_18=1000 * 10**18, underlying_price=10**30
        ):
            reason, available = _check_lending_borrow_capacity_benqi(
                compiler,
                _WALLET,
                _QI_USDC_BENQI,
                "USDC",
                Decimal("500"),
                6,
            )
        assert reason is None
        assert available is not None
        # 1000 USDC * 0.99 = 990 USDC
        assert Decimal("989") < available < Decimal("991")

    def test_borrow_exceeds_capacity_returns_reason(self) -> None:
        """1 AVAX collateral, $100k USDC borrow request — the negative case
        the deleted xfail used to cover."""
        compiler = _make_compiler(chain="avalanche")
        # liquidity = $20 * 1e18 (1 AVAX collateral, ~50% LF, ~$40 AVAX → $20)
        with self._patch_eth_call(
            liquidity_18=20 * 10**18, underlying_price=10**30
        ):
            reason, available = _check_lending_borrow_capacity_benqi(
                compiler,
                _WALLET,
                _QI_USDC_BENQI,
                "USDC",
                Decimal("100000"),
                6,
            )
        assert reason is not None
        assert "USDC" in reason
        assert "benqi" in reason
        assert "100000" in reason
        assert "exceeds wallet capacity" in reason
        assert available is not None
        # Cap is ~ $20 * 0.99 = ~19.8 USDC
        assert available < Decimal("20")

    def test_borrow_exactly_at_capacity_after_margin_passes(self) -> None:
        """A request matching the post-margin cap exactly should pass (≤, not <)."""
        compiler = _make_compiler(chain="avalanche")
        # liquidity = $1000; cap = 1000 * 0.99 = 990 USDC
        with self._patch_eth_call(
            liquidity_18=1000 * 10**18, underlying_price=10**30
        ):
            reason, available = _check_lending_borrow_capacity_benqi(
                compiler,
                _WALLET,
                _QI_USDC_BENQI,
                "USDC",
                Decimal("990"),
                6,
            )
        assert reason is None
        assert available is not None

    def test_already_underwater_returns_reason(self) -> None:
        """Wallet with non-zero shortfall → capacity is zero, any borrow fails."""
        compiler = _make_compiler(chain="avalanche")
        with self._patch_eth_call(
            liquidity_18=0,
            underlying_price=10**30,
            shortfall_18=50 * 10**18,
        ):
            reason, available = _check_lending_borrow_capacity_benqi(
                compiler,
                _WALLET,
                _QI_USDC_BENQI,
                "USDC",
                Decimal("1"),
                6,
            )
        assert reason is not None
        assert "underwater" in reason.lower()
        assert available == Decimal("0")

    def test_comptroller_error_code_fails_open(self) -> None:
        """Non-zero error code from getAccountLiquidity → fail-open (the on-chain
        borrow will surface the same error)."""
        compiler = _make_compiler(chain="avalanche")
        with self._patch_eth_call(
            liquidity_18=1000 * 10**18,
            underlying_price=10**30,
            err_code=4,  # Compound V2 INSUFFICIENT_LIQUIDITY
        ):
            reason, available = _check_lending_borrow_capacity_benqi(
                compiler,
                _WALLET,
                _QI_USDC_BENQI,
                "USDC",
                Decimal("100000"),
                6,
            )
        # Comptroller fault → fail-open.
        assert reason is None
        assert available is None

    def test_unknown_chain_skips(self) -> None:
        """BENQI is Avalanche-only — other chains short-circuit."""
        compiler = _make_compiler(chain="arbitrum")
        with patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._gateway_eth_call_raw"
        ) as mock_call:
            reason, available = _check_lending_borrow_capacity_benqi(
                compiler,
                _WALLET,
                _QI_USDC_BENQI,
                "USDC",
                Decimal("100"),
                6,
            )
        assert reason is None
        assert available is None
        mock_call.assert_not_called()

    def test_rpc_failure_fails_open(self) -> None:
        compiler = _make_compiler(chain="avalanche")
        with patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._gateway_eth_call_raw",
            return_value=None,
        ):
            reason, available = _check_lending_borrow_capacity_benqi(
                compiler,
                _WALLET,
                _QI_USDC_BENQI,
                "USDC",
                Decimal("100000"),
                6,
            )
        assert reason is None
        assert available is None


# ─── Typed error contract ─────────────────────────────────────────────────────


class TestLendingBorrowExceedsCapacityErrorMessage:
    def test_includes_chain_protocol_asset_amounts(self) -> None:
        err = LendingBorrowExceedsCapacityError(
            chain="avalanche",
            protocol="benqi",
            asset_symbol="USDC",
            asset_address=_USDC_ARBITRUM,
            requested_amount=Decimal("100000"),
            available_amount=Decimal("19.8"),
            reason=(
                "BORROW request for 100000 USDC on benqi avalanche exceeds "
                "wallet capacity (19.8 after 1% safety margin)."
            ),
        )
        msg = str(err)
        assert err.ERROR_PREFIX in msg
        assert "USDC" in msg
        assert "benqi" in msg
        assert "avalanche" in msg

    def test_aave_variant_message(self) -> None:
        err = LendingBorrowExceedsCapacityError(
            chain="arbitrum",
            protocol="aave_v3",
            asset_symbol="USDC",
            asset_address=_USDC_ARBITRUM,
            requested_amount=Decimal("5000"),
            available_amount=Decimal("990"),
            reason=(
                "BORROW request for 5000 USDC on aave_v3 arbitrum exceeds wallet "
                "capacity (990 after 1% safety margin)."
            ),
        )
        msg = str(err)
        assert err.ERROR_PREFIX in msg
        assert "aave_v3" in msg
        assert "arbitrum" in msg


# ─── State-machine permanent-error classification ─────────────────────────────


class TestStateMachineClassifiesCapacityErrorAsPermanent:
    @pytest.fixture()
    def sm(self) -> IntentStateMachine:
        return IntentStateMachine.__new__(IntentStateMachine)

    def test_typed_error_prefix_is_permanent(self, sm: IntentStateMachine) -> None:
        msg = (
            "Lending borrow exceeds capacity for USDC on benqi avalanche: "
            "BORROW request for 100000 USDC on benqi avalanche exceeds wallet "
            "capacity (19.8 after 1% safety margin)."
        )
        assert sm._categorize_error(msg) == "COMPILATION_PERMANENT"

    def test_compound_borrow_limit_reached_with_comptroller_is_permanent(
        self, sm: IntentStateMachine
    ) -> None:
        """Real Compound V2 / BENQI revert messages name the Comptroller —
        the contextual word is what scopes the match away from generic
        liquidity errors."""
        msg = "Comptroller borrowAllowed reverted: BORROW_LIMIT_REACHED"
        assert sm._categorize_error(msg) == "COMPILATION_PERMANENT"

    def test_compound_insufficient_liquidity_with_comptroller_is_permanent(
        self, sm: IntentStateMachine
    ) -> None:
        msg = "BENQI Comptroller reverted with code: INSUFFICIENT_LIQUIDITY"
        assert sm._categorize_error(msg) == "COMPILATION_PERMANENT"

    def test_generic_insufficient_liquidity_without_comptroller_is_not_permanent(
        self, sm: IntentStateMachine
    ) -> None:
        """Regression: bare ``insufficient_liquidity`` without Comptroller
        context (e.g. router-level or DEX-pool errors) must NOT be classified
        as permanent — it falls through to REVERT/retry semantics. Pins the
        tightening from PR #2129 review feedback."""
        msg = "Uniswap router reverted: insufficient_liquidity in pool"
        # Must NOT short-circuit to permanent. The "revert" token catches it
        # generically; the runner can retry with adjusted parameters.
        assert sm._categorize_error(msg) != "COMPILATION_PERMANENT"

    def test_aave_collateral_cannot_cover_new_borrow_is_permanent(
        self, sm: IntentStateMachine
    ) -> None:
        """Aave V3 code 35 is unambiguous and protocol-specific; classify
        as permanent without requiring extra context (vs the Comptroller
        gating used for Compound-fork tokens)."""
        msg = "Aave V3 executeBorrow reverted: COLLATERAL_CANNOT_COVER_NEW_BORROW"
        assert sm._categorize_error(msg) == "COMPILATION_PERMANENT"


# ─── Compile-path integration: pre-flight wired into _compile_borrow_* ───────


class TestBorrowCapacityWiredIntoAaveCompiler:
    """Pin that the Aave V3 borrow compiler raises the typed error when the
    capacity pre-flight returns a reason."""

    def _setup(self, collateral_amount: Decimal = Decimal("0")):
        """Default is the borrow-against-existing-collateral path (the path
        that exercises the pre-flight). Pass ``collateral_amount > 0`` to
        exercise the supply+borrow bundle path."""
        from almanak.connectors._strategy_base.base.lending import aave_helpers as cl_mod
        from almanak.framework.intents import BorrowIntent

        compiler = MagicMock()
        compiler.chain = "arbitrum"
        compiler.wallet_address = _WALLET
        compiler._gateway_client = MagicMock()
        compiler._format_amount.side_effect = lambda a, d: str(a)
        compiler._build_approve_tx.return_value = []

        collateral = MagicMock(
            symbol="WETH", address="0x" + "ab" * 20, decimals=18, is_native=False,
        )
        borrow = MagicMock(
            symbol="USDC", address=_USDC_ARBITRUM, decimals=6, is_native=False,
        )
        # This helper is called with both collateral_amount == 0 (valid) and
        # collateral_amount > 0 (the bundled supply+borrow path under test).
        # The model validator now rejects bundled collateral at construction, so
        # build via model_construct to keep feeding the compiler pre-flight a
        # real bundled borrow.
        intent = BorrowIntent.model_construct(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
            borrow_amount=Decimal("100000"),
        )
        return cl_mod, compiler, collateral, borrow, intent

    def test_capacity_exceeded_raises_typed_error_no_pending_supply(self) -> None:
        """When there is no pending supply (existing-collateral path), the
        pre-flight runs and raises the typed error if cap is exceeded."""
        cl_mod, compiler, collateral, borrow, intent = self._setup(Decimal("0"))
        with patch.object(cl_mod, "_check_lending_reserve_active", return_value=None), \
             patch.object(cl_mod, "_check_lending_reserve_borrowable", return_value=None), \
             patch.object(
                 cl_mod,
                 "_check_lending_borrow_capacity_aave_v3",
                 return_value=(
                     "BORROW request for 100000 USDC on aave_v3 arbitrum "
                     "exceeds wallet capacity (990 after 1% safety margin).",
                     Decimal("990"),
                 ),
             ), \
             patch("almanak.framework.intents.compiler_adapters.AaveV3Adapter") as adapter_cls:
            adapter_cls.return_value.get_pool_address.return_value = "0x" + "ee" * 20
            with pytest.raises(LendingBorrowExceedsCapacityError):
                cl_mod._compile_borrow_aave_compatible(
                    compiler, intent, collateral, borrow, Decimal("0"),
                )

    def test_capacity_ok_proceeds_past_pre_flight_no_pending_supply(self) -> None:
        """Capacity OK + no pending supply: pre-flight runs and the compiler
        produces a successful CompilationResult with a populated bundle."""
        from almanak.framework.intents.compiler import CompilationStatus
        cl_mod, compiler, collateral, borrow, intent = self._setup(Decimal("0"))
        with patch.object(cl_mod, "_check_lending_reserve_active", return_value=None), \
             patch.object(cl_mod, "_check_lending_reserve_borrowable", return_value=None), \
             patch.object(
                 cl_mod,
                 "_check_lending_borrow_capacity_aave_v3",
                 return_value=(None, Decimal("9999")),
             ) as cap_mock, \
             patch("almanak.framework.intents.compiler_adapters.AaveV3Adapter") as adapter_cls:
            adapter_cls.return_value.get_pool_address.return_value = "0x" + "ee" * 20
            adapter_cls.return_value.get_supply_calldata.return_value = b"\x00" * 4
            adapter_cls.return_value.get_borrow_calldata.return_value = b"\x00" * 4
            adapter_cls.return_value.estimate_supply_gas.return_value = 250_000
            adapter_cls.return_value.estimate_borrow_gas.return_value = 350_000
            result = cl_mod._compile_borrow_aave_compatible(
                compiler, intent, collateral, borrow, Decimal("0"),
            )
            cap_mock.assert_called_once()
            # Assert the post-check compile result, not just "no exception".
            assert result.status == CompilationStatus.SUCCESS, (
                f"Expected SUCCESS after pre-flight pass; got {result.status} "
                f"(error={result.error!r})"
            )
            assert result.action_bundle is not None
            # The borrow-only path (no pending supply) builds the borrow tx
            # (plus any approves the compiler stubs out).
            adapter_cls.return_value.get_borrow_calldata.assert_called()

    def test_capacity_check_skipped_for_supply_plus_borrow_bundle(self) -> None:
        """Supply+borrow bundle: pre-flight is bypassed because
        getUserAccountData() at ``latest`` does not yet see the pending
        collateral. Without the bypass, fresh wallets would false-fail.

        Pin: cap_mock is NOT called when ``collateral_amount > 0``, and the
        compile succeeds even though the mocked cap helper would have
        returned a reject reason."""
        from almanak.framework.intents.compiler import CompilationStatus
        cl_mod, compiler, collateral, borrow, intent = self._setup(Decimal("1"))
        with patch.object(cl_mod, "_check_lending_reserve_active", return_value=None), \
             patch.object(cl_mod, "_check_lending_reserve_borrowable", return_value=None), \
             patch.object(
                 cl_mod,
                 "_check_lending_borrow_capacity_aave_v3",
                 return_value=(
                     "would-have-rejected if called",
                     Decimal("0"),
                 ),
             ) as cap_mock, \
             patch("almanak.framework.intents.compiler_adapters.AaveV3Adapter") as adapter_cls:
            adapter_cls.return_value.get_pool_address.return_value = "0x" + "ee" * 20
            adapter_cls.return_value.get_supply_calldata.return_value = b"\x00" * 4
            adapter_cls.return_value.get_borrow_calldata.return_value = b"\x00" * 4
            adapter_cls.return_value.estimate_supply_gas.return_value = 250_000
            adapter_cls.return_value.estimate_borrow_gas.return_value = 350_000
            result = cl_mod._compile_borrow_aave_compatible(
                compiler, intent, collateral, borrow, Decimal("1"),
            )
            cap_mock.assert_not_called()
            assert result.status == CompilationStatus.SUCCESS, (
                f"Supply+borrow bundle must compile past the bypassed pre-flight; "
                f"got {result.status} (error={result.error!r})"
            )
            adapter_cls.return_value.get_supply_calldata.assert_called()
            adapter_cls.return_value.get_borrow_calldata.assert_called()


# ─── Compile-path integration: pre-flight wired into _compile_borrow_benqi ───


class TestBorrowCapacityWiredIntoBenqiCompiler:
    """Mirror of TestBorrowCapacityWiredIntoAaveCompiler for BENQI's
    compile path. Pins the same supply+borrow bypass behavior on the
    Compound V2 fork side."""

    def _setup(self, collateral_amount: Decimal = Decimal("0")):
        """Default is the borrow-against-existing-collateral path. Pass
        ``collateral_amount > 0`` to exercise the supply+borrow bundle
        bypass (when scaled wei is non-zero)."""
        from almanak.connectors._strategy_base.base.lending import aave_helpers as cl_mod
        from almanak.framework.intents import BorrowIntent

        compiler = MagicMock()
        compiler.chain = "avalanche"
        compiler.wallet_address = _WALLET
        compiler._gateway_client = MagicMock()
        compiler._format_amount.side_effect = lambda a, d: str(a)
        compiler._build_approve_tx.return_value = []

        collateral = MagicMock(
            symbol="WAVAX", address="0x" + "ab" * 20, decimals=18, is_native=False,
        )
        borrow = MagicMock(
            symbol="USDC", address=_USDC_ARBITRUM, decimals=6, is_native=False,
        )
        # Called with collateral_amount == 0 and > 0; the bundled form is now
        # rejected by the validator, so build via model_construct to feed the
        # compiler pre-flight a real bundled borrow.
        intent = BorrowIntent.model_construct(
            protocol="benqi",
            collateral_token="WAVAX",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
            borrow_amount=Decimal("100000"),
        )
        return cl_mod, compiler, collateral, borrow, intent

    def _benqi_adapter_mock(self) -> MagicMock:
        """Factory for a BenqiAdapter mock with the methods the compiler
        path exercises wired up."""
        market_info = MagicMock(
            qi_token_address="0x" + "be" * 20,
            is_native=False,
        )
        adapter_inst = MagicMock()
        adapter_inst.get_market_info.return_value = market_info
        adapter_inst.supply.return_value = MagicMock(
            success=True,
            tx_data={"to": "0x" + "be" * 20, "value": 0, "data": "0x" + "00" * 4},
            gas_estimate=200_000,
            description="benqi supply",
        )
        adapter_inst.enter_markets.return_value = MagicMock(
            success=True,
            tx_data={"to": "0x" + "ce" * 20, "value": 0, "data": "0x" + "00" * 4},
            gas_estimate=100_000,
            description="benqi enter_markets",
        )
        adapter_inst.borrow.return_value = MagicMock(
            success=True,
            tx_data={"to": "0x" + "be" * 20, "value": 0, "data": "0x" + "00" * 4},
            gas_estimate=350_000,
            description="benqi borrow",
        )
        return adapter_inst

    def test_capacity_check_skipped_for_supply_plus_borrow_bundle(self) -> None:
        """BENQI supply+enterMarkets+borrow bundle must bypass the cap
        helper because Comptroller.getAccountLiquidity() at ``latest``
        does not yet see the pending supply. Mirror of the Aave test;
        gated on scaled collateral wei (not raw Decimal)."""
        from almanak.framework.intents.compiler import CompilationStatus
        cl_mod, compiler, collateral, borrow, intent = self._setup(Decimal("1"))
        adapter_inst = self._benqi_adapter_mock()
        with patch.object(
            cl_mod,
            "_check_lending_borrow_capacity_benqi",
            return_value=("would-have-rejected if called", Decimal("0")),
        ) as cap_mock, patch(
            "almanak.connectors.benqi.adapter.BenqiAdapter",
            return_value=adapter_inst,
        ):
            result = cl_mod._compile_borrow_benqi(
                compiler, intent, collateral, borrow, Decimal("1"),
            )
            cap_mock.assert_not_called()
            assert result.status == CompilationStatus.SUCCESS, (
                f"BENQI supply+borrow bundle must compile past the bypassed "
                f"pre-flight; got {result.status} (error={result.error!r})"
            )
            # The full BENQI bundle must include all three legs in order:
            # supply → enterMarkets → borrow. A regression that returns
            # SUCCESS while skipping enterMarkets would still pass a naive
            # "supply was called, borrow was called" check but fail on-chain
            # because BENQI requires the asset to be marked as collateral
            # before borrow.
            adapter_inst.supply.assert_called()
            adapter_inst.enter_markets.assert_called(), (
                "BENQI bundle MUST include enterMarkets between supply and "
                "borrow; the on-chain Comptroller will not credit the supply "
                "as collateral until the market is entered."
            )
            adapter_inst.borrow.assert_called()
            # action_bundle must reflect the full sequence; pin tx_types so a
            # regression that drops or reorders a leg is caught here, not on
            # mainnet.
            assert result.action_bundle is not None
            # ``ActionBundle.transactions`` are serialized dicts produced by
            # ``to_dict()``. Pin that contract here — accessing via
            # ``tx["tx_type"]`` (no dataclass fallback) ensures a future
            # refactor that stops serializing as dicts will fail this
            # regression guard rather than silently still pass.
            for tx in result.action_bundle.transactions:
                assert isinstance(tx, dict), (
                    "ActionBundle.transactions must be serialized dicts "
                    f"post-to_dict(); got {type(tx).__name__}"
                )
            tx_types = [tx["tx_type"] for tx in result.action_bundle.transactions]
            assert "lending_supply_collateral" in tx_types, (
                f"BENQI bundle missing supply tx; got tx_types={tx_types}"
            )
            assert "lending_enter_markets" in tx_types, (
                f"BENQI bundle missing enterMarkets tx; got tx_types={tx_types}"
            )
            assert "lending_borrow" in tx_types, (
                f"BENQI bundle missing borrow tx; got tx_types={tx_types}"
            )
            # Order: supply must precede enterMarkets must precede borrow.
            supply_idx = tx_types.index("lending_supply_collateral")
            enter_idx = tx_types.index("lending_enter_markets")
            borrow_idx = tx_types.index("lending_borrow")
            assert supply_idx < enter_idx < borrow_idx, (
                "BENQI tx ordering must be supply → enterMarkets → borrow; "
                f"got tx_types={tx_types}"
            )

    def test_dust_collateral_does_not_skip_pre_flight(self) -> None:
        """A dust Decimal that floors to 0 wei must NOT skip the pre-flight
        — there is no real supply tx to mask the false-negative. Pin the
        scaled-wei semantics: bypass uses ``collateral_amount_wei == 0``,
        not ``collateral_amount_decimal == 0``."""
        from almanak.framework.intents.compiler import CompilationStatus
        # 1e-19 WAVAX with 18 decimals scales to 1e-19 * 10^18 = 0.1 -> int(0.1) = 0
        dust = Decimal("1e-19")
        cl_mod, compiler, collateral, borrow, intent = self._setup(dust)
        adapter_inst = self._benqi_adapter_mock()
        with patch.object(
            cl_mod,
            "_check_lending_borrow_capacity_benqi",
            return_value=(
                "BORROW request for 100000 USDC on benqi avalanche exceeds wallet "
                "capacity (19.8 after 1% safety margin).",
                Decimal("19.8"),
            ),
        ) as cap_mock, patch(
            "almanak.connectors.benqi.adapter.BenqiAdapter",
            return_value=adapter_inst,
        ):
            with pytest.raises(LendingBorrowExceedsCapacityError):
                cl_mod._compile_borrow_benqi(
                    compiler, intent, collateral, borrow, dust,
                )
            # Pre-flight MUST have been called; dust does not satisfy the bypass.
            cap_mock.assert_called_once()
            # CompilationStatus is SUCCESS variable not used because we expect raise
            _ = CompilationStatus  # silence unused-import lint when the test passes
