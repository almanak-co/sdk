"""Refresh-on-read Curve pool registry (VIB-5423 / VIB-5424).

``get_pool_info`` / ``get_pool_by_name`` no longer trust the hand-typed
``CURVE_POOLS`` literal blindly: when a gateway / RPC transport is wired they
reconcile the safety-critical fields (``coins`` / ``coin_addresses`` /
``coin_decimals`` / ``virtual_price`` / ``is_ng``) against live chain state. The
static dict becomes a cold-start fallback only — discharging the long-standing
``TECH_DEBT(VIB-581)`` frozen-snapshot note.

Why it matters: a wrong ``is_ng`` selects the wrong add/remove ABI encoder
(malformed calldata / silent revert); a reversed coin order makes approve /
exchange target the wrong token; a frozen ``virtual_price`` drifts NAV.

These tests drive a FAKE gateway transport (no network) so the live-read,
drift-override, and fail-safe-fallback branches are all exercised deterministically.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors.curve.adapter import (
    COINS_UINT256_SELECTOR,
    ERC20_DECIMALS_SELECTOR,
    GET_VIRTUAL_PRICE_SELECTOR,
    NG_CALC_TOKEN_AMOUNT_SELECTOR,
    CurveAdapter,
    CurveConfig,
    PoolInfo,
    PoolType,
)

# Real Ethereum mainnet addresses (used as opaque fixtures here).
DAI = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"  # 3pool
NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"  # Curve native-coin placeholder


def _word_hex(value: int) -> str:
    return "0x" + format(value, "064x")


def _addr_word(addr: str) -> str:
    return "0x" + addr.lower().removeprefix("0x").zfill(64)


class FakeGateway:
    """Minimal connected gateway whose ``eth_call`` answers Curve registry reads.

    Routes on the 4-byte selector. ``coins(uint256)`` returns the configured live
    coin at the requested index; ``decimals()`` returns per-coin decimals keyed by
    the ``to`` address; ``get_virtual_price()`` returns the configured 1e18-scaled
    value; the NG ``calc_token_amount`` probe returns zero (NG present) or raises
    (legacy / revert). Any read may be globally disabled to exercise fail-safe.
    """

    def __init__(
        self,
        *,
        coins: list[str],
        decimals: dict[str, int],
        virtual_price_wei: int,
        is_ng: bool,
        fail_all: bool = False,
    ) -> None:
        self.is_connected = True
        self._coins = coins
        self._decimals = {a.lower(): d for a, d in decimals.items()}
        self._vp = virtual_price_wei
        self._is_ng = is_ng
        self._fail_all = fail_all
        self.calls = 0

    def eth_call(self, *, chain: str, to: str, data: str) -> str:
        self.calls += 1
        if self._fail_all:
            raise ValueError("simulated transport failure")
        selector = data[:10]
        if selector == COINS_UINT256_SELECTOR:
            index = int(data[10:], 16)
            if index >= len(self._coins):
                raise ValueError("index out of range")
            return _addr_word(self._coins[index])
        if selector == ERC20_DECIMALS_SELECTOR:
            return _word_hex(self._decimals[to.lower()])
        if selector == GET_VIRTUAL_PRICE_SELECTOR:
            return _word_hex(self._vp)
        if selector == NG_CALC_TOKEN_AMOUNT_SELECTOR:
            if not self._is_ng:
                raise ValueError("execution reverted: no such function")
            return _word_hex(0)  # NG returns 0 LP for a zero-amount deposit
        raise ValueError(f"unexpected selector {selector}")


def _adapter(gateway: FakeGateway | None, *, rpc_url: str | None = None) -> CurveAdapter:
    config = CurveConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        rpc_url=rpc_url,
        gateway_client=gateway,
    )
    return CurveAdapter(config)


def _cold_start(
    *,
    coins: list[str],
    coin_addresses: list[str],
    is_ng: bool = False,
    virtual_price: Decimal = Decimal("1.0"),
    pool_type: PoolType = PoolType.STABLESWAP,
) -> PoolInfo:
    return PoolInfo(
        address=POOL,
        lp_token=POOL,
        coins=coins,
        coin_addresses=coin_addresses,
        pool_type=pool_type,
        n_coins=len(coins),
        name="3pool",
        virtual_price=virtual_price,
        is_ng=is_ng,
    )


class TestIsNgRefresh:
    def test_inconclusive_probe_never_downgrades_static_ng(self) -> None:
        """Static says NG; the NG probe reverts → is_ng KEEPS the static value.

        A legacy pool's ``calc_token_amount(uint256[],bool)`` call reverts, and the
        ``eth_call`` seam surfaces a contract revert and a transport failure as the
        SAME ``ValueError`` (indistinguishable at this layer). Downgrading a
        possibly-correct NG pool to legacy on that ambiguous signal would select the
        wrong add/remove encoder on real funds, so an inconclusive (raising) probe
        must never flip ``is_ng`` — it keeps the cold-start value. Positive evidence
        (a clean non-raising probe) is the only thing that overrides it; see
        ``test_live_overrides_wrong_static_is_ng_false`` for the False→True path.
        """
        gw = FakeGateway(
            coins=[DAI, USDC, USDT],
            decimals={DAI: 18, USDC: 6, USDT: 6},
            virtual_price_wei=10**18,
            is_ng=False,  # probe reverts (legacy-ABI revert, indistinguishable from a transport error)
        )
        adapter = _adapter(gw)
        cold = _cold_start(coins=["DAI", "USDC", "USDT"], coin_addresses=[DAI, USDC, USDT], is_ng=True)

        refreshed = adapter._refresh_pool_info_from_chain(cold)

        assert refreshed.is_ng is True

    def test_live_overrides_wrong_static_is_ng_false(self) -> None:
        """Static says legacy, chain is NG → refreshed is_ng flips to True."""
        gw = FakeGateway(
            coins=[DAI, USDC],
            decimals={DAI: 18, USDC: 6},
            virtual_price_wei=10**18,
            is_ng=True,
        )
        adapter = _adapter(gw)
        cold = _cold_start(coins=["DAI", "USDC"], coin_addresses=[DAI, USDC], is_ng=False)

        refreshed = adapter._refresh_pool_info_from_chain(cold)

        assert refreshed.is_ng is True

    def test_crypto_pool_never_probed_as_ng(self) -> None:
        """Crypto/Tricrypto are a different ABI family — never NG, no probe call."""
        gw = FakeGateway(
            coins=[DAI, USDC],
            decimals={DAI: 18, USDC: 6},
            virtual_price_wei=10**18,
            is_ng=True,  # would lie 'NG' if probed, but crypto short-circuits
        )
        adapter = _adapter(gw)
        cold = _cold_start(
            coins=["DAI", "USDC"], coin_addresses=[DAI, USDC], is_ng=False, pool_type=PoolType.CRYPTOSWAP
        )

        refreshed = adapter._refresh_pool_info_from_chain(cold)

        assert refreshed.is_ng is False


class TestCoinAndDecimalRefresh:
    def test_reversed_coin_order_corrected_from_chain(self) -> None:
        """A reversed static coin order is re-ordered to live chain truth, symbols realigned."""
        gw = FakeGateway(
            coins=[DAI, USDC, USDT],  # correct on-chain order
            decimals={DAI: 18, USDC: 6, USDT: 6},
            virtual_price_wei=10**18,
            is_ng=False,
        )
        adapter = _adapter(gw)
        # Static literal has USDC and DAI swapped (the class of bug §E1 cites).
        cold = _cold_start(coins=["USDC", "DAI", "USDT"], coin_addresses=[USDC, DAI, USDT])

        refreshed = adapter._refresh_pool_info_from_chain(cold)

        assert [a.lower() for a in refreshed.coin_addresses] == [DAI.lower(), USDC.lower(), USDT.lower()]
        # symbols realign to the live order (reusing the known static symbols)
        assert refreshed.coins == ["DAI", "USDC", "USDT"]

    def test_decimals_come_from_chain(self) -> None:
        """coin_decimals are populated from the live decimals() read."""
        gw = FakeGateway(
            coins=[DAI, USDC, USDT],
            decimals={DAI: 18, USDC: 6, USDT: 6},
            virtual_price_wei=10**18,
            is_ng=False,
        )
        adapter = _adapter(gw)
        cold = _cold_start(coins=["DAI", "USDC", "USDT"], coin_addresses=[DAI, USDC, USDT])

        refreshed = adapter._refresh_pool_info_from_chain(cold)

        assert refreshed.coin_decimals == [18, 6, 6]
        # and the by-index resolver prefers the chain value
        assert adapter._coin_decimals(refreshed, 1) == 6


class TestEnsureLiveCoinOrder:
    """The read-only quote path refreshes coin ORDER only — not is_ng / vp / decimals."""

    def test_coins_only_refresh_corrects_order_without_heavy_reads(self) -> None:
        gw = FakeGateway(
            coins=[DAI, USDC, USDT],  # live order
            decimals={DAI: 18, USDC: 6, USDT: 6},
            virtual_price_wei=1_054_000_000_000_000_000,
            is_ng=True,  # would flip is_ng if the full reconcile ran — it must NOT here
        )
        adapter = _adapter(gw)
        cold = _cold_start(coins=["USDC", "DAI", "USDT"], coin_addresses=[USDC, DAI, USDT], is_ng=False)

        quoted = adapter._ensure_live_coin_order(cold)

        # Coin order corrected from chain for get_coin_index / get_dy.
        assert [a.lower() for a in quoted.coin_addresses] == [DAI.lower(), USDC.lower(), USDT.lower()]
        assert quoted.coins == ["DAI", "USDC", "USDT"]
        # The heavy reconcile did NOT run: is_ng stays static, vp untouched, decimals unread.
        assert quoted.is_ng is False
        assert quoted.coin_decimals is None
        # Only coins(i) reads hit the wire — no get_virtual_price / NG-probe selectors.
        assert gw.calls == 3

    def test_use_underlying_pool_keeps_static_order(self) -> None:
        gw = FakeGateway(
            coins=["0x27F8D03b3a2196956ED754baDc28D73be8830A6e", "0x1a13F4Ca1d028320A707D99520AbFefca3998b7F"],
            decimals={},
            virtual_price_wei=10**18,
            is_ng=False,
        )
        adapter = _adapter(gw)
        underlying = PoolInfo(
            address=POOL,
            lp_token=POOL,
            coins=["DAI", "USDC.e"],
            coin_addresses=[DAI, USDC],
            pool_type=PoolType.STABLESWAP,
            n_coins=2,
            name="am3pool",
            use_underlying=True,
        )

        quoted = adapter._ensure_live_coin_order(underlying)

        assert quoted.coin_addresses == [DAI, USDC]  # static underlying kept; no aToken splice
        assert gw.calls == 0  # short-circuited before any read


class TestVirtualPriceRefresh:
    def test_virtual_price_read_live(self) -> None:
        """Frozen static virtual_price is overridden by the live get_virtual_price()."""
        gw = FakeGateway(
            coins=[DAI, USDC, USDT],
            decimals={DAI: 18, USDC: 6, USDT: 6},
            virtual_price_wei=1_054_000_000_000_000_000,  # 1.054e18
            is_ng=False,
        )
        adapter = _adapter(gw)
        cold = _cold_start(
            coins=["DAI", "USDC", "USDT"], coin_addresses=[DAI, USDC, USDT], virtual_price=Decimal("1.04")
        )

        refreshed = adapter._refresh_pool_info_from_chain(cold)

        assert refreshed.virtual_price == Decimal("1.054")


class TestColdStartFallback:
    def test_no_transport_returns_static_unchanged(self) -> None:
        """No gateway and no RPC → cold-start values returned verbatim, no reads."""
        adapter = _adapter(None)
        cold = _cold_start(
            coins=["USDC", "DAI", "USDT"],
            coin_addresses=[USDC, DAI, USDT],
            is_ng=True,
            virtual_price=Decimal("1.04"),
        )

        refreshed = adapter._refresh_pool_info_from_chain(cold)

        assert refreshed is cold
        assert refreshed.is_ng is True
        assert refreshed.coins == ["USDC", "DAI", "USDT"]
        assert refreshed.virtual_price == Decimal("1.04")
        assert refreshed.coin_decimals is None

    def test_failed_reads_fall_back_to_static(self) -> None:
        """Transport present but every read fails → cold-start static, not a downgrade."""
        gw = FakeGateway(
            coins=[DAI, USDC, USDT],
            decimals={DAI: 18, USDC: 6, USDT: 6},
            virtual_price_wei=10**18,
            is_ng=False,
            fail_all=True,
        )
        adapter = _adapter(gw)
        cold = _cold_start(
            coins=["USDC", "DAI", "USDT"],
            coin_addresses=[USDC, DAI, USDT],
            is_ng=True,
            virtual_price=Decimal("1.04"),
        )

        refreshed = adapter._refresh_pool_info_from_chain(cold)

        # is_ng NOT flipped to legacy on a transport failure (no false downgrade)
        assert refreshed.is_ng is True
        assert refreshed.coins == ["USDC", "DAI", "USDT"]
        assert refreshed.virtual_price == Decimal("1.04")
        assert refreshed.coin_decimals is None


class TestRefreshCaching:
    def test_pool_read_once_per_adapter(self) -> None:
        """Repeated resolves of the same pool reuse the cached refresh (no re-read)."""
        adapter = _adapter(None)
        adapter.pools = {
            "3pool": {
                "address": POOL,
                "lp_token": POOL,
                "coins": ["DAI", "USDC", "USDT"],
                "coin_addresses": [DAI, USDC, USDT],
                "pool_type": "stableswap",
                "n_coins": 3,
                "virtual_price": Decimal("1.04"),
            }
        }
        gw = FakeGateway(
            coins=[DAI, USDC, USDT],
            decimals={DAI: 18, USDC: 6, USDT: 6},
            virtual_price_wei=1_054_000_000_000_000_000,
            is_ng=False,
        )
        adapter._gateway_client = gw

        first = adapter.get_pool_by_name("3pool")
        calls_after_first = gw.calls
        second = adapter.get_pool_by_name("3pool")

        assert calls_after_first > 0
        assert gw.calls == calls_after_first  # second resolve hit the cache
        assert first.virtual_price == Decimal("1.054")
        assert second.virtual_price == Decimal("1.054")


class TestCoinDecimalsHelper:
    def test_prefers_live_then_resolver_fallback(self) -> None:
        """_coin_decimals uses chain decimals when present, else the resolver path."""
        adapter = _adapter(None)
        live = _cold_start(coins=["DAI", "USDC"], coin_addresses=[DAI, USDC])
        live.coin_decimals = [18, 6]
        assert adapter._coin_decimals(live, 0) == 18
        assert adapter._coin_decimals(live, 1) == 6

        # cold-start (coin_decimals=None) falls through to the symbol resolver
        cold = _cold_start(coins=["DAI", "USDC"], coin_addresses=[DAI, USDC])
        assert cold.coin_decimals is None
        assert adapter._coin_decimals(cold, 0) == 18  # resolver knows DAI=18

    def test_native_placeholder_decimals_no_erc20_call(self) -> None:
        """The 0xEeee… native placeholder resolves to 18 without an ERC-20 decimals() call.

        The native coin is not an ERC-20 and has no ``decimals()`` selector; reading
        it would revert. ``_read_coin_decimals`` must short-circuit it to the chain's
        18-decimal native unit and only issue ``decimals()`` for the real ERC-20s.
        """
        gw = FakeGateway(
            coins=[NATIVE, USDC],
            decimals={USDC: 6},  # deliberately no NATIVE entry — must not be looked up
            virtual_price_wei=10**18,
            is_ng=False,
        )
        adapter = _adapter(gw)

        decimals = adapter._read_coin_decimals([NATIVE, USDC])

        assert decimals == [18, 6]
        assert gw.calls == 1  # only USDC hit the wire; the native placeholder did not


class TestUseUnderlyingPools:
    def test_underlying_coin_addresses_not_overwritten_by_atokens(self) -> None:
        """``use_underlying`` (aave-type) pools keep their static UNDERLYING coin set.

        On-chain ``coins(i)`` for an aave-type pool (e.g. Polygon am3pool) returns the
        pool's internal interest-bearing aTokens, while the registry intentionally
        tracks the UNDERLYING tokens users approve / receive. Overwriting them with the
        live aTokens would break approve / exchange_underlying / coin-index resolution,
        so the coin set is NOT refreshed for these pools — only pool-level fields
        (virtual_price) move to live truth.
        """
        atoken_dai = "0x27F8D03b3a2196956ED754baDc28D73be8830A6e"  # amDAI (distinct from DAI)
        atoken_usdc = "0x1a13F4Ca1d028320A707D99520AbFefca3998b7F"  # amUSDC
        gw = FakeGateway(
            coins=[atoken_dai, atoken_usdc],  # chain coins() returns the wrapped aTokens
            decimals={atoken_dai: 18, atoken_usdc: 6},
            virtual_price_wei=1_054_000_000_000_000_000,  # 1.054e18 (drifted from static)
            is_ng=False,
        )
        adapter = _adapter(gw)
        underlying = PoolInfo(
            address=POOL,
            lp_token=POOL,
            coins=["DAI", "USDC.e"],
            coin_addresses=[DAI, USDC],  # the underlying tokens, intentionally
            pool_type=PoolType.STABLESWAP,
            n_coins=2,
            name="am3pool",
            virtual_price=Decimal("1.02"),
            use_underlying=True,
        )

        refreshed = adapter._refresh_pool_info_from_chain(underlying)

        # Coin set stays the static UNDERLYING tokens — NOT the on-chain aTokens.
        assert refreshed.coin_addresses == [DAI, USDC]
        assert refreshed.coins == ["DAI", "USDC.e"]
        assert refreshed.coin_decimals is None  # resolver handles underlying decimals
        # Pool-level virtual_price still refreshes (it is unaffected by the aToken wrap).
        assert refreshed.virtual_price == Decimal("1.054")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
