"""Unit tests for RateMonitor service.

Tests cover:
- Initialization and configuration
- Protocol support per chain
- Rate fetching (mocked)
- Caching behavior
- Cross-protocol comparison
- Error handling
"""

import asyncio
from decimal import Decimal

import pytest

from almanak.framework.data.rates.monitor import (
    PROTOCOL_CHAINS,
    RAY,
    SUPPORTED_PROTOCOLS,
    SUPPORTED_TOKENS,
    BestRateResult,
    LendingRate,
    ProtocolNotSupportedError,
    RateMonitor,
    RateSide,
    TokenNotSupportedError,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def ethereum_monitor() -> RateMonitor:
    """Create a RateMonitor for Ethereum mainnet."""
    return RateMonitor(chain="ethereum")


@pytest.fixture
def arbitrum_monitor() -> RateMonitor:
    """Create a RateMonitor for Arbitrum."""
    return RateMonitor(chain="arbitrum")


@pytest.fixture
def avalanche_monitor() -> RateMonitor:
    """Create a RateMonitor for Avalanche (a chain without a Morpho rate lane)."""
    return RateMonitor(chain="avalanche")


@pytest.fixture
def mocked_monitor() -> RateMonitor:
    """Create a RateMonitor with mocked rates."""
    monitor = RateMonitor(chain="ethereum")
    # Set up mock rates
    monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("4.5"))
    monitor.set_mock_rate("aave_v3", "USDC", "borrow", Decimal("6.0"))
    monitor.set_mock_rate("morpho_blue", "USDC", "supply", Decimal("5.5"))
    monitor.set_mock_rate("morpho_blue", "USDC", "borrow", Decimal("5.5"))
    monitor.set_mock_rate("compound_v3", "USDC", "supply", Decimal("5.0"))
    monitor.set_mock_rate("compound_v3", "USDC", "borrow", Decimal("6.5"))
    monitor.set_mock_rate("aave_v3", "WETH", "supply", Decimal("2.0"))
    monitor.set_mock_rate("aave_v3", "WETH", "borrow", Decimal("3.5"))
    return monitor


# =============================================================================
# Initialization Tests
# =============================================================================


class TestRateMonitorInit:
    """Tests for RateMonitor initialization."""

    def test_default_chain(self) -> None:
        """Test default chain is ethereum."""
        monitor = RateMonitor()
        assert monitor.chain == "ethereum"

    def test_custom_chain(self) -> None:
        """Test custom chain configuration."""
        monitor = RateMonitor(chain="arbitrum")
        assert monitor.chain == "arbitrum"

    def test_protocols_on_ethereum(self, ethereum_monitor: RateMonitor) -> None:
        """Test all protocols available on Ethereum."""
        protocols = ethereum_monitor.protocols
        assert "aave_v3" in protocols
        assert "morpho_blue" in protocols
        assert "compound_v3" in protocols

    def test_protocols_on_arbitrum(self, arbitrum_monitor: RateMonitor) -> None:
        """Test protocols available on Arbitrum."""
        protocols = arbitrum_monitor.protocols
        assert "aave_v3" in protocols
        assert "compound_v3" in protocols
        # Morpho Blue is on the Arbitrum rate lane
        # (MORPHO_MARKETS had catalogued Arbitrum markets all along).
        assert "morpho_blue" in protocols

    def test_protocols_on_avalanche(self, avalanche_monitor: RateMonitor) -> None:
        """Test protocols available on Avalanche."""
        protocols = avalanche_monitor.protocols
        assert "aave_v3" in protocols
        # No Morpho Blue market catalogue on Avalanche.
        assert "morpho_blue" not in protocols

    def test_custom_protocols(self) -> None:
        """Test filtering protocols."""
        monitor = RateMonitor(chain="ethereum", protocols=["aave_v3"])
        assert monitor.protocols == ["aave_v3"]

    def test_custom_cache_ttl(self) -> None:
        """Test custom cache TTL."""
        monitor = RateMonitor(cache_ttl_seconds=60.0)
        assert monitor._cache_ttl_seconds == 60.0


# =============================================================================
# Mock Rate Tests
# =============================================================================


class TestMockRates:
    """Tests for mock rate functionality."""

    def test_set_mock_rate(self) -> None:
        """Test setting a mock rate."""
        monitor = RateMonitor(chain="ethereum")
        monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("5.0"))

        assert "aave_v3" in monitor._mock_rates
        assert "USDC" in monitor._mock_rates["aave_v3"]
        assert monitor._mock_rates["aave_v3"]["USDC"]["supply"] == Decimal("5.0")

    def test_clear_mock_rates(self) -> None:
        """Test clearing mock rates."""
        monitor = RateMonitor(chain="ethereum")
        monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("5.0"))
        monitor.clear_mock_rates()

        assert len(monitor._mock_rates) == 0

    @pytest.mark.asyncio
    async def test_mock_rate_fetched(self, mocked_monitor: RateMonitor) -> None:
        """Test that mock rates are returned."""
        rate = await mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "aave_v3"
        assert rate.token == "USDC"
        assert rate.side == "supply"
        assert rate.apy_percent == Decimal("4.5")


# =============================================================================
# Rate Fetching Tests
# =============================================================================


class TestRateFetching:
    """Tests for rate fetching functionality."""

    @pytest.mark.asyncio
    async def test_get_aave_rate(self, ethereum_monitor: RateMonitor) -> None:
        """Test fetching Aave V3 rate."""
        rate = await ethereum_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "aave_v3"
        assert rate.token == "USDC"
        assert rate.side == "supply"
        assert rate.apy_percent > Decimal("0")
        assert rate.chain == "ethereum"

    @pytest.mark.asyncio
    async def test_get_morpho_rate(self, ethereum_monitor: RateMonitor) -> None:
        """Test fetching Morpho Blue rate."""
        rate = await ethereum_monitor.get_lending_rate("morpho_blue", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "morpho_blue"
        assert rate.token == "USDC"
        assert rate.apy_percent > Decimal("0")

    @pytest.mark.asyncio
    async def test_get_compound_rate(self, ethereum_monitor: RateMonitor) -> None:
        """Test fetching Compound V3 rate."""
        rate = await ethereum_monitor.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "compound_v3"
        assert rate.token == "USDC"
        assert rate.apy_percent > Decimal("0")

    @pytest.mark.asyncio
    async def test_get_spark_rate(self, ethereum_monitor: RateMonitor) -> None:
        """Spark is on the rate lane; with no gateway the offline
        placeholder must return the manifest-declared default APY rather than
        raising TokenNotSupportedError (P2 fix, PR #3210). Spark's manifest
        default supply APY is 0.05 (=5%)."""
        rate = await ethereum_monitor.get_lending_rate("spark", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "spark"
        assert rate.token == "USDC"
        assert rate.apy_percent == Decimal("5")

    @pytest.mark.asyncio
    async def test_spark_manifest_default_for_any_token(self, ethereum_monitor: RateMonitor) -> None:
        """Spark ships no curated token table, so the manifest-derived offline
        placeholder serves the same declared default for any symbol (0.055 borrow)."""
        rate = await ethereum_monitor.get_lending_rate("spark", "WETH", RateSide.BORROW)

        assert rate.protocol == "spark"
        assert rate.apy_percent == Decimal("5.5")

    @pytest.mark.asyncio
    async def test_get_borrow_rate(self, ethereum_monitor: RateMonitor) -> None:
        """Test fetching borrow rate."""
        rate = await ethereum_monitor.get_lending_rate("aave_v3", "USDC", RateSide.BORROW)

        assert rate.side == "borrow"
        # Borrow rate typically higher than supply
        supply_rate = await ethereum_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)
        assert rate.apy_percent > supply_rate.apy_percent

    @pytest.mark.asyncio
    async def test_unsupported_protocol(self, ethereum_monitor: RateMonitor) -> None:
        """Test error for unsupported protocol."""
        with pytest.raises(ProtocolNotSupportedError):
            await ethereum_monitor.get_lending_rate("unknown_protocol", "USDC", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_unsupported_token(self, ethereum_monitor: RateMonitor) -> None:
        """Test error for unsupported token."""
        with pytest.raises(TokenNotSupportedError):
            await ethereum_monitor.get_lending_rate("compound_v3", "UNKNOWN_TOKEN", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_protocol_not_on_chain(self, avalanche_monitor: RateMonitor) -> None:
        """Test error for protocol not available on chain."""
        with pytest.raises(ProtocolNotSupportedError):
            await avalanche_monitor.get_lending_rate("morpho_blue", "USDC", RateSide.SUPPLY)


# =============================================================================
# Caching Tests
# =============================================================================


class TestCaching:
    """Tests for rate caching."""

    @pytest.mark.asyncio
    async def test_rate_cached(self, mocked_monitor: RateMonitor) -> None:
        """Test that rates are cached."""
        # First call
        rate1 = await mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        # Check cache
        cached = mocked_monitor._get_cached_rate("aave_v3", "USDC", "supply")
        assert cached is not None
        assert cached.apy_percent == rate1.apy_percent

    @pytest.mark.asyncio
    async def test_cache_hit(self, mocked_monitor: RateMonitor) -> None:
        """Test cache hit returns same rate."""
        rate1 = await mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)
        rate2 = await mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        # Same timestamp means cache hit
        assert rate1.timestamp == rate2.timestamp

    def test_clear_cache(self, mocked_monitor: RateMonitor) -> None:
        """Test clearing cache."""
        # Populate cache
        asyncio.run(mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY))

        # Clear and verify
        mocked_monitor.clear_cache()
        assert len(mocked_monitor._cache) == 0

    def test_cache_stats(self, mocked_monitor: RateMonitor) -> None:
        """Test cache statistics."""
        # Populate cache
        asyncio.run(mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY))
        asyncio.run(mocked_monitor.get_lending_rate("aave_v3", "WETH", RateSide.SUPPLY))

        stats = mocked_monitor.get_cache_stats()
        assert stats["total_entries"] == 2
        assert "aave_v3" in stats["protocols"]


# =============================================================================
# Best Rate Tests
# =============================================================================


class TestBestRate:
    """Tests for cross-protocol rate comparison."""

    @pytest.mark.asyncio
    async def test_best_supply_rate(self, mocked_monitor: RateMonitor) -> None:
        """Test finding best supply rate."""
        result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)

        assert result.token == "USDC"
        assert result.side == "supply"
        assert result.best_rate is not None
        # Morpho has highest supply rate (5.5%)
        assert result.best_rate.protocol == "morpho_blue"
        assert result.best_rate.apy_percent == Decimal("5.5")

    @pytest.mark.asyncio
    async def test_best_borrow_rate(self, mocked_monitor: RateMonitor) -> None:
        """Test finding best borrow rate (lowest)."""
        result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.BORROW)

        assert result.best_rate is not None
        # Morpho has lowest borrow rate (5.5%)
        assert result.best_rate.protocol == "morpho_blue"
        assert result.best_rate.apy_percent == Decimal("5.5")

    @pytest.mark.asyncio
    async def test_all_rates_returned(self, mocked_monitor: RateMonitor) -> None:
        """Test that all protocol rates are returned (spark is on the Ethereum
        lane and contributes its offline placeholder rate)."""
        result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)

        assert len(result.all_rates) == 4
        protocols = {r.protocol for r in result.all_rates}
        assert "aave_v3" in protocols
        assert "morpho_blue" in protocols
        assert "compound_v3" in protocols
        assert "spark" in protocols

    @pytest.mark.asyncio
    async def test_filter_protocols(self, mocked_monitor: RateMonitor) -> None:
        """Test filtering protocols in comparison."""
        result = await mocked_monitor.get_best_lending_rate(
            "USDC", RateSide.SUPPLY, protocols=["aave_v3", "compound_v3"]
        )

        assert len(result.all_rates) == 2
        protocols = {r.protocol for r in result.all_rates}
        assert "morpho_blue" not in protocols


# =============================================================================
# Protocol Rates Tests
# =============================================================================


class TestProtocolRates:
    """Tests for fetching all rates from a protocol."""

    @pytest.mark.asyncio
    async def test_get_protocol_rates(self, mocked_monitor: RateMonitor) -> None:
        """Test fetching all rates for a protocol."""
        rates = await mocked_monitor.get_protocol_rates("aave_v3", tokens=["USDC", "WETH"])

        assert rates.protocol == "aave_v3"
        assert rates.chain == "ethereum"
        assert "USDC" in rates.rates
        assert "WETH" in rates.rates

    @pytest.mark.asyncio
    async def test_protocol_rates_both_sides(self, mocked_monitor: RateMonitor) -> None:
        """Test that both supply and borrow rates are fetched."""
        rates = await mocked_monitor.get_protocol_rates("aave_v3", tokens=["USDC"])

        usdc_rates = rates.rates["USDC"]
        assert "supply" in usdc_rates
        assert "borrow" in usdc_rates

    @pytest.mark.asyncio
    async def test_get_rate_from_protocol_rates(self, mocked_monitor: RateMonitor) -> None:
        """Test helper to get specific rate from ProtocolRates."""
        rates = await mocked_monitor.get_protocol_rates("aave_v3", tokens=["USDC"])

        supply_rate = rates.get_rate("USDC", "supply")
        assert supply_rate is not None
        assert supply_rate.apy_percent == Decimal("4.5")

    @pytest.mark.asyncio
    async def test_unsupported_protocol_rates(self, avalanche_monitor: RateMonitor) -> None:
        """Test error for unsupported protocol."""
        with pytest.raises(ProtocolNotSupportedError):
            await avalanche_monitor.get_protocol_rates("morpho_blue")


# =============================================================================
# Data Class Tests
# =============================================================================


class TestLendingRate:
    """Tests for LendingRate dataclass."""

    def test_to_dict(self) -> None:
        """Test serialization to dict."""
        rate = LendingRate(
            protocol="aave_v3",
            token="USDC",
            side="supply",
            apy_ray=Decimal("42500000000000000000000000"),  # 4.25%
            apy_percent=Decimal("4.25"),
            utilization_percent=Decimal("72.5"),
            chain="ethereum",
        )

        d = rate.to_dict()
        assert d["protocol"] == "aave_v3"
        assert d["token"] == "USDC"
        assert d["apy_percent"] == 4.25
        assert d["utilization_percent"] == 72.5


class TestBestRateResult:
    """Tests for BestRateResult dataclass."""

    def test_empty_result(self) -> None:
        """Test empty result when no rates found."""
        result = BestRateResult(
            token="UNKNOWN",
            side="supply",
            best_rate=None,
            all_rates=[],
        )

        assert result.best_rate is None
        assert len(result.all_rates) == 0

    def test_to_dict(self) -> None:
        """Test serialization to dict."""
        rate = LendingRate(
            protocol="aave_v3",
            token="USDC",
            side="supply",
            apy_ray=RAY * Decimal("5") / Decimal("100"),
            apy_percent=Decimal("5.0"),
            chain="ethereum",
        )
        result = BestRateResult(
            token="USDC",
            side="supply",
            best_rate=rate,
            all_rates=[rate],
        )

        d = result.to_dict()
        assert d["token"] == "USDC"
        assert d["best_rate"] is not None
        assert len(d["all_rates"]) == 1


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_supported_protocols(self) -> None:
        """Test supported protocols list."""
        assert "aave_v3" in SUPPORTED_PROTOCOLS
        assert "morpho_blue" in SUPPORTED_PROTOCOLS
        assert "compound_v3" in SUPPORTED_PROTOCOLS

    def test_protocol_chains(self) -> None:
        """Test protocol availability per chain."""
        # Ethereum has all protocols (including spark)
        assert PROTOCOL_CHAINS["ethereum"] == ["aave_v3", "compound_v3", "morpho_blue", "spark"]

        # Arbitrum has fewer (no spark; morpho_blue is present)
        assert "morpho_blue" in PROTOCOL_CHAINS["arbitrum"]
        assert "spark" not in PROTOCOL_CHAINS["arbitrum"]

        # Polygon has compound_v3 (VIB-2250: was missing, causing nightly failure)
        assert "compound_v3" in PROTOCOL_CHAINS["polygon"]

    def test_supported_tokens(self) -> None:
        """Test supported tokens per chain."""
        assert "USDC" in SUPPORTED_TOKENS["ethereum"]
        assert "WETH" in SUPPORTED_TOKENS["ethereum"]
        assert "ARB" in SUPPORTED_TOKENS["arbitrum"]


# =============================================================================
# Integration-like Tests
# =============================================================================


class TestRateMonitorIntegration:
    """Integration-like tests using mock rates."""

    @pytest.mark.asyncio
    async def test_rate_arbitrage_scenario(self, mocked_monitor: RateMonitor) -> None:
        """Test scenario: find rate arbitrage opportunity."""
        # Find best supply rate
        supply_result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)
        # Find best borrow rate
        borrow_result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.BORROW)

        assert supply_result.best_rate is not None
        assert borrow_result.best_rate is not None

        # Calculate spread
        spread = supply_result.best_rate.apy_percent - borrow_result.best_rate.apy_percent
        # Morpho has 5.5% supply, 5.5% borrow = 0 spread
        assert spread == Decimal("0")

    @pytest.mark.asyncio
    async def test_cross_protocol_comparison(self, mocked_monitor: RateMonitor) -> None:
        """Test comparing rates across all protocols."""
        result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)

        # Sort by APY
        sorted_rates = sorted(result.all_rates, key=lambda r: r.apy_percent, reverse=True)

        # Morpho (5.5%) is best; compound + spark tie at 5.0% (spark takes its
        # manifest default now that it joined the lane); Aave (4.5%) is last.
        assert sorted_rates[0].protocol == "morpho_blue"
        assert sorted_rates[0].apy_percent == Decimal("5.5")
        assert {sorted_rates[1].protocol, sorted_rates[2].protocol} == {"compound_v3", "spark"}
        assert sorted_rates[1].apy_percent == Decimal("5.0")
        assert sorted_rates[2].apy_percent == Decimal("5.0")
        assert sorted_rates[3].protocol == "aave_v3"


# =============================================================================
# VIB-5729 — market-scoped rates: version-skew guard + cache scoping
# =============================================================================


class TestVib5729MarketScopedRateGuards:
    """A market-scoped rate must be provably from the market we asked for.

    ``market_id`` is an OPTIONAL proto3 request field, so a gateway sidecar older
    than VIB-5729 silently DROPS it and answers with a best-across-markets rate.
    Hosted can run exactly that pairing (gateway image lagging the framework), so
    "proto3 is backward compatible" is true on the wire and unsafe in behaviour:
    the caller would record another market's rate as if measured.

    The defence is a server echo of the market the PROVIDER actually read. These
    tests pin that the client refuses anything it cannot verify.
    """

    def _response(self, *, market_id: str, success: bool = True):
        from types import SimpleNamespace

        return SimpleNamespace(
            success=success,
            error="",
            source="on_chain",
            market_id=market_id,
            point=SimpleNamespace(
                supply_apy_pct="3.1987", borrow_apy_pct="", utilization_pct="90.7", timestamp=0
            ),
        )

    def _assert_scope(self, response, requested):
        from almanak.framework.data.rates.monitor import _assert_market_scope_honoured

        return _assert_market_scope_honoured(
            response, requested=requested, protocol="morpho_blue", chain="robinhood", token="USDG"
        )

    def test_matching_echo_is_accepted(self):
        target = "0xc845da65a020ddca5f132efa8fea79676d8edfdea504226a4c01e7a9e34cddd6"
        self._assert_scope(self._response(market_id=target), target)  # does not raise

    def test_echo_comparison_is_case_insensitive(self):
        target = "0xC845DA65A020DDCA5F132EFA8FEA79676D8EDFDEA504226A4C01E7A9E34CDDD6"
        self._assert_scope(self._response(market_id=target.lower()), target)  # does not raise

    def test_absent_echo_is_rejected_old_gateway(self):
        """THE rollout guard: an old gateway drops the request field and cannot
        set the echo. Without this check its unscoped rate would be recorded as
        this market's measured rate."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        target = "0xc845da65a020ddca5f132efa8fea79676d8edfdea504226a4c01e7a9e34cddd6"
        with pytest.raises(DataSourceUnavailable) as exc:
            self._assert_scope(self._response(market_id=""), target)
        assert "not honoured" in str(exc.value)

    def test_mismatched_echo_is_rejected(self):
        """A provider that answered about a DIFFERENT market is refused."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        target = "0xc845da65a020ddca5f132efa8fea79676d8edfdea504226a4c01e7a9e34cddd6"
        other = "0x919a9b6b94dae7c86620eaf7a08e597aae8a4c3a9e9c7671771fbaf62b6b61c7"
        with pytest.raises(DataSourceUnavailable):
            self._assert_scope(self._response(market_id=other), target)

    def test_unscoped_request_ignores_the_echo(self):
        """Back-compat: a caller that asked for no market proves nothing."""
        self._assert_scope(self._response(market_id=""), None)  # does not raise
        self._assert_scope(self._response(market_id="0xanything"), None)  # does not raise

    def test_cache_key_separates_markets_lending_the_same_token(self):
        """Two isolated markets can lend ONE token at different rates, so a
        market-blind cache key would serve the first market's rate for the
        second — a wrong number, from our own cache."""
        from almanak.framework.data.rates.monitor import RateMonitor

        a = "0xc845da65a020ddca5f132efa8fea79676d8edfdea504226a4c01e7a9e34cddd6"
        b = "0x919a9b6b94dae7c86620eaf7a08e597aae8a4c3a9e9c7671771fbaf62b6b61c7"
        key = RateMonitor._cache_side_key

        assert key("borrow", a) != key("borrow", b)
        assert key("borrow", a) == key("borrow", a.upper())  # case-insensitive
        assert key("borrow", None) == "borrow"  # unscoped lane unchanged
        assert key("borrow", a) != key("borrow", None)  # scoped never aliases unscoped


class TestBuildLendingRateFromPoint:
    """Direct cover for ``_build_lending_rate_from_point`` (VIB-5729).

    The wire->dataclass decoder every gateway rate read passes through, and it
    was almost entirely uncovered (CRAP gate: cc=6 at 9% coverage). Its branches
    are all Empty != Zero decisions, so they are worth pinning explicitly rather
    than exercising incidentally through the RPC path.
    """

    @staticmethod
    def _response(*, supply="", borrow="", util="", source="on_chain", market_id=""):
        from types import SimpleNamespace

        return SimpleNamespace(
            source=source,
            market_id=market_id,
            point=SimpleNamespace(supply_apy_pct=supply, borrow_apy_pct=borrow, utilization_pct=util),
        )

    def _build(self, response, side="supply"):
        from almanak.framework.data.rates.monitor import _build_lending_rate_from_point

        return _build_lending_rate_from_point(
            response, protocol="morpho_blue", token="USDG", side=side, chain="robinhood"
        )

    def test_supply_side_decodes_the_supply_field(self):
        rate = self._build(self._response(supply="3.1987", borrow="9.99"))
        assert rate.apy_percent == Decimal("3.1987")
        assert rate.side == "supply"
        assert rate.protocol == "morpho_blue"
        assert rate.token == "USDG"
        assert rate.chain == "robinhood"

    def test_borrow_side_decodes_the_borrow_field(self):
        rate = self._build(self._response(supply="9.99", borrow="3.5343"), side="borrow")
        assert rate.apy_percent == Decimal("3.5343")
        assert rate.side == "borrow"

    def test_apy_ray_is_derived_from_percent(self):
        from almanak.framework.data.rates.monitor import RAY

        rate = self._build(self._response(supply="5"))
        assert rate.apy_ray == Decimal("5") * RAY / Decimal("100")

    def test_utilisation_is_parsed_when_present(self):
        rate = self._build(self._response(supply="3.1987", util="90.70"))
        assert rate.utilization_percent == Decimal("90.70")

    def test_absent_utilisation_is_unmeasured_not_zero(self):
        """Empty != Zero: no utilisation on the wire => None, never Decimal(0)."""
        rate = self._build(self._response(supply="3.1987", util=""))
        assert rate.utilization_percent is None

    def test_missing_apy_for_the_requested_side_raises(self):
        """The OTHER side being populated must not rescue the requested side."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        with pytest.raises(DataSourceUnavailable) as exc:
            self._build(self._response(supply="", borrow="3.5343"), side="supply")
        assert "no supply APY" in str(exc.value)

    def test_market_id_echo_is_carried_onto_the_rate(self):
        """VIB-5729: callers discovering a market from a rate read rate.market_id."""
        target = "0xc845da65a020ddca5f132efa8fea79676d8edfdea504226a4c01e7a9e34cddd6"
        rate = self._build(self._response(supply="3.1987", market_id=target))
        assert rate.market_id == target

    def test_empty_market_id_echo_is_none_not_empty_string(self):
        """An unscoped venue makes no market-scoping claim — None, not ""."""
        assert self._build(self._response(supply="3.1987", market_id="")).market_id is None

    def test_whitespace_market_id_echo_is_none(self):
        assert self._build(self._response(supply="3.1987", market_id="   ")).market_id is None

    def test_response_without_market_id_attribute_is_tolerated(self):
        """An older gateway stub has no market_id field at all — must not raise."""
        from types import SimpleNamespace

        legacy = SimpleNamespace(
            source="on_chain",
            point=SimpleNamespace(supply_apy_pct="3.1987", borrow_apy_pct="", utilization_pct=""),
        )
        rate = self._build(legacy)
        assert rate.market_id is None
        assert rate.apy_percent == Decimal("3.1987")


class TestVib5729EchoGuardIsNotDecorative:
    """End-to-end proof that the rollout guard actually blocks a wrong rate.

    Asserting that ``_assert_market_scope_honoured`` raises in isolation only
    proves the function works. What matters is the whole lane: a gateway that
    ignores ``market_id`` (an older sidecar) must not be able to get a rate past
    ``_fetch_lending_rate_via_gateway`` into a caller. These tests drive the real
    call chain with a stubbed transport, so if the guard were ever bypassed —
    reordered after the decode, wrapped in a swallow, or dropped — they fail.
    """

    MARKET_A = "0xc845da65a020ddca5f132efa8fea79676d8edfdea504226a4c01e7a9e34cddd6"
    MARKET_B = "0x919a9b6b94dae7c86620eaf7a08e597aae8a4c3a9e9c7671771fbaf62b6b61c7"

    def _client(self, *, echo: str, borrow="2.7744"):
        """A gateway stub returning a real rate carrying ``echo`` as its market."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        response = SimpleNamespace(
            success=True,
            error="",
            source="on_chain",
            market_id=echo,
            point=SimpleNamespace(supply_apy_pct="", borrow_apy_pct=borrow, utilization_pct="72.45"),
        )
        client = MagicMock()
        client.is_connected = True
        client.rate_history.GetLendingRateCurrent = MagicMock(return_value=response)
        return client

    def _fetch(self, monkeypatch, client, market_id):
        import asyncio

        from almanak.framework.data.rates import monitor as m

        monkeypatch.setattr(
            m, "_monitor_get_connected_gateway_client", lambda: (client, m.gateway_pb2)
            if hasattr(m, "gateway_pb2") else (client, __import__("almanak.gateway.proto.gateway_pb2", fromlist=["x"]))
        )
        mon = m.RateMonitor(chain="robinhood", _internal=True)
        return asyncio.run(mon._fetch_lending_rate_via_gateway("morpho_blue", "USDG", "borrow", market_id))

    def test_old_gateway_absent_echo_cannot_deliver_a_rate(self, monkeypatch):
        """The version-skew case: old sidecar drops market_id, sets no echo.

        The response carries a perfectly valid-looking 2.7744% — the OTHER
        market's rate. It must not reach the caller.
        """
        from almanak.framework.data.interfaces import DataSourceUnavailable

        with pytest.raises(DataSourceUnavailable) as exc:
            self._fetch(monkeypatch, self._client(echo=""), self.MARKET_A)
        assert "not honoured" in str(exc.value)

    def test_mismatched_echo_cannot_deliver_a_rate(self, monkeypatch):
        """A provider that answered about a DIFFERENT market is blocked."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        with pytest.raises(DataSourceUnavailable):
            self._fetch(monkeypatch, self._client(echo=self.MARKET_B), self.MARKET_A)

    def test_matching_echo_delivers_the_rate_and_carries_the_market(self, monkeypatch):
        """The guard must not be so tight it blocks the honest path."""
        rate = self._fetch(monkeypatch, self._client(echo=self.MARKET_A, borrow="3.5343"), self.MARKET_A)
        assert rate.apy_percent == Decimal("3.5343")
        assert rate.market_id == self.MARKET_A  # echo propagated (codex P2)

    def test_unscoped_read_is_unaffected_by_the_guard(self, monkeypatch):
        """Aave-family lane: no market_id sent => no proof demanded."""
        rate = self._fetch(monkeypatch, self._client(echo=""), None)
        assert rate.apy_percent == Decimal("2.7744")
        assert rate.market_id is None
