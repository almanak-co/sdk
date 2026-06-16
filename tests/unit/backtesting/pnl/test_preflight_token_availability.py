"""Tests for preflight token-availability classification and the CLI address map.

Covers two units extracted during the TOKEN_IDS-removal change:

- ``classify_token_availability`` (engine.py): the preflight Check-2 strategy
  picker. Pins membership vs probe-fetch selection and the Refinement R3
  transient-vs-miss distinction (a resolution miss is unavailable; a transient
  error propagates and is NOT misreported).
- ``build_token_address_map`` (run_helpers.py): the SYMBOL -> (chain, address)
  map builder (Refinement R1). Pins token_funding ingestion, registry
  resolution of remaining tracked symbols, native skipping, and honest misses.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.backtesting.pnl.engine import classify_token_availability

_TS = datetime(2024, 1, 1, tzinfo=UTC)


class _RateLimitError(Exception):
    """Stand-in transient error (not a ValueError)."""


class TestClassifyTokenAvailability:
    @pytest.mark.asyncio
    async def test_membership_provider_uses_set_no_io(self) -> None:
        """A fixed-allowlist provider classifies by membership without get_price."""
        provider = MagicMock()
        provider.supported_tokens = ["WETH", "USDC"]
        provider.resolution_based_availability = False
        provider.get_price = AsyncMock(side_effect=AssertionError("must not probe"))

        available, unavailable = await classify_token_availability(provider, ["WETH", "ARB"], _TS)

        assert available == ["WETH"]
        assert unavailable == ["ARB"]
        provider.get_price.assert_not_called()

    @pytest.mark.asyncio
    async def test_resolution_based_probe_classifies_on_fetch(self) -> None:
        """Resolution-based provider probe-fetches: success -> available, ValueError -> unavailable."""
        provider = MagicMock()
        provider.supported_tokens = ["WETH"]  # membership-only, not authoritative
        provider.resolution_based_availability = True

        async def fake_get_price(token: str, ts: datetime) -> object:
            if token.upper() == "FOOBAR":
                raise ValueError("Unknown token: FOOBAR")
            return 1

        provider.get_price = AsyncMock(side_effect=fake_get_price)

        available, unavailable = await classify_token_availability(provider, ["WETH", "FOOBAR"], _TS)

        assert available == ["WETH"]
        assert unavailable == ["FOOBAR"]

    @pytest.mark.asyncio
    async def test_resolution_based_transient_error_propagates(self) -> None:
        """R3: a transient (non-ValueError) error from get_price propagates, not a miss."""
        provider = MagicMock()
        provider.supported_tokens = ["WETH"]
        provider.resolution_based_availability = True
        provider.get_price = AsyncMock(side_effect=_RateLimitError("429"))

        with pytest.raises(_RateLimitError):
            await classify_token_availability(provider, ["WETH"], _TS)

    @pytest.mark.asyncio
    async def test_resolution_based_transient_valueerror_propagates(self) -> None:
        """R3: a timeout/network failure (surfaced as ValueError) propagates, NOT a false miss.

        _make_request wraps transient timeout/network errors as ValueError, so a
        bare `except ValueError -> unavailable` would abort a backtest on a
        network blip. The transient screen must re-raise it instead.
        """
        provider = MagicMock()
        provider.supported_tokens = ["WETH"]
        provider.resolution_based_availability = True
        provider.get_price = AsyncMock(side_effect=ValueError("Request timed out after 30s"))

        with pytest.raises(ValueError, match="timed out"):
            await classify_token_availability(provider, ["WSTETH"], _TS)

    @pytest.mark.asyncio
    async def test_cash_equivalent_short_circuits_without_probe(self) -> None:
        """USDC/USDT/DAI are valued as cash at $1: available without a get_price probe.

        A transient error on a stablecoin (a token that needs no market price)
        must never reach the probe and abort preflight.
        """
        provider = MagicMock()
        provider.resolution_based_availability = True
        provider.supported_tokens = []
        provider.get_price = AsyncMock(side_effect=AssertionError("must not probe cash-equivalents"))

        available, unavailable = await classify_token_availability(provider, ["USDC", "USDT", "DAI"], _TS)

        assert set(available) == {"USDC", "USDT", "DAI"}
        assert unavailable == []
        provider.get_price.assert_not_called()

    @pytest.mark.asyncio
    async def test_minimal_provider_without_get_price_degrades(self) -> None:
        """A provider with no supported_tokens and no get_price degrades to unavailable."""
        provider = MagicMock(spec=[])  # no attributes at all

        available, unavailable = await classify_token_availability(provider, ["WETH"], _TS)

        assert available == []
        assert unavailable == ["WETH"]


class TestBuildTokenAddressMap:
    def _resolver(self, mapping: dict[str, object]) -> MagicMock:
        """A fake TokenResolver whose resolve() returns the mapped ResolvedToken-likes."""
        resolver = MagicMock()

        def resolve(symbol: str, chain: str) -> object:
            from almanak.framework.data.tokens.exceptions import TokenNotFoundError

            key = symbol.upper()
            if key not in mapping:
                raise TokenNotFoundError(token=symbol, chain=chain, reason="not found")
            return mapping[key]

        resolver.resolve = MagicMock(side_effect=resolve)
        return resolver

    def _patch_resolver(self, monkeypatch, mapping: dict[str, object]) -> MagicMock:
        """Patch the lazily-imported ``get_token_resolver`` to return a fake.

        ``build_token_address_map`` imports ``get_token_resolver`` from
        ``almanak.framework.data.tokens`` *inside* the function, so the patch
        target is that package attribute.
        """
        import almanak.framework.data.tokens as tokens_pkg

        resolver = self._resolver(mapping)
        monkeypatch.setattr(tokens_pkg, "get_token_resolver", lambda *a, **k: resolver, raising=False)
        return resolver

    def test_token_funding_entries_seed_map(self, monkeypatch) -> None:
        from almanak.framework.cli.backtest import run_helpers

        self._patch_resolver(monkeypatch, {})

        cfg = {
            "token_funding": [
                {
                    "symbol": "USDC",
                    "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    "amount": "5000",
                    "amount_type": "usd",
                }
            ]
        }
        result = run_helpers.build_token_address_map(cfg, ["USDC"], "arbitrum")

        assert result["USDC"] == ("arbitrum", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

    def test_native_symbol_is_skipped(self, monkeypatch) -> None:
        from almanak.framework.cli.backtest import run_helpers

        resolver = self._patch_resolver(monkeypatch, {})

        # WETH is wrapped-native on arbitrum -> registry-resolved by the provider,
        # so it should NOT be added to the address map (and resolve() not consulted).
        result = run_helpers.build_token_address_map({}, ["WETH", "ETH"], "arbitrum")

        assert "WETH" not in result
        assert "ETH" not in result
        resolver.resolve.assert_not_called()

    def test_registry_resolves_remaining_tracked_symbol(self, monkeypatch) -> None:
        from almanak.framework.cli.backtest import run_helpers

        resolved = MagicMock()
        resolved.address = "0x912CE59144191C1204E64559FE8253a0e49E6548"
        resolved.is_native = False
        self._patch_resolver(monkeypatch, {"ARB": resolved})

        result = run_helpers.build_token_address_map({}, ["ARB"], "arbitrum")

        assert result["ARB"] == ("arbitrum", "0x912CE59144191C1204E64559FE8253a0e49E6548")

    def test_native_resolution_result_is_not_mapped(self, monkeypatch) -> None:
        from almanak.framework.cli.backtest import run_helpers

        # A symbol the registry resolves to a *native* token must not be added:
        # the provider resolves natives via the chain registry, no address needed.
        resolved = MagicMock()
        resolved.address = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
        resolved.is_native = True
        self._patch_resolver(monkeypatch, {"GASLIKE": resolved})

        result = run_helpers.build_token_address_map({}, ["GASLIKE"], "arbitrum")

        assert "GASLIKE" not in result

    def test_unresolvable_symbol_is_honest_miss(self, monkeypatch) -> None:
        from almanak.framework.cli.backtest import run_helpers

        self._patch_resolver(monkeypatch, {})

        # Neither native nor registry-resolvable -> left out of the map entirely
        # (becomes a preflight miss, never a fabricated price).
        result = run_helpers.build_token_address_map({}, ["NOTAREALTOKEN"], "arbitrum")

        assert "NOTAREALTOKEN" not in result

    def test_funding_takes_precedence_no_double_resolve(self, monkeypatch) -> None:
        from almanak.framework.cli.backtest import run_helpers

        resolver = self._patch_resolver(monkeypatch, {})

        cfg = {
            "token_funding": [
                {
                    "symbol": "USDC",
                    "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    "amount": "5000",
                    "amount_type": "usd",
                }
            ]
        }
        result = run_helpers.build_token_address_map(cfg, ["USDC"], "arbitrum")

        assert result["USDC"][1] == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        # Already covered by funding -> resolver.resolve never called for USDC.
        resolver.resolve.assert_not_called()


# =============================================================================
# Priceability guard: run_preflight_validation severity escalation.
#
# A tracked NON-cash token that a *resolution-based* provider (CoinGecko) cannot
# price for the window is a BLOCKING preflight error (severity="error" ->
# report.passed False -> the run aborts loudly via PreflightValidationError).
# Cash-equivalent stablecoins are exempt (valued as cash at $1), and
# NON-resolution-based providers (membership / mock best-effort) stay on the
# historical non-blocking warning path so unrelated backtests never hard-stop.
# =============================================================================


class _FakePriceProvider:
    """Minimal historical provider exercising run_preflight_validation Check 2.

    Implements only the surface preflight reads: ``provider_name``,
    ``historical_capability`` (FULL so Check 1 passes), ``resolution_based_availability``,
    ``supported_tokens``, and ``get_price`` (raises ``ValueError`` for unpriceable
    symbols, mirroring a resolution miss). Deliberately omits
    ``verify_archive_access`` / ``min_timestamp`` / ``max_timestamp`` so Checks 3
    and 4 are skipped / pass by default.
    """

    provider_name = "fake"

    def __init__(self, *, resolution_based: bool, unavailable: set[str]) -> None:
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability

        self.historical_capability = HistoricalDataCapability.FULL
        self.resolution_based_availability = resolution_based
        self.supported_tokens: list[str] = []
        self._unavailable = {t.upper() for t in unavailable}

    async def get_price(self, token: str, ts: datetime) -> object:
        if token.upper() in self._unavailable:
            raise ValueError(f"Unknown token: {token}")
        return 1


def _preflight_backtester(provider: object):
    from almanak.framework.backtesting.pnl.engine import PnLBacktester

    return PnLBacktester(data_provider=provider, fee_models={}, slippage_models={})


def _config(tokens: list[str]):
    from decimal import Decimal

    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 2, 1, tzinfo=UTC),
        initial_capital_usd=Decimal("10000"),
        tokens=tokens,
    )


def _token_check(report):
    return next(c for c in report.checks if c.check_name == "token_availability")


class TestPriceabilityGuard:
    @pytest.mark.asyncio
    async def test_resolution_based_noncash_miss_is_blocking_error(self) -> None:
        """CoinGecko-style provider: a non-cash token with no price -> error, report fails."""
        provider = _FakePriceProvider(resolution_based=True, unavailable={"WSTETH"})
        report = await _preflight_backtester(provider).run_preflight_validation(_config(["WSTETH", "USDC"]))

        check = _token_check(report)
        assert check.severity == "error"
        assert check.passed is False
        assert "WSTETH" in check.details["blocking_unavailable"]
        assert report.passed is False

    @pytest.mark.asyncio
    async def test_resolution_based_cash_only_unpriceable_does_not_block(self) -> None:
        """Cash-equivalent stablecoins are valued at $1; an unpriceable USDC must not block.

        The cash short-circuit in classify_token_availability marks USDC available
        without probing, so preflight passes cleanly (not even a warning) even
        though the provider would have raised for it.
        """
        provider = _FakePriceProvider(resolution_based=True, unavailable={"USDC"})
        report = await _preflight_backtester(provider).run_preflight_validation(_config(["WETH", "USDC"]))

        check = _token_check(report)
        assert check.passed is True
        assert "USDC" in check.details["available"]
        assert report.passed is True

    @pytest.mark.asyncio
    async def test_non_resolution_provider_miss_stays_warning(self) -> None:
        """Regression gate: mock/membership providers never hard-stop on a best-effort miss."""
        provider = _FakePriceProvider(resolution_based=False, unavailable={"WSTETH"})
        report = await _preflight_backtester(provider).run_preflight_validation(_config(["WSTETH", "USDC"]))

        check = _token_check(report)
        assert check.severity == "warning"
        assert report.passed is True


class TestBuildTokenAvailabilityCheck:
    """Direct unit coverage of the extracted priceability-guard helper."""

    @staticmethod
    def _provider(resolution_based: bool):
        p = MagicMock()
        p.resolution_based_availability = resolution_based
        return p

    def test_all_available_passes_no_recs(self) -> None:
        from almanak.framework.backtesting.pnl.engine import _build_token_availability_check

        check, recs = _build_token_availability_check(self._provider(True), ["WETH", "USDC"], [])
        assert check.passed is True
        assert recs == []

    def test_resolution_based_noncash_is_error(self) -> None:
        from almanak.framework.backtesting.pnl.engine import _build_token_availability_check

        check, recs = _build_token_availability_check(self._provider(True), ["USDC"], ["WSTETH"])
        assert check.severity == "error"
        assert check.passed is False
        assert "WSTETH" in check.details["blocking_unavailable"]
        assert any("allow-missing-prices" in r for r in recs)

    def test_resolution_based_cash_only_is_warning(self) -> None:
        from almanak.framework.backtesting.pnl.engine import _build_token_availability_check

        check, _ = _build_token_availability_check(self._provider(True), ["WETH"], ["USDC"])
        assert check.severity == "warning"

    def test_non_resolution_provider_is_warning(self) -> None:
        from almanak.framework.backtesting.pnl.engine import _build_token_availability_check

        check, _ = _build_token_availability_check(self._provider(False), ["USDC"], ["WSTETH"])
        assert check.severity == "warning"
