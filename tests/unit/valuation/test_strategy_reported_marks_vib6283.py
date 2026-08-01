"""VIB-6283 limb 2 — a strategy self-report must never read as a measurement.

Field origin (mainnet batch ``20260731-1745-lpdash4``, 2026-07-31): a Uniswap V4
LP leg on Base whose real cost basis was **$2.6373** and whose realized PnL was
**-$0.0026** persisted ``portfolio_snapshots.total_value_usd = 4.4434`` at
**HIGH** confidence for 48 consecutive snapshots. The dashboard rendered
``Strategy PnL +$1.80 (+68.28%)`` and, because max-drawdown is a running extreme,
a **-23.1% max DD** badge that stayed wrong permanently even after the mark
corrected at close.

Three independent defects produced that, and each has its own class below:

* **D1** ``portfolio_valuer.py`` generic LP tail returned the strategy's own
  ``value_usd`` with ``repriced=True`` and NO marker → HIGH confidence.
* **D2** ``_is_v4_lp_position`` gates on a 64-hex PoolKey hash the shipped V4
  demo never emits, so the whole V4 valuation ladder was dead in production and
  a V4 tokenId fell into the Uniswap-V3 NFT read.
* **D4** the drawdown fold keeps every ``ESTIMATED`` sample by design, so
  demoting confidence alone could not have removed the phantom badge.

**Why this file builds its fixture from the SHIPPED strategy.** The reason D2
survived is a process defect, not a coding one: every test in
``test_v4_lp_valuation_vib5018.py`` builds ``_v4_position()`` with a 64-hex
``pool_address``, a shape the shipped demo never produced. The fixture shape and
the production shape had never met, so a fully-tested valuation path was dead in
the box. :func:`shipped_v4_position` therefore calls the demo's REAL
``get_open_positions()`` body — if someone changes the details it emits, these
tests follow.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.portfolio.models import STRATEGY_REPORTED_VALUATION_SOURCE
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

# The exact numbers observed on mainnet, so a regression reproduces the incident
# rather than an abstraction of it.
_CONFIG_AMOUNT0 = Decimal("0.0012")  # WETH the strategy REQUESTED (config.json)
_CONFIG_AMOUNT1 = Decimal("2.2")  # USDC the strategy REQUESTED
_WETH_PRICE = Decimal("1869.53")
_PHANTOM_VALUE = _CONFIG_AMOUNT0 * _WETH_PRICE + _CONFIG_AMOUNT1  # 4.443436
_MEASURED_AMOUNT0_WEI = 356737985951854  # 0.000356737985951854 WETH actually minted
_MEASURED_AMOUNT1_WEI = 1970092  # 1.970092 USDC actually minted
_MEASURED_COST_BASIS = Decimal("2.637320077727371026344289670")
_POSITION_ID = "2886046"


class _Market:
    """Minimal MarketDataSource: only ``price`` is reached by these paths."""

    def price(self, symbol: str, chain: str | None = None) -> Decimal:
        return {"WETH": _WETH_PRICE, "USDC": Decimal("1")}[symbol]


def shipped_v4_open_position(*, pool_id: str | None) -> PositionInfo:
    """Invoke the SHIPPED demo's own ``get_open_positions()`` and return its row.

    Constructed with ``object.__new__`` + only the attributes the method reads, so
    the REAL method body runs without needing a gateway, a config file, or a
    chain. That is the whole point: the assertion "the shipped strategy emits no
    pool_id" is only meaningful if the shipped strategy produced the fixture.
    """
    from almanak.demo_strategies.uniswap_v4_hooks.strategy import UniswapV4HooksStrategy

    strategy = object.__new__(UniswapV4HooksStrategy)
    strategy._current_position_id = _POSITION_ID
    strategy._chain = "base"
    strategy._deployment_id = "deployment:ca41637eaebc"
    strategy.pool = "WETH/USDC/3000"
    strategy.token0_symbol = "WETH"
    strategy.token1_symbol = "USDC"
    strategy.hook_address = "0x" + "0" * 40
    strategy.hook_flags = MagicMock(active_flags=[])
    # Pool discovery is best-effort and fails silently in production; ``None`` is
    # the state that produced the incident.
    strategy._pool_discovery = MagicMock(pool_id=pool_id) if pool_id else None
    strategy.amount0 = _CONFIG_AMOUNT0
    strategy.amount1 = _CONFIG_AMOUNT1
    strategy.create_market_snapshot = lambda: _Market()  # type: ignore[method-assign]

    return strategy.get_open_positions().positions[0]


def shipped_v4_position(value_usd: Decimal = _PHANTOM_VALUE) -> PositionInfo:
    """The position the SHIPPED ``uniswap_v4_hooks`` demo actually reports.

    Deliberately built by invoking the demo's own ``get_open_positions()`` rather
    than hand-authoring a details dict. A hand-authored fixture is what let D2
    hide: it can drift from the shipped strategy without any test noticing, and
    the ONE property that mattered — "the shipped strategy emits no pool_id" —
    is invisible unless the fixture comes from the strategy itself.

    ``_pool_discovery`` is left ``None`` (pool discovery is best-effort and its
    failure path is silent), which is exactly the production state that produced
    the incident.
    """
    position = shipped_v4_open_position(pool_id=None)
    # The caller may want a different reported mark; identity/details stay the
    # shipped ones.
    return PositionInfo(
        position_type=position.position_type,
        position_id=position.position_id,
        chain=position.chain,
        protocol=position.protocol,
        value_usd=value_usd,
        details=dict(position.details),
    )


def _valuer_with_open_event(*, amount0_wei: int, amount1_wei: int) -> PortfolioValuer:
    """A valuer whose accounting store holds the REAL receipt-parsed OPEN row."""
    store = MagicMock()
    store.get_position_events_sync.return_value = [
        {
            "position_id": _POSITION_ID,
            "position_type": "LP",
            "event_type": "OPEN",
            "token0": "WETH",
            "token1": "USDC",
            "amount0": str(amount0_wei),
            "amount1": str(amount1_wei),
            "value_usd": str(_MEASURED_COST_BASIS),
        }
    ]
    valuer = PortfolioValuer(gateway_client=None)
    valuer._accounting_store = store
    valuer._deployment_id = "deployment:ca41637eaebc"
    return valuer


# ---------------------------------------------------------------------------
# The shipped-shape contract (the process defect that hid D2)
# ---------------------------------------------------------------------------


class TestShippedStrategyShape:
    def test_shipped_demo_emits_no_64_hex_pool_id_when_discovery_did_not_run(self):
        """The premise of D2, asserted against the real strategy.

        If this ever fails it means the shipped demo started emitting a pool id
        even without discovery — good news, but the shape-gate regression tests
        below would then be testing a case production no longer produces, so
        they must be re-derived rather than left silently vacuous.
        """
        position = shipped_v4_position()
        assert PortfolioValuer._extract_v4_pool_id(position) is None
        assert PortfolioValuer._is_v4_lp_position(position) is False

    def test_shipped_demo_publishes_pool_id_once_discovery_has_run(self):
        """D2b — the id we already computed must reach the valuer."""
        pool_id = "0x" + "ab" * 32
        position = shipped_v4_open_position(pool_id=pool_id)
        assert position.details["pool_id"] == pool_id
        assert PortfolioValuer._is_v4_lp_position(position) is True

    def test_teardown_safety_lane_still_receives_a_position_size(self):
        """Regression guard for a fix that would have LOOSENED a safety cap.

        ``get_open_positions`` feeds ``TeardownPositionSummary``, whose total
        drives ``calculate_max_acceptable_loss`` — a $0 total selects the most
        PERMISSIVE 3% loss cap (``teardown/discovery.py:to_teardown_summary``).
        "Stop the strategy inventing money" is right for VALUATION and wrong
        here: the mark must stay non-zero for the safety lane. Valuation safety
        comes from the framework refusing to trust it, not from zeroing it.
        """
        position = shipped_v4_position()
        assert position.value_usd > 0


# ---------------------------------------------------------------------------
# D1 — a self-report may never be HIGH
# ---------------------------------------------------------------------------


class TestStrategyReportedNeverHigh:
    def test_generic_lp_repricer_miss_stamps_unmeasured_provenance(self):
        """The mainnet incident, reproduced end-to-end at the dispatch boundary."""
        valuer = PortfolioValuer(gateway_client=None)
        position = PositionInfo(
            position_type=PositionType.LP,
            position_id=_POSITION_ID,
            chain="arbitrum",
            protocol="some_amm_without_a_repricer",
            value_usd=_PHANTOM_VALUE,
            details={"pool": "WETH/USDC/3000", "token0": "WETH", "token1": "USDC"},
        )

        value_usd, details, repriced = valuer._reprice_position_enriched(position, "arbitrum", _Market())

        # The mark is carried (the position must not vanish from NAV) ...
        assert value_usd == _PHANTOM_VALUE
        assert repriced is True
        # ... but it can never read as measured.
        assert details["valuation_source"] == STRATEGY_REPORTED_VALUATION_SOURCE
        assert details["valuation_status"] == "estimated"

    def test_confidence_derivation_demotes_a_strategy_reported_snapshot(self):
        """The stamp must actually move snapshot confidence off HIGH.

        Guards the seam between producer and confidence policy: a marker nothing
        reads is the same as no marker at all.
        """
        marked = MagicMock(details=PortfolioValuer._strategy_reported_details())
        confidence = PortfolioValuer._determine_value_confidence(
            positions=[marked],
            wallet_balances=[],
            positions_unavailable=False,
            wallet_data_incomplete=False,
            stable_depeg=False,
        )
        assert confidence.value == "ESTIMATED"

    def test_a_measured_zero_is_still_refused_as_a_mark(self):
        """Empty != Zero: a non-positive self-report yields no_path, not a $0 mark."""
        valuer = PortfolioValuer(gateway_client=None)
        position = PositionInfo(
            position_type=PositionType.LP,
            position_id=_POSITION_ID,
            chain="arbitrum",
            protocol="some_amm_without_a_repricer",
            value_usd=Decimal("0"),
            details={"pool": "WETH/USDC/3000"},
        )
        _value, _details, repriced = valuer._reprice_position_enriched(position, "arbitrum", _Market())
        assert repriced is False

    def test_no_lp_branch_returns_an_unmarked_trusted_self_report(self):
        """Census guard — the defect was ONE branch out of four disagreeing.

        The fungible and Curve tails already refused a strategy mark; the generic
        tail did not, and ``_v4_no_path`` half-did. Rather than assert each
        branch separately (which a fifth branch would silently escape), assert
        the property over every LP dispatch outcome: a result that carries a
        non-zero value at ``repriced=True`` with NO ``valuation_source`` must
        have come from a real on-chain read.
        """
        valuer = PortfolioValuer(gateway_client=None)
        protocols = ["some_amm_without_a_repricer", "uniswap_v4", "curve", "fluid_dex_lp", "traderjoe_v2"]
        for protocol in protocols:
            position = PositionInfo(
                position_type=PositionType.LP,
                position_id=_POSITION_ID,
                chain="arbitrum",
                protocol=protocol,
                value_usd=_PHANTOM_VALUE,
                details={"pool": "WETH/USDC/3000", "token0": "WETH", "token1": "USDC"},
            )
            value_usd, details, repriced = valuer._reprice_position_enriched(position, "arbitrum", _Market())
            if repriced and value_usd == _PHANTOM_VALUE:
                assert details.get("valuation_source") == STRATEGY_REPORTED_VALUATION_SOURCE, (
                    f"{protocol}: echoed the strategy's mark with no provenance — "
                    "this is the VIB-6283 defect reappearing on a new branch"
                )


# ---------------------------------------------------------------------------
# D2 — routing, and the confidence inversion it caused
# ---------------------------------------------------------------------------


class TestV4RoutingByFamily:
    def test_shipped_shape_routes_to_the_v4_path_despite_having_no_pool_id(self):
        valuer = PortfolioValuer(gateway_client=None)
        position = shipped_v4_position()
        assert PortfolioValuer._is_v4_lp_position(position) is False
        assert PortfolioValuer._is_v4_family_protocol(position.protocol) is True

    def test_v4_family_position_never_reaches_the_v3_nft_reader(self):
        """The $289M mis-route VIB-5018 exists to prevent, re-armed for the shape
        that actually ships. A V4 tokenId read through ``positions(uint256)``
        returns an unrelated NFT."""
        valuer = PortfolioValuer(gateway_client=None)
        valuer._reprice_lp_on_chain_enriched = MagicMock(  # type: ignore[method-assign]
            side_effect=AssertionError("V4 position reached the Uniswap-V3 NFT reader")
        )
        valuer._reprice_position_enriched(shipped_v4_position(), "base", _Market())

    def test_v4_ladder_produces_the_measured_value_not_the_requested_one(self):
        """The headline fix: $4.4434 phantom becomes the real ~$2.637.

        Tier 2 re-marks the receipt-parsed OPEN amounts at live prices — the data
        was correct and persisted all along; nothing consumed it because the
        routing gate rejected the position first.
        """
        valuer = _valuer_with_open_event(amount0_wei=_MEASURED_AMOUNT0_WEI, amount1_wei=_MEASURED_AMOUNT1_WEI)

        value_usd, details, repriced = valuer._reprice_position_enriched(shipped_v4_position(), "base", _Market())

        assert repriced is True
        assert details["valuation_source"] == "v4_open_amounts"
        # Within a cent of the measured cost basis, and nowhere near the phantom.
        assert abs(value_usd - _MEASURED_COST_BASIS) < Decimal("0.01")
        assert abs(value_usd - _PHANTOM_VALUE) > Decimal("1.50")

    def test_failing_the_detector_never_outranks_passing_it(self):
        """Confidence-ordering invariant.

        Before the fix this was inverted: a position that PASSED the shape gate
        and then failed to value routed through ``_v4_no_path`` and was stamped
        ``estimated``, while one that FAILED the gate fell to the unmarked
        generic branch and persisted at HIGH. A routing miss must never buy more
        confidence than a valuation miss.
        """
        valuer = PortfolioValuer(gateway_client=None)
        shaped = shipped_v4_position()
        shaped.details["pool_id"] = "0x" + "cd" * 32

        _v0, details_unshaped, _r0 = valuer._reprice_position_enriched(shipped_v4_position(), "base", _Market())
        _v1, details_shaped, _r1 = valuer._reprice_position_enriched(shaped, "base", _Market())

        assert details_unshaped.get("valuation_status") == details_shaped.get("valuation_status") == "estimated"

    def test_family_routing_fails_safe_when_the_registry_is_unavailable(self):
        assert PortfolioValuer._is_v4_family_protocol("") is False
        assert PortfolioValuer._is_v4_family_protocol("not_a_registered_protocol") is False


# ---------------------------------------------------------------------------
# D4 — an unmeasured mark may not drive a running extreme
# ---------------------------------------------------------------------------


class TestDrawdownExcludesStrategyReported:
    @staticmethod
    def _row(total: str, cash: str, positions: list[dict[str, Any]], confidence: str = "HIGH"):
        payload = json.dumps({"schema_version": 1, "positions": positions})
        return ("2026-07-31T19:00:00Z", total, cash, "0", payload, confidence)

    @staticmethod
    def _position(value_usd: str, source: str | None):
        details = {"pool": "WETH/USDC/3000"}
        if source is not None:
            details["valuation_source"] = source
        return {"position_type": "LP", "protocol": "uniswap_v4", "value_usd": value_usd, "details": details}

    def test_the_mainnet_phantom_drawdown_is_no_longer_manufactured(self):
        """The incident in miniature: a frozen mark that evaporates at close.

        Peak is set while the phantom inflates NAV, trough after it disappears —
        the fold read that as a 23% loss on a position that lost cents.
        """
        from almanak.framework.dashboard.quant_aggregations import (
            _drawdown_stats,
            _wallet_navs_from_nav_text,
        )

        phantom = self._position(str(_PHANTOM_VALUE), STRATEGY_REPORTED_VALUATION_SOURCE)
        rows = [
            self._row("0", "6.07", []),
            self._row(str(_PHANTOM_VALUE), "3.43", [phantom]),
            self._row(str(_PHANTOM_VALUE), "3.43", [phantom]),
            self._row("0", "6.05", []),
        ]
        navs = _wallet_navs_from_nav_text(rows)
        max_dd, _current = _drawdown_stats(navs)
        assert max_dd < Decimal("1"), f"phantom still drives max-DD: {max_dd}%"

    def test_a_measured_mark_still_produces_a_real_drawdown(self):
        """Negative control — the gate must not mask genuine losses.

        This is the failure mode the UNAVAILABLE gate's authors warned about:
        skipping too much makes a strategy show 0% max-DD forever. A measured
        mark that really falls must still register.
        """
        from almanak.framework.dashboard.quant_aggregations import (
            _drawdown_stats,
            _wallet_navs_from_nav_text,
        )

        rows = [
            self._row("100", "0", [self._position("100", "on_chain")]),
            self._row("50", "0", [self._position("50", "on_chain")]),
        ]
        navs = _wallet_navs_from_nav_text(rows)
        max_dd, _current = _drawdown_stats(navs)
        assert max_dd == pytest.approx(50, abs=0.01)

    def test_estimated_but_measured_marks_are_still_folded(self):
        """``ESTIMATED`` alone must NOT be excluded — only ``strategy_reported``.

        A CEX estimate or a price-ratio reconstruction is priced, just
        imprecisely; dropping those would mask real drawdowns. This asserts the
        new gate keys on provenance, not on confidence.
        """
        from almanak.framework.dashboard.quant_aggregations import (
            _drawdown_stats,
            _wallet_navs_from_nav_text,
        )

        rows = [
            self._row("100", "0", [self._position("100", "v4_open_amounts")], confidence="ESTIMATED"),
            self._row("60", "0", [self._position("60", "v4_open_amounts")], confidence="ESTIMATED"),
        ]
        navs = _wallet_navs_from_nav_text(rows)
        max_dd, _current = _drawdown_stats(navs)
        assert max_dd == pytest.approx(40, abs=0.01)

    def test_gate_survives_an_already_deserialized_payload(self):
        """Hosted Postgres hands back parsed JSON, not text.

        A hand-rolled ``json.loads`` here would raise/return ``[]`` and the gate
        would silently never fire in hosted mode — the VIB-5170 inert-feature
        shape. Asserted directly because that failure is invisible locally.
        """
        from almanak.framework.dashboard.quant_aggregations import _has_strategy_reported_mark

        positions = [self._position("4.44", STRATEGY_REPORTED_VALUATION_SOURCE)]
        as_text = json.dumps({"schema_version": 1, "positions": positions})
        as_parsed_envelope = {"schema_version": 1, "positions": positions}
        as_bare_list = positions

        assert _has_strategy_reported_mark(as_text) is True
        assert _has_strategy_reported_mark(as_parsed_envelope) is True
        assert _has_strategy_reported_mark(as_bare_list) is True
        assert _has_strategy_reported_mark(None) is False
        assert _has_strategy_reported_mark("not json") is False
