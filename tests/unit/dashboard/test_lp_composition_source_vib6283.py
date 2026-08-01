"""VIB-6283 limb 2 (dashboard half) — the Position Status card must show the
position, not the wallet and not the entry.

Field origin (mainnet batch ``20260731-1745-lpdash4``): the uniswap_v3 leg was
valued CORRECTLY on-chain on all 48 snapshots — its persisted composition moved
0.000910 → 0.001355 WETH and 2.347 → 1.516 USDC as price traversed the range —
yet the dashboard's Position Status card showed ``0.0009713 WETH / 2.23 USDC``,
the LP_OPEN mint amounts, unchanged to the digit across all five open-phase
captures. Post-teardown it showed ``Position ID: None`` above
``WETH 0.001723 / USDC 1.56`` — the WALLET's residual balances.

Two wrong sources, no right one:

* ``_load_token_amounts`` read ``get_position()["token_balances"]`` — wallet
  balances, a different quantity from LP composition entirely;
* ``_hydrate_active_position_from_events`` then OVERWROTE that with the LP_OPEN
  event's mint amounts, using direct assignment where every sibling field in the
  same function uses ``setdefault``.

The correct source — the valuer's measured ``amount0``/``amount1``, already in
``portfolio_snapshots.positions_json`` — was never consulted.

This is the ONLY leg in that batch where the valuation and dashboard layers
disagreed, which is precisely what made this defect visible: on the V4 and
TraderJoe legs both layers were frozen at the same wrong number, so they agreed.
"""

from __future__ import annotations

from typing import Any

from almanak.framework.dashboard.templates.lp_dashboard import (
    LPDashboardConfig,
    _fmt_token_amount,
    _hydrate_active_position_from_events,
    _load_token_amounts,
    _measured_lp_composition,
)
from almanak.framework.portfolio.models import STRATEGY_REPORTED_VALUATION_SOURCE

_CONFIG = LPDashboardConfig(
    protocol="uniswap_v3",
    token0="WETH",
    token1="USDC",
    fee_tier="0.05%",
    chain="arbitrum",
)

# The real mainnet numbers: what the position WAS at mint vs what it became.
_MINT_AMOUNT0 = "0.0009713"
_MINT_AMOUNT1 = "2.23"
_LIVE_AMOUNT0 = "0.0013552687"
_LIVE_AMOUNT1 = "1.51617983"


def _measured_lp_row(amount0: str = _LIVE_AMOUNT0, amount1: str = _LIVE_AMOUNT1, source: str = "on_chain"):
    return {
        "position_type": "LP",
        "position_id": "5627514",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "value_usd": "4.0398",
        "details": {
            "amount0": amount0,
            "amount1": amount1,
            "token0_symbol": "WETH",
            "token1_symbol": "USDC",
            "valuation_source": source,
        },
    }


def _api_client(strategy_positions: list[dict[str, Any]], token_balances: list[dict[str, Any]] | None = None):
    class _Client:
        def get_position(self) -> dict[str, Any]:
            return {
                "strategy_positions": strategy_positions,
                # Wallet residuals — deliberately present and deliberately WRONG
                # for this panel, so a regression that re-reads them is caught.
                "token_balances": token_balances
                if token_balances is not None
                else [
                    {"symbol": "WETH", "balance": "0.001723", "value_usd": "3.2"},
                    {"symbol": "USDC", "balance": "1.56", "value_usd": "1.56"},
                ],
            }

    return _Client()


def _open_event(amount0_wei: str = "971300000000000", amount1_wei: str = "2230000"):
    """An LP_OPEN position_events row (wei, as the DB stores it)."""
    return [
        {
            "position_id": "5627514",
            "event_type": "OPEN",
            "timestamp": "2026-07-31T19:10:05Z",
            "amount0": amount0_wei,
            "amount1": amount1_wei,
            "tick_lower": -201100,
            "tick_upper": -200900,
            "value_usd": "4.049",
        }
    ]


class TestMeasuredCompositionWins:
    def test_live_composition_is_used_not_wallet_balances(self):
        result: dict[str, Any] = {}
        _load_token_amounts(_api_client([_measured_lp_row()]), result, _CONFIG)

        assert result["token0_amount"] == float(_LIVE_AMOUNT0)
        assert result["token1_amount"] == float(_LIVE_AMOUNT1)
        # The wallet residuals must not appear.
        assert result["token0_amount"] != 0.001723
        assert result["token1_amount"] != 1.56

    def test_open_event_amounts_cannot_overwrite_measured_composition(self):
        """The clobber, asserted in the real call order.

        ``_hydrate_active_position_from_events`` runs AFTER ``_load_token_amounts``
        in ``prepare_lp_session_state``, so a direct assignment there beat every
        measured source. Entry amounts may FILL, never OVERWRITE.
        """
        result: dict[str, Any] = {}
        _load_token_amounts(_api_client([_measured_lp_row()]), result, _CONFIG)
        _hydrate_active_position_from_events(result, _open_event(), _CONFIG)

        assert result["token0_amount"] == float(_LIVE_AMOUNT0), (
            "LP_OPEN mint amounts overwrote the measured composition — the freeze is back"
        )
        assert result["token1_amount"] == float(_LIVE_AMOUNT1)

    def test_open_event_amounts_still_fill_when_nothing_measured(self):
        """Entry amounts remain a legitimate LAST-RESORT fallback.

        Removing the overwrite must not remove the information: with no measured
        composition the card should still show something rather than nothing.
        """
        result: dict[str, Any] = {}
        _hydrate_active_position_from_events(result, _open_event(), _CONFIG)
        assert result.get("token0_amount") is not None
        assert result.get("token1_amount") is not None


class TestUnmeasuredRendersAsEmDash:
    def test_no_measured_composition_leaves_the_keys_absent(self):
        """Empty != Zero. The old code defaulted BOTH the miss path and the
        exception path to ``0.0``, which renders as a real "0" holding."""
        result: dict[str, Any] = {}
        _load_token_amounts(_api_client([]), result, _CONFIG)
        assert "token0_amount" not in result
        assert "token1_amount" not in result

    def test_absent_amount_renders_an_em_dash_and_measured_zero_renders_zero(self):
        assert _fmt_token_amount(None) == "—"
        assert _fmt_token_amount(0) == "0"
        assert _fmt_token_amount("0") == "0"

    def test_a_read_failure_does_not_fabricate_a_zero_holding(self):
        class _Boom:
            def get_position(self):
                raise RuntimeError("gateway down")

        result: dict[str, Any] = {}
        _load_token_amounts(_Boom(), result, _CONFIG)
        assert "token0_amount" not in result
        assert "token1_amount" not in result


class TestStrategyReportedCompositionIsRefused:
    def test_a_strategy_reported_row_is_not_treated_as_live_composition(self):
        """The two halves of VIB-6283 meeting.

        A ``strategy_reported`` mark is the strategy's requested config amounts,
        frozen at entry. Rendering it as the live composition would reintroduce
        the freeze through the new source — the panel would look measured while
        being exactly as stale as the mint amounts it replaced.
        """
        row = _measured_lp_row(source=STRATEGY_REPORTED_VALUATION_SOURCE)
        assert _measured_lp_composition([row], _CONFIG) is None

        result: dict[str, Any] = {}
        _load_token_amounts(_api_client([row]), result, _CONFIG)
        assert "token0_amount" not in result

    def test_a_mismatched_token_pair_is_not_borrowed_from_another_leg(self):
        """Multi-LP guard: a WBTC/USDC row must not populate a WETH/USDC card."""
        row = _measured_lp_row()
        row["details"]["token0_symbol"] = "WBTC"
        assert _measured_lp_composition([row], _CONFIG) is None

    def test_a_non_lp_strategy_position_is_ignored(self):
        pt_row = _measured_lp_row()
        pt_row["position_type"] = "TOKEN"
        assert _measured_lp_composition([pt_row], _CONFIG) is None


class TestGatewaySurfacesLpComposition:
    def test_lp_rows_reach_strategy_positions_with_provenance(self):
        """The gateway hop that makes the measured composition reachable at all.

        Without this the dashboard has no live source and the fix above is inert
        — the exact "built it but nothing feeds it" shape that made the V4
        valuation ladder dead code.
        """
        from types import SimpleNamespace

        from almanak.gateway.services._dashboard_helpers import _lp_strategy_positions_from_snapshot

        snapshot = SimpleNamespace(
            positions=[
                SimpleNamespace(
                    position_type="LP",
                    protocol="uniswap_v3",
                    chain="arbitrum",
                    label="uniswap_v3 LP",
                    value_usd="4.0398",
                    details={
                        "amount0": _LIVE_AMOUNT0,
                        "amount1": _LIVE_AMOUNT1,
                        "token0_symbol": "WETH",
                        "token1_symbol": "USDC",
                        "valuation_source": "on_chain",
                        "position_id": "5627514",
                    },
                )
            ]
        )
        rows = _lp_strategy_positions_from_snapshot(snapshot)
        assert len(rows) == 1
        assert rows[0].position_type == "LP"
        assert rows[0].details["amount0"] == _LIVE_AMOUNT0
        assert rows[0].details["valuation_source"] == "on_chain"

    def test_a_position_with_no_measured_composition_is_not_surfaced(self):
        """Empty != Zero at the gateway boundary: no composition, no row (rather
        than a row of empty strings the panel would render as amounts)."""
        from types import SimpleNamespace

        from almanak.gateway.services._dashboard_helpers import _lp_strategy_positions_from_snapshot

        snapshot = SimpleNamespace(
            positions=[
                SimpleNamespace(
                    position_type="LP",
                    protocol="traderjoe_v2",
                    chain="avalanche",
                    label="traderjoe_v2 LP",
                    value_usd="4.45",
                    details={"pool": "WAVAX/USDC/20"},
                )
            ]
        )
        assert _lp_strategy_positions_from_snapshot(snapshot) == []
