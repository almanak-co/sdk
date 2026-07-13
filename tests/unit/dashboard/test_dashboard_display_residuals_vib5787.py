"""Regression tests for the VIB-5787 dashboard display-residual cluster.

Pure display/rendering logic only — every fix here is in the dashboard
render path, not in snapshot/valuation classification. Each test pins one
confirmed lying-tile behaviour from the 20260713-0947 robinhood mainnet run:

1. "active exposure" literal on a flat/closed position (Open position NAV badge)
2. Max drawdown rendered as a positive number with an up-arrow
4. "BUY SIGNAL" badge still shown after teardown (lifecycle gating)
5. 24h-PnL fabricated "$0.00" vs a real Strategy PnL (Empty ≠ Zero)
6. TOKEN_DECIMALS static map missing a chain's stable (USDG) → registry-first
7. LP tick→price conversion using wrong decimals → collapsed price axis
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

# --- Bug 6 + 7: per-chain decimals resolution (registry-first) ------------


def _install_fake_resolver(monkeypatch, decimals_by_chain_symbol):
    """Patch the canonical token resolver so tests don't need a gateway.

    ``decimals_by_chain_symbol`` maps (chain, SYMBOL) -> decimals; a miss
    raises (mirrors ``get_decimals`` raising ``TokenNotFoundError``).
    """
    import almanak.framework.data.tokens.resolver as resolver_mod

    class _FakeResolver:
        def get_decimals(self, chain, token):
            try:
                return decimals_by_chain_symbol[(chain, token.upper())]
            except KeyError as exc:
                raise LookupError(f"unknown token {token} on {chain}") from exc

    monkeypatch.setattr(resolver_mod, "get_token_resolver", lambda: _FakeResolver())


def test_resolve_token_decimals_prefers_registry_over_static_map(monkeypatch):
    from almanak.framework.dashboard.custom import _token_decimals as td

    # USDG is a 6-dec stable NOT in the static map. The registry knows it.
    _install_fake_resolver(monkeypatch, {("robinhood", "USDG"): 6})
    assert td.resolve_token_decimals("USDG", "robinhood") == 6
    # A symbol whose registry answer differs from the static map: registry wins.
    _install_fake_resolver(monkeypatch, {("weird", "USDC"): 8})
    assert td.resolve_token_decimals("USDC", "weird") == 8


def test_resolve_token_decimals_static_fallback_when_registry_misses(monkeypatch):
    from almanak.framework.dashboard.custom import _token_decimals as td

    _install_fake_resolver(monkeypatch, {})  # registry knows nothing
    # Common major still resolves via the last-resort static map.
    assert td.resolve_token_decimals("WETH", "robinhood") == 18
    # Genuinely unresolvable token: None (caller decides) or explicit default.
    assert td.resolve_token_decimals("ZZZ", "robinhood") is None
    assert td.resolve_token_decimals("ZZZ", "robinhood", default=18) == 18


def test_resolve_token_decimals_no_chain_uses_static_only(monkeypatch):
    from almanak.framework.dashboard.custom import _token_decimals as td

    # No chain context → registry is skipped; only the static map answers.
    _install_fake_resolver(monkeypatch, {("robinhood", "USDG"): 6})
    assert td.resolve_token_decimals("USDG") is None
    assert td.resolve_token_decimals("USDC") == 6


def test_lp_tick_to_display_price_resolves_stable_via_registry(monkeypatch):
    """USDG (6-dec) must scale correctly — the collapsed-axis root cause."""
    from almanak.framework.dashboard.templates.lp_dashboard import (
        LPDashboardConfig,
        _tick_to_display_price,
    )

    _install_fake_resolver(monkeypatch, {("robinhood", "WETH"): 18, ("robinhood", "USDG"): 6})
    config = LPDashboardConfig(protocol="uniswap_v3", token0="WETH", token1="USDG", chain="robinhood")
    # A WETH/USDG pool trades around ~1800; the price must land in a sane band,
    # not the ~1e-9 nonsense a wrong 18-dec USDG produced (all ticks "0.00").
    price = _tick_to_display_price(-201531, config)
    assert price is not None
    assert 100 < price < 100_000


def test_position_event_adapter_tick_to_price_uses_chain(monkeypatch):
    from almanak.framework.dashboard.custom import position_event_adapter as pea

    _install_fake_resolver(monkeypatch, {("robinhood", "WETH"): 18, ("robinhood", "USDG"): 6})
    price = pea._tick_to_price(-201531, "WETH", "USDG", "robinhood")
    assert 100 < price < 100_000
    # Unknown token + no registry answer → 0.0 (unchanged skip behaviour).
    _install_fake_resolver(monkeypatch, {})
    assert pea._tick_to_price(-201531, "WETH", "ZZZ", "robinhood") == 0.0


# --- Bug 1 + 2: Open-position badge + drawdown sign -----------------------


def _pnl(**overrides):
    from almanak.framework.dashboard.gateway_client import PnLSummary

    base = {
        "deployed_usd": Decimal("2.90"),
        "nav_usd": Decimal("3.85"),
        "lifetime_pnl_usd": Decimal("-0.03"),
        "lifetime_pnl_pct": Decimal("-1"),
        "net_apr_pct": Decimal("0"),
        "max_drawdown_pct": Decimal("0"),
        "current_drawdown_pct": Decimal("0"),
        "value_confidence": "HIGH",
        "age_days": 0,
        "deployed_capital_usd": Decimal("0"),
        "available_cash_usd": Decimal("3.85"),
        "open_position_count": 0,
        "primary_risk_kind": "",
        "primary_risk_label": "",
        "primary_risk_value": "",
        "primary_risk_color": "neutral",
    }
    base.update(overrides)
    return PnLSummary(**base)


class _CaptureSt:
    """Minimal streamlit stub capturing st.metric(label, value, delta=...)."""

    def __init__(self):
        self.metrics: list[dict] = []

    def markdown(self, *a, **k):
        pass

    def columns(self, spec):
        n = spec if isinstance(spec, int) else len(spec)
        return tuple(self for _ in range(n))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def metric(self, label, value, **kw):
        self.metrics.append({"label": label, "value": value, "delta": kw.get("delta")})


def test_open_position_badge_flat_has_no_active_exposure_literal(monkeypatch):
    import almanak.framework.dashboard.pages._detail_header as dh

    cap = _CaptureSt()
    monkeypatch.setattr(dh, "st", cap)
    dh.render_money_trail(_pnl(open_position_count=0))

    nav = next(m for m in cap.metrics if m["label"] == "Open position NAV")
    assert nav["delta"] == "no open positions"
    assert "active exposure" not in str(nav["delta"])


def test_open_position_badge_counts_when_open(monkeypatch):
    import almanak.framework.dashboard.pages._detail_header as dh

    cap = _CaptureSt()
    monkeypatch.setattr(dh, "st", cap)
    dh.render_money_trail(_pnl(open_position_count=2))
    nav = next(m for m in cap.metrics if m["label"] == "Open position NAV")
    assert nav["delta"] == "2 open position(s)"


def test_max_drawdown_renders_as_loss_not_up_arrow(monkeypatch):
    import almanak.framework.dashboard.pages._detail_header as dh

    cap = _CaptureSt()
    monkeypatch.setattr(dh, "st", cap)
    dh.render_money_trail(_pnl(max_drawdown_pct=Decimal("1.3")))

    apr = next(m for m in cap.metrics if m["label"] == "Strategy APR")
    delta = str(apr["delta"])
    # A drawdown is a loss: leading "-" makes streamlit draw a down glyph, and
    # the sign must read as one with the meaning — never a bare "+1.3%".
    assert delta.startswith("-")
    assert "+" not in delta
    assert "1.3%" in delta and "max DD" in delta


def test_age_shown_when_no_drawdown(monkeypatch):
    import almanak.framework.dashboard.pages._detail_header as dh

    cap = _CaptureSt()
    monkeypatch.setattr(dh, "st", cap)
    dh.render_money_trail(_pnl(max_drawdown_pct=Decimal("0"), age_days=5))
    apr = next(m for m in cap.metrics if m["label"] == "Strategy APR")
    assert apr["delta"] == "5d age"


# --- Bug 4: signal badge gated on lifecycle -------------------------------


class _SignalSt:
    def __init__(self):
        self.successes: list[str] = []
        self.errors: list[str] = []
        self.infos: list[str] = []

    def subheader(self, *a, **k):
        pass

    def success(self, msg):
        self.successes.append(msg)

    def error(self, msg):
        self.errors.append(msg)

    def info(self, msg):
        self.infos.append(msg)


def _rsi_config(**over):
    from almanak.framework.dashboard.templates.ta_dashboard import TADashboardConfig

    base = {
        "indicator_name": "RSI",
        "indicator_period": 14,
        "upper_threshold": 70,
        "lower_threshold": 30,
        "signal_type": "reversion",
    }
    base.update(over)
    return TADashboardConfig(**base)


def test_signal_badge_suppressed_when_terminal(monkeypatch):
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    st = _SignalSt()
    monkeypatch.setattr(tad, "st", st)
    # RSI 20 < 30 → would normally be a BUY SIGNAL.
    tad._render_signal_status({"rsi_value": 20}, {}, _rsi_config(), is_terminal=True)
    assert st.successes == [] and st.errors == []
    assert len(st.infos) == 1 and "stopped" in st.infos[0].lower()


def test_signal_badge_shown_when_live(monkeypatch):
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    st = _SignalSt()
    monkeypatch.setattr(tad, "st", st)
    tad._render_signal_status({"rsi_value": 20}, {}, _rsi_config(), is_terminal=False)
    assert len(st.successes) == 1 and "BUY SIGNAL" in st.successes[0]


@pytest.mark.parametrize(
    ("status", "expected_terminal"),
    [("INACTIVE", True), ("ARCHIVED", True), ("RUNNING", False), ("PAUSED", False), ("STALE", False)],
)
def test_deployment_terminal_classification(monkeypatch, status, expected_terminal):
    import almanak.framework.dashboard.data_source as ds
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    monkeypatch.setattr(ds, "get_strategy_details", lambda _id: SimpleNamespace(status=status))
    assert tad._deployment_is_terminal("deployment:abc") is expected_terminal


def test_deployment_terminal_fails_open(monkeypatch):
    """A gateway error must NOT wrongly suppress a live signal (fail open)."""
    import almanak.framework.dashboard.data_source as ds
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    def _boom(_id):
        raise RuntimeError("gateway down")

    monkeypatch.setattr(ds, "get_strategy_details", _boom)
    assert tad._deployment_is_terminal("deployment:abc") is False


# --- Bug 5: 24h PnL Empty ≠ Zero ------------------------------------------


def test_convert_summary_empty_24h_pnl_is_none():
    from almanak.framework.dashboard.gateway_client import GatewayDashboardClient

    proto = SimpleNamespace(
        deployment_id="d",
        name="n",
        status="INACTIVE",
        chain="robinhood",
        protocol="uniswap_v3",
        total_value_usd="3.85",
        pnl_24h_usd="",  # unmeasured
        last_action_at=0,
        attention_required=False,
        attention_reason="",
        is_multi_chain=False,
        chains=[],
        execution_mode="live",
        paper_metrics_json="",
    )
    summary = GatewayDashboardClient._convert_summary(object.__new__(GatewayDashboardClient), proto)
    assert summary.pnl_24h_usd is None

    proto.pnl_24h_usd = "0"  # a genuine measured zero stays a Decimal
    summary2 = GatewayDashboardClient._convert_summary(object.__new__(GatewayDashboardClient), proto)
    assert summary2.pnl_24h_usd == Decimal("0")


def test_summary_to_dict_serialises_unmeasured_24h_as_empty():
    from almanak.framework.dashboard.custom.api_client import DashboardAPIClient

    client = object.__new__(DashboardAPIClient)
    client._deployment_id = "d"
    summary_none = SimpleNamespace(pnl_24h_usd=None)
    assert client._summary_to_dict(summary_none)["pnl_24h_usd"] == ""
    summary_zero = SimpleNamespace(pnl_24h_usd=Decimal("0"))
    assert client._summary_to_dict(summary_zero)["pnl_24h_usd"] == "0"


def test_populate_performance_pnl_skips_empty_string(monkeypatch):
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    client = SimpleNamespace(get_summary=lambda: {"pnl_24h_usd": ""})
    result: dict = {}
    tad._populate_performance_pnl(client, result)
    # "" (unmeasured) must NOT populate total_pnl → tile renders "—".
    assert "total_pnl" not in result

    client_measured = SimpleNamespace(get_summary=lambda: {"pnl_24h_usd": "0"})
    result2: dict = {}
    tad._populate_performance_pnl(client_measured, result2)
    assert result2["total_pnl"] == "0"
