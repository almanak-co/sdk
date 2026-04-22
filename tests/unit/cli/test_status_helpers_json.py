"""Unit tests for the Phase 5A.1 helpers in `almanak/framework/cli/status_helpers.py`.

Covers the JSON-rendering path of `strat status --json`:
    - _validate_status_args
    - _fetch_strategy_details (transport + disconnect ordering + error string)
    - _render_json_summary
    - _render_json_position (incl. strategy_positions empty-string stripping)
    - _render_json_timeline
    - _render_json_chain_health
    - _render_json_operator_card
    - _render_details_as_json (top-level orchestration + optional keys)

These tests use lightweight SimpleNamespace-based fakes instead of the real
proto classes. The helpers treat inputs as duck-typed attribute bags, so this
is faithful to the runtime shape coming from `gateway_pb2`.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.cli import status_helpers

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


def _make_summary(**overrides: Any) -> SimpleNamespace:
    """Build a minimal `StrategySummary`-like object."""
    defaults = {
        "strategy_id": "demo",
        "name": "Demo Strategy",
        "status": "RUNNING",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "total_value_usd": "1000.00",
        "pnl_24h_usd": "12.34",
        "last_action_at": 1_700_000_000,
        "attention_required": False,
        "attention_reason": "",
        "consecutive_errors": 0,
        "last_iteration_at": 1_700_000_500,
        "pnl_since_deploy_usd": "",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_strategy_position(**overrides: Any) -> SimpleNamespace:
    """Build a StrategyPosition-like object.

    All optional monitoring fields default to empty strings (the proto3
    empty-string-as-unset sentinel preserved by the refactor).
    """
    defaults = {
        "position_type": "PERP",
        "position_id": "ETH-PERP",
        "chain": "arbitrum",
        "protocol": "gmx_v2",
        "value_usd": "500.00",
        "liquidation_risk": False,
        "direction": "",
        "entry_price": "",
        "current_price": "",
        "unrealized_pnl_usd": "",
        "unrealized_pnl_pct": "",
        "size_usd": "",
        "collateral_usd": "",
        "leverage": "",
        "health_factor": "",
        "details": {},
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_position(
    *,
    token_balances: list | None = None,
    lp_positions: list | None = None,
    health_factor: Any = None,
    strategy_positions: list | None = None,
) -> SimpleNamespace:
    """Build a Position-like object. `None` kwargs -> empty list/None."""
    return SimpleNamespace(
        token_balances=list(token_balances or []),
        lp_positions=list(lp_positions or []),
        health_factor=health_factor,
        strategy_positions=list(strategy_positions or []),
    )


def _make_details(
    *,
    summary: SimpleNamespace | None = None,
    position: Any = None,
    timeline: list | None = None,
    chain_health: dict | None = None,
    operator_card: SimpleNamespace | None = None,
) -> SimpleNamespace:
    """Build a GetStrategyDetailsResponse-like object."""
    return SimpleNamespace(
        summary=summary or _make_summary(),
        position=position,
        timeline=list(timeline or []),
        chain_health=dict(chain_health or {}),
        operator_card=operator_card,
    )


# ---------------------------------------------------------------------------
# _validate_status_args
# ---------------------------------------------------------------------------


def test_validate_status_args_accepts_positive() -> None:
    """Positive timeline_limit returns without side effects."""
    # No exception
    status_helpers._validate_status_args(1)
    status_helpers._validate_status_args(10)
    status_helpers._validate_status_args(1000)


def test_validate_status_args_rejects_zero(capsys: pytest.CaptureFixture) -> None:
    """`timeline_limit=0` exits with code 1 and the exact error string."""
    with pytest.raises(SystemExit) as excinfo:
        status_helpers._validate_status_args(0)
    assert excinfo.value.code == 1
    err = capsys.readouterr().err
    assert "--timeline-limit must be >= 1." in err


def test_validate_status_args_rejects_negative(capsys: pytest.CaptureFixture) -> None:
    """Negative timeline_limit exits with code 1."""
    with pytest.raises(SystemExit) as excinfo:
        status_helpers._validate_status_args(-5)
    assert excinfo.value.code == 1
    err = capsys.readouterr().err
    assert "--timeline-limit must be >= 1." in err


# ---------------------------------------------------------------------------
# _fetch_strategy_details
# ---------------------------------------------------------------------------


class _FakeDashboard:
    """Stand-in for `client.dashboard`."""

    def __init__(self, response: Any = None, raise_exc: Exception | None = None) -> None:
        self._response = response
        self._raise = raise_exc
        self.last_request: Any = None

    def GetStrategyDetails(self, request: Any) -> Any:  # noqa: N802 (proto naming)
        self.last_request = request
        if self._raise is not None:
            raise self._raise
        return self._response


class _FakeClient:
    """Stand-in for GatewayClient."""

    def __init__(self, response: Any = None, raise_exc: Exception | None = None) -> None:
        self.dashboard = _FakeDashboard(response=response, raise_exc=raise_exc)
        self.disconnect_called = 0

    def disconnect(self) -> None:
        self.disconnect_called += 1


def test_fetch_strategy_details_happy_path() -> None:
    """Returns the RPC response and calls disconnect() exactly once."""
    fake_response = _make_details()
    client = _FakeClient(response=fake_response)
    result = status_helpers._fetch_strategy_details(
        client,  # type: ignore[arg-type]
        "my_strategy",
        include_timeline=True,
        timeline_limit=10,
    )
    assert result is fake_response
    assert client.disconnect_called == 1
    # Request was built with the exact fields
    req = client.dashboard.last_request
    assert req.strategy_id == "my_strategy"
    assert req.include_timeline is True
    assert req.include_pnl_history is False
    assert req.timeline_limit == 10


def test_fetch_strategy_details_rpc_error_exits_with_exact_string(
    capsys: pytest.CaptureFixture,
) -> None:
    """RPC exception => click.secho error, sys.exit(1), disconnect still called."""
    client = _FakeClient(raise_exc=RuntimeError("boom"))
    with pytest.raises(SystemExit) as excinfo:
        status_helpers._fetch_strategy_details(
            client,  # type: ignore[arg-type]
            "bad_strategy",
            include_timeline=False,
            timeline_limit=10,
        )
    assert excinfo.value.code == 1
    err = capsys.readouterr().err
    # Exact string is grep-asserted in smoke tests
    assert "Failed to get strategy details: boom" in err
    # finally clause ran
    assert client.disconnect_called == 1


def test_fetch_strategy_details_disconnect_runs_even_on_success() -> None:
    """`finally: disconnect` always runs on the happy path."""
    client = _FakeClient(response=_make_details())
    status_helpers._fetch_strategy_details(
        client,  # type: ignore[arg-type]
        "demo",
        include_timeline=False,
        timeline_limit=5,
    )
    assert client.disconnect_called == 1


# ---------------------------------------------------------------------------
# _render_json_summary
# ---------------------------------------------------------------------------


def test_render_json_summary_minimal() -> None:
    """All summary fields are emitted with correct values."""
    s = _make_summary()
    out = status_helpers._render_json_summary(s)
    assert out["strategy_id"] == "demo"
    assert out["name"] == "Demo Strategy"
    assert out["status"] == "RUNNING"
    assert out["chain"] == "arbitrum"
    assert out["protocol"] == "uniswap_v3"
    assert out["total_value_usd"] == "1000.00"
    assert out["pnl_24h_usd"] == "12.34"
    assert out["last_action_at"] == 1_700_000_000
    assert out["attention_required"] is False
    assert out["attention_reason"] == ""
    assert out["consecutive_errors"] == 0
    assert out["last_iteration_at"] == 1_700_000_500
    # pnl_since_deploy_usd empty string -> None
    assert out["pnl_since_deploy_usd"] is None


def test_render_json_summary_pnl_since_deploy_populated() -> None:
    """Non-empty `pnl_since_deploy_usd` is passed through verbatim."""
    s = _make_summary(pnl_since_deploy_usd="42.50")
    out = status_helpers._render_json_summary(s)
    assert out["pnl_since_deploy_usd"] == "42.50"


# ---------------------------------------------------------------------------
# _render_json_position
# ---------------------------------------------------------------------------


def test_render_json_position_empty_returns_empty_dict() -> None:
    """A position with no sub-fields populated returns {}."""
    pos = _make_position()
    out = status_helpers._render_json_position(pos)
    assert out == {}


def test_render_json_position_token_balances_only() -> None:
    """Only `token_balances` populated -> single key output."""
    pos = _make_position(
        token_balances=[
            SimpleNamespace(symbol="USDC", balance="100", value_usd="100"),
            SimpleNamespace(symbol="WETH", balance="0.5", value_usd="1500"),
        ],
    )
    out = status_helpers._render_json_position(pos)
    assert list(out.keys()) == ["token_balances"]
    assert out["token_balances"] == [
        {"symbol": "USDC", "balance": "100", "value_usd": "100"},
        {"symbol": "WETH", "balance": "0.5", "value_usd": "1500"},
    ]


def test_render_json_position_lp_positions_only() -> None:
    """LP positions emit pool/token0/token1/liquidity_usd fields."""
    pos = _make_position(
        lp_positions=[
            SimpleNamespace(
                pool="WETH/USDC",
                token0="WETH",
                token1="USDC",
                liquidity_usd="2500",
            ),
        ],
    )
    out = status_helpers._render_json_position(pos)
    assert out == {
        "lp_positions": [
            {
                "pool": "WETH/USDC",
                "token0": "WETH",
                "token1": "USDC",
                "liquidity_usd": "2500",
            }
        ]
    }


def test_render_json_position_health_factor_float_coercion() -> None:
    """`health_factor` is coerced to float when not None."""
    pos = _make_position(health_factor="1.5")
    out = status_helpers._render_json_position(pos)
    assert out["health_factor"] == 1.5
    assert isinstance(out["health_factor"], float)


def test_render_json_position_health_factor_none_omitted() -> None:
    """`health_factor` is omitted when None."""
    pos = _make_position(health_factor=None)
    out = status_helpers._render_json_position(pos)
    assert "health_factor" not in out


def test_render_json_position_strategy_positions_all_fields() -> None:
    """All optional monitoring fields propagated when non-empty."""
    sp = _make_strategy_position(
        direction="LONG",
        entry_price="1000",
        current_price="1100",
        unrealized_pnl_usd="50",
        unrealized_pnl_pct="5.0",
        size_usd="500",
        collateral_usd="100",
        leverage="5",
        health_factor="1.8",
        details={"key": "value"},
    )
    pos = _make_position(strategy_positions=[sp])
    out = status_helpers._render_json_position(pos)
    assert len(out["strategy_positions"]) == 1
    entry = out["strategy_positions"][0]
    # Required fields always present
    assert entry["position_type"] == "PERP"
    assert entry["position_id"] == "ETH-PERP"
    assert entry["chain"] == "arbitrum"
    assert entry["protocol"] == "gmx_v2"
    assert entry["value_usd"] == "500.00"
    assert entry["liquidation_risk"] is False
    # Optional fields
    assert entry["direction"] == "LONG"
    assert entry["entry_price"] == "1000"
    assert entry["current_price"] == "1100"
    assert entry["unrealized_pnl_usd"] == "50"
    assert entry["unrealized_pnl_pct"] == "5.0"
    assert entry["size_usd"] == "500"
    assert entry["collateral_usd"] == "100"
    assert entry["leverage"] == "5"
    assert entry["health_factor"] == "1.8"
    assert entry["details"] == {"key": "value"}


def test_render_json_position_strategy_positions_empty_strings_stripped() -> None:
    """Optional monitoring fields with empty strings are NOT emitted."""
    sp = _make_strategy_position()  # all optional fields default to ""
    pos = _make_position(strategy_positions=[sp])
    out = status_helpers._render_json_position(pos)
    entry = out["strategy_positions"][0]
    # Only the six required fields (no optional ones)
    assert set(entry.keys()) == {
        "position_type",
        "position_id",
        "chain",
        "protocol",
        "value_usd",
        "liquidation_risk",
    }


def test_render_json_position_strategy_positions_details_empty_omitted() -> None:
    """`details` key is omitted when the proto map is empty."""
    sp = _make_strategy_position(details={})
    pos = _make_position(strategy_positions=[sp])
    out = status_helpers._render_json_position(pos)
    entry = out["strategy_positions"][0]
    assert "details" not in entry


def test_render_json_position_combined_fields_order_preserved() -> None:
    """When multiple sub-fields populate, all are emitted in the original order."""
    pos = _make_position(
        token_balances=[SimpleNamespace(symbol="USDC", balance="10", value_usd="10")],
        lp_positions=[
            SimpleNamespace(pool="p", token0="a", token1="b", liquidity_usd="1")
        ],
        health_factor="2.0",
        strategy_positions=[_make_strategy_position()],
    )
    out = status_helpers._render_json_position(pos)
    # Ordering per original implementation
    assert list(out.keys()) == [
        "token_balances",
        "lp_positions",
        "health_factor",
        "strategy_positions",
    ]


# ---------------------------------------------------------------------------
# _render_json_timeline
# ---------------------------------------------------------------------------


def test_render_json_timeline_maps_fields() -> None:
    """Timeline events map the 5 documented fields."""
    events = [
        SimpleNamespace(
            timestamp=1_700_000_000,
            event_type="TRADE",
            description="Buy 1 ETH",
            tx_hash="0xabc",
            chain="arbitrum",
        ),
        SimpleNamespace(
            timestamp=1_700_000_100,
            event_type="REBALANCE",
            description="Rebalance",
            tx_hash="",
            chain="arbitrum",
        ),
    ]
    out = status_helpers._render_json_timeline(events)
    assert out == [
        {
            "timestamp": 1_700_000_000,
            "event_type": "TRADE",
            "description": "Buy 1 ETH",
            "tx_hash": "0xabc",
            "chain": "arbitrum",
        },
        {
            "timestamp": 1_700_000_100,
            "event_type": "REBALANCE",
            "description": "Rebalance",
            "tx_hash": "",
            "chain": "arbitrum",
        },
    ]


# ---------------------------------------------------------------------------
# _render_json_chain_health
# ---------------------------------------------------------------------------


def test_render_json_chain_health_maps_fields() -> None:
    """Chain health map emits status/rpc_latency_ms/gas_price_gwei per entry."""
    health = {
        "arbitrum": SimpleNamespace(
            status="HEALTHY", rpc_latency_ms=42, gas_price_gwei="0.1"
        ),
        "base": SimpleNamespace(
            status="DEGRADED", rpc_latency_ms=500, gas_price_gwei="0.05"
        ),
    }
    out = status_helpers._render_json_chain_health(health)
    assert out == {
        "arbitrum": {
            "status": "HEALTHY",
            "rpc_latency_ms": 42,
            "gas_price_gwei": "0.1",
        },
        "base": {
            "status": "DEGRADED",
            "rpc_latency_ms": 500,
            "gas_price_gwei": "0.05",
        },
    }


# ---------------------------------------------------------------------------
# _render_json_operator_card
# ---------------------------------------------------------------------------


def test_render_json_operator_card_maps_fields() -> None:
    """Operator card emits severity/reason/risk_description/suggested_actions."""
    oc = SimpleNamespace(
        strategy_id="demo",
        severity="HIGH",
        reason="Stuck iteration",
        risk_description="Strategy has not iterated in 1h",
        suggested_actions=["pause", "investigate"],
    )
    out = status_helpers._render_json_operator_card(oc)
    assert out == {
        "severity": "HIGH",
        "reason": "Stuck iteration",
        "risk_description": "Strategy has not iterated in 1h",
        "suggested_actions": ["pause", "investigate"],
    }


# ---------------------------------------------------------------------------
# _render_details_as_json — orchestration + optional keys
# ---------------------------------------------------------------------------


def test_render_details_as_json_summary_only() -> None:
    """No position/timeline/chain_health/operator_card => only summary keys."""
    details = _make_details(
        summary=_make_summary(),
        position=None,
        timeline=[],
        chain_health={},
        operator_card=None,
    )
    out_str = status_helpers._render_details_as_json(details)
    data = json.loads(out_str)
    assert "position" not in data
    assert "timeline" not in data
    assert "chain_health" not in data
    assert "operator_card" not in data
    # summary fields present
    assert data["strategy_id"] == "demo"
    assert data["pnl_since_deploy_usd"] is None


def test_render_details_as_json_empty_position_short_circuits() -> None:
    """Position with no populated sub-fields => no `position` key in output."""
    details = _make_details(position=_make_position())
    out_str = status_helpers._render_details_as_json(details)
    data = json.loads(out_str)
    assert "position" not in data


def test_render_details_as_json_position_populated_included() -> None:
    """Populated position makes it through to the final JSON."""
    pos = _make_position(
        token_balances=[
            SimpleNamespace(symbol="USDC", balance="100", value_usd="100"),
        ]
    )
    details = _make_details(position=pos)
    out_str = status_helpers._render_details_as_json(details)
    data = json.loads(out_str)
    assert data["position"]["token_balances"][0]["symbol"] == "USDC"


def test_render_details_as_json_timeline_present() -> None:
    """Timeline emitted when non-empty."""
    events = [
        SimpleNamespace(
            timestamp=1_700_000_000,
            event_type="TRADE",
            description="Buy 1 ETH",
            tx_hash="0xabc",
            chain="arbitrum",
        )
    ]
    details = _make_details(timeline=events)
    out_str = status_helpers._render_details_as_json(details)
    data = json.loads(out_str)
    assert "timeline" in data
    assert data["timeline"][0]["event_type"] == "TRADE"


def test_render_details_as_json_timeline_absent() -> None:
    """Empty timeline => no `timeline` key."""
    details = _make_details(timeline=[])
    out_str = status_helpers._render_details_as_json(details)
    data = json.loads(out_str)
    assert "timeline" not in data


def test_render_details_as_json_chain_health_populated() -> None:
    """Non-empty chain_health emitted."""
    health = {
        "arbitrum": SimpleNamespace(
            status="HEALTHY", rpc_latency_ms=42, gas_price_gwei="0.1"
        )
    }
    details = _make_details(chain_health=health)
    out_str = status_helpers._render_details_as_json(details)
    data = json.loads(out_str)
    assert data["chain_health"]["arbitrum"]["status"] == "HEALTHY"


def test_render_details_as_json_operator_card_included_when_present() -> None:
    """Operator card is emitted whenever the sub-message is present (#1704).

    After the #1704 fix, presence is determined by
    `parent.HasField("operator_card")` (or, for duck-typed fakes,
    `details.operator_card is not None`). An empty-string `strategy_id` is
    no longer used as a presence sentinel: a card with an intentionally
    empty `strategy_id` MUST still render.
    """
    oc_empty_sid = SimpleNamespace(
        strategy_id="",  # explicitly empty — must still render (#1704)
        severity="HIGH",
        reason="x",
        risk_description="y",
        suggested_actions=[],
    )
    details = _make_details(operator_card=oc_empty_sid)
    data = json.loads(status_helpers._render_details_as_json(details))
    assert "operator_card" in data, (
        "empty-string strategy_id must not suppress the operator card (#1704)"
    )
    assert data["operator_card"]["severity"] == "HIGH"

    # And a fully-populated card likewise renders.
    oc_full = SimpleNamespace(
        strategy_id="demo",
        severity="HIGH",
        reason="Stuck",
        risk_description="Risk",
        suggested_actions=["pause"],
    )
    details = _make_details(operator_card=oc_full)
    data = json.loads(status_helpers._render_details_as_json(details))
    assert data["operator_card"]["severity"] == "HIGH"
    assert data["operator_card"]["suggested_actions"] == ["pause"]


def test_render_details_as_json_operator_card_none_omitted() -> None:
    """`details.operator_card = None` => no operator_card key."""
    details = _make_details(operator_card=None)
    data = json.loads(status_helpers._render_details_as_json(details))
    assert "operator_card" not in data


def test_render_details_as_json_operator_card_hasfield_respected() -> None:
    """When parent exposes `HasField`, it takes precedence over the fallback.

    Simulates the real proto3 message: `HasField("operator_card")` is the
    authoritative presence signal. When it returns False, the card must be
    suppressed even if the attribute is set (proto3 default-instance
    semantics — accessing the sub-message returns an "empty" message, not
    None).
    """

    class _ProtoLikeDetails(SimpleNamespace):
        def __init__(self, **kw: Any) -> None:
            present = set(kw.pop("_present", ()))
            super().__init__(**kw)
            self._present: set[str] = present

        def HasField(self, name: str) -> bool:  # noqa: N802 (proto naming)
            return name in self._present

    # Build a proto-like `details` where operator_card attr exists but
    # HasField returns False (matches proto3 default-instance behavior).
    oc = SimpleNamespace(
        strategy_id="anything",  # irrelevant — HasField is authoritative
        severity="HIGH",
        reason="",
        risk_description="",
        suggested_actions=[],
    )
    details = _ProtoLikeDetails(
        summary=_make_summary(),
        position=None,
        timeline=[],
        chain_health={},
        operator_card=oc,
    )
    # operator_card is NOT registered as present -> must be omitted
    data = json.loads(status_helpers._render_details_as_json(details))
    assert "operator_card" not in data

    # Now mark it present -> must appear
    details._present.add("operator_card")
    data = json.loads(status_helpers._render_details_as_json(details))
    assert data["operator_card"]["severity"] == "HIGH"


def test_render_details_as_json_indent_is_2() -> None:
    """JSON is indented with 2 spaces (matches the original json.dumps call)."""
    details = _make_details()
    out_str = status_helpers._render_details_as_json(details)
    # indented output contains a newline followed by 2 spaces before the first key
    assert '\n  "strategy_id"' in out_str


def test_render_details_as_json_kitchen_sink() -> None:
    """Every optional section populated at once — end-to-end orchestration."""
    pos = _make_position(
        token_balances=[SimpleNamespace(symbol="USDC", balance="1", value_usd="1")],
        health_factor="2.5",
        strategy_positions=[_make_strategy_position(direction="LONG")],
    )
    events = [
        SimpleNamespace(
            timestamp=1,
            event_type="TRADE",
            description="d",
            tx_hash="0xab",
            chain="arbitrum",
        )
    ]
    health = {
        "arbitrum": SimpleNamespace(
            status="HEALTHY", rpc_latency_ms=1, gas_price_gwei="0.1"
        )
    }
    oc = SimpleNamespace(
        strategy_id="demo",
        severity="CRITICAL",
        reason="r",
        risk_description="rd",
        suggested_actions=["a", "b"],
    )
    details = _make_details(
        position=pos,
        timeline=events,
        chain_health=health,
        operator_card=oc,
    )
    data = json.loads(status_helpers._render_details_as_json(details))
    assert "position" in data
    assert "timeline" in data
    assert "chain_health" in data
    assert "operator_card" in data
    assert data["position"]["health_factor"] == 2.5
    assert data["position"]["strategy_positions"][0]["direction"] == "LONG"
    assert data["operator_card"]["severity"] == "CRITICAL"


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: transport edge cases
# ---------------------------------------------------------------------------


def test_fetch_strategy_details_disconnect_runs_on_rpc_error() -> None:
    """finally-clause disconnect fires exactly once even when RPC raises."""
    client = _FakeClient(raise_exc=RuntimeError("rpc dropped"))
    with pytest.raises(SystemExit):
        status_helpers._fetch_strategy_details(
            client,  # type: ignore[arg-type]
            "demo",
            include_timeline=True,
            timeline_limit=1,
        )
    # `finally` ran exactly once despite the error
    assert client.disconnect_called == 1


def test_fetch_strategy_details_request_fields_include_timeline_false() -> None:
    """`include_timeline=False` propagates verbatim to the proto request."""
    fake_response = _make_details()
    client = _FakeClient(response=fake_response)
    status_helpers._fetch_strategy_details(
        client,  # type: ignore[arg-type]
        "demo",
        include_timeline=False,
        timeline_limit=25,
    )
    req = client.dashboard.last_request
    assert req.include_timeline is False
    # Plan target: include_pnl_history is always False (wired by helper, not arg)
    assert req.include_pnl_history is False
    assert req.timeline_limit == 25


def test_fetch_strategy_details_grpc_like_error_string(
    capsys: pytest.CaptureFixture,
) -> None:
    """A gRPC-style error class stringifies verbatim into the error message."""

    class _FakeGrpcError(Exception):
        def __str__(self) -> str:
            return "StatusCode.UNAVAILABLE: channel closed"

    client = _FakeClient(raise_exc=_FakeGrpcError())
    with pytest.raises(SystemExit) as excinfo:
        status_helpers._fetch_strategy_details(
            client,  # type: ignore[arg-type]
            "demo",
            include_timeline=True,
            timeline_limit=10,
        )
    assert excinfo.value.code == 1
    err = capsys.readouterr().err
    # Whole error string grep — smoke test anchor
    assert "Failed to get strategy details: StatusCode.UNAVAILABLE: channel closed" in err
    assert client.disconnect_called == 1


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _render_json_position edge cases
# ---------------------------------------------------------------------------


def test_render_json_position_strategy_positions_partial_fields() -> None:
    """Only some optional fields populated -> only those emitted (proto3 sentinel)."""
    sp = _make_strategy_position(
        direction="SHORT",
        size_usd="250",
        # entry_price, current_price, etc. remain "" -> stripped
    )
    pos = _make_position(strategy_positions=[sp])
    out = status_helpers._render_json_position(pos)
    entry = out["strategy_positions"][0]
    # Required always emitted
    assert "position_type" in entry
    # Explicitly set optionals present
    assert entry["direction"] == "SHORT"
    assert entry["size_usd"] == "250"
    # Empty-string optionals NOT present
    assert "entry_price" not in entry
    assert "current_price" not in entry
    assert "unrealized_pnl_usd" not in entry
    assert "unrealized_pnl_pct" not in entry
    assert "collateral_usd" not in entry
    assert "leverage" not in entry
    assert "health_factor" not in entry


def test_render_json_position_strategy_positions_details_map_preserved() -> None:
    """Non-empty `details` map is dict-cloned (proto map semantics preserved)."""
    sp = _make_strategy_position(details={"risk_band": "amber", "trace": "xyz"})
    pos = _make_position(strategy_positions=[sp])
    out = status_helpers._render_json_position(pos)
    assert out["strategy_positions"][0]["details"] == {
        "risk_band": "amber",
        "trace": "xyz",
    }
    # Ensure it's a dict, not the proto object
    assert isinstance(out["strategy_positions"][0]["details"], dict)


def test_render_json_position_multiple_strategy_positions() -> None:
    """More than one position entry flows through the serializer."""
    sp1 = _make_strategy_position(position_id="ETH-PERP", direction="LONG")
    sp2 = _make_strategy_position(position_id="BTC-PERP", direction="SHORT")
    pos = _make_position(strategy_positions=[sp1, sp2])
    out = status_helpers._render_json_position(pos)
    assert len(out["strategy_positions"]) == 2
    assert out["strategy_positions"][0]["position_id"] == "ETH-PERP"
    assert out["strategy_positions"][1]["position_id"] == "BTC-PERP"


def test_render_json_position_health_factor_zero_included() -> None:
    """`health_factor=0` is NOT None, so it's emitted (coerced to float)."""
    pos = _make_position(health_factor=0)
    out = status_helpers._render_json_position(pos)
    assert out["health_factor"] == 0.0


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _render_json_summary
# ---------------------------------------------------------------------------


def test_render_json_summary_pnl_since_deploy_proto3_empty_string_to_none() -> None:
    """proto3 empty-string for `pnl_since_deploy_usd` becomes JSON `None`.

    This preserves the documented behavior: proto3 string fields never have
    `is set` semantics -- an empty string IS the sentinel for unset. The
    refactor keeps this behavior byte-for-byte.
    """
    s = _make_summary(pnl_since_deploy_usd="")
    out = status_helpers._render_json_summary(s)
    assert out["pnl_since_deploy_usd"] is None


def test_render_json_summary_zero_pnl_since_deploy_is_not_none() -> None:
    """A non-empty `"0"` string is kept verbatim (distinguished from empty)."""
    s = _make_summary(pnl_since_deploy_usd="0")
    out = status_helpers._render_json_summary(s)
    # `"0"` is truthy as a non-empty string -> passes the `or None` guard
    assert out["pnl_since_deploy_usd"] == "0"


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _render_json_timeline
# ---------------------------------------------------------------------------


def test_render_json_timeline_empty_returns_empty_list() -> None:
    """Empty timeline serializer returns `[]`, not None."""
    assert status_helpers._render_json_timeline([]) == []


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _render_json_chain_health
# ---------------------------------------------------------------------------


def test_render_json_chain_health_empty_returns_empty_dict() -> None:
    """Empty chain_health map serializer returns `{}`."""
    assert status_helpers._render_json_chain_health({}) == {}


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _render_json_operator_card
# ---------------------------------------------------------------------------


def test_render_json_operator_card_suggested_actions_empty_list() -> None:
    """Empty `suggested_actions` list stays as an empty list in the JSON."""
    oc = SimpleNamespace(
        strategy_id="demo",
        severity="LOW",
        reason="FYI",
        risk_description="",
        suggested_actions=[],
    )
    out = status_helpers._render_json_operator_card(oc)
    assert out["suggested_actions"] == []
    # risk_description passes through even when empty (no guard in helper)
    assert out["risk_description"] == ""


def test_render_json_operator_card_suggested_actions_list_coerced() -> None:
    """RepeatedScalarContainer-like inputs are coerced to plain list."""
    oc = SimpleNamespace(
        strategy_id="demo",
        severity="MEDIUM",
        reason="Watch",
        risk_description="",
        suggested_actions=("a", "b", "c"),  # tuple -> list
    )
    out = status_helpers._render_json_operator_card(oc)
    assert out["suggested_actions"] == ["a", "b", "c"]
    assert isinstance(out["suggested_actions"], list)


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _render_details_as_json orchestration guards
# ---------------------------------------------------------------------------


def test_render_details_as_json_position_falsy_skips_render() -> None:
    """`details.position` being truthy-empty container is skipped by outer guard."""
    # An empty SimpleNamespace stands in for a proto message where bool(...) is
    # True but all sub-fields are empty -- the inner `if pos_data` short-circuits.
    details = _make_details(position=_make_position())
    data = json.loads(status_helpers._render_details_as_json(details))
    assert "position" not in data


def test_render_details_as_json_operator_card_truthy_no_strategy_id() -> None:
    """Operator card with empty `strategy_id` MUST render when present (#1704).

    Previously this test asserted the buggy behavior: an empty-string
    `strategy_id` suppressed the entire card (proto3 empty-string-as-falsy
    used as presence sentinel). The #1704 fix switches presence to the
    parent's `HasField("operator_card")` (with a `is not None` fallback for
    test fakes), so an intentionally-empty `strategy_id` no longer hides
    the card.
    """
    oc = SimpleNamespace(
        strategy_id="",  # empty but card IS present -> must render
        severity="HIGH",
        reason="present-but-unset",
        risk_description="",
        suggested_actions=[],
    )
    details = _make_details(operator_card=oc)
    data = json.loads(status_helpers._render_details_as_json(details))
    assert "operator_card" in data
    assert data["operator_card"]["severity"] == "HIGH"
    assert data["operator_card"]["reason"] == "present-but-unset"


def test_render_details_as_json_chain_health_empty_dict_omitted() -> None:
    """Empty chain_health dict -> the top-level key is omitted."""
    details = _make_details(chain_health={})
    data = json.loads(status_helpers._render_details_as_json(details))
    assert "chain_health" not in data


def test_render_details_as_json_is_valid_json() -> None:
    """Top-level output is a JSON string that round-trips via json.loads."""
    details = _make_details()
    out_str = status_helpers._render_details_as_json(details)
    assert isinstance(out_str, str)
    # Round-trip parse -- no exception = valid JSON
    data = json.loads(out_str)
    assert data["strategy_id"] == "demo"
