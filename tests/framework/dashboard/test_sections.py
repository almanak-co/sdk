"""Smoke tests for ``almanak.framework.dashboard.sections``.

The sections module is the public entry point that strategy authors
hit from inside their ``dashboard/ui.py:render_custom_dashboard()``.
The contract these tests pin (VIB-3969):

1. The three section helpers are importable from the public surface
   ``almanak.framework.dashboard`` (per the convention documented in
   the package ``__init__.py``).
2. Each helper delegates to the corresponding focused gateway RPC and
   passes the result to the matching public renderer — no client-side
   recomputation, no re-shaping of the gateway's payload.
3. Each helper degrades gracefully when its RPC returns None (gateway
   down / fresh strategy with no data) — the page renders an info
   banner rather than crashing.
"""

from __future__ import annotations

from unittest.mock import patch

# ---- render_trade_tape_section ----


def test_render_trade_tape_section_is_in_public_api() -> None:
    """The author-facing import path must work."""
    from almanak.framework.dashboard import render_trade_tape_section

    assert callable(render_trade_tape_section)


def test_render_trade_tape_section_delegates_to_render_trade_tape() -> None:
    """The wrapper draws a divider + heading and forwards to the
    underlying renderer with the same args — no surprises."""
    from almanak.framework.dashboard import sections

    with (
        patch.object(sections.st, "divider") as mock_divider,
        patch.object(sections.st, "markdown") as mock_markdown,
        patch.object(sections, "render_trade_tape") as mock_render,
    ):
        sections.render_trade_tape_section("my-deployment-id", limit=25)

    mock_divider.assert_called_once_with()
    mock_markdown.assert_called_once_with("### Trade Tape")
    mock_render.assert_called_once_with("my-deployment-id", limit=25)


def test_render_trade_tape_section_default_limit_is_50() -> None:
    """Default limit matches the underlying renderer's default."""
    from almanak.framework.dashboard import sections

    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections, "render_trade_tape") as mock_render,
    ):
        sections.render_trade_tape_section("sid")

    mock_render.assert_called_once_with("sid", limit=50)


# ---- render_pnl_section ----


def test_render_pnl_section_is_in_public_api() -> None:
    from almanak.framework.dashboard import render_pnl_section

    assert callable(render_pnl_section)


def test_render_pnl_section_calls_pnl_summary_rpc_and_money_trail_renderer() -> None:
    """Helper fetches ``get_pnl_summary`` + ``get_cost_stack`` and forwards
    both to ``render_money_trail`` — no recomputation of NAV / PnL on the
    client side. The cost stack carries the realized-PnL components the
    Strategy PnL / APR tiles need."""
    from almanak.framework.dashboard import sections

    fake_pnl = object()
    fake_cost = object()

    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections, "get_pnl_summary", return_value=fake_pnl) as mock_get,
        patch.object(sections, "get_cost_stack", return_value=fake_cost) as mock_cost,
        patch.object(sections, "render_money_trail") as mock_render,
    ):
        sections.render_pnl_section("sid")

    mock_get.assert_called_once_with("sid")
    mock_cost.assert_called_once_with("sid")
    mock_render.assert_called_once_with(fake_pnl, fake_cost)


def test_render_pnl_section_degrades_cost_to_none_on_disconnect() -> None:
    """If the cost-stack RPC is down but PnL is present, still render the
    money trail (Strategy PnL / APR tiles degrade to "—") rather than
    dropping the whole PnL row."""
    from almanak.framework.dashboard import sections

    fake_pnl = object()

    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections, "get_pnl_summary", return_value=fake_pnl),
        patch.object(
            sections,
            "get_cost_stack",
            side_effect=sections.GatewayConnectionError("test"),
        ),
        patch.object(sections, "render_money_trail") as mock_render,
    ):
        sections.render_pnl_section("sid")

    mock_render.assert_called_once_with(fake_pnl, None)


def test_render_pnl_section_degrades_to_info_when_rpc_returns_none() -> None:
    """Gateway-down or fresh-strategy case — show an info banner, do
    not call the renderer with None (which would raise on field access)."""
    from almanak.framework.dashboard import sections

    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections.st, "info") as mock_info,
        patch.object(sections, "get_pnl_summary", return_value=None),
        patch.object(sections, "render_money_trail") as mock_render,
    ):
        sections.render_pnl_section("sid")

    mock_info.assert_called_once()
    mock_render.assert_not_called()


def test_render_pnl_section_fails_loud_on_gateway_disconnect() -> None:
    """Gateway-down / UNAUTHENTICATED case — ``get_pnl_summary`` re-raises
    ``GatewayConnectionError``. VIB-4047: the section must fail LOUD (a banner,
    never crash) — a quiet ``st.info`` hid a dashboard that could not read live
    money for a whole session. A generic disconnect renders ``st.warning`` or
    ``st.error``; the raw error text is not shown to the user."""
    from almanak.framework.dashboard import sections

    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections.st, "info") as mock_info,
        patch.object(sections.st, "warning") as mock_warning,
        patch.object(sections.st, "error") as mock_error,
        patch.object(
            sections,
            "get_pnl_summary",
            side_effect=sections.GatewayConnectionError("test raw detail"),
        ),
        patch.object(sections, "render_money_trail") as mock_render,
    ):
        sections.render_pnl_section("sid")

    assert mock_warning.called or mock_error.called
    mock_info.assert_not_called()
    mock_render.assert_not_called()


def test_render_pnl_section_auth_failure_renders_red_banner() -> None:
    """An UNAUTHENTICATED gateway (managed mainnet session-token mismatch)
    renders the red auth banner, not a benign info line (VIB-4047)."""
    from almanak.framework.dashboard import sections

    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections.st, "error") as mock_error,
        patch.object(
            sections,
            "get_pnl_summary",
            side_effect=sections.GatewayConnectionError(
                "Failed to get PnL summary: <_InactiveRpcError ... StatusCode.UNAUTHENTICATED ...>"
            ),
        ),
        patch.object(sections, "render_money_trail") as mock_render,
    ):
        sections.render_pnl_section("sid")

    mock_error.assert_called_once()
    assert "authenticate" in mock_error.call_args.args[0].lower()
    mock_render.assert_not_called()


# ---- render_cost_stack_section ----


def test_render_cost_stack_section_is_in_public_api() -> None:
    from almanak.framework.dashboard import render_cost_stack_section

    assert callable(render_cost_stack_section)


def test_render_cost_stack_section_calls_cost_stack_rpc_and_renderer() -> None:
    from almanak.framework.dashboard import sections

    fake_cost = object()

    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections, "get_cost_stack", return_value=fake_cost) as mock_get,
        patch.object(sections, "render_cost_stack") as mock_render,
    ):
        sections.render_cost_stack_section("sid")

    mock_get.assert_called_once_with("sid")
    mock_render.assert_called_once_with(fake_cost)


def test_render_cost_stack_section_default_heading_is_emitted() -> None:
    """Default heading lets the section stand alone; explicit
    ``heading=''`` suppresses it for composed-Audit layouts."""
    from almanak.framework.dashboard import sections

    fake_cost = object()
    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown") as mock_markdown,
        patch.object(sections, "get_cost_stack", return_value=fake_cost),
        patch.object(sections, "render_cost_stack"),
    ):
        sections.render_cost_stack_section("sid")
    mock_markdown.assert_called_once_with("### Cost Stack")


def test_render_cost_stack_section_empty_heading_suppresses_heading() -> None:
    from almanak.framework.dashboard import sections

    fake_cost = object()
    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown") as mock_markdown,
        patch.object(sections, "get_cost_stack", return_value=fake_cost),
        patch.object(sections, "render_cost_stack"),
    ):
        sections.render_cost_stack_section("sid", heading="")
    mock_markdown.assert_not_called()


def test_render_cost_stack_section_degrades_to_info_when_rpc_returns_none() -> None:
    from almanak.framework.dashboard import sections

    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections.st, "info") as mock_info,
        patch.object(sections, "get_cost_stack", return_value=None),
        patch.object(sections, "render_cost_stack") as mock_render,
    ):
        sections.render_cost_stack_section("sid")

    mock_info.assert_called_once()
    mock_render.assert_not_called()


def test_render_cost_stack_section_degrades_to_info_on_gateway_disconnect() -> None:
    """Gateway-down case — section must catch ``GatewayConnectionError``."""
    from almanak.framework.dashboard import sections

    with (
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections.st, "info") as mock_info,
        patch.object(
            sections,
            "get_cost_stack",
            side_effect=sections.GatewayConnectionError("test"),
        ),
        patch.object(sections, "render_cost_stack") as mock_render,
    ):
        sections.render_cost_stack_section("sid")

    mock_info.assert_called_once()
    mock_render.assert_not_called()
