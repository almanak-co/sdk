"""Hosted-parity single-strategy dashboard entrypoint (Problem A1).

This entrypoint mirrors how the hosted platform launches a dashboard for a
single strategy: it connects to one gateway, scopes to one ``deployment_id``,
loads that strategy's ``dashboard/ui.py`` (when present), and renders it
through the same ``render_custom_dashboard_safe(...)`` path the hosted image
uses.

It deliberately does NOT:

- discover other strategies in the repo,
- expose Command Center / Strategy Library navigation,
- fall back to a mock API client when the gateway is unreachable.

Fail-closed semantics on gateway unavailability are mandatory — a mock-fed
dashboard silently invalidates the whole reason this entrypoint exists.

The launcher (``run_helpers._start_dashboard_background``) injects:

- ``ALMANAK_DASHBOARD_DEPLOYMENT_ID`` — resolved deployment_id to scope to
- ``ALMANAK_DASHBOARD_WORKING_DIR`` — strategy folder containing ``config.json``
  and (optionally) ``dashboard/ui.py``
- ``GATEWAY_HOST`` / ``GATEWAY_PORT`` / ``ALMANAK_GATEWAY_AUTH_TOKEN`` —
  gateway connection (same shape Command Center receives)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import streamlit as st

if TYPE_CHECKING:
    from almanak.framework.dashboard.custom.api_client import DashboardAPIClient

# Mirror app.py: add project root to sys.path so framework imports resolve
# when streamlit launches this file directly.
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

logger = logging.getLogger(__name__)

PAGE_TITLE = "Almanak Strategy Dashboard"
PAGE_ICON = "📊"

ENV_DEPLOYMENT_ID = "ALMANAK_DASHBOARD_DEPLOYMENT_ID"
ENV_WORKING_DIR = "ALMANAK_DASHBOARD_WORKING_DIR"
# Optional — when set, the dashboard prefers this serialised runtime
# config over re-reading ``working_dir/config.json``. Set by the launcher
# to forward the post-bootstrap config (covers --config pointing outside
# working_dir, copy-trading runtime overrides, and the resolved deployment_id
# field). Falls back to config.json on missing / malformed JSON so the
# subprocess never crashes on env shape regressions.
ENV_STRATEGY_CONFIG = "ALMANAK_DASHBOARD_STRATEGY_CONFIG"

# Where ``_load_strategy_config`` resolved the rendered config from. Drives the
# provenance banner (``_render_config_provenance_banner``) so a manual
# ``streamlit run app_single.py`` reattach that didn't forward
# ``ALMANAK_DASHBOARD_STRATEGY_CONFIG`` can't silently mislabel the pair / chain /
# display name of a real run (VIB-5802).
CONFIG_SOURCE_RUNTIME = "runtime"  # launcher-forwarded env var — authoritative
CONFIG_SOURCE_FILE = "file"  # fell back to working_dir/config.json on disk
CONFIG_SOURCE_NONE = "none"  # neither available — labels come from defaults


class _StrategyConfigResolution(NamedTuple):
    """The resolved config plus WHERE it came from (for the provenance banner)."""

    config: dict
    source: str


def _read_env_required(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _load_strategy_config(working_dir: Path) -> _StrategyConfigResolution:
    """Resolve the strategy config the dashboard should render against.

    Resolution order:

    1. ``ALMANAK_DASHBOARD_STRATEGY_CONFIG`` (JSON-serialised runtime
       config from the launcher) — preferred because it reflects
       ``--config`` flag, runtime overrides (copy-trading flags), and
       the resolved ``deployment_id`` field as the running strategy sees
       them. Reported as ``CONFIG_SOURCE_RUNTIME`` (authoritative).
    2. ``working_dir/config.json`` (file on disk) — fallback for the
       case where the env var is missing (someone launched the
       dashboard manually) or the JSON was malformed. Reported as
       ``CONFIG_SOURCE_FILE`` — the caller surfaces a loud banner because
       the on-disk file may be a demo default / stale config, not the
       ``--config`` the live run actually used (VIB-5802).

    Returns the config plus its source. Empty dict + ``CONFIG_SOURCE_NONE``
    on total failure — never crashes the subprocess; the dashboard falls
    back to whatever defaults the custom UI prescribes.
    """
    serialised = os.environ.get(ENV_STRATEGY_CONFIG, "").strip()
    if serialised:
        try:
            parsed = json.loads(serialised)
        except json.JSONDecodeError as e:
            logger.warning(
                "Failed to parse %s — falling back to %s/config.json: %s",
                ENV_STRATEGY_CONFIG,
                working_dir,
                e,
            )
        else:
            if isinstance(parsed, dict):
                return _StrategyConfigResolution(parsed, CONFIG_SOURCE_RUNTIME)
            logger.warning(
                "%s did not parse to a dict (got %s); falling back to config.json",
                ENV_STRATEGY_CONFIG,
                type(parsed).__name__,
            )

    config_path = working_dir / "config.json"
    if not config_path.is_file():
        return _StrategyConfigResolution({}, CONFIG_SOURCE_NONE)
    try:
        with config_path.open() as f:
            loaded = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Failed to read %s: %s", config_path, e)
        return _StrategyConfigResolution({}, CONFIG_SOURCE_NONE)
    if isinstance(loaded, dict):
        return _StrategyConfigResolution(loaded, CONFIG_SOURCE_FILE)
    # Valid JSON that isn't an object (e.g. a top-level list) parses cleanly but
    # would crash every downstream ``strategy_config.get(...)`` — mirror the
    # env-var branch's dict guard and render with defaults instead.
    logger.warning(
        "%s did not parse to a dict (got %s); rendering with defaults",
        config_path,
        type(loaded).__name__,
    )
    return _StrategyConfigResolution({}, CONFIG_SOURCE_NONE)


def _render_config_provenance_banner(source: str, working_dir: Path) -> None:
    """Warn loudly when the rendered labels did NOT come from the live run's config.

    The launcher forwards the running strategy's post-bootstrap config via
    ``ALMANAK_DASHBOARD_STRATEGY_CONFIG``, so a normal
    ``almanak strat run --dashboard`` shows no banner. A manual
    ``streamlit run app_single.py`` reattach (e.g. viewing a stopped /
    torn-down strategy) does NOT forward it, so the header pair / chain /
    display name fall back to whatever ``working_dir/config.json`` holds —
    which may be a demo default or a stale config, not the ``--config`` the
    run actually used. Money data (positions, ledger, PnL) is gateway-backed
    and stays accurate; only the config-derived LABELS can be wrong, so the
    warning is scoped to labels (VIB-5802).
    """
    if source == CONFIG_SOURCE_RUNTIME:
        return
    if source == CONFIG_SOURCE_FILE:
        st.warning(
            f"Showing strategy **labels** (pair / chain / display name) from "
            f"`{working_dir / 'config.json'}` on disk — the running strategy's actual "
            f"config was not forwarded to this dashboard, so these labels may not match "
            f"the live run's `--config` / runtime overrides. **Money data below "
            f"(positions, ledger, PnL) is read from the gateway and is accurate.** For "
            f"exact labels, launch via `uv run almanak strat run --dashboard` or set "
            f"`{ENV_STRATEGY_CONFIG}` to the run's serialised config."
        )
        return
    st.warning(
        f"No strategy config resolved for this dashboard (neither "
        f"`{ENV_STRATEGY_CONFIG}` nor `{working_dir / 'config.json'}` was available) — "
        f"labels fall back to defaults and may not reflect the live run. Money data "
        f"below is gateway-backed and accurate."
    )


def _connect_gateway_fail_closed(deployment_id: str) -> DashboardAPIClient | None:
    """Build a real gateway-backed API client, or return ``None`` on failure.

    Returns ``None`` when the gateway cannot be reached OR is unhealthy.
    Callers MUST treat ``None`` as a render-blocking error and surface a
    clear "gateway unreachable" message — substituting a mock client
    would silently invalidate the hosted-parity premise of this entrypoint.

    Always calls ``gateway.connect()`` regardless of the local
    ``is_connected`` flag. ``GatewayDashboardClient.is_connected`` only
    reports whether a gRPC channel object exists, NOT whether the
    gateway is responding — once a connection has been established, a
    subsequently-killed gateway will leave ``is_connected`` reporting
    True. Calling ``connect()`` always forces the health check at
    ``gateway_client.py:454`` to run, surfacing the stale-connection
    case as ``GatewayConnectionError`` and triggering the fail-closed
    UX (rather than silently returning a client whose RPCs swallow
    failures into empty dicts/lists — Codex P2 on PR #2372).
    """
    try:
        from almanak.framework.dashboard.custom.api_client import create_api_client
        from almanak.framework.dashboard.gateway_client import (
            GatewayConnectionError,
            get_dashboard_client,
        )
    except ImportError as e:
        logger.error("Failed to import gateway client modules: %s", e)
        return None

    gateway = get_dashboard_client()
    try:
        gateway.connect()
    except GatewayConnectionError as e:
        logger.warning("Gateway connection / health-check failed for %s: %s", deployment_id, e)
        return None
    except Exception:
        # Broaden beyond GatewayConnectionError for any unexpected
        # exception in the connect path (broken stub, malformed env,
        # gRPC channel surprises). Fail-closed is the safer outcome
        # for any connect-side error — operators see "Gateway
        # unreachable" instead of a stack trace or silent mock.
        logger.exception("Unexpected error connecting to gateway for %s", deployment_id)
        return None

    if not gateway.is_connected:
        return None

    return create_api_client(gateway, deployment_id)


def _render_gateway_unreachable_error(deployment_id: str) -> None:
    st.error("Gateway unreachable")
    st.markdown(
        f"The hosted-parity dashboard for `{deployment_id}` requires a live gateway "
        "connection. None is available.\n\n"
        "**The dashboard will not render mock data** — a mock-fed dashboard "
        "silently invalidates validation of hosted parity (the whole reason "
        "this entrypoint exists)."
    )
    host = os.environ.get("GATEWAY_HOST", "?")
    port = os.environ.get("GATEWAY_PORT", "?")
    st.markdown(
        f"**Action**: confirm the gateway is running at `{host}:{port}` and the auth token is current, then refresh."
    )


def _render_missing_ui_fallback(working_dir: Path, deployment_id: str) -> None:
    """Render the same hosted fallback shape used when a strategy has no custom ui.py.

    The hosted image renders a generic operator shell pointing at the strategy
    via the gateway. We mirror that here with a minimal status header — the
    point is to not crash, and to make the "no dashboard authored" state
    obvious so it can be fixed.
    """
    st.warning(f"No custom dashboard for `{deployment_id}`")
    st.markdown(
        f"No `dashboard/ui.py` was found under `{working_dir}`.\n\n"
        "Hosted-parity mode renders the strategy's own dashboard module. "
        "Generate one with `almanak strat new` (writes a `dashboard/` folder) "
        "or add `dashboard/ui.py` exporting `render_custom_dashboard(...)`."
    )


def _render_missing_context_error() -> None:
    st.error("Missing strategy context")
    st.markdown(
        "This entrypoint must be launched by `uv run almanak strat run --dashboard`. "
        f"It requires `{ENV_DEPLOYMENT_ID}` and `{ENV_WORKING_DIR}` to be set "
        "in the environment. To browse all strategies, use `uv run almanak strat run "
        "--dashboard --dashboard-mode=command-center`."
    )


def main() -> None:
    st.set_page_config(
        page_title=PAGE_TITLE,
        page_icon=PAGE_ICON,
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    deployment_id = _read_env_required(ENV_DEPLOYMENT_ID)
    working_dir_raw = _read_env_required(ENV_WORKING_DIR)
    if not deployment_id or not working_dir_raw:
        _render_missing_context_error()
        return

    working_dir = Path(working_dir_raw).expanduser().resolve()
    resolved = _load_strategy_config(working_dir)
    strategy_config = resolved.config

    st.caption(f"Strategy: `{deployment_id}` · Source: `{working_dir}`")
    _render_config_provenance_banner(resolved.source, working_dir)

    # Connect first so a missing-ui fallback can still talk to the gateway
    # if/when we add a generic fallback view later.
    api_client = _connect_gateway_fail_closed(deployment_id)
    if api_client is None:
        _render_gateway_unreachable_error(deployment_id)
        return

    dashboard_dir = working_dir / "dashboard"
    ui_path = dashboard_dir / "ui.py"
    if not ui_path.is_file():
        _render_missing_ui_fallback(working_dir, deployment_id)
        return

    try:
        from almanak.framework.dashboard.custom.discoverer import CustomDashboardInfo
        from almanak.framework.dashboard.custom.renderer import (
            render_custom_dashboard_safe,
        )
    except ImportError as e:
        st.error(f"Dashboard rendering modules unavailable: {e}")
        st.code(traceback.format_exc())
        return

    # Display name first so the resolution order is readable: explicit
    # display name → deployment_id → working_dir folder name. ``display_name``
    # then falls back to ``strategy_name`` so an unnamed strategy still
    # gets a sensible tab title (working_dir folder name).
    display_name = strategy_config.get("strategy_display_name")
    strategy_name = strategy_config.get("deployment_id") or display_name or working_dir.name
    display_name = display_name or strategy_name

    dashboard_info = CustomDashboardInfo(
        strategy_name=str(strategy_name),
        dashboard_path=dashboard_dir,
        display_name=str(display_name),
        icon=None,
    )

    # Pass api_client explicitly so the renderer's internal mock fallback
    # (``custom/renderer.py:_resolve_api_client``) is never reached — that
    # path can substitute a mock client on connection failure, which is the
    # exact behaviour this entrypoint exists to prevent.
    render_custom_dashboard_safe(
        dashboard_info=dashboard_info,
        deployment_id=deployment_id,
        strategy_config=strategy_config,
        api_client=api_client,
        session_state=dict(st.session_state),
    )


if __name__ == "__main__":
    main()
