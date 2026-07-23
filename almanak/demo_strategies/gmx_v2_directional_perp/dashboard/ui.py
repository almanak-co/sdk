"""Custom dashboard binding for this perp demo.

See :mod:`almanak.framework.dashboard.custom.basic`. The dashboard loader
discovers the ``render_custom_dashboard`` name (the VIB-3969 interface); this
demo wraps the shared ``render_basic_dashboard`` with ``include_perp_section=True``
so the snapshot-derived perp position story (direction / market / leverage /
notional / entry / mark — VIB-5942 / ALM-2977) renders under the PnL cards.
Non-perp demos bind the basic layout without the flag.
"""

from typing import Any

from almanak.framework.dashboard.custom.basic import render_basic_dashboard


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    render_basic_dashboard(
        deployment_id,
        strategy_config,
        api_client,
        session_state,
        include_perp_section=True,
    )


__all__ = ["render_custom_dashboard"]
