"""Custom dashboard — delegates to the shared demo-agnostic charted renderer.

See :mod:`almanak.framework.dashboard.custom.basic`. The dashboard loader
discovers the ``render_custom_dashboard`` name (the VIB-3969 interface), which
we bind to the shared ``render_basic_dashboard`` so every basic demo draws from
one single-source implementation (no per-demo duplication).
"""

from almanak.framework.dashboard.custom.basic import (
    render_basic_dashboard as render_custom_dashboard,
)

__all__ = ["render_custom_dashboard"]
