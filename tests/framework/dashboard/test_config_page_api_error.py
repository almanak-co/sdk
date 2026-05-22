"""Regression test for issue #1714.

The config-editor's ``call_config_update_api`` previously told operators
to start the API with ``python -m src.api.main`` -- a stale entrypoint
that no longer exists in this repo. The current entrypoint is the
``almanak gateway`` CLI command (the gateway exposes the HTTP API the
dashboard talks to on port 8000).

This test pins the new error string so the stale advice does not
silently regress.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import requests

from almanak.framework.dashboard.pages.config import call_config_update_api


def test_config_update_connection_error_points_to_almanak_gateway() -> None:
    """When the HTTP API is unreachable the error must point operators
    at the current CLI startup command, not the removed
    ``python -m src.api.main`` entrypoint.
    """
    with patch(
        "almanak.framework.dashboard.pages.config.requests.post",
        side_effect=requests.exceptions.ConnectionError("connection refused"),
    ):
        result = call_config_update_api(
            deployment_id="test-strategy",
            updates={"max_slippage": Decimal("0.01")},
        )

    assert result["success"] is False
    assert result["api_unavailable"] is True

    error = result["error"]

    # The stale entrypoint MUST NOT appear -- that was the whole bug.
    assert "src.api.main" not in error
    assert "python -m" not in error

    # The current entrypoint MUST be referenced.
    assert "almanak gateway" in error

    # And the dashboard docs link must be included so operators can
    # self-serve.
    assert "https://sdk.docs.almanak.co/cli/almanak-dashboard/" in error
