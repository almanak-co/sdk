from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner._teardown_helpers import fetch_positions_or_fallback


class GeneratedPerpStrategy:
    deployment_id = "deployment:test"

    def get_open_positions(self):
        raise AttributeError("PerpsPositionOnChain has no attribute 'notional_usd'")


@pytest.mark.asyncio
async def test_position_enumeration_error_names_strategy_hook_and_keeps_traceback(caplog):
    early_result = object()
    runner = SimpleNamespace(
        config=SimpleNamespace(allow_unsafe_teardown_fallback=False),
        _request_teardown_failure_shutdown=MagicMock(),
        _create_error_result=MagicMock(return_value=early_result),
        _execute_teardown_inline=AsyncMock(),
    )

    positions, result = await fetch_positions_or_fallback(
        runner,
        GeneratedPerpStrategy(),
        [],
        None,
        datetime.now(UTC),
        None,
        MagicMock(),
    )

    assert positions is None
    assert result is early_result
    error_message = runner._create_error_result.call_args.args[2]
    assert "GeneratedPerpStrategy.get_open_positions()" in error_message
    assert "AttributeError: PerpsPositionOnChain has no attribute 'notional_usd'" in error_message
    assert any(record.exc_info is not None for record in caplog.records)
