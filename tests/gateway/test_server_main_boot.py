"""Boot-path test for ``almanak.gateway.server.main`` (Phase 1, #2107).

Pins the production gateway entrypoint contract:

    main() -> load_config() -> serve(settings = config.gateway)

Without this test the handoff is implicit — a future refactor that
swaps ``load_config().gateway`` for a different attribute, or drops
the call entirely, would slip past CI.

The test patches ``almanak.config.service.load_config`` to return a
sentinel config object and patches ``serve`` to a no-op coroutine,
then asserts identity between ``serve``'s argument and the sentinel's
``.gateway``. Patching the source of truth (rather than resolving the
real env twice and comparing) keeps the test deterministic — ambient
``AGENT_ID`` / ``ALMANAK_GATEWAY_*`` vars in the test runner cannot
flip modes or perturb the comparison.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from almanak.gateway import server as gateway_server


def test_main_passes_load_config_gateway_to_serve() -> None:
    """``main()`` resolves config and forwards ``config.gateway`` to ``serve``."""
    captured: dict[str, object] = {}

    async def fake_serve(settings: object) -> None:
        captured["settings"] = settings

    # Sentinel config: a SimpleNamespace identity-comparable via ``is``.
    # ``main()`` logs ``settings.grpc_port`` before handing off to ``serve``,
    # so the sentinel exposes the attribute as a sentinel value too.
    expected_gateway = SimpleNamespace(grpc_port=50051)
    fake_config = SimpleNamespace(gateway=expected_gateway)

    # ``main()`` does ``from almanak.config.service import load_config`` at
    # call time, so the patch target is the source module — not the gateway
    # server module (where the name isn't bound at import time).
    with (
        patch("almanak.config.service.load_config", return_value=fake_config),
        patch.object(gateway_server, "serve", side_effect=fake_serve) as serve_mock,
    ):
        gateway_server.main()

    assert serve_mock.call_count == 1, "serve() must be called exactly once from main()"
    assert "settings" in captured, "fake_serve was never awaited by asyncio.run"
    assert captured["settings"] is expected_gateway, (
        "main() must hand load_config().gateway directly to serve() — identity, "
        "not equality. Any intermediate re-construction or attribute swap breaks "
        "the boot contract."
    )
