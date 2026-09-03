"""``TeardownMode.from_cli_string`` — the single canonical mapping from the
CLI-facing ``--mode`` string ("graceful"/"emergency") to the internal
``TeardownMode`` enum (SOFT/HARD).

Not scoped to any one bug. Before this classmethod existed, the SAME mapping
was copy-pasted inline as ``TeardownMode.SOFT if mode == "graceful" else
TeardownMode.HARD`` at four independent call sites across
``teardown_manager.py`` / ``teardown.py`` / ``teardown_helpers.py`` — and a
fifth caller (``update_teardown_requests_lifecycle``) needed the identical
conversion, didn't have it, and silently failed on every invocation
(ALM-3473 investigation). This test exercises the shared function directly so
ANY future caller — not just the five that exist today — inherits a single,
tested contract instead of a sixth hand-rolled copy.
"""

from __future__ import annotations

import pytest

from almanak.framework.teardown.models import TeardownMode


class TestTeardownModeFromCliString:
    def test_graceful_maps_to_soft(self) -> None:
        assert TeardownMode.from_cli_string("graceful") is TeardownMode.SOFT

    def test_emergency_maps_to_hard(self) -> None:
        assert TeardownMode.from_cli_string("emergency") is TeardownMode.HARD

    @pytest.mark.parametrize("bad_value", ["SOFT", "HARD", "Graceful", "EMERGENCY", "", "fast", None])
    def test_anything_else_raises_rather_than_silently_defaulting(self, bad_value) -> None:
        """A caller bug (unrecognized mode string) must fail loud. Silently
        defaulting to HARD/emergency — the previous inline ternary's
        behaviour for any non-"graceful" input — is the wrong direction to
        guess wrong in: it would silently escalate an unrecognized mode to
        the fastest, highest-slippage, most costly teardown path."""
        with pytest.raises(ValueError, match="unknown teardown mode"):
            TeardownMode.from_cli_string(bad_value)
