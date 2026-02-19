"""Tests for submitter selection with enabled flag handling."""

import pytest

from almanak.framework.execution.interfaces import SubmissionError
from almanak.framework.execution.submitter.private import PrivateRelaySubmitter
from almanak.framework.execution.submitter.selector import select_submitter


class _FakeSubmitter:
    """Minimal submitter stand-in for testing."""

    def __init__(self, name: str = "fake", enabled: bool = True):
        self.name = name
        self.enabled = enabled

    async def submit(self, txs):
        return []

    async def get_receipt(self, tx_hash, timeout=120.0):
        return None


class TestPublicMode:
    def test_returns_public_submitter(self) -> None:
        pub = _FakeSubmitter("public")
        result = select_submitter("public", pub)
        assert result.submitter is pub
        assert result.resolved_mode == "public"

    def test_ignores_private_submitter(self) -> None:
        pub = _FakeSubmitter("public")
        priv = _FakeSubmitter("private")
        result = select_submitter("public", pub, priv)
        assert result.submitter is pub


class TestPrivateMode:
    def test_returns_private_submitter(self) -> None:
        pub = _FakeSubmitter("public")
        priv = _FakeSubmitter("private")
        result = select_submitter("private", pub, priv)
        assert result.submitter is priv
        assert result.resolved_mode == "private"

    def test_raises_without_private_submitter(self) -> None:
        pub = _FakeSubmitter("public")
        with pytest.raises(SubmissionError, match="no private submitter"):
            select_submitter("private", pub)


class TestAutoMode:
    def test_prefers_enabled_private(self) -> None:
        pub = _FakeSubmitter("public")
        priv = _FakeSubmitter("private", enabled=True)
        result = select_submitter("auto", pub, priv)
        assert result.submitter is priv
        assert result.resolved_mode == "private"

    def test_falls_back_to_public_when_private_disabled(self) -> None:
        pub = _FakeSubmitter("public")
        priv = _FakeSubmitter("private", enabled=False)
        result = select_submitter("auto", pub, priv)
        assert result.submitter is pub
        assert result.resolved_mode == "public"

    def test_falls_back_to_public_when_no_private(self) -> None:
        pub = _FakeSubmitter("public")
        result = select_submitter("auto", pub)
        assert result.submitter is pub
        assert result.resolved_mode == "public"

    def test_private_relay_stub_is_disabled_by_default(self) -> None:
        pub = _FakeSubmitter("public")
        priv = PrivateRelaySubmitter()
        assert priv.enabled is False

        result = select_submitter("auto", pub, priv)
        assert result.submitter is pub
        assert result.resolved_mode == "public"


class TestUnknownMode:
    def test_raises_for_unknown_mode(self) -> None:
        pub = _FakeSubmitter("public")
        with pytest.raises(SubmissionError, match="Unknown submission mode"):
            select_submitter("flashbots", pub)
