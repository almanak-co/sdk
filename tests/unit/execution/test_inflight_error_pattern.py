"""Tests for RPC in-flight transaction limit error classification.

VIB-136: Alchemy enforces a 2-TX in-flight limit for delegated accounts
on Base mainnet.  The submitter must classify these errors correctly so
they can be retried with backoff.
"""

import pytest

from almanak.framework.execution.submitter.public import (
    INFLIGHT_LIMIT_PATTERNS,
    PublicMempoolSubmitter,
)


# ---------------------------------------------------------------------------
# Pattern matching
# ---------------------------------------------------------------------------


class TestInflightLimitPatterns:
    """INFLIGHT_LIMIT_PATTERNS must match real Alchemy error strings."""

    ALCHEMY_ERRORS = [
        "in-flight transaction limit reached for delegated accounts",
        "gapped-nonce tx from delegated accounts is temporarily not allowed",
    ]

    @pytest.mark.parametrize("error_msg", ALCHEMY_ERRORS)
    def test_patterns_match_alchemy_errors(self, error_msg: str) -> None:
        import re

        matched = any(re.search(p, error_msg.lower()) for p in INFLIGHT_LIMIT_PATTERNS)
        assert matched, f"No pattern matched: {error_msg}"


# ---------------------------------------------------------------------------
# _classify_error integration
# ---------------------------------------------------------------------------


class TestClassifyInflightError:
    """_classify_error must return 'inflight_limit' for in-flight errors."""

    def _make_submitter(self) -> PublicMempoolSubmitter:
        return PublicMempoolSubmitter(rpc_url="http://localhost:8545")

    def test_classify_inflight_limit(self) -> None:
        sub = self._make_submitter()
        assert sub._classify_error("in-flight transaction limit reached for delegated accounts") == "inflight_limit"

    def test_classify_gapped_nonce(self) -> None:
        sub = self._make_submitter()
        assert (
            sub._classify_error("gapped-nonce tx from delegated accounts is temporarily not allowed")
            == "inflight_limit"
        )

    def test_classify_nonce_still_nonce(self) -> None:
        """Nonce errors must NOT be reclassified as inflight_limit."""
        sub = self._make_submitter()
        assert sub._classify_error("nonce too low") == "nonce"

    def test_classify_connection_still_connection(self) -> None:
        sub = self._make_submitter()
        assert sub._classify_error("connection refused") == "connection"

    def test_classify_unknown_unchanged(self) -> None:
        sub = self._make_submitter()
        assert sub._classify_error("some random error") == "unknown"
