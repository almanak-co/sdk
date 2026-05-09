"""F5 — augment_accounting_payload mode-aware contract preserved through T2.

Cases:
* F5.1 live + malformed JSON → raises AccountingPersistenceError
* F5.2 paper + malformed JSON → logs ERROR + returns original payload
* F5.3 live + unknown event_type → raises AccountingPersistenceError(cause=UnknownIntentTypeError)
* F5.4 paper + unknown event_type → logs ERROR + falls back to UTILITY's matching_policy_version
"""

from __future__ import annotations

import json
import logging

import pytest

from almanak.framework.accounting.payload_schemas import MATCHING_POLICY_VERSIONS
from almanak.framework.accounting.writer import augment_accounting_payload
from almanak.framework.primitives.taxonomy import UnknownIntentTypeError
from almanak.framework.primitives.types import Primitive
from almanak.framework.state.exceptions import AccountingPersistenceError


def test_live_raises_on_malformed_json() -> None:
    """F5.1 — live mode raises AccountingPersistenceError on broken JSON."""
    with pytest.raises(AccountingPersistenceError):
        augment_accounting_payload('{"event_type": "LP_OPEN", BROKEN', is_live=True)


def test_paper_logs_and_returns_unchanged(caplog: pytest.LogCaptureFixture) -> None:
    """F5.2 — paper mode logs ERROR and returns the original payload."""
    bad = '{"event_type": "LP_OPEN", BROKEN'
    with caplog.at_level(logging.ERROR, logger="almanak.framework.accounting.writer"):
        result = augment_accounting_payload(bad, is_live=False)
    assert result == bad
    assert any("not valid JSON" in r.message for r in caplog.records)


def test_live_raises_on_unknown_event_type() -> None:
    """F5.3 — live mode raises AccountingPersistenceError with UnknownIntentTypeError cause."""
    payload = json.dumps({"event_type": "FROBNICATE"})
    with pytest.raises(AccountingPersistenceError) as excinfo:
        augment_accounting_payload(payload, is_live=True)
    # The chain: AccountingPersistenceError raised from UnknownIntentTypeError.
    assert isinstance(excinfo.value.__cause__, UnknownIntentTypeError)


def test_paper_unknown_event_type_falls_back_to_utility(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F5.4 — paper mode logs ERROR + falls back to UTILITY's matching_policy_version."""
    payload = json.dumps({"event_type": "FROBNICATE"})
    with caplog.at_level(logging.ERROR, logger="almanak.framework.accounting.writer"):
        result = augment_accounting_payload(payload, is_live=False)
    decoded = json.loads(result)
    assert decoded["event_type"] == "FROBNICATE"
    assert decoded["matching_policy_version"] == MATCHING_POLICY_VERSIONS[Primitive.UTILITY]
    assert any("FROBNICATE" in r.message for r in caplog.records)


def test_live_known_event_type_stamps_per_primitive_version() -> None:
    """LP_OPEN payload gets stamped with MATCHING_POLICY_VERSIONS[Primitive.LP]."""
    payload = json.dumps({"event_type": "LP_OPEN"})
    result = augment_accounting_payload(payload, is_live=True)
    decoded = json.loads(result)
    assert decoded["matching_policy_version"] == MATCHING_POLICY_VERSIONS[Primitive.LP]


def test_perp_open_stamps_perp_version() -> None:
    """PERP_OPEN payload gets stamped with MATCHING_POLICY_VERSIONS[Primitive.PERP]."""
    payload = json.dumps({"event_type": "PERP_OPEN"})
    result = augment_accounting_payload(payload, is_live=True)
    decoded = json.loads(result)
    assert decoded["matching_policy_version"] == MATCHING_POLICY_VERSIONS[Primitive.PERP]
