"""The keeper fixture validates canonical envelopes against the configured emitter and key."""

from copy import deepcopy

import pytest

from tests import gmx_fork
from tests.gmx_evidence import accepted, checkpoint


def test_driver_checks_the_configured_emitter_and_retained_request_key(monkeypatch):
    state = accepted()
    request = state.requests[0]
    receipt = checkpoint().outcomes[2].request.events[-1].receipt
    monkeypatch.setattr(gmx_fork, "pending_request", lambda *args: (state, request))
    retained = []
    monkeypatch.setattr(
        gmx_fork, "retain_keeper", lambda *args, **kwargs: retained.append((args, kwargs))
    )
    selected = object()

    def execute(node, target, pending, source):
        assert target == state.config.target(request.allocation)
        assert pending == request and source is selected
        return receipt

    monkeypatch.setattr(gmx_fork, "execute_keeper", execute)
    gmx_fork.drive_external(None, None, None, lambda pending: selected)
    assert retained[0][0][-1] == receipt
    assert retained[0][1] == {"block_interval": 1}
    other = deepcopy(receipt)
    for log in other["logs"]:
        log["address"] = "0x" + "99" * 20
    monkeypatch.setattr(gmx_fork, "execute_keeper", lambda *args: other)
    with pytest.raises(StopIteration):
        gmx_fork.drive_external(None, None, None, lambda pending: selected)
