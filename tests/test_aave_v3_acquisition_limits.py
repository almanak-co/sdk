"""Preparation refuses incomplete time boundaries and changed acquisition policies."""

from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v3.economic_acquisition import bounds, group_at
from almanak.connectors.aave_v3.economic_prepare import provision
from tests.test_aave_v3_prepared import prepared as prepared


class Headers:
    def __init__(self, times):
        self.blocks = tuple(SimpleNamespace(number=n, timestamp=t) for n, t in enumerate(times))
        self.upper = self.blocks[-1]

    def floor(self, at):
        return max((b for b in self.blocks if b.timestamp <= at), key=lambda b: b.number)

    def header(self, number):
        return self.blocks[number]


def test_timestamp_groups_retain_all_eligible_blocks_and_following_boundary():
    history = Headers([10, 20, 20, 30])
    group, following = group_at(history, 20, 10, 2)
    assert [b.number for b in group] == [1, 2]
    assert following.number == 3
    group, _ = group_at(history, 20, 20, 1)
    assert [b.number for b in group] == [2]
    with pytest.raises(ValueError, match="explicit block limit"):
        group_at(history, 20, 10, 1)
    history.floor = lambda at: history.blocks[1]
    with pytest.raises(ValueError, match="latest eligible block"):
        group_at(history, 20, 20, 2)


@pytest.mark.parametrize("fault", ["none", "boundary", "blocks", "samples"])
def test_risk_bounds_refuse_unclosed_history_and_collection_overruns(prepared, fault):
    settings = prepared.settings
    start, end = settings.schedule.start, settings.schedule.deadline
    times = [start, end] if fault == "boundary" else [start, end, end + 12]
    history = Headers(times)
    data = settings.data.model_copy(
        update={
            "max_risk_blocks": 1 if fault == "blocks" else 2,
            "max_risk_samples": 3 if fault == "samples" else 4,
        }
    )
    settings = settings.model_copy(update={"data": data})
    histories = {a.chain: history for a in prepared.config.allocations}
    if fault == "none":
        assert all(
            (a.number, b.number) == (0, 1)
            for a, b in bounds(histories, prepared.config, settings).values()
        )
    else:
        with pytest.raises(ValueError, match="boundary|explicit block or sample limit"):
            bounds(histories, prepared.config, settings)


def test_acquisition_identity_and_token_survive_reuse_but_policy_cannot_change(prepared, tmp_path):
    directory = tmp_path / "acquisition"
    directory.mkdir()
    (directory / "acquisition.lock").touch()
    provision(directory, prepared.config, prepared.settings)
    token = directory / "gateway-auth.token"
    assert token.stat().st_mode & 0o777 == 0o600
    original = token.read_bytes()
    provision(directory, prepared.config, prepared.settings)
    assert token.read_bytes() == original
    changed = prepared.settings.model_copy(
        update={
            "data": prepared.settings.data.model_copy(
                update={"sample_seconds": prepared.settings.data.sample_seconds + 1}
            )
        }
    )
    with pytest.raises(ValueError, match="cannot change"):
        provision(directory, prepared.config, changed)
    (directory / "acquisition.json").unlink()
    with pytest.raises(ValueError, match="new or matching"):
        provision(directory, prepared.config, prepared.settings)
