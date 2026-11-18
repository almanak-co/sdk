"""Retained on-chain permission probes and configured identity selection replay offline."""

import gzip
import json
from types import SimpleNamespace

import pytest

from almanak.connectors.compound_v3 import permissions
from almanak.connectors.compound_v3.state import Checkpoint
from tests.compound_v3_helpers import CASES, EVIDENCE
from tests.compound_v3_helpers import candidate_compound_support as candidate_compound_support
from tests.lending_captures import read_capture
from tests.strategy_fork.compound_authority import assert_permissions


class CapturedCalls:
    def __init__(self, checks, config):
        self.config = config
        self.allowed = {self.key(c) for c in checks["allowed_controls"]}
        self.rejected = {self.key(c["call"]): c["error_selector"] for c in checks["rejected"]}

    @staticmethod
    def key(call):
        return json.dumps(call, sort_keys=True)

    def _validate_origin(self):
        assert self.config.fork_instance is not None or self.config.fork_bindings

    def rpc(self, method, params):
        assert method == "eth_call" and params[1] == "latest"
        key = self.key(params[0])
        if key in self.rejected:
            raise RuntimeError(self.rejected[key])
        assert key in self.allowed, "no recorded permission probe for this request"
        return "0x"


SAFE_CASES = [c for c in CASES if c["profile"] == "safe"]


def recorded(case):
    directory = EVIDENCE / case["name"]
    state = Checkpoint.model_validate_json(read_capture(directory / "checkpoint.json.gz"))
    result = json.loads(gzip.decompress((directory / "result.json.gz").read_bytes()))
    return state, result


# The captured probes include amounts above the former quotas and the collateral
# sentinel, which scope-only grants now admit, so the matrix no longer matches.
@pytest.mark.skip(reason="recapture pending (blueprint 27)")
@pytest.mark.parametrize("case", SAFE_CASES, ids=lambda c: c["name"])
def test_current_denial_matrix_matches_captured_installed_roles_checks(
    case, candidate_compound_support
):
    state, result = recorded(case)
    checks = result["authority_checks"]
    assert assert_permissions(CapturedCalls(checks, state.config), state.config) == checks


@pytest.mark.parametrize("case", SAFE_CASES, ids=lambda c: c["name"])
def test_configured_identities_match_the_captured_partitions(
    case, candidate_compound_support, monkeypatch
):
    state, _ = recorded(case)
    for partition in state.permissions.partitions:
        observed = next(o for o in state.baseline if o.target.account == partition.account)
        contracts = {c.address: c for c in partition.contracts}
        reader = SimpleNamespace(reader=None, block=observed.block, identity=contracts.__getitem__)
        monkeypatch.setattr(
            permissions, "read_market", lambda *_, market=observed.account.market: market
        )
        assert (
            permissions.identities(state.config, partition.account, reader) == partition.contracts
        )
