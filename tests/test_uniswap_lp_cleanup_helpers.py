"""The Uniswap LP cleanup lane covers every promoted LP reference with family-exact checks."""

from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import EconomicAction
from almanak.gateway.fork_config import ForkEoaFund
from scripts.legacy_configuration.author import read_author
from tests.fork import test_uniswap_lp_cleanup as lane
from tests.legacy_references import harness_directory
from tests.qa import worker
from tests.qa.catalog import ROOT, Case, uniswap_lp_cleanup_references, uniswap_lp_family

EXPECTED = (
    "pancakeswap_v3_bnb_lp",
    "uniswap_v3_arbitrum_lp",
    "uniswap_v3_avalanche_lp",
    "uniswap_v3_base_lp",
    "uniswap_v3_bnb_lp",
    "uniswap_v3_ethereum_lp",
    "uniswap_v3_optimism_lp",
    "uniswap_v3_polygon_lp",
    "uniswap_v3_robinhood_lp",
    "uniswap_v4_arbitrum_lp",
    "uniswap_v4_avalanche_lp",
    "uniswap_v4_base_lp",
    "uniswap_v4_bnb_lp",
    "uniswap_v4_lp",
    "uniswap_v4_optimism_lp",
    "uniswap_v4_polygon_lp",
    "uniswap_v4_robinhood_lp",
)


def test_every_promoted_uniswap_lp_reference_is_in_the_cleanup_lane():
    assert uniswap_lp_cleanup_references() == EXPECTED
    assert {uniswap_lp_family(name) for name in EXPECTED} == {
        "pancakeswap_v3",
        "uniswap_v3",
        "uniswap_v4",
    }
    assert lane.APPROVALS["pancakeswap_v3"] == ("approve", "approve")
    assert lane.APPROVALS["uniswap_v3"] == ("approve", "approve")
    assert lane.APPROVALS["uniswap_v4"] == ("approve_erc20", "revoke_erc20")


def test_cleanup_family_resolves_the_retained_pancake_connector_identity():
    assert uniswap_lp_family(ConnectorId("pancakeswap_v3")) == "pancakeswap_v3"


def test_pancake_cleanup_eoa_uses_its_identified_lp_fund_range():
    reference = "pancakeswap_v3_bnb_lp"
    source = read_author(harness_directory(reference) / "config.yaml").fork_config()
    author = lane.eoa_lp_configuration(reference, source)
    assert all(isinstance(f, ForkEoaFund) for f in author.funds)
    assert tuple(f.foundry_account_index for f in author.funds) == tuple(
        range(560908001, 560908001 + len(source.funds))
    )
    assert tuple((f.account, f.assets) for f in author.funds) == tuple(
        (f.account, f.assets) for f in source.funds
    )


# A converted reference has no author execution block, so its own fork configuration
# needs a resolution. Only the reference name selects the family index range, so every
# range is checked against a frozen pre-conversion fixture.
@pytest.mark.parametrize(
    "reference,fixture,start",
    (
        ("uniswap_v3_ethereum_lp", "uniswap_v4_lp", 941267),
        ("uniswap_v4_lp", "uniswap_v4_lp", 1094401),
        ("uniswap_v4_base_lp", "uniswap_v4_base_lp", 84539401),
    ),
)
def test_eoa_cleanup_funds_use_the_family_origin_qualified_indices(reference, fixture, start):
    source = read_author(harness_directory(fixture) / "config.yaml").fork_config()
    author = lane.eoa_lp_configuration(reference, source)
    assert all(isinstance(f, ForkEoaFund) for f in author.funds)
    assert tuple(f.foundry_account_index for f in author.funds) == tuple(
        range(start, start + len(source.funds))
    )
    assert tuple((f.account, f.assets) for f in author.funds) == tuple(
        (f.account, f.assets) for f in source.funds
    )


def outcome(kind, amount, *, needs_cleanup, asset="a", target="t"):
    return SimpleNamespace(
        success=True,
        operation=SimpleNamespace(kind=kind, amount=amount, asset=asset, target=target),
        after=SimpleNamespace(needs_cleanup=needs_cleanup),
    )


def interrupted(reference, **overrides):
    family = uniswap_lp_family(reference)
    approval, _ = lane.APPROVALS[family]
    values = dict(
        config=SimpleNamespace(connector=family),
        outcomes=(outcome(approval, 5, needs_cleanup=True),),
        delivered=0,
        pending=None,
        owned=(SimpleNamespace(position=None),),
    )
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(
    "reference", ("pancakeswap_v3_bnb_lp", "uniswap_v3_ethereum_lp", "uniswap_v4_lp")
)
def test_committed_approval_requires_one_undelivered_positive_allowance(reference):
    lane.assert_approval_committed(interrupted(reference))
    with pytest.raises(AssertionError):
        lane.assert_approval_committed(interrupted(reference, delivered=1))
    with pytest.raises(AssertionError):
        lane.assert_approval_committed(
            interrupted(reference, owned=(SimpleNamespace(position=object()),))
        )
    other = {
        "pancakeswap_v3_bnb_lp": "uniswap_v4_lp",
        "uniswap_v3_ethereum_lp": "uniswap_v4_lp",
        "uniswap_v4_lp": "pancakeswap_v3_bnb_lp",
    }[reference]
    foreign = interrupted(other, config=SimpleNamespace(connector=uniswap_lp_family(reference)))
    with pytest.raises(AssertionError):
        lane.assert_approval_committed(foreign)


def closed(reference, *outcomes, **overrides):
    values = dict(
        config=SimpleNamespace(connector=uniswap_lp_family(reference)),
        mode="closed",
        pending=None,
        delivered=len(outcomes),
        outcomes=outcomes,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(
    "reference", ("pancakeswap_v3_bnb_lp", "uniswap_v3_ethereum_lp", "uniswap_v4_lp")
)
def test_closure_must_revoke_the_same_allowance_without_trading(reference):
    approval, revocation = lane.APPROVALS[uniswap_lp_family(reference)]
    before = interrupted(reference)
    first = before.outcomes[0]
    revoked = outcome(revocation, 0, needs_cleanup=False)
    lane.assert_closed_effects(before, closed(reference, first, revoked))
    with pytest.raises(AssertionError):
        lane.assert_closed_effects(
            before,
            closed(reference, first, revoked, outcome("swap", 1, needs_cleanup=False)),
        )
    with pytest.raises(AssertionError):
        lane.assert_closed_effects(
            before,
            closed(reference, first, outcome(approval, 0, needs_cleanup=False, asset="b")),
        )
    with pytest.raises(AssertionError):
        lane.assert_closed_effects(before, closed(reference, first, revoked, delivered=1))


def test_family_observation_checks_require_no_liquidity_and_no_allowance():
    owned = SimpleNamespace(inventory0=1, inventory1=2)
    v4 = SimpleNamespace(
        wallet0=1,
        wallet1=2,
        needs_cleanup=False,
        owned=SimpleNamespace(position=None),
        position_liquidity=0,
    )
    lane.assert_observed_clear(v4, owned, "uniswap_v4")
    with pytest.raises(AssertionError):
        lane.assert_observed_clear(
            SimpleNamespace(**{**vars(v4), "position_liquidity": 7}), owned, "uniswap_v4"
        )
    v3 = SimpleNamespace(wallet0=1, wallet1=2, needs_cleanup=False, position=None)
    lane.assert_observed_clear(v3, owned, "uniswap_v3")
    with pytest.raises(AssertionError):
        lane.assert_observed_clear(
            SimpleNamespace(**{**vars(v3), "needs_cleanup": True}), owned, "uniswap_v3"
        )


def cleanup_case(reference, profile="eoa"):
    return Case(
        id=f"cleanup/pinned/{reference}/{profile}",
        lane="fork",
        reference=reference,
        connector=ConnectorId(uniswap_lp_family(reference)),
        chains=(ChainId("eip155:1"),),
        actions=(EconomicAction.REVOKE,),
        profile=profile,
        mode="fork",
        scenario="uniswap_cleanup",
    )


@pytest.mark.parametrize("reference", ("pancakeswap_v3_bnb_lp", "uniswap_v3_ethereum_lp"))
def test_worker_routes_the_uniswap_cleanup_scenario_to_the_lp_cleanup_harness(
    monkeypatch, tmp_path, reference
):
    case = cleanup_case(reference)
    assert worker.uses_uniswap_cleanup_harness(case)
    assert not worker.uses_curve_harness(case)
    curve = Case(id="c", lane="fork", connector=ConnectorId("curve"), scenario="curve_cleanup")
    assert not worker.uses_uniswap_cleanup_harness(curve)
    calls = []
    monkeypatch.setattr(worker, "uniswap_cleanup_case", lambda *args: calls.append(args))
    worker.execute_fork_case(case, "source", "config", None, tmp_path, tmp_path / "evidence")
    assert calls == [(case, "source", "config", None, tmp_path, tmp_path / "evidence")]


@pytest.mark.parametrize("profile", ("eoa", "safe_roles"))
def test_uniswap_cleanup_case_retains_the_approval_scenario_record(monkeypatch, tmp_path, profile):
    """Both LP families are converted, so the lane keeps the resolved configuration.

    The resolution itself needs a provider, so it is replaced here by the frozen
    pre-conversion fixture; the profile comes from the case operator file and the
    lane applies no EOA transform of its own.
    """
    from tests.strategy_fork import adapter, harness

    reference = "uniswap_v4_lp"
    config = ROOT / "strategies" / reference / "config.yaml"
    resolved = read_author(harness_directory(reference) / "config.yaml").fork_config()
    exercised, retained = [], []
    monkeypatch.setattr(lane, "exercise", lambda *args: exercised.append(args))
    monkeypatch.setattr(adapter, "resolve_reference", lambda *args: None)
    monkeypatch.setattr(adapter, "fork_config_for", lambda *args: resolved)
    monkeypatch.setattr(harness, "resolve_config", lambda author, env, cache: author)
    monkeypatch.setattr(harness, "retain", lambda *args: retained.append(args))
    artifact = tmp_path / "evidence"
    worker.uniswap_cleanup_case(
        cleanup_case(reference, profile), "source", config, None, tmp_path, artifact
    )
    assert artifact.is_dir() and len(exercised) == 1 and len(retained) == 1
    source, author, env, temporary, record = exercised[0]
    assert (source, env, temporary) == ("source", None, tmp_path)
    assert author is resolved
    assert record == {
        "strategy": reference,
        "account_profile": profile,
        "scenario": "uniswap_cleanup",
        "phases": [],
        "status": "passed",
    }
    assert retained[0] == (tmp_path, artifact, record)
