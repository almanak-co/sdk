"""StableSwap outcomes cannot discharge independently declared Crypto proofs."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from almanak.connectors.support import (
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
)
from tests.qa.catalog import Case, required
from tests.qa.variants import matches
from tests.qa.worker import completed_actions, fork_actions

CURVE = ConnectorId("curve")
CHAIN = ChainId("eip155:1")
STABLE = ProtocolVariant("stableswap_ng")
TWO = ProtocolVariant("twocrypto_ng")
TRI = ProtocolVariant("tricrypto_ng")
TRI2024 = ProtocolVariant("tricrypto_ng_2024")
GMX = ConnectorId("gmx_v2")


def declaration(variants=(STABLE, TWO, TRI), model_variants=(TWO, TRI)):
    return ConnectorChainSupport(
        chain=CHAIN,
        modes=(RunMode.FORK, RunMode.BACKTEST),
        actions=(EconomicAction.SWAP,),
        account_profiles=("eoa", "safe_roles"),
        restrictions=(),
        variants=variants,
        historical=(
            HistoricalSupport(
                model="crypto-model",
                mode="economic",
                actions=(EconomicAction.SWAP,),
                variants=model_variants,
            ),
        ),
    )


def test_variants_multiply_only_their_declared_execution_and_model_obligations():
    expected = required(((CURVE, SimpleNamespace(chain_support=(declaration(),))),))
    assert len(expected) == 8
    assert {o.variant for o in expected if o.mode == "fork"} == {STABLE, TWO, TRI}
    assert {o.variant for o in expected if o.mode == "backtest"} == {TWO, TRI}
    case = Case(
        id="stable",
        lane="fork",
        connector=CURVE,
        chains=(CHAIN,),
        mode="fork",
        profile="eoa",
        actions=(EconomicAction.SWAP,),
        variants=(STABLE,),
    )
    covered = case.obligations()
    assert covered <= expected
    assert len(expected - covered) == 7
    assert not covered & replace(case, variants=(TRI,)).obligations()
    assert len(replace(case, variants=(STABLE, TWO, TRI)).obligations()) == 3


@pytest.mark.parametrize(
    "variants,model_variants,message",
    [
        ((STABLE, STABLE), (STABLE,), "variants must be unique"),
        ((STABLE,), (STABLE, STABLE), "historical protocol variants must be unique"),
        ((STABLE,), (), "must declare"),
        ((), (STABLE,), "must declare"),
        ((STABLE,), (TRI,), "unsupported protocol variant"),
    ],
)
def test_invalid_variant_declarations_fail(variants, model_variants, message):
    with pytest.raises(ValueError, match=message):
        declaration(variants, model_variants)


def test_variantless_declarations_keep_the_existing_obligations():
    expected = required(((CURVE, SimpleNamespace(chain_support=(declaration((), ()),))),))
    assert len(expected) == 3 and {o.variant for o in expected} == {None}


@pytest.mark.parametrize("actual", [STABLE, TWO, TRI, TRI2024])
def test_native_action_proof_uses_the_retained_target_family(actual):
    outcomes = [
        {
            "operation": {"kind": "swap", "target": target(actual)},
            "success": True,
        }
    ]
    for claimed in (STABLE, TWO, TRI, TRI2024):
        observed = fork_actions(outcomes, {"main"}, connector=CURVE, variant=claimed)
        assert observed == ({"swap_exact_input"} if claimed == actual else set())
    outcomes[0]["success"] = False
    assert not fork_actions(outcomes, {"main"}, connector=CURVE, variant=actual)


def target(variant):
    pool = {"family": str(variant)}
    if variant == TRI2024:
        pool = {"family": "tricrypto_ng", "implementation_revision": "tricrypto-ng-2024-01-04"}
    return {"allocation": "main", "pool": pool}


@pytest.mark.parametrize("actual", [STABLE, TWO, TRI, TRI2024])
def test_prepared_action_proof_uses_the_retained_target_family(actual):
    value = prepared_target(actual)
    chain = value.fund.chain.chain_id
    result = SimpleNamespace(success=True, steps=())
    item = SimpleNamespace(
        pending=SimpleNamespace(intent=SimpleNamespace(target=value, kind="swap_exact_input")),
        transition=SimpleNamespace(result=result),
    )
    state = SimpleNamespace(completed=(item,))
    for claimed in (STABLE, TWO, TRI, TRI2024):
        observed = completed_actions(state, chain, connector=CURVE, variant=claimed)
        assert observed == ({"swap_exact_input"} if claimed == actual else set())
    result.success = False
    assert not completed_actions(state, chain, connector=CURVE, variant=actual)


def prepared_target(variant):
    from almanak.connectors.curve.models import Target
    from almanak.models import Account
    from scripts.legacy_configuration.author import read_author
    from tests.legacy_references import harness_directory

    references = {
        STABLE: "curve_ethereum_stable_lp",
        TWO: "curve_ethereum_twocrypto_lp",
        TRI: "curve_ethereum_tricrypto_lp",
        TRI2024: "curve_optimism_tricrypto_lp",
    }
    pool = read_author(harness_directory(references[variant]) / "config.yaml").fork_config()
    pool = pool.allocations[0].pool
    assert pool.variant == variant
    return Target(
        pool=pool,
        allocation="main",
        account="fund",
        fund=Account(chain=pool.chain, address="0x" + "12" * 20),
        funding_asset=pool.coins[0],
        trading_asset=pool.coins[1],
        liquidity_enabled=True,
        single_coin_exit=pool.coins[0],
    )


def test_unknown_variant_mapping_and_untyped_identity_are_refused():
    with pytest.raises(ValueError, match="mapping is missing"):
        matches(ConnectorId("unknown"), target(STABLE), STABLE)
    with pytest.raises(ValueError, match="validated identity"):
        matches(CURVE, target(STABLE), "stableswap_ng")
    assert matches(CURVE, {}, None)


def test_variant_identity_round_trips_through_json():
    value = declaration()
    assert ConnectorChainSupport.model_validate_json(value.model_dump_json()) == value
    with pytest.raises(ValueError):
        ProtocolVariant("TriCrypto NG")


def test_target_support_rejects_an_unproved_family_and_missing_mapper():
    from almanak.connectors.curve.connector import connector

    binding = replace(connector(), chain_support=(declaration(),))
    stable = SimpleNamespace(pool=SimpleNamespace(variant=STABLE))
    tri = SimpleNamespace(pool=SimpleNamespace(variant=TRI))
    binding.require_target(CHAIN, stable, RunMode.FORK, EconomicAction.SWAP, "safe_roles")
    binding.require_target(CHAIN, tri, RunMode.BACKTEST, model="crypto-model")
    with pytest.raises(ValueError, match="protocol variant"):
        binding.require_target(CHAIN, stable, RunMode.BACKTEST, model="crypto-model")
    with pytest.raises(ValueError, match="protocol variant"):
        replace(binding, variant=lambda target: None).require_target(CHAIN, tri, RunMode.FORK)
    declaration((), ()).require_variant(None)
    with pytest.raises(ValueError, match="protocol variant"):
        declaration((), ()).require_variant(TRI)


@pytest.mark.parametrize(
    "field,value",
    [
        ("measurement_status", "missing"),
        ("observation", None),
        ("missing_input", "archive unavailable"),
    ],
)
def test_curve_qa_rejects_incomplete_position_measurements(field, value):
    from tests.qa.worker import assert_measurements

    case = Case(id="curve", lane="backtest", connector=CURVE)
    position = {"measurement_status": "modeled", "observation": {}, "missing_input": None}
    report = {"positions": [position], "missing_inputs": [], "failures": []}
    assert_measurements(case, report)
    position[field] = value
    with pytest.raises(AssertionError):
        assert_measurements(case, report)


@pytest.mark.parametrize("field", ["missing_inputs", "failures"])
def test_curve_qa_rejects_report_failures(field):
    from tests.qa.worker import assert_measurements

    report = {
        "positions": [{"measurement_status": "modeled", "observation": {}, "missing_input": None}],
        "missing_inputs": [],
        "failures": [],
    }
    report[field] = ["unavailable"]
    with pytest.raises(AssertionError):
        assert_measurements(Case(id="curve", lane="backtest", connector=CURVE), report)


@pytest.mark.parametrize(
    "field,value",
    [
        ("measurement_status", "unavailable"),
        ("observation", None),
        ("missing_input", {"reason": "archive unavailable"}),
        ("remaining_obligations", True),
    ],
)
def test_gmx_qa_requires_complete_measurements_and_no_remaining_obligations(field, value):
    from tests.qa.worker import assert_measurements

    case = Case(id="gmx", lane="backtest", connector=GMX, model_mode="economic")
    position = {
        "measurement_status": "modeled",
        "observation": {},
        "missing_input": None,
        "remaining_obligations": False,
    }
    report = {"positions": [position], "missing_inputs": [], "failures": []}
    assert_measurements(case, report)
    position[field] = value
    with pytest.raises(AssertionError):
        assert_measurements(case, report)


@pytest.mark.parametrize("field", ["missing_inputs", "failures"])
def test_gmx_qa_rejects_unavailable_inputs_and_failed_requests(field):
    from tests.qa.worker import assert_measurements

    report = {
        "positions": [
            {
                "measurement_status": "modeled",
                "observation": {},
                "missing_input": None,
                "remaining_obligations": False,
            }
        ],
        "missing_inputs": [],
        "failures": [],
    }
    report[field] = ["unavailable"]
    with pytest.raises(AssertionError):
        assert_measurements(
            Case(id="gmx", lane="backtest", connector=GMX, model_mode="economic"), report
        )


def test_qa_rejects_a_different_retained_economic_model():
    from tests.qa.worker import assert_model_claim

    case = Case(
        id="curve", lane="backtest", connector=CURVE, model="crypto-model", model_mode="economic"
    )
    assert_model_claim(case, {"model": "crypto-model"})
    with pytest.raises(AssertionError, match="retained model differs"):
        assert_model_claim(case, {"model": "stable-model"})
