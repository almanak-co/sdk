"""The Across fork binding: a constructed quote, plain fixture accounts and one source chain."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.configuration.unresolved import read_inputs
from almanak.connectors.across import execution
from almanak.connectors.across import quote as venue
from almanak.connectors.across.authority import declared
from almanak.connectors.across.connector import AcrossProjection
from almanak.connectors.across.fork import (
    FORK_FEE_BPS,
    QUOTE_VENUE,
    AcrossFork,
    ForkQuote,
    fork_allocation,
)
from almanak.connectors.across.operations import ApproveOperation, DepositOperation
from almanak.fork_operator import FixturePlan
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_execution import validate_signer_keys
from almanak.gateway.operator_config import OperatorConfig
from tests.across_support import AMOUNT, ARBITRUM, BASE, INTENT, PIN, POOL, allocation, snapshot
from tests.across_support import state as retained
from tests.test_across_records import (  # noqa: F401
    FORK_ARB,
    FORK_BASE,
    FORK_BLOCK,
    checkpoint,
    config,
    mined_deposit,
    observed,
)

ROOT = Path(__file__).parents[1]
REFERENCE = ROOT / "strategies" / "across_usdc_arbitrum_base_bridge" / "config.yaml"
CONSTRUCTED = ForkQuote(fee_bps=FORK_FEE_BPS, fill_seconds=3600)


def intent(config):  # noqa: F811
    return config.bridge("move", AMOUNT)


def decided(config):  # noqa: F811
    """The checkpoint fields planning reads, for the fork deployment's own decision."""
    decision = SimpleNamespace(intent=intent(config), identity="0" * 32)
    return retained(config=config, capabilities=declared(config), decision=decision)


def backend(config, **fields):  # noqa: F811
    return SimpleNamespace(config=config, guard=lambda: None, **fields)


def test_a_constructed_quote_states_its_fee_its_fork_time_and_its_label(config):  # noqa: F811
    target = config.target("move")
    quote = CONSTRUCTED.quote(target, AMOUNT, FORK_BLOCK.timestamp)
    assert quote.output_amount == AMOUNT - AMOUNT * FORK_FEE_BPS // 10_000
    assert (quote.timestamp, quote.fill_deadline) == (
        FORK_BLOCK.timestamp,
        FORK_BLOCK.timestamp + 3600,
    )
    answer = json.loads(quote.response)
    assert answer["forkConstructed"].startswith("fork-constructed quote")
    assert answer["spokePoolAddress"] == PIN.address and answer["feeBps"] == FORK_FEE_BPS
    # The gateway's admission runs on it unchanged.
    venue.admit(quote, target, AMOUNT, POOL, FORK_BLOCK.timestamp)


def test_a_constructed_quote_above_the_authors_ceiling_is_refused(config):  # noqa: F811
    costly = ForkQuote(fee_bps=50, fill_seconds=3600)
    quote = costly.quote(config.target("move"), AMOUNT, FORK_BLOCK.timestamp)
    with pytest.raises(DataError) as error:
        venue.admit(quote, config.target("move"), AMOUNT, POOL, FORK_BLOCK.timestamp)
    assert error.value.kind == "fee_above_ceiling"


def test_a_fork_plans_from_its_constructed_quote_and_never_asks_the_venue(
    config,  # noqa: F811
    monkeypatch,
):
    def fetch(*args, **kwargs):
        raise AssertionError("a fork run asked the venue")

    monkeypatch.setattr(venue, "fetch", fetch)
    state = decided(config)
    fork = backend(config, fork_quote=CONSTRUCTED)
    approve = execution.quoted_operation(fork, state, intent(config), observed(config))
    assert isinstance(approve, ApproveOperation)
    planned = execution.quoted_operation(
        fork, state, intent(config), observed(config, allowance=AMOUNT)
    )
    assert isinstance(planned, DepositOperation)
    assert "forkConstructed" in planned.quote.response
    assert planned.quote.timestamp == FORK_BLOCK.timestamp


def test_a_fork_route_that_names_no_quote_is_refused(config, monkeypatch):  # noqa: F811
    monkeypatch.setattr(venue, "fetch", lambda *a, **k: pytest.fail("venue asked"))
    state = decided(config)
    for unnamed in (backend(config), backend(config, fork_quote=None)):
        with pytest.raises(DataError) as error:
            execution.quoted_operation(unnamed, state, intent(config), observed(config))
        assert error.value.kind == "quote_unavailable"


def test_a_mainnet_plan_ignores_a_constructed_quote(monkeypatch):
    from tests.across_support import CONFIG, quote

    asked = []
    monkeypatch.setattr(venue, "fetch", lambda *a, **k: asked.append(a) or quote())
    live = backend(CONFIG, fork_quote=CONSTRUCTED)
    planned = execution.quoted_operation(live, retained(), INTENT, snapshot(allowance=AMOUNT))
    assert len(asked) == 1 and "forkConstructed" not in planned.quote.response


def test_only_the_source_chain_route_names_the_constructed_quote(config):  # noqa: F811
    venues = AcrossFork.venues(config, host=None)
    assert set(venues) == {FORK_ARB}
    assert ForkQuote.model_validate(venues[FORK_ARB][QUOTE_VENUE]) == ForkQuote(
        fee_bps=FORK_FEE_BPS, fill_seconds=3600
    )


def test_the_fork_adapter_carries_the_named_quote_and_refuses_another_venue(monkeypatch):
    built = []

    def gateway(config, endpoint, key_file, *, chain=None):  # noqa: F811
        built.append(chain)
        return SimpleNamespace()

    monkeypatch.setattr("almanak.gateway.fork_execution.ForkGateway", gateway)
    named = {QUOTE_VENUE: CONSTRUCTED.model_dump(mode="json")}
    source = AcrossFork.fork_adapter(None, "e", None, chain=FORK_ARB, venues=named)
    destination = AcrossFork.fork_adapter(None, "e", None, chain=FORK_BASE, venues={})
    assert (source.fork_quote, destination.fork_quote) == (CONSTRUCTED, None)
    assert built == [FORK_ARB, FORK_BASE]
    with pytest.raises(ValueError, match="only its constructed quote"):
        AcrossFork.fork_adapter(None, "e", None, chain=FORK_ARB, venues={"other": {}})


def test_an_allocation_moves_to_its_source_fork_and_keeps_its_destination():
    moved = fork_allocation(allocation(), FORK_ARB)
    assert moved.chain == FORK_ARB
    assert moved.route.destination_chain == BASE.chain_id
    assert moved.model_copy(update={"route": allocation().route}) == allocation()
    assert allocation().chain == ARBITRUM


def test_the_generated_operator_provisions_plain_accounts_for_an_eoa_only_connector():
    parsed = read_inputs(REFERENCE)
    chains = sorted({account.chain for account in parsed.inputs.accounts.values()}, key=str)
    plan = FixturePlan(parsed.inputs, chains)
    assert plan.plain == {"arbitrum_fund", "base_fund"}
    anchors = [
        {"chain": str(chain), "height": 1, "reference": "0x" + "ab" * 32} for chain in chains
    ]
    operator = plan.document(anchors, None)
    assert {fund["kind"] for fund in operator["fork"]["funds"]} == {"eoa"}
    assert operator["fork"]["operators"] == []
    OperatorConfig.model_validate_json(json.dumps(operator))


def test_the_fork_key_file_names_the_receiving_account_too(config):  # noqa: F811
    validate_signer_keys(config, {"arb": "k", "base": "k"})
    for keys in ({"arb": "k"}, {"arb": "k", "base": "k", "other": "k"}, ["arb", "base"]):
        with pytest.raises(ValueError, match="fork key file must match"):
            validate_signer_keys(config, keys)


def test_a_report_states_the_run_so_a_closed_one_can_shut_down(config, monkeypatch):  # noqa: F811
    outcome = mined_deposit(monkeypatch, config)
    closed = checkpoint(config, mode="closed", transfers=(outcome.transfer,))
    report = AcrossProjection.report(closed)
    assert report["transfers"][0]["reference"] == outcome.transfer.reference
    assert (report["mode"], report["connector"], report["execution_environment"]) == (
        "closed",
        "across",
        "fork",
    )
    assert (report["undelivered_outcomes"], report["accounting_issues"]) == (0, [])
    assert report["transaction_accounting_complete"] and report["evidence_status"] == "recorded"
