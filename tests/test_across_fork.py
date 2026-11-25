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
    ForkTransport,
    fork_allocation,
)
from almanak.connectors.across.operations import ApproveOperation, DepositOperation
from almanak.fork_operator import FixturePlan
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.gateway.execution import VenueAnswers
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


def backend(config, transport=None):  # noqa: F811
    transports = {} if transport is None else {QUOTE_VENUE: transport}
    return SimpleNamespace(
        config=config,
        guard=lambda: None,
        venue_transports=transports,
        venue_answers=VenueAnswers(),
    )


def constructed(config, descriptor=CONSTRUCTED, at=FORK_BLOCK.timestamp):  # noqa: F811
    return venue.fetch(config.target("move"), AMOUNT, ForkTransport(descriptor), at)


def test_a_constructed_quote_states_its_fee_its_fork_time_and_its_source(config):  # noqa: F811
    target = config.target("move")
    quote = constructed(config)
    assert quote.source == venue.FORK_SOURCE == "across_fork_constructed"
    assert quote.output_amount == AMOUNT - AMOUNT * FORK_FEE_BPS // 10_000
    assert (quote.timestamp, quote.fill_deadline) == (
        FORK_BLOCK.timestamp,
        FORK_BLOCK.timestamp + 3600,
    )
    answer = json.loads(quote.response)
    assert answer["forkConstructed"].startswith("fork-constructed quote")
    assert answer["spokePoolAddress"] == PIN.address and answer["feeBps"] == FORK_FEE_BPS
    # The gateway's admission runs on it, and admits its source on the fork.
    venue.admit(quote, target, AMOUNT, POOL, FORK_BLOCK.timestamp)


def test_a_constructed_quote_is_refused_off_a_fork(config):  # noqa: F811
    from tests.across_support import TARGET

    quote = constructed(config)
    with pytest.raises(DataError, match="identified fork only") as error:
        venue.admit(quote, TARGET, AMOUNT, POOL, FORK_BLOCK.timestamp)
    assert error.value.kind == "quote_source"
    # Its transport answers no production target either.
    with pytest.raises(DataError, match="identified fork only") as error:
        venue.fetch(TARGET, AMOUNT, ForkTransport(CONSTRUCTED), FORK_BLOCK.timestamp)
    assert error.value.kind == "quote_unavailable"


def test_a_constructed_quote_takes_the_venue_answer_path(config, monkeypatch):  # noqa: F811
    large = ForkTransport(CONSTRUCTED)
    monkeypatch.setattr(
        CONSTRUCTED.__class__,
        "answer",
        lambda self, *a: {"pad": "x" * venue.MAX_ANSWER_BYTES},
    )
    with pytest.raises(DataError, match="exceeds") as error:
        venue.fetch(config.target("move"), AMOUNT, large, FORK_BLOCK.timestamp)
    assert error.value.kind == "malformed"


def test_a_constructed_quote_older_than_the_buffer_is_refused(config):  # noqa: F811
    target = config.target("move")
    quote = constructed(config)
    late = FORK_BLOCK.timestamp + POOL.quote_time_buffer + 1
    with pytest.raises(DataError, match="older than the SpokePool's quote buffer") as error:
        venue.admit(quote, target, AMOUNT, POOL, late)
    assert error.value.kind == "quote_time"
    with pytest.raises(DataError, match="ahead of the pinned block") as error:
        venue.admit(quote, target, AMOUNT, POOL, FORK_BLOCK.timestamp - 1)
    assert error.value.kind == "quote_time"
    short = constructed(config, ForkQuote(fee_bps=FORK_FEE_BPS, fill_seconds=60))
    with pytest.raises(DataError, match="too little time") as error:
        venue.admit(short, target, AMOUNT, POOL, FORK_BLOCK.timestamp)
    assert error.value.kind == "fill_deadline"


def test_a_constructed_quote_above_the_authors_ceiling_is_refused(config):  # noqa: F811
    quote = constructed(config, ForkQuote(fee_bps=50, fill_seconds=3600))
    with pytest.raises(DataError) as error:
        venue.admit(quote, config.target("move"), AMOUNT, POOL, FORK_BLOCK.timestamp)
    assert error.value.kind == "fee_above_ceiling"


def test_a_fork_plans_through_its_transport_and_never_asks_the_venue(
    config,  # noqa: F811
    monkeypatch,
):
    def asked(*args, **kwargs):
        raise AssertionError("a fork run asked the venue")

    monkeypatch.setattr(venue.HttpTransport, "get", asked)
    state = decided(config)
    fork = backend(config, ForkTransport(CONSTRUCTED))
    approve = execution.quoted_operation(fork, state, intent(config), observed(config))
    assert isinstance(approve, ApproveOperation)
    planned = execution.quoted_operation(
        fork, state, intent(config), observed(config, allowance=AMOUNT)
    )
    assert isinstance(planned, DepositOperation)
    assert planned.quote.source == venue.FORK_SOURCE
    assert planned.quote.timestamp == FORK_BLOCK.timestamp


def test_a_gateway_that_declares_no_quote_transport_is_refused(config, monkeypatch):  # noqa: F811
    monkeypatch.setattr(venue.HttpTransport, "get", lambda *a: pytest.fail("venue asked"))
    with pytest.raises(DataError, match="declares no Across quote transport") as error:
        execution.quoted_operation(
            backend(config), decided(config), intent(config), observed(config)
        )
    assert error.value.kind == "quote_unavailable"


def bare_gateway(monkeypatch):
    """Gateway construction without a node: only what the subclasses add is under test."""
    from almanak.connectors.across.connector import connector
    from almanak.gateway import execution as shared

    def init(self, deployment, *args, **kwargs):
        self.config = deployment

    monkeypatch.setattr(shared.EvmGateway, "__init__", init)
    monkeypatch.setattr(shared, "connector_for", lambda _: connector())


def test_production_declares_the_venue_api_and_takes_no_other_transport(monkeypatch):
    import inspect

    from almanak.gateway import production

    bare_gateway(monkeypatch)
    monkeypatch.setattr(production, "validate_signer_files", lambda *a: None)
    monkeypatch.setattr(production, "validate_endpoint", lambda *a: None)
    monkeypatch.setattr(production, "production_signers", lambda *a: {})
    gateway = production.ProductionGateway(SimpleNamespace(production=object()), "endpoint")
    assert set(gateway.venue_transports) == {QUOTE_VENUE}
    transport = gateway.venue_transports[QUOTE_VENUE]
    assert isinstance(transport, venue.HttpTransport) and transport.source == "across_api"
    assert "venue_transports" not in inspect.signature(production.ProductionGateway).parameters


def test_a_fork_gateway_declares_only_what_its_route_names(monkeypatch):
    from almanak.gateway import fork_execution

    bare_gateway(monkeypatch)
    for name in ("validate_endpoint", "validate_fork_scope", "load_signers"):
        monkeypatch.setattr(fork_execution, name, lambda *a: {})
    named = ForkTransport(CONSTRUCTED)
    source = fork_execution.ForkGateway(
        None, "e", None, chain=FORK_ARB, venue_transports={QUOTE_VENUE: named}
    )
    assert source.venue_transports == {QUOTE_VENUE: named}
    # A fork route that names none leaves the gateway without one: never the venue's API.
    assert fork_execution.ForkGateway(None, "e", None, chain=FORK_BASE).venue_transports == {}


def test_a_mainnet_plan_with_a_fork_transport_is_refused():
    from tests.across_support import CONFIG

    live = backend(CONFIG, ForkTransport(CONSTRUCTED))
    with pytest.raises(DataError, match="identified fork only"):
        execution.quoted_operation(live, retained(), INTENT, snapshot(allowance=AMOUNT))


def test_only_the_source_chain_route_names_the_constructed_quote(config):  # noqa: F811
    venues = AcrossFork.venues(config, host=None)
    assert set(venues) == {FORK_ARB}
    assert ForkQuote.model_validate(venues[FORK_ARB][QUOTE_VENUE]) == ForkQuote(
        fee_bps=FORK_FEE_BPS, fill_seconds=3600
    )


def test_the_fork_adapter_declares_the_named_transport_and_refuses_another_venue(monkeypatch):
    built = []

    def gateway(config, endpoint, key_file, *, chain=None, venue_transports=None):  # noqa: F811
        built.append(chain)
        return SimpleNamespace(venue_transports=venue_transports)

    monkeypatch.setattr("almanak.gateway.fork_execution.ForkGateway", gateway)
    named = {QUOTE_VENUE: CONSTRUCTED.model_dump(mode="json")}
    source = AcrossFork.fork_adapter(None, "e", None, chain=FORK_ARB, venues=named)
    destination = AcrossFork.fork_adapter(None, "e", None, chain=FORK_BASE, venues={})
    transport = source.venue_transports[QUOTE_VENUE]
    assert isinstance(transport, ForkTransport) and transport.constructed == CONSTRUCTED
    assert destination.venue_transports == {}
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
    # No retained deposit, so no source is claimed for the offered amount.
    assert report["transfers"][0]["quote_source"] is None
    recorded = closed.model_copy(update={"outcomes": (outcome,), "delivered": 1})
    assert AcrossProjection.report(recorded)["transfers"][0]["quote_source"] == (
        outcome.operation.quote.source
    )
    assert (report["mode"], report["connector"], report["execution_environment"]) == (
        "closed",
        "across",
        "fork",
    )
    assert (report["undelivered_outcomes"], report["accounting_issues"]) == (0, [])
    assert report["transaction_accounting_complete"] and report["evidence_status"] == "recorded"


def fixture_wire(**changes):
    """The in-process fixture of the one-route deployment: both chains, both accounts."""
    from tests.across_support import BASE_USDC, CONFIG, USDC
    from tests.across_support import allocation as authored

    def provider(network):
        return {"kind": "alchemy", "network": network, "api_key_env": "UNUSED"}

    def fund(name, chain, index, token):
        native = {"asset": str(native_asset(chain.chain_id)), "amount": 10**18}
        held = {"balance": {"asset": str(token), "amount": AMOUNT}, "method": "local_storage"}
        return {
            "account": name,
            "native_balance": native,
            "assets": [held],
            "foundry_account_index": index,
        }

    return {
        "connector": "across",
        "decision_trigger": CONFIG.decision_trigger.model_dump(mode="json"),
        "allocations": [authored().model_dump(mode="json")],
        "forks": [
            {
                "chain": ARBITRUM.model_dump(mode="json"),
                "provider": provider("arb-mainnet"),
                "block": "latest",
            },
            {
                "chain": BASE.model_dump(mode="json"),
                "provider": provider("base-mainnet"),
                "block": "latest",
            },
        ],
        "funds": [fund("arb", ARBITRUM, 0, USDC), fund("base", BASE, 1, BASE_USDC)],
        "operators": [],
        **changes,
    }


def fixture(**changes):
    from almanak.connectors.composition import author_configuration_from_json

    return author_configuration_from_json(json.dumps(fixture_wire(**changes)))


def test_the_in_process_fixture_of_a_bridge_forks_both_chains_of_its_route():
    from almanak.connectors.across.fork import ForkConfig
    from almanak.connectors.composition import connector_for
    from tests.across_support import CONFIG

    built = fixture()
    assert isinstance(built, ForkConfig)
    assert type(connector_for(built).fork) is type(connector_for(CONFIG).fork)
    assert [target.chain for target in built.forks] == [ARBITRUM, BASE]
    assert ForkConfig.model_validate(built.model_dump()) == built
    assert ForkConfig.arrays("unchanged") == "unchanged"
    wire = fixture_wire()
    # A route's two chains are its fork targets; a fixture without one of them, or
    # with a chain no route names, is refused.
    for forks in (wire["forks"][:1], wire["forks"][1:]):
        with pytest.raises(ValueError, match="source and destination chain"):
            fixture(forks=forks)


def test_a_bridge_fixture_funds_exactly_its_two_named_eoa_accounts():
    wire = fixture_wire()
    with pytest.raises(ValueError, match="exactly the funded accounts"):
        fixture(funds=wire["funds"][:1])
    bare = {**wire["funds"][0], "assets": []}
    with pytest.raises(ValueError, match="cover the allocation capital"):
        fixture(funds=[bare, wire["funds"][1]])
    other_gas = {**wire["funds"][0], "native_balance": wire["funds"][1]["native_balance"]}
    with pytest.raises(ValueError, match="pays gas on its source chain"):
        fixture(funds=[other_gas, wire["funds"][1]])
    plain = {
        "account": "arb",
        "native_balance": wire["funds"][0]["native_balance"],
        "token_balance": 0,
        "funding": "native",
    }
    with pytest.raises(ValueError, match="EOA accounts only"):
        fixture(funds=[plain, wire["funds"][1]])


def gas_policy(account, chain):
    native = str(native_asset(chain.chain_id))
    return {
        "accounts": [account],
        "asset": native,
        "source": "network_rpc_fee_history",
        "reward_percentile": 50,
        "base_fee_multiplier_bps": 12_000,
        "max_gas_price_wei": 10**10,
        "max_gas_limit": 500_000,
        "max_transaction_fee": {"asset": native, "amount": 10**16},
        "exit_reserve": {"asset": native, "amount": 10**16},
        "gas_limit_buffer_bps": 2_000,
    }


def production_profile(*networks):
    chains = {"arb-mainnet": ARBITRUM, "base-mainnet": BASE}
    return {
        "networks": [
            {
                "chain": chains[network].model_dump(mode="json"),
                "provider": {"kind": "alchemy", "network": network, "api_key_env": "UNUSED"},
                "max_observation_age_seconds": 180,
            }
            for network in networks
        ],
        "gas": [gas_policy("arb", ARBITRUM), gas_policy("base", BASE)],
    }


def test_a_production_rehearsal_fixture_rests_on_the_source_chains_declaration(monkeypatch):
    """The rehearsal fixture is refused while Across declares no production support, and
    accepted, with both chains as production networks, once the source chain declares it."""
    from almanak.connectors.across import support

    both = production_profile("arb-mainnet", "base-mainnet")
    monkeypatch.setattr(support, "PRODUCTION_SOURCES", ())
    with pytest.raises(ValueError, match="production"):
        fixture(production=both)
    monkeypatch.setattr(support, "PRODUCTION_SOURCES", support.FORK_SOURCES)
    assert fixture(production=both).production is not None
    with pytest.raises(ValueError, match="cover the same author chains"):
        fixture(production=production_profile("arb-mainnet"))


def test_a_bridge_fixture_is_prepared_onto_its_fork_chains(config):  # noqa: F811
    from almanak.connectors.across.fork import AcrossFork
    from almanak.connectors.across.models import Config

    authored = fixture()
    prepared = AcrossFork.prepare_chains(authored, config.accounts.bindings, config.fork_bindings)
    assert isinstance(prepared, Config) and prepared.fork_bindings == config.fork_bindings
    assert prepared.allocations == config.allocations
