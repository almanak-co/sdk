"""`almanak ax bridge`, `ax revoke-bridge` and `ax bridge-status` for Across.

Component evidence without signing or chain access. The deployment is built from a
route resolved over fixed reads, the gateway's admission is the real service over one
stated observation, and the status command reads retained mainnet receipts through a
reader that serves only them. No fork rehearsal of the direct command exists.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from eth_abi import decode
from pydantic import ValidationError

from almanak import ax_actions, ax_bridge
from almanak.caip import ChainId
from almanak.chains import configured_chains, evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct
from almanak.connectors.across import direct as across_direct
from almanak.connectors.across import planning
from almanak.connectors.across import quote as venue
from almanak.connectors.across.authority import declared
from almanak.connectors.across.connector import connector as across_connector
from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.across.lifecycle import AcrossLifecycle, adopt_owned
from almanak.connectors.across.observations import PoolState, Snapshot
from almanak.connectors.across.operations import DEPOSIT_TYPES, DepositOperation, plan_call
from almanak.connectors.across.receipts import opened_transfer
from almanak.connectors.across.relay import deposited_relay
from almanak.connectors.across.resolution import MarketSelection, ResolvedRoute
from almanak.connectors.across.state import Checkpoint, DecisionRecord, Outcome, Pending
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.gateway import read_endpoints
from almanak.gateway.accounts import prepare_call
from almanak.gateway.ethereum import DataError, ProviderBusy
from almanak.gateway.execution import EvmGateway, VenueAnswers
from almanak.gateway.providers import PUBLIC_RPC, PublicProvider
from almanak.gateway.service import GatewayService
from almanak.models import Asset, AssetAmount, Block, Chain
from almanak.operator_deployment import supports_operator
from almanak.operator_runtime import OperatorRuntime
from almanak.request_runtime import release_pending
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import OperatorEvent
from tests.across_settlement_support import FakeChain, fill, payment
from tests.across_support import (
    AMOUNT,
    ARBITRUM,
    BASE,
    BASE_USDC,
    BLOCK,
    CONFIG,
    NOW,
    POOL,
    RECIPIENT,
    SENDER,
    USDC,
    body,
    parameters,
    quote,
)
from tests.across_support import allocation as stated_allocation
from tests.across_support import route as stated_route
from tests.test_across_deposit import MINED, MINED_AT, deposit_log, receipt
from tests.test_across_quote import BASE_WETH
from tests.test_ax_swap import Observed
from tests.test_expired_attempt_release import CLEAN, gateway_service
from tests.test_operator_runtime import GatewayJournal
from tests.test_production import Reader

WALLET = Wallet(account="fund", address=SENDER)
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=7, max_transaction_fee=9)
BASE_PIN = SPOKE_POOLS[BASE.chain_id]
BASE_POOL = PoolState(
    address=BASE_PIN.address,
    implementation=BASE_PIN.implementation,
    implementation_code_hash=BASE_PIN.implementation_code_hash,
    quote_time_buffer=3600,
    fill_deadline_buffer=21600,
    deposits_paused=False,
)
BASE_BLOCK = Block(chain=BASE, number=30_000_000, hash="0x" + "bc" * 32, timestamp=NOW)
SELECTION = MarketSelection(
    connector="across",
    chain=ARBITRUM.chain_id,
    asset=USDC,
    destination_chain=BASE.chain_id,
    destination_asset=BASE_USDC,
    max_fee_bps=10,
    quote="across_api",
)
SOURCE_ONLY = ResolvedRoute(
    selection=SELECTION,
    block=BLOCK,
    asset=Asset(chain=ARBITRUM, identifier=USDC, decimals=6),
    pool=POOL,
)
ROUTE = ResolvedRoute(
    **{
        **dict(SOURCE_ONLY),
        "destination": Asset(chain=BASE, identifier=BASE_USDC, decimals=6),
        "destination_block": BASE_BLOCK,
        "destination_pool": BASE_POOL,
    }
)
BRIDGE = ActionRequest("bridge", AMOUNT, recipient=RECIPIENT)
REVOKE = ActionRequest("revoke-bridge")
CAPTURES = json.loads(
    (Path(__file__).with_name("fixtures") / "across-mainnet-captures.json").read_text()
)


def operator_config(request=BRIDGE, wallet=WALLET):
    positions = across_direct.positions(None, BLOCK, ROUTE, wallet, request)
    provider = ax_actions.alchemy(ARBITRUM.chain_id)
    return ax_actions.configuration("across", positions, wallet, provider, ARBITRUM, LIMITS)


def created(config, strategy=None):
    return AcrossLifecycle.initial(config, strategy, declared(config), None)


def seen(config, **changes):
    target = config.target(ALLOCATION)
    values = {
        "target": target,
        "block": BLOCK,
        "wallet_balance": AMOUNT,
        "allowance": 0,
        "pool": POOL,
        "native_balance": AssetAmount(
            asset=evm_network(ARBITRUM.chain_id).native_asset, amount=10**18
        ),
        "payer": target.fund,
        **changes,
    }
    return Snapshot(**values)


HYPEREVM = ChainId("eip155:999")


def test_across_declares_its_direct_actions_on_its_production_sources():
    found = across_connector()
    assert supports_operator(found.codec.checkpoint)
    assert direct.binding("across").actions == ("bridge", "revoke-bridge")
    assert "across" not in direct.NOT_DIRECT
    assert direct.connectors_for("bridge") == ("across",)
    from almanak.connectors.across.support import PRODUCTION_SOURCES

    cells = {cell.chain: cell for cell in found.chain_support}
    served = {chain for chain, cell in cells.items() if cell.direct_actions}
    assert served == set(PRODUCTION_SOURCES)
    for chain in served:
        assert [str(a) for a in cells[chain].direct_actions] == ["bridge", "revoke_bridge"]
        direct.require_direct("across", chain, BRIDGE)
        direct.require_direct("across", chain, REVOKE)
    # HyperEVM holds no pinned SpokePool, so it is no source of a direct bridge.
    assert BASE.chain_id in served and HYPEREVM not in cells
    with pytest.raises(ValueError, match="across serves no direct bridge on eip155:999"):
        direct.require_direct("across", HYPEREVM, BRIDGE)
    assert (BRIDGE.kind, REVOKE.kind) == ("bridge", "revoke_bridge")


def test_the_deployment_binds_the_wallet_on_the_chain_it_leaves_and_states_the_recipient():
    config = operator_config()
    (allocation,) = config.allocations
    assert [b.name for b in config.accounts.bindings] == ["fund"]
    assert (allocation.name, allocation.account) == (ALLOCATION, "fund")
    assert (allocation.destination_account, allocation.capital) == ("recipient", AMOUNT)
    assert allocation.route == ROUTE.route and allocation.minimum_native_balance == 0
    target = config.target(ALLOCATION)
    assert (target.fund.chain, target.fund.address) == (ARBITRUM, SENDER)
    assert (target.destination_fund.chain, target.destination_fund.address) == (BASE, RECIPIENT)
    # One chain: nothing is read, planned or signed on the destination.
    assert configured_chains(config) == (ARBITRUM,)
    assert [n.chain for n in config.production.networks] == [ARBITRUM]
    assert direct.binding_for(config) is direct.binding("across")
    intent = across_direct.intent(config, (), BRIDGE)
    assert (intent.kind, intent.amount.amount, intent.target) == ("bridge", AMOUNT, target)


def test_an_operator_checkpoint_has_one_chain_no_strategy_and_round_trips():
    state = created(operator_config())
    assert (state.operator, state.strategy, state.work, state.initialized) == (
        True,
        None,
        (),
        False,
    )
    wire = json.loads(state.model_dump_json())
    assert (wire["operator"], wire["strategy"]) == (True, None)
    assert wire["config"]["allocations"][0]["adopt"]["fund"]["address"] == RECIPIENT
    assert Checkpoint.model_validate_json(json.dumps(wire)) == state
    strategy = StrategyDefinition(filename="strategy.py", source="").model_dump()
    with pytest.raises(ValueError, match="an operator deployment retains no strategy"):
        Checkpoint.model_validate_json(json.dumps({**wire, "strategy": strategy}))
    # A strategy deployment never carries a stated recipient.
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        Checkpoint.model_validate_json(json.dumps({**wire, "operator": False}))
    with pytest.raises(ValueError, match="only an operator deployment adopts"):
        Checkpoint.model_validate_json(
            json.dumps({**wire, "operator": False, "strategy": strategy})
        )
    report = across_connector().projection.report(state)
    assert report["strategy"] is None and report["transfers"] == []


def test_a_strategy_deployment_keeps_its_wire_form():
    strategy = StrategyDefinition(filename="strategy.py", source="")
    parameters = json.loads(CONFIG.model_dump_json())
    assert "adopt" not in parameters["allocations"][0]
    state = SimpleNamespace(operator=False, strategy=strategy, config=CONFIG)
    from almanak.operator_deployment import validate_exclusive_origin

    validate_exclusive_origin(state)
    fields = Checkpoint.model_fields
    assert fields["operator"].default is False and fields["superseded"].default == ()


def test_a_stated_recipient_is_on_the_destination_chain_and_names_no_bound_account():
    document = json.loads(operator_config().model_dump_json())
    allocation = document["allocations"][0]
    elsewhere = {"chain": allocation["route"]["chain"], "address": RECIPIENT}
    wrong = {**allocation, "adopt": {"fund": elsewhere}}
    with pytest.raises(ValidationError, match="recipient is on the route's destination chain"):
        type(operator_config()).model_validate_json(
            json.dumps({**document, "allocations": [wrong]})
        )
    # Without the statement the destination account must be bound, as for a strategy.
    bare = {k: v for k, v in allocation.items() if k != "adopt"}
    with pytest.raises(ValidationError, match="unbound account recipient"):
        type(operator_config()).model_validate_json(json.dumps({**document, "allocations": [bare]}))
    # A stated recipient is never also a bound account.
    stated = stated_allocation(adopt={"fund": {"chain": BASE, "address": RECIPIENT}})
    with pytest.raises(ValidationError, match="a stated Across recipient names no bound account"):
        parameters(stated)
    # A wallet whose account is called `recipient` gets another label for the recipient.
    named = Wallet(account="recipient", address=SENDER)
    assert operator_config(wallet=named).allocations[0].destination_account == "destination"


def test_a_bridge_states_its_recipient_and_a_revocation_needs_none():
    with pytest.raises(ValueError, match="bridge requires --recipient"):
        across_direct.positions(None, BLOCK, ROUTE, WALLET, ActionRequest("bridge", AMOUNT))
    with pytest.raises(ValueError, match="bridge requires --amount"):
        across_direct.positions(
            None, BLOCK, ROUTE, WALLET, ActionRequest("bridge", recipient=RECIPIENT)
        )
    with pytest.raises(ValueError, match="not verified on its chain"):
        across_direct.positions(None, BLOCK, SOURCE_ONLY, WALLET, BRIDGE)
    config = operator_config(REVOKE)
    target = config.target(ALLOCATION)
    # The route of a revocation names the wallet itself; nothing is sent to it.
    assert target.destination_fund.address == SENDER and config.allocations[0].capital == 0
    assert across_direct.intent(config, (), REVOKE).kind == "revoke_bridge"
    assert across_direct.note(config, (), REVOKE) is None
    assert "ax bridge-status" in across_direct.note(config, (), BRIDGE)


def test_adoption_takes_nothing_over_and_covers_every_route():
    config = operator_config()
    state = created(config)
    assert adopt_owned(state.owned, (seen(config, allowance=5),)) == state.owned
    with pytest.raises(DataError) as error:
        adopt_owned(state.owned, ())
    assert error.value.kind == "ownership_mismatch"


class Desk:
    """An operator deployment behind the real gateway service, over one observation."""

    def __init__(self, tmp_path, config, **observed):
        self.store = Journal(tmp_path / "state.sqlite")
        self.store.create(config, None)
        self.gateway = Observed(seen(config, **observed))
        self.service = GatewayService(
            config, self.store.path, self.gateway, lease_seconds=2, operator_actions=True
        )
        self.config = config

    def runtime(self):
        runtime = object.__new__(OperatorRuntime)
        runtime.connector = across_connector()
        runtime.journal = GatewayJournal(self.store, self.service)
        runtime.gateway = self.gateway
        runtime.closed = False
        runtime.clock = lambda: datetime(2026, 11, 25, tzinfo=UTC)
        runtime.fault = lambda *arguments: None
        runtime.poll = 0
        return runtime


def test_the_gateway_admits_the_bridge_and_the_revocation_as_operator_requests(tmp_path):
    # The wallet already gave the SpokePool an allowance: the deployment still starts.
    desk = Desk(tmp_path, operator_config(), allowance=7)
    record = desk.runtime().submit(
        lambda state, observed: across_direct.intent(state.config, observed, BRIDGE)
    )
    retained = desk.store.load()
    assert (record.intent.kind, record.trigger.kind) == ("bridge", "operator")
    assert retained.decision == record and retained.initialized and retained.operator
    assert retained.baseline == (desk.gateway.seen,) and retained.work == ()
    desk.runtime().discard()
    revoke = desk.runtime().submit(desk.config.revoke(ALLOCATION))
    assert revoke.intent.kind == "revoke_bridge"


def test_the_steps_follow_the_observed_allowance():
    steps = across_direct.planned_steps
    assert steps("bridge", 0, AMOUNT) == ["approve", "deposit"]
    assert steps("bridge", AMOUNT, AMOUNT) == ["deposit"]
    assert steps("bridge", 7, AMOUNT) == ["revoke", "approve", "deposit"]
    assert steps("revoke_bridge", 7, None) == ["revoke"]


class Venue:
    source = venue.SOURCE

    def __init__(self, **changes):
        self.changes, self.asked = changes, 0

    def get(self, target, amount, block_timestamp):
        self.asked += 1
        return httpx.Response(200, json=body(**self.changes))


def planned(config, request=BRIDGE, **observed):
    """The checkpoint fields `details` reads, with the first step planned."""
    intent = across_direct.intent(config, (), request)
    decision = SimpleNamespace(intent=intent, identity="0" * 32)
    return SimpleNamespace(decision=decision, plan=SimpleNamespace(before=seen(config, **observed)))


def backend(transport=None):
    transports = {} if transport is None else {venue.QUOTE_VENUE: transport}
    return SimpleNamespace(venue_transports=transports, venue_answers=VenueAnswers())


def test_the_preview_states_the_admitted_venue_quote_under_the_ceiling():
    config, asked = operator_config(), Venue()
    gateway = backend(asked)
    result = across_direct.details(gateway, planned(config))
    assert result == {
        "steps": ["approve", "deposit"],
        "recipient": RECIPIENT,
        "recipient_is_sender": False,
        "quote": {
            "source": "across_api",
            "input_amount": str(AMOUNT),
            "output_amount": "999897038",
            "fee": str(AMOUNT - 999897038),
            "fee_ceiling": str(AMOUNT * 10 // 10_000),
            "max_fee_bps": 10,
            "timestamp": NOW - 69,
            "fill_deadline": NOW - 69 + 7200,
        },
    }
    # The same request at the same block reads the gateway's own answer.
    across_direct.details(gateway, planned(config))
    assert asked.asked == 1
    own = ActionRequest("bridge", AMOUNT, recipient=SENDER)
    kept = across_direct.details(backend(Venue()), planned(operator_config(own), own))
    assert (kept["recipient"], kept["recipient_is_sender"]) == (SENDER, True)


CHECKSUMMED = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"


def test_the_note_names_a_recipient_that_is_not_the_wallet_or_carries_no_checksum():
    def noted(stated):
        request = ActionRequest("bridge", AMOUNT, recipient=stated)
        return across_direct.note(operator_config(request), (), request)

    other = noted(CHECKSUMMED)
    assert f"The recipient {CHECKSUMMED.lower()} is not the sending wallet" in other
    assert "without a checksum" not in other
    for unchecked in (CHECKSUMMED.lower(), "0x" + CHECKSUMMED[2:].upper()):
        assert "without a checksum" in noted(unchecked)
    # The wallet's own address, stated in one case, is only told about the checksum.
    own = noted(SENDER)
    assert "is not the sending wallet" not in own and "without a checksum" in own
    assert "deposit's transaction hash" in own


def test_the_deposit_calldata_names_the_wallet_as_depositor_and_the_stated_recipient():
    call = plan_call(deposit(operator_config()))
    stated = decode(DEPOSIT_TYPES, bytes.fromhex(call.data[10:]))
    assert stated[0] == bytes(12) + bytes.fromhex(SENDER[2:])
    assert stated[1] == bytes(12) + bytes.fromhex(RECIPIENT[2:])
    assert stated[1] != stated[0]


def test_a_stated_recipient_is_never_paid_a_wrapped_native_output(monkeypatch):
    """A stated recipient is an address of unknown kind, so it is held as an EOA is:
    Across would pay it the native asset, which this scope does not record."""
    wrapped = SimpleNamespace(route=stated_route(destination_asset=BASE_WETH))
    positions = across_direct.positions(None, BLOCK, wrapped, WALLET, BRIDGE)
    provider = ax_actions.alchemy(ARBITRUM.chain_id)
    config = ax_actions.configuration("across", positions, WALLET, provider, ARBITRUM, LIMITS)
    name = config.allocations[0].name
    with pytest.raises(ValueError, match="native asset"):
        config.bridge(name, AMOUNT)
    # The gateway holds a deposit to the same rule, whatever intent reached it.
    from almanak.connectors.across.execution import validate_entry
    from almanak.connectors.across.operations import ApproveOperation
    from almanak.gateway.ethereum import DataError

    state = SimpleNamespace(config=config, mode="running", outcomes=())
    operation = ApproveOperation.model_construct(target=config.target(name))
    monkeypatch.setattr("almanak.connectors.across.execution.require_reviewed", lambda before: None)
    with pytest.raises(DataError, match="native asset"):
        validate_entry(state, operation, None)


def test_a_revocation_is_not_held_to_the_payout_rule_of_a_deposit():
    """A revocation pays nobody, so a wrapped native asset named as received refuses nothing."""
    wrapped = SimpleNamespace(route=stated_route(destination_asset=BASE_WETH))
    positions = across_direct.positions(None, BLOCK, wrapped, WALLET, REVOKE)
    provider = ax_actions.alchemy(ARBITRUM.chain_id)
    config = ax_actions.configuration("across", positions, WALLET, provider, ARBITRUM, LIMITS)
    name = config.allocations[0].name
    assert config.revoke(name).kind == "revoke_bridge"
    with pytest.raises(ValueError, match="native asset"):
        config.bridge(name, AMOUNT)


def test_a_connector_without_details_never_costs_a_preview_its_checkpoint_read():
    class Journal:
        def load(self):
            raise OSError("storage is away")

    assert ax_actions.stated_details(SimpleNamespace(details=None), None, Journal()) == {}
    with pytest.raises(OSError):
        ax_actions.stated_details(SimpleNamespace(details=lambda b, s: {}), None, Journal())


def test_a_stated_recipient_is_in_the_environment_of_the_route():
    document = json.loads(operator_config().model_dump_json())
    allocation = document["allocations"][0]
    fork = {"chain_id": allocation["route"]["destination_chain"], "environment": "fork"}
    elsewhere = {**allocation, "adopt": {"fund": {"chain": fork, "address": RECIPIENT}}}
    with pytest.raises(ValidationError, match="recipient is on the route's destination chain"):
        type(operator_config()).model_validate_json(
            json.dumps({**document, "allocations": [elsewhere]})
        )


def test_a_quote_above_the_ceiling_or_no_transport_refuses_the_preview():
    config = operator_config()
    with pytest.raises(DataError) as above:
        across_direct.details(backend(Venue(outputAmount="998000000")), planned(config))
    assert above.value.kind == "fee_above_ceiling"
    with pytest.raises(DataError) as absent:
        across_direct.details(backend(), planned(config))
    assert absent.value.kind == "quote_unavailable"


def test_a_revocation_asks_no_quote():
    config = operator_config(REVOKE)
    asked = Venue()
    stated = across_direct.details(backend(asked), planned(config, REVOKE, allowance=7))
    assert stated == {"steps": ["revoke"]} and asked.asked == 0


def deposit(config):
    return DepositOperation(
        target=config.target(ALLOCATION),
        amount=config.bridge(ALLOCATION, AMOUNT).amount,
        quote=quote(),
        quote_time_buffer=POOL.quote_time_buffer,
    )


def decision(config):
    return DecisionRecord(
        trigger=OperatorEvent(started_at=datetime.fromtimestamp(NOW, UTC), request="1" * 32),
        identity="0" * 32,
        intent=config.bridge(ALLOCATION, AMOUNT),
        observations=(seen(config),),
        memory=Memory(),
    )


def signed(config):
    operation = deposit(config)
    call = plan_call(operation)
    return Pending(
        attempt_id="attempt-1",
        decision=decision(config),
        operation=operation,
        before=seen(config, allowance=AMOUNT),
        call=call,
        execution=prepare_call(config.accounts, "fund", call),
        nonce=3,
        gas_estimate=200_000,
        gas=200_000,
        gas_price=10**8,
        raw_transaction="0x02",
        tx_hash="0x" + "cd" * 32,
    )


def mined_deposit(config):
    attempt = signed(config)
    operation = attempt.operation
    after = seen(config, wallet_balance=0, native_balance=attempt.before.native_balance)
    fields = {
        **receipt(operation),
        "status": "0x1",
        "transactionHash": attempt.tx_hash,
        "gasUsed": "0x0",
        "effectiveGasPrice": "0x0",
    }
    return Outcome(
        decision=attempt.decision,
        operation=operation,
        tx_hash=attempt.tx_hash,
        nonce=attempt.nonce,
        before=attempt.before,
        after=after,
        success=True,
        assets=AMOUNT,
        transfer=opened_transfer(operation, MINED),
        gas_paid=AssetAmount(asset=after.native_balance.asset, amount=0),
        receipt=fields,
        call=attempt.call,
        execution=attempt.execution,
    )


def test_a_mined_deposit_completes_the_request_and_reports_its_transfer():
    config = operator_config()
    outcome = mined_deposit(config)
    state = created(config).model_copy(
        update={
            "initialized": True,
            "baseline": (seen(config),),
            "outcomes": (outcome,),
            "transfers": (outcome.transfer,),
            "delivered": 1,
        }
    )
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state
    result = ax_actions.executed_result(state, state.outcomes, None)
    assert (result["executed"], result["completed"]) == (True, True)
    (transfer,) = result["position_after"]["transfers"]
    assert transfer == {
        "reference": outcome.transfer.reference,
        "sent": str(AMOUNT),
        "offered": "999897038",
        "fill_deadline": NOW - 69 + 7200,
        "quote_source": "across_api",
        "recipient": RECIPIENT,
        "destination_chain": "eip155:8453",
    }
    # No received amount is stated: the arrival is measured on the destination chain.
    assert "received" not in transfer and result["position_after"]["allowance"] == "0"
    assert across_direct.position(created(config)) == {"allowance": None, "transfers": []}


def late_gateway(config, attempt, head_time, **responses):
    gateway = object.__new__(EvmGateway)
    gateway.config = config
    gateway.reader = Reader(BLOCK.model_copy(update={"timestamp": head_time}))
    gateway.reader.responses.update(
        {**CLEAN, "eth_getTransactionCount": hex(attempt.nonce), **responses}
    )
    gateway.guard = lambda: None
    gateway.verify_authority = lambda manifest, account: None
    gateway.validate_pending = lambda pending: None
    gateway.validate_current = lambda pending: None
    return gateway


def test_recovery_releases_an_expired_signed_deposit_of_a_direct_bridge():
    """What `ax recover` rests on for a deposit: past the quote deadline, with no
    receipt, no provider copy and an unused nonce, the gateway proves the release and
    the operator deployment drops the step and its request. Nothing is planned again."""
    config = operator_config()
    attempt = signed(config)
    state = created(config).model_copy(
        update={
            "initialized": True,
            "baseline": (seen(config),),
            "decision": attempt.decision,
            "pending": attempt,
        }
    )
    deadline = attempt.operation.deadline
    live = late_gateway(config, attempt, deadline)
    with pytest.raises(ValueError, match="never sent or past its protocol deadline"):
        live.require_releasable(attempt)
    late = late_gateway(config, attempt, deadline + 1)
    late.require_releasable(attempt)
    released = release_pending(state)
    assert (released.pending, released.decision, released.plan) == (None, None, None)
    assert released.released == (attempt,) and released.transfers == ()
    service = gateway_service(state, late)
    service.validate_pending_resolution(state, released)
    service.validate_released_record(state, released)
    assert Checkpoint.model_validate_json(released.model_dump_json()) == released
    # A signed deposit has no reference yet: its identifier is assigned when it is
    # mined. The kept record and the `steps_sent` event name its transaction, which
    # `ax bridge-status --deposit-tx` turns into the reference.
    assert ax_actions.unresolved(state) == {
        "unresolved": {"operation": "deposit", "transaction": attempt.tx_hash}
    }
    result = ax_actions.executed_result(released, released.outcomes, None)
    assert result["released_expired"] == [{"operation": "deposit", "transaction": attempt.tx_hash}]
    assert (result["executed"], result["completed"]) == (True, False)
    # A provider that still holds the bytes keeps the step.
    held = late_gateway(config, attempt, deadline + 1, eth_getTransactionByHash={})
    with pytest.raises(ValueError, match="provider still holds"):
        held.require_releasable(attempt)


COMMAND = (
    "ax",
    "bridge",
    "--connector",
    "across",
    "--chain",
    "eip155:42161",
    "--asset",
    str(USDC),
    "--destination-chain",
    "eip155:8453",
    "--destination-asset",
    str(BASE_USDC),
    "--amount",
    str(AMOUNT),
    "--max-fee-bps",
    "10",
    "--recipient",
    RECIPIENT,
    "--wallet",
    "wallet",
    "--max-gas-price-wei",
    "7",
    "--max-transaction-fee",
    "9",
)
STATED = (*COMMAND, "--env-file", ".env")


def parsed(*extra, base=STATED, without=()):
    argv = list(base)
    for flag in without:
        index = argv.index(flag)
        del argv[index : index + 2]
    return parse_validated(build_parser(), [*argv, *extra])


def test_the_command_line_states_the_route_the_amount_the_ceiling_and_the_recipient(capsys):
    args = parsed()
    assert (args.product, args.ax_command, args.execute) == ("operator-action", "bridge", False)
    assert ax_actions.action_request(args) == BRIDGE
    assert ax_actions.market_request(args) == SELECTION
    for missing in ("--chain", "--asset", "--destination-chain", "--destination-asset"):
        with pytest.raises(SystemExit):
            parsed(without=(missing,))
    # No default chooses the amount, the fee ceiling or who receives.
    for missing in ("--amount", "--max-fee-bps", "--recipient"):
        with pytest.raises(SystemExit):
            parsed(without=(missing,))
    for refused in (("--max-fee-bps", "10001"), ("--max-fee-bps", "-1"), ("--amount", "0")):
        with pytest.raises(SystemExit):
            parsed(*refused)
    for refused in ("0x12", "0x" + "00" * 20):
        with pytest.raises(SystemExit):
            parsed("--recipient", refused)
    assert parsed("--max-fee-bps", "0").max_fee_bps == 0
    capsys.readouterr()


def refused(capsys, *extra, base=COMMAND) -> str:
    with pytest.raises(SystemExit):
        parsed(*extra, base=base)
    return capsys.readouterr().err


def keyless_flags(source="arbitrum", destination="base"):
    return ("--public-rpc", source, "--destination-public-rpc", destination)


def test_a_bridge_takes_a_key_or_one_named_public_endpoint_for_each_chain(capsys):
    keyed = parsed()
    assert (keyed.env_file, keyed.public_rpc) == (Path(".env"), None)
    assert keyed.destination_public_rpc is None
    keyless = parsed(*keyless_flags(), base=COMMAND)
    assert (keyless.public_rpc, keyless.destination_public_rpc) == ("arbitrum", "base")
    assert keyless.env_file is None and not keyless.execute
    assert parsed(*keyless_flags(), "--execute", base=COMMAND).execute
    # The destination's endpoint is never chosen for the caller.
    assert "--destination-public-rpc" in ax_bridge.DESTINATION_REQUIRED
    assert ax_bridge.DESTINATION_REQUIRED in refused(capsys, "--public-rpc", "arbitrum")
    # A key serves both chains, so a second endpoint beside it names nothing.
    alone = refused(capsys, "--destination-public-rpc", "base", base=STATED)
    assert ax_bridge.DESTINATION_ALONE in alone
    # One transport, as for every direct action, and one is required.
    assert "not allowed with" in refused(capsys, *keyless_flags(), "--env-file", ".env")
    assert "one of the arguments" in refused(capsys)


def test_an_endpoint_that_does_not_serve_its_chain_is_refused_with_the_names_that_do(capsys):
    # `base` serves Base only: it is no endpoint of the chain the asset leaves.
    wrong_source = refused(capsys, *keyless_flags(source="base"))
    assert "no public endpoint named base serves eip155:42161; choose from arbitrum" in wrong_source
    wrong_destination = refused(capsys, *keyless_flags(destination="arbitrum"))
    assert (
        "no public endpoint named arbitrum serves eip155:8453; choose from base, drpc"
        in wrong_destination
    )
    assert "invalid choice" in refused(capsys, *keyless_flags(destination="unlisted"))
    # HyperEVM has no public endpoint; Across has no pin there either.
    to_hyperevm = list(COMMAND)
    to_hyperevm[to_hyperevm.index("--destination-chain") + 1] = str(HYPEREVM)
    message = refused(capsys, *keyless_flags(), base=to_hyperevm)
    assert "serves eip155:999; use a provider key instead" in message


def revocation(*transport):
    index = COMMAND.index("--amount")
    route = [*COMMAND[:index], *COMMAND[index + 6 :], *transport]
    route[1] = "revoke-bridge"
    return route


def test_a_revocation_names_the_source_endpoint_only(capsys):
    args = parse_validated(build_parser(), revocation("--public-rpc", "arbitrum"))
    assert (args.public_rpc, args.destination_public_rpc) == ("arbitrum", None)
    # It reads the chain the asset leaves only, so it takes no second endpoint.
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), revocation(*keyless_flags()))
    assert "unrecognized arguments: --destination-public-rpc" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), revocation("--public-rpc", "base"))
    assert "choose from arbitrum" in capsys.readouterr().err


def test_a_revocation_names_the_route_only():
    route = revocation("--env-file", ".env")
    args = parse_validated(build_parser(), route)
    assert ax_actions.action_request(args) == REVOKE
    # The strictest ceiling: no deposit with a fee could be admitted along this route.
    assert ax_actions.market_request(args).max_fee_bps == 0
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), [*route, "--amount", "5"])


TRANSPORT = ax_actions.transport


def test_the_command_refuses_an_undeclared_chain_and_passes_the_bridge_on(monkeypatch):
    passed = {}

    def deployment(far, market, action, wallet, provider, endpoint, limits):
        passed.update(far=far, market=market, action=action, limits=limits)
        passed.update(provider=provider, endpoint=endpoint)
        raise StopIteration

    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_bridge, "deployment", deployment)
    with pytest.raises(StopIteration):
        ax_actions.command(parsed())
    assert (passed["market"], passed["action"]) == (SELECTION, BRIDGE)
    assert (passed["far"], passed["limits"]) == (
        ax_bridge.Far(env_file=Path(".env")),
        ax_actions.FeeLimits(7, 9, None),
    )
    # Without a key the action runs on the named source endpoint, as every direct
    # action on a public endpoint does, and hands on the destination's name.
    monkeypatch.setattr(ax_actions, "transport", TRANSPORT)
    with pytest.raises(StopIteration):
        ax_actions.command(parsed(*keyless_flags(), "--execute", base=COMMAND))
    assert passed["far"] == ax_bridge.Far(public_rpc="base")
    assert passed["provider"] == PublicProvider(kind="public_rpc", name="arbitrum")
    assert passed["endpoint"] == PUBLIC_RPC["arbitrum"][ARBITRUM.chain_id]
    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    undeclared = [*STATED]
    undeclared[undeclared.index("--chain") + 1] = str(HYPEREVM)
    undeclared[undeclared.index("--asset") + 1] = f"{HYPEREVM}/erc20:0x{'ab' * 20}"
    # The route itself refuses first: the chain holds no pinned SpokePool.
    with pytest.raises(ValueError, match="no pinned SpokePool on eip155:999"):
        ax_actions.command(parse_validated(build_parser(), undeclared))


class Resolving:
    """A reader that is only handed on to the resolver, with a head and a genesis."""

    def __init__(self, chain, head, genesis=None, code="0x"):
        self.chain, self.head, self.code = chain, head, code
        self.genesis = genesis or evm_network(chain.chain_id).genesis

    def _rpc(self, method, params, *, allow_none=False):
        assert method == "eth_getCode" and params[1] == {"blockHash": self.head.hash}
        return self.code

    def __enter__(self):
        return self

    def __exit__(self, *arguments):
        return None

    def pin_block(self, selector):
        if selector == 0:
            return self.head.model_copy(update={"number": 0, "hash": self.genesis})
        return self.head


CONTRACT, DELEGATING = "0x6080604052", "0xef0100" + "ab" * 20


def resolving(
    monkeypatch,
    destination_pool=BASE_POOL,
    genesis=None,
    request=BRIDGE,
    codes=("0x", "0x"),
    source=SOURCE_ONLY,
    far=None,
    keyless=None,
):
    resolver = ax_actions.resolution("across")
    monkeypatch.setattr(
        "almanak.gateway.ethereum.EthereumReader",
        lambda chain, endpoint: Resolving(chain, BLOCK, code=codes[0]),
    )
    if keyless is not None:
        return keyless_deployment(monkeypatch, keyless, request)
    monkeypatch.setattr(
        "almanak.gateway.read_endpoints.verified_reader",
        far or (lambda chain, env_file: Resolving(BASE, BASE_BLOCK, genesis, codes[1])),
    )
    monkeypatch.setattr(resolver, "resolve_market", lambda reader, selection, block: source)
    route = {**dict(ROUTE), "pool": source.pool, "destination_pool": destination_pool}
    completed = ResolvedRoute(**route)
    monkeypatch.setattr(resolver, "complete_market", lambda reader, resolved, block: completed)
    wallet = {"account": WALLET.account, "address": WALLET.address}
    provider = ax_actions.alchemy(ARBITRUM.chain_id)
    far_transport = ax_bridge.Far(env_file=Path(".env"))
    return lambda: ax_bridge.deployment(
        far_transport, SELECTION, request, wallet, provider, "endpoint", LIMITS
    )


class Proofs(list):
    """What a keyless deployment did, in order, and what its destination endpoint is."""

    def __init__(self, destination="base", far_genesis=None, far_stale=False):
        super().__init__()
        self.destination, self.far_genesis, self.far_stale = destination, far_genesis, far_stale


def keyless_deployment(monkeypatch, proofs: Proofs, request):
    """A deployment through two named public endpoints, with each proof and each
    resolution step recorded in the order the command made it."""
    resolver = ax_actions.resolution("across")

    def prove(reader):
        proofs.append(("chain", str(reader.chain.chain_id)))

    def verify_public(reader):
        proofs.append(("public", str(reader.chain.chain_id)))
        if reader.genesis != evm_network(reader.chain.chain_id).genesis:
            raise DataError("chain_mismatch", "public endpoint has a different genesis")
        if getattr(reader, "stale", False):
            raise DataError("stale", "public endpoint head is 500 seconds old")

    def opened(chain, endpoint, **options):
        reader = Resolving(Chain(chain_id=chain, environment="mainnet"), BASE_BLOCK)
        reader.genesis = proofs.far_genesis or reader.genesis
        reader.verify_chain = lambda: prove(reader)
        reader.stale = proofs.far_stale
        proofs.append(("open", endpoint))
        return reader

    def source_proof(reader):
        prove(reader)
        verify_public(reader)

    def resolve(reader, selection, block):
        proofs.append("resolve")
        return SOURCE_ONLY

    def complete(reader, resolved, block):
        proofs.append("complete")
        return ROUTE

    monkeypatch.setattr(ax_actions, "prove_public", source_proof)
    monkeypatch.setattr(read_endpoints, "mainnet_reader", opened)
    monkeypatch.setattr(read_endpoints, "verify_public", verify_public)
    monkeypatch.setattr(read_endpoints, "verified_reader", pytest.fail)
    monkeypatch.setattr(resolver, "resolve_market", resolve)
    monkeypatch.setattr(resolver, "complete_market", complete)
    wallet = {"account": WALLET.account, "address": WALLET.address}
    provider = PublicProvider(kind="public_rpc", name="arbitrum")
    far = ax_bridge.Far(public_rpc=proofs.destination)
    return lambda: ax_bridge.deployment(
        far, SELECTION, request, wallet, provider, "endpoint", LIMITS
    )


def test_both_public_endpoints_give_the_public_proof_before_their_chain_is_resolved(monkeypatch):
    proofs = Proofs()
    config = resolving(monkeypatch, keyless=proofs)()
    assert proofs == [
        ("chain", "eip155:42161"),
        ("public", "eip155:42161"),
        "resolve",
        ("open", PUBLIC_RPC["base"][BASE.chain_id]),
        ("chain", "eip155:8453"),
        ("public", "eip155:8453"),
        "complete",
    ]
    # The record names the public provider the action ran on, so recovery finds it.
    network = config.production.networks[0]
    assert network.provider == PublicProvider(kind="public_rpc", name="arbitrum")
    assert config.positions == operator_config().positions


@pytest.mark.parametrize(
    ("fault", "kind"),
    [({"far_stale": True}, "stale"), ({"far_genesis": "0x" + "77" * 32}, "chain_mismatch")],
)
def test_a_stale_or_wrong_genesis_destination_endpoint_refuses_before_the_route_completes(
    monkeypatch, fault, kind
):
    proofs = Proofs(**fault)
    with pytest.raises(DataError) as error:
        resolving(monkeypatch, keyless=proofs)()
    assert error.value.kind == kind
    # Two endpoints are proven, so the refusal says which one failed, and on which chain.
    assert "the public endpoint base on eip155:8453 did not give the proof" in str(error.value)
    # Nothing was completed, so no deployment exists and nothing can be planned.
    assert "complete" not in proofs and proofs[-1] == ("public", "eip155:8453")


def test_a_keyless_bridge_without_a_destination_endpoint_opens_nothing(monkeypatch):
    proofs = Proofs(destination=None)
    with pytest.raises(ValueError, match="--destination-public-rpc"):
        resolving(monkeypatch, keyless=proofs)()
    assert not [step for step in proofs if step[0] == "open"]
    # A revocation reads the source chain only and needs none.
    proofs = Proofs(destination=None)
    assert resolving(monkeypatch, keyless=proofs, request=REVOKE)() is not None
    assert proofs == [("chain", "eip155:42161"), ("public", "eip155:42161"), "resolve"]


def unavailable(chain, env_file):
    raise DataError("unavailable", "the destination provider does not answer")


def test_a_revocation_reads_the_source_chain_only(monkeypatch):
    """A destination outage must not keep an approval standing."""
    config = resolving(monkeypatch, request=REVOKE, far=unavailable)()
    assert config == operator_config(REVOKE)
    target = config.target(ALLOCATION)
    assert target.route.spoke_pool == POOL.address
    assert target.route.destination_pool == BASE_PIN
    # The same outage refuses a bridge, which completes its route there.
    with pytest.raises(DataError) as error:
        resolving(monkeypatch, far=unavailable)()
    assert error.value.kind == "unavailable"


def test_a_revocation_survives_a_spoke_pool_that_differs_from_its_pin(monkeypatch, tmp_path):
    """A pin difference refuses new deposits only: the approval the wallet gave the
    reviewed proxy address is cleared whatever implementation that proxy runs now."""
    other = POOL.model_copy(update={"implementation": "0x" + "99" * 20})
    changed = ResolvedRoute(**{**dict(SOURCE_ONLY), "pool": other})
    # Neither chain's pool is held to its pin: the destination's is not even read.
    config = resolving(monkeypatch, request=REVOKE, source=changed, far=pytest.fail)()
    assert config == operator_config(REVOKE)
    desk = Desk(tmp_path, config, pool=other, allowance=7)
    record = desk.runtime().submit(desk.config.revoke(ALLOCATION))
    state = desk.store.load()
    step = planning.operation(config, state, record.intent, desk.gateway.seen)
    assert (step.kind, step.final) == ("revoke", True)
    spender = decode(("address", "uint256"), bytes.fromhex(plan_call(step).data[10:]))
    assert spender == (POOL.address, 0)
    # The same pool refuses a bridge at resolution.
    with pytest.raises(DataError) as error:
        resolving(monkeypatch, source=changed)()
    assert error.value.kind == "deployment_changed"


def test_a_contract_recipient_with_no_code_on_the_destination_is_refused(monkeypatch):
    with pytest.raises(DataError) as error:
        resolving(monkeypatch, codes=(CONTRACT, "0x"))()
    assert error.value.kind == "recipient_not_deployed"
    assert RECIPIENT in str(error.value) and "eip155:8453" in str(error.value)
    # A contract on both chains, a plain account, a delegating plain account and a
    # contract that exists on the destination only are all admitted.
    for codes in ((CONTRACT, CONTRACT), ("0x", "0x"), (DELEGATING, "0x"), ("0x", CONTRACT)):
        assert resolving(monkeypatch, codes=codes)() == operator_config()
    with pytest.raises(DataError) as malformed:
        resolving(monkeypatch, codes=(None, "0x"))()
    assert malformed.value.kind == "malformed"


def test_the_route_is_resolved_on_the_source_and_completed_on_the_destination(monkeypatch):
    assert resolving(monkeypatch)() == operator_config()


def test_a_destination_provider_of_another_genesis_is_refused(monkeypatch):
    with pytest.raises(DataError) as error:
        resolving(monkeypatch, genesis="0x" + "77" * 32)()
    assert error.value.kind == "chain_mismatch"


def test_a_destination_spoke_pool_that_is_not_the_reviewed_one_is_refused(monkeypatch):
    other = BASE_POOL.model_copy(update={"implementation": "0x" + "99" * 20})
    with pytest.raises(DataError) as error:
        resolving(monkeypatch, destination_pool=other)()
    assert error.value.kind == "deployment_changed"


# The retained mainnet deposit and its fill: Arbitrum to Base, 24 November 2026.
DEPOSIT, FILL = CAPTURES["deposit"], CAPTURES["fill"]
REFERENCE = "4832241:" + FILL["relay_hash"] + ":1795498091"
FILL_DEADLINE = 1795505291
DEPOSITOR = DEPOSIT["from"]
ARB_POOL, FAR_POOL = SPOKE_POOLS[ARBITRUM.chain_id].address, BASE_PIN.address


class Ledger:
    """A chain that holds the given receipts and nothing else, one block a second."""

    def __init__(self, chain, anchor, head, receipts=(), calls=None, finalized=None):
        self.chain, self.anchor, self.head = chain, anchor, head
        self.finalized = head if finalized is None else finalized
        self.receipts = {r["transactionHash"]: r for r in receipts}
        # A block that holds a retained receipt carries that receipt's own hash.
        self.hashes = {int(r["blockNumber"], 16): r["blockHash"] for r in receipts}
        self.calls, self.queries, self.pinned = calls or {}, [], []

    def __enter__(self):
        return self

    def __exit__(self, *arguments):
        return None

    def pin_block(self, selector) -> Block:
        self.pinned.append(selector)
        tags = {"latest": self.head, "finalized": self.finalized}
        number = tags.get(selector, selector)
        first, at = self.anchor
        return Block(
            chain=self.chain,
            number=number,
            hash=self.hashes.get(number, "0x" + number.to_bytes(32, "big").hex()),
            timestamp=at + number - first,
        )

    def matches(self, log, query) -> bool:
        topics = query["topics"]
        return (
            log["address"].lower() == query["address"]
            and log["topics"][: len(topics)] == topics
            and int(query["fromBlock"], 16)
            <= int(log["blockNumber"], 16)
            <= int(query["toBlock"], 16)
        )

    def _rpc(self, method, params, *, allow_none=False):
        if method == "eth_getTransactionReceipt":
            return self.receipts.get(params[0])
        assert method == "eth_getLogs"
        self.queries.append(params[0])
        return [
            log
            for found in self.receipts.values()
            for log in found["logs"]
            if self.matches(log, params[0])
        ]

    def call(self, to, signature, block, *, inputs=(), args=(), outputs=()):
        return self.calls[signature]


def source(head_after=10_000, receipts=None, deferred=0, finalized_after=None):
    number = int(DEPOSIT["receipt"]["blockNumber"], 16)
    return Ledger(
        ARBITRUM,
        (number, DEPOSIT["block_timestamp"]),
        number + head_after,
        [DEPOSIT["receipt"]] if receipts is None else receipts,
        {"getRelayerRefund(address,address)": (deferred,)},
        None if finalized_after is None else number + finalized_after,
    )


def destination(status, head_after=10, receipts=None, finalized_after=None):
    number = int(FILL["receipt"]["blockNumber"], 16)
    return Ledger(
        BASE,
        (number, FILL["block_timestamp"]),
        number + head_after,
        [FILL["receipt"]] if receipts is None else receipts,
        {"fillStatuses(bytes32)": (status,)},
        None if finalized_after is None else number + finalized_after,
    )


def status(monkeypatch, near, far, **options):
    readers = {ARBITRUM.chain_id: near, BASE.chain_id: far}
    monkeypatch.setattr("almanak.ax.observation_reader", lambda args, chain: readers[chain])
    monkeypatch.setattr("almanak.ax.public_reader", lambda chain, name: readers[chain])
    monkeypatch.setattr(
        "almanak.connectors.across.observations.read_pool", lambda reader, pool, block: POOL
    )
    values = {
        "connector": "across",
        "chain": "eip155:42161",
        "destination_chain": "eip155:8453",
        "reference": REFERENCE,
        "deposit_tx": None,
        "block": "latest",
        "refund_from_block": None,
        "env_file": Path(".env"),
        "public_rpc": None,
        "destination_public_rpc": None,
        **options,
    }
    return ax_bridge.status(SimpleNamespace(**values))


def test_the_status_of_an_arrived_transfer_is_its_measured_fill(monkeypatch):
    near, far = source(), destination(2)
    result = status(monkeypatch, near, far)
    assert result["status"] == "arrived"
    assert result["deposit"] == {
        "transaction": DEPOSIT["receipt"]["transactionHash"],
        "block": int(DEPOSIT["receipt"]["blockNumber"], 16),
        "depositor": DEPOSITOR,
        "recipient": DEPOSITOR,
        "sent": {"asset": str(USDC), "amount": "1562605440"},
        "offered": "1562446253",
        "fill_deadline": FILL_DEADLINE,
    }
    assert result["arrival"] == {
        "transaction": FILL["receipt"]["transactionHash"],
        "block": int(FILL["receipt"]["blockNumber"], 16),
        "received": "1562446253",
        "fill_type": "fast",
    }
    assert result["read_at"] == {"block": "latest", "destination_block": far.head}
    assert result["reference"] == REFERENCE and "refund" not in result
    # Every log query stays inside the bounded page span.
    for query in (*near.queries, *far.queries):
        assert int(query["toBlock"], 16) - int(query["fromBlock"], 16) < 2_000


def test_an_unfilled_transfer_before_its_deadline_is_in_flight_and_states_no_amount(
    monkeypatch,
):
    result = status(monkeypatch, source(), destination(0, receipts=[]))
    assert result["status"] == "in_flight"
    assert "arrival" not in result and "refund" not in result
    assert "received" not in json.dumps(result)
    # Only a read behind the head says that a later block may hold the fill.
    assert result["note"] == "No fill is in the destination chain's latest block or before it."
    final = status(monkeypatch, source(), destination(0, receipts=[]), block="finalized")
    assert "destination chain's finalized block" in final["note"]
    assert "a finalized block trails the head by minutes" in final["note"]


def test_the_deposit_is_found_at_the_latest_block_and_the_outcome_at_the_named_one(
    monkeypatch,
):
    """A finalized block trails the head by minutes. The operator holds the deposit's
    receipt, so the deposit is looked up at the head; a fill is evidence only at the
    block the command names."""
    # Neither chain has finalized the deposit's or the fill's block yet.
    near, far = source(finalized_after=-50), destination(0, finalized_after=-5)
    waiting = status(monkeypatch, near, far, block="finalized")
    assert waiting["status"] == "in_flight"
    assert waiting["deposit"]["transaction"] == DEPOSIT["receipt"]["transactionHash"]
    assert waiting["read_at"] == {"block": "finalized", "destination_block": far.finalized}
    assert "latest" in near.pinned and "finalized" not in near.pinned
    assert "finalized" in far.pinned and "latest" not in far.pinned
    seen = status(monkeypatch, source(finalized_after=-50), destination(2))
    assert seen["status"] == "arrived" and seen["read_at"]["block"] == "latest"


def test_the_fill_deadline_itself_is_still_in_flight(monkeypatch):
    """The SpokePool fills a relay in a block stamped with its fill deadline and
    reverts one second later, so only a block past the deadline shows it unfilled."""
    at_deadline = FILL_DEADLINE - FILL["block_timestamp"]
    on_time = status(monkeypatch, source(), destination(0, head_after=at_deadline, receipts=[]))
    assert on_time["status"] == "in_flight"
    late = status(monkeypatch, source(), destination(0, head_after=at_deadline + 1, receipts=[]))
    assert late["status"] == "unfilled"
    assert late["read_at"]["source_block"] == source().head


def test_a_lost_reference_is_rebuilt_from_the_deposit_transaction(monkeypatch):
    """Held to a known answer: the retained mainnet deposit's receipt yields the
    reference whose relay hash the Base SpokePool filled."""
    named = {"reference": None, "deposit_tx": DEPOSIT["receipt"]["transactionHash"]}
    result = status(monkeypatch, source(), destination(2), **named)
    assert result["reference"] == REFERENCE and result["status"] == "arrived"
    assert result["arrival"]["received"] == "1562446253"
    with pytest.raises(DataError) as unknown:
        status(monkeypatch, source(), destination(2), reference=None, deposit_tx="0x" + "ab" * 32)
    assert unknown.value.kind == "not_found"
    failed = {**DEPOSIT["receipt"], "status": "0x0"}
    with pytest.raises(DataError) as reverted:
        status(monkeypatch, source(receipts=[failed]), destination(2), **named)
    assert reverted.value.kind == "not_found"
    # A transaction that deposited nothing through the reviewed pool towards this chain.
    foreign = [log for log in DEPOSIT["receipt"]["logs"] if log["address"].lower() != ARB_POOL]
    with pytest.raises(DataError, match="holds 0 deposits") as none:
        status(
            monkeypatch,
            source(receipts=[{**DEPOSIT["receipt"], "logs": foreign}]),
            far := destination(2),
            **named,
        )
    assert none.value.kind == "not_found" and far.queries == []
    replaced = source()
    replaced.hashes.clear()
    with pytest.raises(DataError) as reorganized:
        status(monkeypatch, replaced, destination(2), **named)
    assert reorganized.value.kind == "reorganization"


def test_a_reference_with_another_quote_timestamp_is_refused(monkeypatch):
    """The relay hash does not cover the quote's timestamp, which sets where the
    arrival scan starts, so the reference is held to the deposit event's own."""
    earlier = REFERENCE.rsplit(":", 1)[0] + ":1795498090"
    with pytest.raises(DataError, match="mined with 1795498091") as error:
        status(monkeypatch, source(), destination(2), reference=earlier)
    assert error.value.kind == "integrity"


class Constructed(FakeChain):
    """A constructed chain the status command reads through, with both block tags."""

    def __enter__(self):
        return self

    def __exit__(self, *arguments):
        return None

    def pin_block(self, selector):
        return super().pin_block("latest" if selector == "finalized" else selector)


def test_the_status_reads_a_transfer_whose_recipient_is_not_its_depositor(monkeypatch):
    """The case the command exists for: the fill pays the stated recipient, and the
    amount is the transfer to that address, not to the depositor."""
    config = operator_config()
    operation = deposit(config)
    outcome = mined_deposit(config)
    emitted = deposit_log(operation)
    near = Constructed(ARBITRUM, NOW - 1_000, 1, 2_000)
    row = (emitted["address"], emitted["topics"], bytes.fromhex(emitted["data"][2:]))
    near.mine(1_000 + MINED_AT - NOW, "0x" + "d1" * 32, [row])
    far = Constructed(BASE, NOW - 1_000, 1, 1_100)
    far.views["fillStatuses(bytes32)"] = 2
    relay = deposited_relay(operation, MINED)
    paid = operation.quote.output_amount
    rows = [
        fill(FAR_POOL, relay),
        payment(BASE_USDC.reference, "0x" + "77" * 20, RECIPIENT, paid),
    ]
    far.mine(1_020, "0x" + "f1" * 32, rows)
    result = status(monkeypatch, near, far, reference=outcome.transfer.reference)
    assert result["status"] == "arrived" and result["arrival"]["received"] == str(paid)
    assert (result["deposit"]["depositor"], result["deposit"]["recipient"]) == (SENDER, RECIPIENT)
    # The same fill paying the depositor instead is not this transfer's arrival.
    misdirected = Constructed(BASE, NOW - 1_000, 1, 1_100)
    misdirected.views["fillStatuses(bytes32)"] = 2
    wrong = [fill(FAR_POOL, relay), payment(BASE_USDC.reference, "0x" + "77" * 20, SENDER, paid)]
    misdirected.mine(1_020, "0x" + "f2" * 32, wrong)
    with pytest.raises(DataError):
        status(monkeypatch, near, misdirected, reference=outcome.transfer.reference)


def test_a_bridge_record_is_recovered_through_a_named_public_endpoint(monkeypatch):
    """A keyless execution that leaves a record must be recoverable without a key."""
    proved = []
    monkeypatch.setattr(
        ax_actions, "proven_public", lambda chain, endpoint: proved.append(endpoint) or endpoint
    )
    own = PUBLIC_RPC["arbitrum"][ARBITRUM.chain_id]
    keyed = operator_config()
    network = keyed.production.networks[0].model_copy(
        update={"provider": PublicProvider(kind="public_rpc", name="arbitrum")}
    )
    production = keyed.production.model_copy(update={"networks": (network,)})
    keyless = keyed.model_copy(update={"production": production})
    # Its own endpoint with no flag while that is still listed, or the one the caller
    # names, which gives the public proof first. A record made with a key takes one too.
    assert ax_actions.record_endpoint(keyless, None) == own
    assert ax_actions.record_endpoint(keyless, None, "arbitrum") == own
    assert ax_actions.record_endpoint(keyed, None, "arbitrum") == own
    assert proved == [own, own]
    # Only an endpoint of the record's one chain, the chain the asset left.
    with pytest.raises(ValueError, match="no public endpoint named base serves eip155:42161"):
        ax_actions.record_endpoint(keyless, None, "base")
    # No connector's record refuses a named public endpoint.
    assert not hasattr(across_direct.DIRECT, "public_recovery")
    assert not hasattr(direct.binding("aave_v3"), "public_recovery")


def test_past_the_deadline_the_transfer_is_unfilled_and_no_refund_is_attributed(monkeypatch):
    late = FILL_DEADLINE - FILL["block_timestamp"] + 100
    near = source(head_after=late)
    result = status(monkeypatch, near, destination(0, head_after=late, receipts=[]), block="latest")
    assert result["status"] == "unfilled" and "arrival" not in result
    refund = result["refund"]
    first = int(DEPOSIT["receipt"]["blockNumber"], 16) + FILL_DEADLINE - DEPOSIT["block_timestamp"]
    assert refund["payments"] == [] and refund["deferred_in_spoke_pool"] == "0"
    # The block stamped with the deadline can still fill, so the scan starts after it.
    assert refund["scanned"] == {"from": first + 1, "to": near.head} and refund["complete"]
    assert "none is attributed to this transfer" in refund["note"]


def test_a_refund_leaf_payment_is_listed_and_never_tied_to_the_transfer():
    paid = CAPTURES["refund"]["receipt"]
    page = CAPTURES["pages"]["refund"]
    number = int(paid["blockNumber"], 16)
    near = Ledger(
        ARBITRUM,
        (number, page["mined_at"]),
        number + 5_000,
        [paid],
        {"getRelayerRefund(address,address)": (3,)},
    )

    def deposit_of(sent):
        return ax_bridge.Deposit(
            quoted_at=page["mined_at"] - 100,
            transaction="0x" + "00" * 32,
            block=number - 100,
            depositor=page["payee"],
            recipient=page["payee"],
            input_token=USDC.reference,
            output_token=BASE_USDC.reference,
            sent=sent,
            offered=sent,
            fill_deadline=page["mined_at"] - 50,
        )

    head = near.pin_block("latest")
    refund = ax_bridge.read_refunds(near, ARB_POOL, deposit_of(page["total"]), head, None)
    (payment,) = refund["payments"]
    assert payment["transaction"] == paid["transactionHash"] and payment["block"] == number
    assert payment["amounts"] == [str(page["total"])] and payment["equals_sent"] is True
    # Two matching logs of one transaction are one payment, since a record covers it all.
    from almanak.connectors.across import settlement_reader

    single = settlement_reader.refund_logs
    twice = lambda *args: [row for row in single(*args) for _ in range(2)]  # noqa: E731
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(settlement_reader, "refund_logs", twice)
        again = ax_bridge.read_refunds(near, ARB_POOL, deposit_of(page["total"]), head, None)
    assert again["payments"] == refund["payments"]
    assert refund["deferred_in_spoke_pool"] == "3" and refund["complete"]
    other = ax_bridge.read_refunds(near, ARB_POOL, deposit_of(7), head, number - 10)
    assert other["payments"][0]["equals_sent"] is False
    assert other["scanned"]["from"] == number - 10
    # The pool's own transfer is reported beside the leaf, and both must equal the amount.
    assert payment["paid"] == [str(page["total"])]
    # A head not yet past the deadline leaves an empty range that still names where to go on.
    at = near.pin_block(refund["scanned"]["from"] - 1)
    empty = ax_bridge.read_refunds(near, ARB_POOL, deposit_of(7), at, None)
    assert empty["scanned"] == {"from": at.number + 1, "to": at.number} and empty["complete"]
    # A cursor before the fill deadline is moved to the first block after it.
    early = ax_bridge.read_refunds(near, ARB_POOL, deposit_of(7), head, 1)
    assert early["scanned"]["from"] == refund["scanned"]["from"] > 1
    # A scan that starts after the payment finds none.
    short = ax_bridge.read_refunds(
        near, ARB_POOL, deposit_of(7), near.pin_block(number + 5_000), number + 1
    )
    assert short["payments"] == [] and short["complete"]
    # A payment the SpokePool made outside a refund leaf is not listed.
    plain = {**paid, "logs": [log for log in paid["logs"] if log["address"] != ARB_POOL]}
    near.receipts[paid["transactionHash"]] = plain
    assert ax_bridge.read_refunds(near, ARB_POOL, deposit_of(7), head, None)["payments"] == []


def test_a_refund_scan_stops_at_its_page_limit_and_names_where(monkeypatch):
    monkeypatch.setattr(ax_bridge, "MAXIMUM_REFUND_PAGES", 2)
    near = source(head_after=10_000, receipts=[])
    deposit = ax_bridge.Deposit("0x", 0, DEPOSITOR, DEPOSITOR, USDC.reference, "0x", 1, 1, 0, 0)
    start = near.head - 9_000
    refund = ax_bridge.read_refunds(near, ARB_POOL, deposit, near.pin_block("latest"), start)
    assert refund["scanned"] == {"from": start, "to": start + 3_999}
    assert refund["complete"] is False


def test_a_reference_that_is_no_deposit_of_the_route_is_refused(monkeypatch):
    with pytest.raises(DataError) as absent:
        status(monkeypatch, source(receipts=[]), destination(2))
    assert absent.value.kind == "not_found"
    # The identifier alone is not the deposit: the whole relay must hash to the reference.
    other = REFERENCE.replace(FILL["relay_hash"], "0x" + "ab" * 32)
    with pytest.raises(DataError) as mismatch:
        status(monkeypatch, source(), destination(2), reference=other)
    assert mismatch.value.kind == "not_found"
    with pytest.raises(DataError) as malformed:
        status(monkeypatch, source(), destination(2), reference="4832241")
    assert malformed.value.kind == "integrity"


def test_a_filled_status_without_its_fill_reports_nothing_as_received(monkeypatch):
    with pytest.raises(DataError) as error:
        status(monkeypatch, source(), destination(2, receipts=[]))
    assert error.value.kind == "integrity"


def test_the_status_command_takes_a_read_transport_or_two_public_endpoints(monkeypatch, capsys):
    argv = [
        "ax",
        "bridge-status",
        "--connector",
        "across",
        "--chain",
        "eip155:42161",
        "--destination-chain",
        "eip155:8453",
        "--reference",
        REFERENCE,
    ]
    args = parse_validated(build_parser(), [*argv, "--env-file", ".env"])
    assert (args.product, args.block, args.refund_from_block) == (
        "ax-bridge-status",
        "latest",
        None,
    )
    # A tag only: one block number names no block of two chains.
    assert parse_validated(build_parser(), [*argv, "--env-file", ".env", "--block", "latest"])
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), [*argv, "--env-file", ".env", "--block", "12"])
    # The transfer is named once: by its reference or by its deposit transaction.
    by_hash = [*argv[:-2], "--deposit-tx", "0x" + "AB" * 32, "--env-file", ".env"]
    named = parse_validated(build_parser(), by_hash)
    assert (named.deposit_tx, named.reference) == ("0x" + "ab" * 32, None)
    for refused in ([*by_hash, "--reference", REFERENCE], [*argv[:-2], "--env-file", ".env"]):
        with pytest.raises(SystemExit):
            parse_validated(build_parser(), refused)
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), [*by_hash[:-3], "0x12", "--env-file", ".env"])
    continued = parse_validated(
        build_parser(), [*argv, "--env-file", ".env", "--refund-from-block", "12"]
    )
    assert continued.refund_from_block == 12
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), [*argv, "--env-file", ".env", "--refund-from-block", "x"])
    # A public endpoint serves one chain, so the caller names one for each.
    keyless = parse_validated(build_parser(), [*argv, *keyless_flags()])
    assert (keyless.public_rpc, keyless.destination_public_rpc) == ("arbitrum", "base")
    assert (keyless.env_file, keyless.gateway_url, keyless.block) == (None, None, "latest")
    capsys.readouterr()
    for flags, cause in (
        (("--public-rpc", "arbitrum"), ax_bridge.DESTINATION_REQUIRED),
        (("--env-file", ".env", "--destination-public-rpc", "base"), ax_bridge.DESTINATION_ALONE),
        (keyless_flags(source="base"), "choose from arbitrum"),
        (keyless_flags(destination="optimism"), "choose from base, drpc"),
        ((*keyless_flags(), "--env-file", ".env"), "not allowed with"),
        ((*keyless_flags(), "--gateway-auth-file", "t"), "belongs to --gateway-url"),
    ):
        with pytest.raises(SystemExit):
            parse_validated(build_parser(), [*argv, *flags])
        assert cause in capsys.readouterr().err
    served = parse_validated(
        build_parser(), [*argv, "--gateway-url", "http://127.0.0.1:1", "--gateway-auth-file", "t"]
    )
    assert (served.env_file, served.public_rpc) == (None, None)
    assert served.destination_public_rpc is None
    from almanak import ax

    monkeypatch.setattr(ax_bridge, "status", lambda given: {"status": "in_flight"})
    monkeypatch.setattr(ax, "ax_bridge", ax_bridge)
    assert ax.command(args) == 0


KEYLESS = {"env_file": None, "public_rpc": "arbitrum", "destination_public_rpc": "base"}


def test_a_keyless_status_reads_each_chain_through_its_own_proven_endpoint(monkeypatch):
    from almanak import ax

    near, far = source(), destination(2)
    opened = []

    def public_reader(chain, name):
        opened.append((str(chain), name))
        return {ARBITRUM.chain_id: near, BASE.chain_id: far}[chain]

    monkeypatch.setattr(ax, "public_reader", public_reader)
    monkeypatch.setattr(ax, "verified_reader", pytest.fail)
    monkeypatch.setattr(
        "almanak.connectors.across.observations.read_pool", lambda reader, pool, block: POOL
    )
    args = SimpleNamespace(
        connector="across",
        chain="eip155:42161",
        destination_chain="eip155:8453",
        reference=REFERENCE,
        deposit_tx=None,
        block="finalized",
        refund_from_block=None,
        gateway_url=None,
        **KEYLESS,
    )
    assert ax_bridge.status(args)["status"] == "arrived"
    # `public_reader` is the reader that proves chain number, genesis and head.
    assert opened == [("eip155:42161", "arbitrum"), ("eip155:8453", "base")]


class Refusing(Ledger):
    """A chain whose endpoint refuses every log query, as a public endpoint may."""

    def _rpc(self, method, params, *, allow_none=False):
        if method == "eth_getLogs":
            raise ProviderBusy("eth_getLogs", 7.0)
        return super()._rpc(method, params, allow_none=allow_none)


def test_a_public_endpoint_that_refuses_a_status_read_is_named_with_its_chain(monkeypatch):
    late = FILL_DEADLINE - FILL["block_timestamp"] + 100
    far = destination(2)
    far.__class__ = Refusing
    by_hash = {"reference": None, "deposit_tx": DEPOSIT["receipt"]["transactionHash"]}
    with pytest.raises(DataError) as error:
        status(monkeypatch, source(), far, **KEYLESS, **by_hash)
    message = str(error.value)
    # The refusal is the endpoint's own: its class and its requested wait are kept.
    assert isinstance(error.value, ProviderBusy) and error.value.retry_after == 7.0
    assert (error.value.kind, error.value.code) == ("http", 429)
    assert message.startswith("eth_getLogs: HTTP 429; ")
    assert "the public endpoint base on eip155:8453" in message and "--env-file" in message
    # The deposit's lookup and the refund scan read the source chain's endpoint.
    near = source(head_after=late)
    near.__class__ = Refusing
    with pytest.raises(DataError) as error:
        status(monkeypatch, near, destination(0), **KEYLESS)
    assert "the public endpoint arbitrum on eip155:42161" in str(error.value)
    unfilled = destination(0, head_after=late, receipts=[])
    with pytest.raises(DataError) as error:
        status(monkeypatch, near, unfilled, block="latest", **KEYLESS, **by_hash)
    assert "the public endpoint arbitrum on eip155:42161" in str(error.value)
    # A provider key's refusal keeps its own words, and so does a finding of the
    # command that is no limit of the endpoint.
    with pytest.raises(DataError) as error:
        status(monkeypatch, source(), far, **by_hash)
    assert str(error.value) == "eth_getLogs: HTTP 429"
    with pytest.raises(DataError) as error:
        status(monkeypatch, source(receipts=[]), destination(2), **KEYLESS)
    assert error.value.kind == "not_found" and "public endpoint" not in str(error.value)


def test_a_status_scan_pages_by_what_the_named_endpoint_serves_in_one_log_query(monkeypatch):
    from almanak.gateway import providers

    assert ax_bridge.log_span(None) == ax_bridge.log_span("arbitrum") == 2_000
    assert ax_bridge.log_span("drpc") == providers.PUBLIC_LOG_SPAN["drpc"] == 100
    # Only listed endpoints carry a span, and none is wider than a settlement page.
    assert set(providers.PUBLIC_LOG_SPAN) <= set(PUBLIC_RPC)
    assert max(providers.PUBLIC_LOG_SPAN.values()) <= 2_000
    late = FILL_DEADLINE - FILL["block_timestamp"] + 250
    by_hash = {"reference": None, "deposit_tx": DEPOSIT["receipt"]["transactionHash"]}

    def refund_queries(name):
        near = source(head_after=late)
        unfilled = destination(0, head_after=late, receipts=[])
        options = {**KEYLESS, **by_hash, "public_rpc": name, "block": "latest"}
        refund = status(monkeypatch, near, unfilled, **options)["refund"]
        assert refund["complete"] and refund["scanned"]["to"] == near.head
        return [int(q["toBlock"], 16) - int(q["fromBlock"], 16) + 1 for q in near.queries]

    # The same blocks either way: whole pages, or the fewer blocks one query may span.
    narrow, wide = refund_queries("drpc"), refund_queries("arbitrum")
    assert len(wide) == 1 and sum(narrow) == wide[0]
    assert max(narrow) == 100 and len(narrow) > 1


def widths(ledger) -> list[int]:
    return [int(q["toBlock"], 16) - int(q["fromBlock"], 16) + 1 for q in ledger.queries]


@pytest.mark.parametrize(
    ("names", "narrow"),
    [(("drpc", "base"), "source"), (("arbitrum", "drpc"), "destination")],
)
def test_each_chain_of_a_status_by_reference_pages_by_its_own_endpoint(monkeypatch, names, narrow):
    """The deposit lookup pages by the source endpoint's span and the arrival scan by
    the destination's: neither ignores its span, and neither takes the other's."""
    near, far = source(), destination(2, head_after=5_000)
    options = {**KEYLESS, "public_rpc": names[0], "destination_public_rpc": names[1]}
    assert status(monkeypatch, near, far, **options)["status"] == "arrived"
    scans = {"source": widths(near), "destination": widths(far)}
    wide = "destination" if narrow == "source" else "source"
    assert scans[narrow] and max(scans[narrow]) == 100
    # Each range is wider than one narrow page, so a whole page shows on the other chain.
    assert scans[wide] and scans[wide][0] > 100


def test_a_public_endpoint_that_fails_its_proof_is_named_with_its_chain(monkeypatch):
    from almanak import ax

    def stale(reader):
        raise DataError("stale", "public endpoint head is 500 seconds old")

    # The source endpoint of an action.
    monkeypatch.setattr(ax_actions, "prove_public", stale)
    monkeypatch.setattr(
        "almanak.gateway.ethereum.EthereumReader", lambda chain, endpoint: Resolving(chain, BLOCK)
    )
    wallet = {"account": WALLET.account, "address": WALLET.address}
    provider = PublicProvider(kind="public_rpc", name="arbitrum")
    with pytest.raises(DataError) as error:
        ax_bridge.deployment(
            ax_bridge.Far(public_rpc="base"), SELECTION, BRIDGE, wallet, provider, "e", LIMITS
        )
    assert error.value.kind == "stale"
    assert str(error.value).startswith("public endpoint head is 500 seconds old; ")
    assert "the public endpoint arbitrum on eip155:42161 did not give the proof" in str(error.value)

    # Either endpoint of a status read.
    def public_reader(chain, name):
        if name == "base":
            raise DataError("chain_mismatch", "public endpoint has a different genesis")
        return source()

    monkeypatch.setattr(ax, "public_reader", public_reader)
    args = SimpleNamespace(chain="eip155:42161", destination_chain="eip155:8453", **KEYLESS)
    with pytest.raises(DataError) as error:
        ax_bridge.status(args)
    assert error.value.kind == "chain_mismatch"
    assert "the public endpoint base on eip155:8453 did not give the proof" in str(error.value)
    # A missing destination name is never filled with the source endpoint's.
    alone = SimpleNamespace(**{**vars(args), "destination_public_rpc": None})
    with pytest.raises(ValueError, match="--destination-public-rpc"):
        ax_bridge.status(alone)
    # A provider key's reader is not a public endpoint and is named as none.
    monkeypatch.setattr(ax, "observation_reader", lambda args, chain: stale(None))
    keyed = SimpleNamespace(**{**vars(args), "public_rpc": None, "destination_public_rpc": None})
    with pytest.raises(DataError) as error:
        ax_bridge.status(keyed)
    assert str(error.value) == "public endpoint head is 500 seconds old"
