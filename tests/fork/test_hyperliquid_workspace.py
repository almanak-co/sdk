"""The Hyperliquid fork workspace on a HyperEVM mainnet fork (blueprint 30, 9.1 and 13).

`ForkSet.prepare` builds the workspace from a Hyperliquid fork configuration: the Anvil
node, the Safe and Roles, the fixture's USDC, and the `hypercore-model` process beside the
node, seeded from mainnet `/info` and named with the agent key file in the fork route.
The fork gateway adapter (`HyperCoreForkGateway`) is then built from that route, installs
the generated grants and drives phase 5's intents as their steps: a deposit's custody
steps, `set_leverage`'s agent approval and cross leverage, an `Ioc` entry, the
reduce-only exit and the withdrawal, each request resolved by the model process. Every
outcome is committed with the runtime's own commit functions and admitted by the
gateway's own verification, as the offline adapter test does.

The HyperEVM side is authentic fork evidence. Every HyperCore effect comes from the
synthesized model, so nothing here is evidence of HyperCore's behaviour, Fork
Qualification or promotion evidence.
"""

import json
import time
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from almanak import external_effects
from almanak.caip import AssetType, ChainId
from almanak.chains import HYPEREVM
from almanak.connectors.hyperliquid import corewriter as cw
from almanak.connectors.hyperliquid.authority import declared
from almanak.connectors.hyperliquid.external import ExternalEffects
from almanak.connectors.hyperliquid.fork import ForkConfig
from almanak.connectors.hyperliquid.fork_model import REGISTER
from almanak.connectors.hyperliquid.fork_venue import ModelKeeper
from almanak.connectors.hyperliquid.lifecycle import Lifecycle
from almanak.connectors.hyperliquid.markets import find_market, perp_markets
from almanak.connectors.hyperliquid.models import (
    Allocation,
    ClosePosition,
    Deposit,
    IncreasePosition,
    SetLeverage,
    Withdraw,
)
from almanak.connectors.hyperliquid.requests import Requests
from almanak.connectors.hyperliquid.resolution import perp_market
from almanak.connectors.hyperliquid.state import (
    CustodyOutcome,
    DecisionRecord,
    TransactionPlan,
    VenuePlan,
)
from almanak.gateway import admission
from almanak.gateway import external_effects as gateway_effects
from almanak.gateway import requests as gateway_requests
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.fork_config import (
    AlchemyProvider,
    ForkAssetFunding,
    ForkOperator,
    ForkSafeFund,
    ForkTarget,
)
from almanak.gateway.fork_permissions import prepare_permissions
from almanak.gateway.fork_workspace import ForkSet, fork_routes
from almanak.gateway.hypercore_gateway import HyperCoreForkGateway
from almanak.gateway.hypercore_info import (
    HyperCoreInfo,
    InfoRequest,
    L2Book,
    MetaAndAssetCtxs,
    typed,
)
from almanak.gateway.hypercore_shapes import RESPONSES
from almanak.gateway.providers import provider_endpoint
from almanak.models import AssetAmount, Chain
from almanak.request_runtime import commit_request
from almanak.runtime import commit_outcome
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import IntervalEvent, IntervalTrigger

pytestmark = pytest.mark.fork

ROOT = Path(__file__).resolve().parents[2]
# The HyperEVM reference's identified anchor: a 3,000,000-gas small block.
ANCHOR = (49017682, "0xd6e25a1a30734e9231ff1256c22710f7574fbc563202c8e97b6668babd5da229")
MAINNET = Chain(chain_id=ChainId("eip155:999"), environment="mainnet")
PROVIDER = AlchemyProvider(
    kind="alchemy", network="hyperliquid-mainnet", api_key_env="ALCHEMY_API_KEY"
)
USDC = AssetType("eip155:999/erc20:" + cw.USDC_CONTRACT)
CAPITAL = 30_000_000
LEVERAGE = 10
# The entry's notional in USDC: a tenth of it is margin at the configured leverage.
NOTIONAL = Decimal(100)
STRATEGY = "def decide(context):\n    return None\n"


def fork_config(origin, market) -> ForkConfig:
    return ForkConfig(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        forks=(ForkTarget(chain=MAINNET, provider=PROVIDER, block=origin),),
        funds=(
            ForkSafeFund(
                account="safe",
                operator="operator",
                assets=(
                    ForkAssetFunding(
                        balance=AssetAmount(asset=USDC, amount=CAPITAL), method="local_storage"
                    ),
                ),
            ),
        ),
        operators=(
            ForkOperator(
                name="operator",
                native_balance=AssetAmount(asset=HYPEREVM.native_asset, amount=5 * 10**18),
                foundry_account_index=0,
            ),
        ),
        allocations=(
            Allocation(
                name="perp",
                account="safe",
                chain=MAINNET,
                market=market,
                capital=AssetAmount(asset=USDC, amount=CAPITAL),
                leverage=LEVERAGE,
                slippage_bps=50,
            ),
        ),
    )


def anchored(env_file):
    with EthereumReader(MAINNET, provider_endpoint(PROVIDER, env_file)) as reader:
        origin = reader.pin_block(ANCHOR[0])
    assert origin.hash == ANCHOR[1]
    return origin


def resolved_market():
    """BTC as resolution pins it: from mainnet `metaAndAssetCtxs`, read now."""
    with HyperCoreInfo() as info:
        answer = info.fetch(InfoRequest(info=MetaAndAssetCtxs()))
    contexts = RESPONSES["metaAndAssetCtxs"].validate_json(json.dumps(answer.result))
    return perp_market(find_market(perp_markets(contexts), "BTC"))


class Prepared:
    """A stand-in for the frozen preparation: the configuration declares the capabilities."""

    def __init__(self, deployment):
        self.capabilities = declared(deployment)

    def require_configuration(self, config, strategy):
        return None


@pytest.fixture(scope="module")
def workspace(request, tmp_path_factory):
    env_file = request.config.getoption("--fork-env")
    config = fork_config(anchored(env_file), resolved_market())
    directory = tmp_path_factory.mktemp("hyperliquid") / "deployment"
    with ForkSet(config, env_file) as nodes:
        deployment = nodes.prepare(config, directory)
        source = directory.parent / "strategy.py"
        source.write_text(STRATEGY)
        manifest = prepare_permissions(
            nodes, deployment, source, directory, prepared=Prepared(deployment)
        )
        router = fork_routes(
            deployment, directory / "fork-routes.json", directory / "fork-keys.json"
        )
        try:
            gateway = router.for_chain(deployment.allocations[0].chain)
            yield SimpleNamespace(
                nodes=nodes,
                directory=directory,
                deployment=deployment,
                manifest=manifest,
                gateway=gateway,
                strategy=StrategyDefinition.read(source),
                route=json.loads((directory / "fork-routes.json").read_text()),
                report={"register": REGISTER, "anchor": {"block": ANCHOR[0], "hash": ANCHOR[1]}},
            )
        finally:
            router.close()


# The runtime's steps, as the coordinator and the gateway service take them.


def now(world) -> int:
    return world.gateway.fork_clock()


def target(world):
    return world.deployment.target("perp")


def initial(world):
    state = Lifecycle.initial(
        world.deployment, world.strategy, declared(world.deployment), world.manifest
    )
    return state.model_copy(update={"initialized": True})


def observed(world, state):
    (snapshot,) = world.gateway.observe(state)
    return snapshot


def decide(world, state, intent):
    started = datetime.fromtimestamp(now(world) / 1000, UTC)
    record = DecisionRecord(
        trigger=IntervalEvent(started_at=started, due_at=started),
        identity=uuid4().hex,
        intent=intent,
        observations=(observed(world, state),),
        memory=Memory(),
    )
    return state.model_copy(update={"decision": record})


def delivered(state):
    return state.model_copy(update={"delivered": len(state.outcomes)})


def planned(world, state):
    before = observed(world, state)
    intent = state.decision.intent
    plan = world.gateway.prepare(state, intent, before, uuid4().hex)
    assert world.gateway.prepare(state, intent, before, plan.attempt_id) == plan
    return plan


def signed(world, state, plan):
    reserved = state.model_copy(update={"plan": plan})
    pending = world.gateway.sign(reserved)
    assert world.gateway.verify_signed_admission(reserved, pending)
    return state.model_copy(update={"pending": pending})


def sent(world, state):
    """The service's send: its checks, never-sent durably cleared, then the submission."""
    world.gateway.prepare_submission(state)
    pending = state.pending.model_copy(update={"unsent": False})
    state, _ = world.gateway.submit(state.model_copy(update={"pending": pending}))
    return state


def settled(world, state):
    pending = state.pending
    receipt = world.gateway.receipt(pending)
    deadline = time.monotonic() + 30
    while receipt is None and time.monotonic() < deadline:
        time.sleep(0.2)
        receipt = world.gateway.receipt(pending)
    assert receipt is not None
    outcome = world.gateway.reconcile(pending, receipt)
    after = commit_outcome(state, pending, outcome)
    admission.validate_outcome(world.gateway, state, after)
    return delivered(after), outcome


def custody_step(world, state, kind):
    plan = planned(world, state)
    assert isinstance(plan, TransactionPlan) and plan.operation.kind == kind
    state, outcome = settled(world, sent(world, signed(world, state, plan)))
    assert outcome.success and outcome.operation.kind == kind
    if kind != "hyperliquid_approve":
        state, outcome = resolved(world, state)
    return state, outcome


def resolved(world, state):
    """The custody request's outcome once the model process identifies its effect."""
    deadline = time.monotonic() + 30
    outcome = Requests.reconcile(world.gateway, state)
    while outcome is None and time.monotonic() < deadline:
        time.sleep(0.5)
        ModelKeeper(world.route[0]["venues"]["hypercore"]["model"]).settle(state)
        outcome = Requests.reconcile(world.gateway, state)
    assert isinstance(outcome, CustodyOutcome)
    after = commit_request(state, state, outcome)
    gateway_requests.verified_commit(world.gateway, state, after)
    return delivered(after), outcome


def venue_step(world, state, purpose):
    plan = planned(world, state)
    assert isinstance(plan, VenuePlan) and plan.operation.attempt.purpose == purpose
    state = sent(world, signed(world, state, plan))
    while state.pending.resolved is None:
        state, posted = world.gateway.submit(state)
        assert not posted
    state, outcome = settled(world, state)
    assert outcome.success and outcome.attempt.status == "applied"
    return state, outcome


def effects(world, state):
    page = ExternalEffects.reconcile(world.gateway, state)
    if page is None:
        return state
    after = external_effects.commit(state, state, page)
    gateway_effects.verified_commit(world.gateway, state, after)
    return delivered(after)


def control(world, op):
    return ModelKeeper(world.route[0]["venues"]["hypercore"]["model"]).control({"op": op})


# The lifecycle, one step function each: fork tests run outside CI, so each stays simple
# enough for the CRAP gate at zero coverage.


def route_names_the_model(world, state):
    (route,) = world.route
    venue = route["venues"]["hypercore"]
    keys = Path(venue["agent_keys"])
    assert keys.read_text().strip() == "{}" and keys.stat().st_mode & 0o077 == 0
    assert isinstance(world.gateway, HyperCoreForkGateway)
    model = control(world, "state")["model"]
    assert model["register"] == REGISTER and not model["accounts"]
    assert model["seed"]["source"]["anchor"]["number"] == ANCHOR[0]
    world.gateway.require_empty_venue(state)
    world.report["seed"] = model["seed"]["source"]
    return state


def deposited(world, state):
    state = decide(world, state, Deposit(target=target(world), amount=CAPITAL))
    for kind in (
        "hyperliquid_approve",
        "hyperliquid_deposit",
        "hyperliquid_standard_mode",
        "hyperliquid_class_transfer",
    ):
        state, outcome = custody_step(world, state, kind)
    assert outcome.complete and state.decision is None
    world.report["deposit"] = [r.model_dump(mode="json") for r in state.custody.requests]
    return effects(world, state)


def hype_funded(world, state):
    """The owners' HyperCore HYPE, sent by the process once the deposit activated the
    account, before any withdrawal needs it."""
    deadline = time.monotonic() + 30
    fixture = control(world, "state")["model"]["fixture"]["hypercore_hype"]
    while fixture["status"] != "sent" and time.monotonic() < deadline:
        time.sleep(0.5)
        fixture = control(world, "state")["model"]["fixture"]["hypercore_hype"]
    assert fixture["status"] == "sent"
    world.report["hypercore_hype"] = fixture
    return state


def leveraged(world, state):
    """`set_leverage`: the first agent's approval, then the configured cross leverage."""
    state = decide(world, state, SetLeverage(target=target(world), leverage=LEVERAGE))
    state, approval = custody_step(world, state, "hyperliquid_agent_approval")
    (agent,) = state.custody.agents.approvals
    assert agent.status == "active" and state.decision is not None
    state, leverage = venue_step(world, state, "leverage")
    assert leverage.complete and state.decision is None
    world.report["leverage"] = {
        "agent": agent.model_dump(mode="json"),
        "leverage": leverage.attempt.model_dump(mode="json"),
    }
    return state


def entered(world, state):
    book = world.gateway.venue.fetch(InfoRequest(info=L2Book(coin="BTC")))
    mark = Decimal(typed(book).levels[1][0].px)
    places = Decimal(1).scaleb(-target(world).market.size_decimals)
    size = (NOTIONAL / mark).quantize(places, ROUND_DOWN)
    intent = IncreasePosition(target=target(world), side="long", size=str(size))
    state = decide(world, state, intent)
    # The leverage already applies, so the entry is the intent's only step.
    state, entry = venue_step(world, state, "entry")
    assert entry.complete and state.decision is None
    state = effects(world, state)
    assert state.owned[0].position == size
    world.report["entry"] = entry.attempt.model_dump(mode="json")
    return state


def exited(world, state):
    state = decide(world, state, ClosePosition(target=target(world)))
    state, exit_order = venue_step(world, state, "exit")
    assert exit_order.attempt.request.action.order.reduce_only and exit_order.complete
    state = effects(world, state)
    assert state.owned[0].position == 0
    world.report["exit"] = exit_order.attempt.model_dump(mode="json")
    return state


def withdrawn(world, state):
    state = decide(world, state, Withdraw(target=target(world)))
    state, _ = custody_step(world, state, "hyperliquid_class_transfer")
    state, outcome = custody_step(world, state, "hyperliquid_withdrawal")
    assert outcome.complete and state.decision is None
    (paid,) = outcome.assets
    assert paid.amount > 0
    state = effects(world, state)
    world.gateway.require_empty_venue(state.model_copy(update={"mode": "closed"}))
    world.report["withdrawal"] = state.custody.requests[-1].model_dump(mode="json")
    return state


def test_workspace_model_process_serves_the_gateway_through_a_lifecycle(workspace):
    state = initial(workspace)
    steps = (route_names_the_model, deposited, hype_funded, leveraged, entered, exited, withdrawn)
    for step in steps:
        state = step(workspace, state)
    workspace.report["model"] = control(workspace, "state")["model"]
    path = ROOT / "artifacts/hyperliquid-workspace-fork/report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(workspace.report, indent=1, default=str) + "\n")
