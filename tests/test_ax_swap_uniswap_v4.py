"""`almanak ax swap` for Uniswap V4: a direct exact-input swap in one pool named by its key.

Component evidence without signing or chain access: the ERC-20 pool is resolved from
its retained resolution reads, and observations are built over it. The fork rehearsal
of the whole command is tests/fork/test_ax_swap_uniswap_v4.py.
"""

import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.connectors.support import RunMode, uniswap_candidate_entry, uniswap_support
from almanak.connectors.uniswap_v4 import planning as v4_planning
from almanak.connectors.uniswap_v4 import reconciliation
from almanak.connectors.uniswap_v4.domain import PoolObservation, UniswapPool
from almanak.connectors.uniswap_v4.models import Target
from almanak.connectors.uniswap_v4.networks import roles_for
from almanak.connectors.uniswap_v4.reader import Quote
from almanak.connectors.uniswap_v4.resolution import MarketSelection, resolve_market
from almanak.connectors.uniswap_v4.state import Snapshot, Swap
from almanak.dex_intents import ExitExchange, RevokeExchange
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import ZERO_ADDRESS, AssetAmount, Block
from almanak.operator_deployment import supports_operator
from almanak.strategy import StrategyDefinition
from tests import test_ax_swap as family

FIXTURES = Path(__file__).with_name("fixtures")
READS = FIXTURES / "uniswap-v4-ethereum-resolution-reads.json.gz"
USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
NATIVE = "eip155:1/slip44:60"
OTHER = "eip155:1/erc20:0x" + "ab" * 20
KEY = {"currency0": USDC, "currency1": WETH, "fee": 500, "tick_spacing": 10}
# An address with the beforeSwap bit set that no reviewed profile lists.
UNREVIEWED_HOOK = "0x" + "00" * 18 + "0080"
# SHA-256 of a strategy checkpoint's JSON, taken before the checkpoint learned of
# operator deployments. The checkpoint is `strategy_checkpoint` below.
STRATEGY_WIRE = "1d5d153b0821f865a4763d275043d1c2025726ce078404205a57ad5266302f49"
WALLET = Wallet(account="fund", address="0x" + "12" * 20)
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=7, max_transaction_fee=9)
AMOUNT, QUOTED = 1000, 4321
CONNECTOR = "uniswap_v4"


def retained():
    raw = json.loads(gzip.decompress(READS.read_bytes()))
    block = Block.model_validate_json(json.dumps(raw["block"]))
    rows = tuple(ResolutionRead.model_validate_json(json.dumps(row)) for row in raw["evidence"])
    return block, rows


def selection(**changes):
    values = {"connector": CONNECTOR, "chain": "eip155:1", **KEY, **changes}
    return MarketSelection.model_validate_json(json.dumps(values))


def resolved():
    block, rows = retained()
    return resolve_market(RetainedResolutionReader(block.chain, rows), selection(), block)


def configuration(positions, market):
    chain = market.pool.chain
    provider = ax_actions.alchemy(chain.chain_id)
    return ax_actions.configuration(CONNECTOR, positions, WALLET, provider, chain, LIMITS)


def stated_allowances(target, token, permit):
    """Every allowance an observation reads, all zero but the sold asset's two."""
    sold, contracts = target.funding_currency, target.contracts
    rows = []
    for asset in target.assets:
        granted = token if asset == sold else 0
        rows.append({"asset": asset, "spender": contracts.permit2, "amount": granted})
        for spender in (contracts.router, contracts.position_manager):
            mine = (asset, spender) == (sold, contracts.router)
            amount, expiration = permit if mine else (0, 0)
            rows.append(
                {
                    "asset": asset,
                    "spender": spender,
                    "amount": amount,
                    "expiration": expiration,
                    "nonce": 0,
                }
            )
    return tuple(rows)


def observation(config, owned, market, *, wallet=5000, bought=0, token=0, permit=(0, 0), **quotes):
    """An on-chain observation of the deployment's one position at the resolved block.

    `token` is the sold asset's ERC-20 allowance to Permit2; `permit` is the amount and
    expiration of its Permit2 allowance to the router.
    """
    target = config.target(ALLOCATION)
    pool = PoolObservation(
        source=market.pool,
        block=market.block,
        currency0=market.asset0,
        currency1=market.asset1,
        sqrt_price_x96=2**96,
        tick=0,
        liquidity=10**18,
        protocol_fee=0,
        lp_fee=market.pool.fee,
    )
    held = {
        asset: wallet if asset == target.funding_currency else bought for asset in target.assets
    }
    return Snapshot(
        target=target,
        pool=pool,
        owned=owned,
        wallet0=AssetAmount(asset=target.assets[0], amount=held[target.assets[0]]),
        wallet1=AssetAmount(asset=target.assets[1], amount=held[target.assets[1]]),
        allowances=stated_allowances(target, token, permit),
        native_balance=AssetAmount(
            asset=evm_network(market.pool.chain.chain_id).native_asset, amount=10**18
        ),
        payer=target.fund,
        **quotes,
    )


def created(config, strategy=None):
    found = connector_from_wire({"connector": CONNECTOR})
    capabilities = found.codec.capability.declared(config)
    return found.lifecycle.initial(config, strategy, capabilities, None)


def strategy_checkpoint():
    """A strategy deployment after its start, built from the retained resolution only."""
    market = resolved()
    document = {
        "allocations": [
            {
                "name": "position",
                "account": "fund",
                "pool": market.pool.model_dump(mode="json"),
                "contracts": market.contracts.model_dump(mode="json"),
                "capital": {"asset": USDC, "amount": AMOUNT},
                "slippage_bps": 50,
                "deadline_seconds": 300,
                "native_asset": str(evm_network(market.pool.chain.chain_id).native_asset),
                "minimum_native_balance": 0,
            }
        ]
    }
    config = configuration(document, market)
    strategy = StrategyDefinition(filename="strategy.py", source="def decide(context):\n    pass\n")
    state = created(config, strategy)
    seen = observation(config, state.owned[0], market)
    return state.model_copy(
        update={"deployment_id": "0" * 32, "initialized": True, "baseline": (seen,)}
    )


def request(*, sell=USDC, amount=AMOUNT, slippage_bps=50):
    return ActionRequest("swap", amount, sell=sell, slippage_bps=slippage_bps, deadline_seconds=300)


def operator_config(market, **stated):
    positions = direct.binding(CONNECTOR).positions(None, None, market, WALLET, request(**stated))
    return configuration(positions, market)


def quote(market, amount=AMOUNT, output=QUOTED):
    return Quote(
        block=market.block,
        amount=AssetAmount(asset=AssetType(USDC), amount=amount),
        output=AssetAmount(asset=AssetType(WETH), amount=output),
        quoter_gas=1,
    )


def native_market():
    """The Ethereum pool of the native coin and USDC on the reviewed deployment."""
    block, _ = retained()
    roles = roles_for(block.chain.chain_id)
    pool = UniswapPool(
        kind="uniswap_v4",
        chain=block.chain,
        pool_manager=roles.pool_manager,
        state_view=roles.state_view,
        currency0=AssetType(NATIVE),
        currency1=AssetType(USDC),
        fee=3000,
        tick_spacing=60,
        hooks=ZERO_ADDRESS,
    )
    return SimpleNamespace(pool=pool, contracts=roles.contracts, block=block)


def test_the_connector_supports_operator_deployments_and_serves_the_swap():
    found = connector_from_wire({"connector": CONNECTOR})
    assert supports_operator(found.codec.checkpoint)
    assert direct.binding(CONNECTOR).actions == ("swap",)
    assert CONNECTOR in direct.connectors_for("swap") and CONNECTOR not in direct.NOT_DIRECT
    production = [cell for cell in found.chain_support if RunMode.PRODUCTION in cell.modes]
    assert len(production) == 8
    for cell in production:
        assert [str(action) for action in cell.direct_actions] == ["swap_exact_input"]
        direct.require_direct(CONNECTOR, cell.chain, direct.ActionRequest("swap", 5))
        with pytest.raises(ValueError, match="unsupported as a direct action"):
            cell.require_direct("exit_exchange")
    with pytest.raises(ValueError, match="uniswap_v4 serves no direct swap on eip155:31337"):
        direct.require_direct(CONNECTOR, ChainId("eip155:31337"), direct.ActionRequest("swap", 5))


def test_a_candidate_cell_serves_no_direct_action():
    template = next(cell for cell in uniswap_support() if cell.direct_actions)
    start = template.historical[0].backtest_start_at
    candidate = uniswap_candidate_entry(template, "eip155:59144", start)
    assert candidate.direct_actions == () and RunMode.PRODUCTION not in candidate.modes


def test_a_strategy_checkpoint_keeps_its_wire_form():
    state = strategy_checkpoint()
    body = state.model_dump_json()
    assert hashlib.sha256(body.encode()).hexdigest() == STRATEGY_WIRE
    assert "operator" not in json.loads(body) and state.operator is False
    restored = type(state).model_validate_json(body)
    assert restored == state and restored.model_dump_json() == body


def test_an_operator_checkpoint_retains_no_strategy_and_round_trips():
    state = created(operator_config(resolved()))
    assert (state.operator, state.strategy, state.initialized) == (True, None, False)
    body = state.model_dump_json()
    wire = json.loads(body)
    assert (wire["operator"], wire["strategy"]) == (True, None)
    assert type(state).model_validate_json(body) == state
    strategy = strategy_checkpoint().strategy
    with pytest.raises(ValueError, match="an operator deployment retains no strategy"):
        type(state).model_validate_json(json.dumps({**wire, "strategy": strategy.model_dump()}))
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        type(state).model_validate_json(json.dumps({**wire, "operator": False}))
    report = connector_from_wire({"connector": CONNECTOR}).projection.report(state)
    assert report["strategy"] is None and report["connector"] == CONNECTOR


@pytest.mark.parametrize("sold", [USDC, WETH])
def test_the_deployment_is_one_trading_allocation_funded_with_the_amount_sold(sold):
    market = resolved()
    config = operator_config(market, sell=sold, amount=77, slippage_bps=25)
    (allocation,) = config.allocations
    assert (allocation.name, allocation.account) == (ALLOCATION, WALLET.account)
    assert (allocation.pool, allocation.contracts) == (market.pool, market.contracts)
    assert (str(allocation.capital.asset), allocation.capital.amount) == (sold, 77)
    assert (allocation.entry_swap_bps, allocation.range, allocation.band) == (10000, None, None)
    assert (allocation.slippage_bps, allocation.deadline_seconds) == (25, 300)
    target = config.target(ALLOCATION)
    assert str(target.funding_currency) == sold and target.fund.address == WALLET.address
    assert direct.binding_for(config) is direct.binding(CONNECTOR)
    state = created(config)
    assert [item.target for item in state.capabilities.requirements] == [target]
    held = {str(v.asset): v.amount for v in (state.owned[0].inventory0, state.owned[0].inventory1)}
    assert held == {sold: 77, (WETH if sold == USDC else USDC): 0}


def test_a_native_pool_is_sold_and_held_as_its_wrapped_token():
    market = native_market()
    positions = direct.binding(CONNECTOR).positions
    config = configuration(positions(None, None, market, WALLET, request(sell=WETH)), market)
    target = config.target(ALLOCATION)
    # The key keeps the native coin; the wallet's two tokens are the wrapped one and USDC.
    assert (str(target.pool.currency0), target.native_index) == (NATIVE, 0)
    assert [str(asset) for asset in target.assets] == [WETH, USDC]
    assert str(config.allocations[0].capital.asset) == WETH
    held = created(config).owned[0]
    assert (str(held.inventory0.asset), held.inventory0.amount) == (WETH, AMOUNT)
    with pytest.raises(ValueError, match=f"sells as its wrapped token: name {WETH} or {USDC}"):
        positions(None, None, market, WALLET, request(sell=NATIVE))
    with pytest.raises(ValueError, match="one of the two tokens the wallet holds for this pool"):
        positions(None, None, market, WALLET, request(sell=OTHER))


def test_the_sold_asset_must_be_held_for_the_pool_and_the_request_complete():
    market = resolved()
    positions = direct.binding(CONNECTOR).positions
    for sold in (NATIVE, OTHER):
        with pytest.raises(ValueError, match=f"wallet holds for this pool, {USDC} or {WETH}"):
            positions(None, None, market, WALLET, request(sell=sold))
    with pytest.raises(ValueError, match="swap requires --amount, --slippage-bps"):
        positions(None, None, market, WALLET, ActionRequest("swap", AMOUNT, sell=USDC))


class Head(RetainedResolutionReader):
    """The retained reads, with the retained block as the chain's head."""

    def pin_block(self, selector):
        block, _ = retained()
        return block if selector == "latest" else super().pin_block(selector)


def deployment(monkeypatch, market, rows):
    monkeypatch.setattr(
        "almanak.gateway.ethereum.EthereumReader", lambda chain, endpoint: Head(chain, rows)
    )
    wallet = {"account": WALLET.account, "address": WALLET.address}
    provider = ax_actions.alchemy(ChainId("eip155:1"))
    return ax_actions.deployment(CONNECTOR, market, request(), wallet, provider, "endpoint", LIMITS)


def test_the_command_resolves_the_pool_key_through_the_connectors_resolver(monkeypatch):
    _, rows = retained()
    assert deployment(monkeypatch, selection(), rows) == operator_config(resolved())


def test_an_unreviewed_hook_is_refused_by_the_resolver_before_any_read(monkeypatch):
    # No read is retained, so a refusal of any other kind would mean the resolver read.
    with pytest.raises(DataError) as refusal:
        deployment(monkeypatch, selection(hooks=UNREVIEWED_HOOK), ())
    assert refusal.value.kind == "unsupported_pool" and "hook" in str(refusal.value)


def test_adoption_keeps_the_stated_inventory_only_when_the_wallet_covers_it():
    market = resolved()
    config = operator_config(market)
    found = connector_from_wire({"connector": CONNECTOR})
    owned = created(config).owned
    covered = observation(config, owned[0], market, wallet=AMOUNT)
    assert found.lifecycle.adopt(owned, (covered,)) == owned
    short = observation(config, owned[0], market, wallet=AMOUNT - 1)
    with pytest.raises(DataError, match=f"the wallet holds {AMOUNT - 1} base units") as refusal:
        found.lifecycle.adopt(owned, (short,))
    assert refusal.value.kind == "inventory" and USDC in str(refusal.value)


def admitted(**observed):
    """A deployment whose request is being admitted with one observation."""
    market = resolved()
    config = operator_config(market)
    state = created(config)
    seen = observation(config, state.owned[0], market, entry_quote=quote(market), **observed)
    return market, config, state, seen


def test_the_intent_follows_the_quote_of_the_observation_it_is_admitted_with():
    market, config, state, seen = admitted()
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request())
    assert intent.kind == "swap_exact_input" and intent.target == config.target(ALLOCATION)
    assert intent.amount == AssetAmount(asset=AssetType(USDC), amount=AMOUNT)
    # 50 basis points below the quoted output, rounded down.
    assert intent.minimum_output == AssetAmount(
        asset=AssetType(WETH), amount=QUOTED * 9950 // 10000
    )
    assert intent.deadline == market.block.timestamp + 300
    found = connector_from_wire({"connector": CONNECTOR})
    found.codec.capability.admit(state.capabilities, intent)
    assert found.lifecycle.increases_exposure(intent)


def test_a_missing_or_differently_sized_quote_builds_no_intent():
    market, config, state, seen = admitted()
    build = direct.binding(CONNECTOR).intent
    bare = observation(config, state.owned[0], market)
    with pytest.raises(ValueError, match="the pool quotes no output for the stated amount"):
        build(config, (bare,), request())
    unmeasured = observation(
        config, state.owned[0], market, entry_quote_error="entry quote unavailable: reverted"
    )
    with pytest.raises(ValueError, match="entry quote unavailable: reverted"):
        build(config, (unmeasured,), request())
    for other in (request(amount=AMOUNT + 1), request(sell=WETH)):
        with pytest.raises(ValueError, match="not for the stated amount of the sold asset"):
            build(config, (seen,), other)


def planning(config, state, intent, seen):
    """The planned operation of one observation, checked as the gateway checks it."""
    execution = connector_from_wire({"connector": CONNECTOR}).execution
    deciding = SimpleNamespace(
        config=config,
        owned=state.owned,
        capabilities=state.capabilities,
        decision=None,
        outcomes=(),
        mode="running",
    )

    def step(before, admitted=intent):
        deciding.decision = SimpleNamespace(intent=admitted, observations=(seen,))
        operation = execution.operation(config, deciding, admitted, before)
        execution.validate_operation(None, deciding, operation, before)
        return operation

    return step


def test_the_gateway_admits_the_two_exact_approvals_then_the_swap():
    market, config, state, seen = admitted()
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request())
    step = planning(config, state, intent, seen)
    owned, contracts = state.owned[0], config.target(ALLOCATION).contracts

    first = step(seen)
    assert (first.kind, first.spender, first.amount) == ("approve_erc20", contracts.permit2, AMOUNT)
    assert first.expiration is None
    second = step(observation(config, owned, market, token=AMOUNT))
    assert (second.kind, second.spender) == ("approve_permit2", contracts.router)
    # The Permit2 allowance is the stated amount and lapses with the swap it serves.
    assert (second.amount, second.expiration) == (AMOUNT, market.block.timestamp + 300)
    ready = observation(config, owned, market, token=AMOUNT, permit=(AMOUNT, intent.deadline))
    swap = step(ready)
    assert (swap.kind, swap.amount, swap.minimum) == ("swap", intent.amount, intent.minimum_output)


def test_allowances_that_cover_the_amount_are_used_and_lapsed_ones_renewed():
    market, config, state, seen = admitted()
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request())
    step = planning(config, state, intent, seen)
    owned = state.owned[0]
    # An unlimited ERC-20 approval to Permit2 needs no approval of its own.
    unlimited = observation(config, owned, market, token=2**256 - 1)
    assert step(unlimited).kind == "approve_permit2"
    # A Permit2 allowance that expired at the observed block is granted again.
    lapsed = observation(
        config, owned, market, token=AMOUNT, permit=(AMOUNT, market.block.timestamp)
    )
    assert step(lapsed).kind == "approve_permit2"
    larger = observation(
        config, owned, market, token=2**256 - 1, permit=(AMOUNT * 2, intent.deadline + 9)
    )
    assert step(larger).kind == "swap"


def test_nothing_larger_or_looser_than_the_stated_swap_is_admitted():
    market, config, state, seen = admitted()
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request())
    step = planning(config, state, intent, seen)
    ready = observation(
        config, state.owned[0], market, token=AMOUNT * 2, permit=(AMOUNT * 2, intent.deadline)
    )
    larger = intent.model_copy(
        update={"amount": AssetAmount(asset=AssetType(USDC), amount=AMOUNT + 1)}
    )
    with pytest.raises(DataError, match="swap exceeds the attributed input inventory"):
        step(ready, larger)
    wider = intent.model_copy(
        update={"minimum_output": AssetAmount(asset=AssetType(WETH), amount=1)}
    )
    with pytest.raises(ValueError, match="widens the configured slippage limit"):
        step(ready, wider)


def test_the_position_is_the_attributed_inventory_and_the_allowances_last_observed():
    market, config, state, seen = admitted()
    report = direct.binding(CONNECTOR).position
    before = [{"asset": USDC, "amount": str(AMOUNT)}, {"asset": WETH, "amount": "0"}]
    assert report(state) == {"inventory": before, "allowances": []}
    started = state.model_copy(update={"initialized": True, "baseline": (seen,)})
    assert report(started) == {"inventory": before, "allowances": []}

    # After mined approvals whose swap never followed, both are reported; only the
    # Permit2 allowance carries an expiration.
    owned, contracts = state.owned[0], config.target(ALLOCATION).contracts
    approved = observation(config, owned, market, token=AMOUNT, permit=(AMOUNT, 1234))
    stopped = SimpleNamespace(
        owned=state.owned, baseline=(seen,), outcomes=(SimpleNamespace(after=approved),)
    )
    assert report(stopped)["allowances"] == [
        {"asset": USDC, "spender": contracts.permit2, "amount": str(AMOUNT)},
        {"asset": USDC, "spender": contracts.router, "amount": str(AMOUNT), "expiration": 1234},
    ]
    # A spent Permit2 allowance keeps its expiration with no amount; it is not listed.
    spent = observation(config, owned, market, permit=(0, 1234))
    done = SimpleNamespace(owned=state.owned, baseline=(), outcomes=(SimpleNamespace(after=spent),))
    assert report(done)["allowances"] == []


COMMON = {
    "--connector": CONNECTOR,
    "--chain": "eip155:1",
    "--currency0": USDC,
    "--currency1": WETH,
    "--fee": "500",
    "--tick-spacing": "10",
    "--sell": USDC,
    "--amount": "1000",
    "--slippage-bps": "50",
    "--wallet": "wallet",
    "--env-file": ".env",
    "--max-gas-price-wei": "7",
    "--max-transaction-fee": "9",
}
KEY_FLAGS = ("--currency0", "--currency1", "--fee", "--tick-spacing")


def arguments(*extra, without=(), **changed):
    stated = {**COMMON, **{"--" + key.replace("_", "-"): value for key, value in changed.items()}}
    flags = [
        part for name, value in stated.items() if name not in without for part in (name, value)
    ]
    return parse_validated(build_parser(), ["ax", "swap", *flags, *extra])


def test_the_command_line_names_the_pool_by_its_key():
    args = arguments()
    assert (args.product, args.ax_command, args.hooks) == ("operator-action", "swap", None)
    assert ax_actions.market_request(args) == selection()
    assert ax_actions.action_request(args) == request()
    assert ax_actions.market_request(arguments("--hooks", UNREVIEWED_HOOK)).hooks == UNREVIEWED_HOOK
    # A typed address is compared in the canonical spelling resolution reports.
    assert arguments(sell=USDC.replace("a0b8", "A0B8")).sell == USDC


def refused(capsys, *extra, **options):
    with pytest.raises(SystemExit):
        arguments(*extra, **options)
    return capsys.readouterr().err


def test_the_parser_refuses_a_pool_named_the_way_another_connector_names_it(capsys):
    keyed = "uniswap_v4 names its pool with --currency0, --currency1, --fee, --tick-spacing"
    pool = ("--pool", "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640")
    assert f"{keyed} and optionally --hooks; it takes no --pool" in refused(capsys, *pool)
    missing = refused(capsys, without=("--fee", "--currency1"))
    assert "and optionally --hooks; missing --currency1, --fee" in missing
    foreign = refused(capsys, connector="uniswap_v3")
    assert "uniswap_v3 names its pool with --pool; it takes no --currency0, --currency1," in foreign
    bare = refused(capsys, connector="uniswap_v3", without=KEY_FLAGS)
    assert "uniswap_v3 names its pool with --pool; missing --pool" in bare
    assert "expected a CAIP-19 asset type" in refused(capsys, sell="usdc")


def test_the_command_refuses_an_undeclared_chain_and_passes_the_stated_swap_on(monkeypatch):
    seen = {}

    def deployment(name, market, action, wallet, provider, endpoint, limits, **options):
        seen.update(connector=name, market=market, action=action)
        raise StopIteration

    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_actions, "deployment", deployment)
    with pytest.raises(StopIteration):
        ax_actions.command(arguments())
    assert (seen["connector"], seen["market"], seen["action"]) == (
        CONNECTOR,
        selection(),
        request(),
    )

    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    elsewhere = {
        name: value.replace("eip155:1/", "eip155:59144/")
        for name, value in (("currency0", USDC), ("currency1", WETH), ("sell", USDC))
    }
    with pytest.raises(ValueError, match="uniswap_v4 serves no direct swap on eip155:59144"):
        ax_actions.command(arguments(chain="eip155:59144", **elsewhere))


UNLIMITED_TOKEN, UNLIMITED_PERMIT = 2**256 - 1, 2**160 - 1
EXPIRES = 2**48 - 1


def mined_swap(before_token, before_permit, after_token, after_permit, *, success=True):
    """Run the connector's own balance reconciliation over a mined swap of AMOUNT.

    The sold token's ERC-20 allowance to Permit2 and its Permit2 allowance to the router
    are stated before and after; wallets and inventory move as the swap moved them.
    """
    market, config, state, _ = admitted()
    owned, target = state.owned[0], config.target(ALLOCATION)
    before = observation(config, owned, market, token=before_token, permit=(before_permit, EXPIRES))
    moved = (-AMOUNT, QUOTED) if success else (0, 0)
    swapped = owned.model_copy(
        update={
            "inventory0": AssetAmount(asset=AssetType(USDC), amount=AMOUNT + moved[0]),
            "inventory1": AssetAmount(asset=AssetType(WETH), amount=moved[1]),
            "entry_complete": success,
        }
    )
    after = observation(
        config,
        swapped,
        market,
        wallet=5000 + moved[0],
        bought=moved[1],
        token=after_token,
        permit=(after_permit, EXPIRES),
    )
    operation = Swap(
        target=target,
        amount=AssetAmount(asset=AssetType(USDC), amount=AMOUNT),
        minimum=AssetAmount(asset=AssetType(WETH), amount=1),
        deadline=market.block.timestamp + 300,
    )
    pending = SimpleNamespace(before=before, operation=operation)
    spent = (moved[0], 0) if success else None
    return reconciliation.validate_balances(pending, after, moved, 0, success, spent)


def refused_reconciliation(*allowances, **options):
    with pytest.raises(DataError, match="ERC-20 or Permit2 allowance does not reconcile") as seen:
        mined_swap(*allowances, **options)
    return seen.value.kind == "receipt"


def test_a_finite_allowance_falls_by_exactly_what_the_swap_spent():
    assert mined_swap(AMOUNT, AMOUNT, 0, 0) == ()
    assert mined_swap(AMOUNT * 3, AMOUNT * 2, AMOUNT * 2, AMOUNT) == ()
    # Neither a finite ERC-20 allowance nor a finite Permit2 one may stay as it was,
    # fall by another amount, or grow.
    assert refused_reconciliation(AMOUNT * 3, AMOUNT * 2, AMOUNT * 3, AMOUNT)
    assert refused_reconciliation(AMOUNT * 3, AMOUNT * 2, AMOUNT * 2, AMOUNT * 2)
    assert refused_reconciliation(AMOUNT * 3, AMOUNT * 2, AMOUNT * 2 + 1, AMOUNT)
    assert refused_reconciliation(AMOUNT * 3, AMOUNT * 2, AMOUNT * 2, AMOUNT - 1)
    assert refused_reconciliation(AMOUNT, AMOUNT, AMOUNT * 2, 0)


def test_an_unlimited_permit2_allowance_is_unchanged_by_the_swap_it_serves():
    assert mined_swap(AMOUNT, UNLIMITED_PERMIT, 0, UNLIMITED_PERMIT) == ()
    # Permit2 never lowers its largest allowance, so a lowered one is not this swap's.
    assert refused_reconciliation(AMOUNT, UNLIMITED_PERMIT, 0, UNLIMITED_PERMIT - AMOUNT)
    # One base unit less than the largest is finite and falls like any other.
    assert mined_swap(AMOUNT, UNLIMITED_PERMIT - 1, 0, UNLIMITED_PERMIT - 1 - AMOUNT) == ()
    assert refused_reconciliation(AMOUNT, UNLIMITED_PERMIT - 1, 0, UNLIMITED_PERMIT - 1)


def test_an_unlimited_token_allowance_stays_or_falls_as_its_token_decides():
    # WETH9, USDT, DAI and OpenZeppelin tokens leave it; USDC lowers it.
    assert mined_swap(UNLIMITED_TOKEN, AMOUNT, UNLIMITED_TOKEN, 0) == ()
    assert mined_swap(UNLIMITED_TOKEN, AMOUNT, UNLIMITED_TOKEN - AMOUNT, 0) == ()
    assert refused_reconciliation(UNLIMITED_TOKEN, AMOUNT, UNLIMITED_TOKEN - AMOUNT + 1, 0)
    assert refused_reconciliation(UNLIMITED_TOKEN, AMOUNT, 0, 0)
    # Once lowered it is finite: the next swap must lower it by exactly its amount.
    lowered = UNLIMITED_TOKEN - AMOUNT
    assert mined_swap(lowered, AMOUNT, lowered - AMOUNT, 0) == ()
    assert refused_reconciliation(lowered, AMOUNT, lowered, 0)
    # Both unlimited at once, as a wallet that used the pool's own application has them.
    assert mined_swap(UNLIMITED_TOKEN, UNLIMITED_PERMIT, UNLIMITED_TOKEN, UNLIMITED_PERMIT) == ()


def test_a_reverted_swap_changes_no_allowance_and_a_modelled_step_assumes_no_decrement():
    assert mined_swap(UNLIMITED_TOKEN, AMOUNT, UNLIMITED_TOKEN, AMOUNT, success=False) == ()
    assert refused_reconciliation(
        UNLIMITED_TOKEN, AMOUNT, UNLIMITED_TOKEN - AMOUNT, 0, success=False
    )
    market, config, state, _ = admitted()
    before = observation(
        config, state.owned[0], market, token=UNLIMITED_TOKEN, permit=(UNLIMITED_PERMIT, EXPIRES)
    )
    operation = Swap(
        target=config.target(ALLOCATION),
        amount=AssetAmount(asset=AssetType(USDC), amount=AMOUNT),
        minimum=AssetAmount(asset=AssetType(WETH), amount=1),
        deadline=market.block.timestamp + 300,
    )
    modelled = reconciliation.allowance_after(before, operation, (-AMOUNT, QUOTED), 0)
    assert modelled == before.allowances


def test_a_standing_permit2_allowance_must_outlast_the_swap_it_serves():
    market, config, state, seen = admitted()
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request())
    step = planning(config, state, intent, seen)
    owned, now = state.owned[0], market.block.timestamp

    def standing(expiration):
        return observation(
            config, owned, market, token=UNLIMITED_TOKEN, permit=(UNLIMITED_PERMIT, expiration)
        )

    # Unexpired now, but lapsing before the swap's deadline: it is granted again.
    renewed = step(standing(now + 5))
    assert (renewed.kind, renewed.amount, renewed.expiration) == (
        "approve_permit2",
        AMOUNT,
        intent.deadline,
    )
    assert step(standing(intent.deadline - 1)).kind == "approve_permit2"
    assert step(standing(intent.deadline)).kind == "swap"
    # An exit leg takes its deadline from each observation, so the allowance its own
    # approval granted is relied on while it has not lapsed.
    asset, router = config.target(ALLOCATION).funding_currency, intent.target.contracts.router
    relied = v4_planning.approval(standing(now + 5), asset, AMOUNT, router, intent.deadline)
    assert relied is None
    lapsed = v4_planning.approval(standing(now), asset, AMOUNT, router, intent.deadline)
    assert lapsed.kind == "approve_permit2"


def test_an_operation_on_another_pool_is_refused_by_the_connectors_validation():
    market, config, state, seen = admitted()
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request())
    step = planning(config, state, intent, seen)
    ready = observation(
        config, state.owned[0], market, token=AMOUNT, permit=(AMOUNT, intent.deadline)
    )
    swap = step(ready)
    bound = config.target(ALLOCATION)
    other = bound.model_copy(update={"pool": bound.pool.model_copy(update={"fee": 3000})})
    execution = connector_from_wire({"connector": CONNECTOR}).execution
    deciding = SimpleNamespace(
        config=config,
        owned=state.owned,
        capabilities=state.capabilities,
        decision=SimpleNamespace(intent=intent, observations=(seen,)),
        outcomes=(),
        mode="running",
    )
    # The bound request cannot carry an operation of another pool.
    foreign = swap.model_copy(update={"target": other})
    with pytest.raises(ValueError, match="operation and observation require the bound target"):
        execution.validate_operation(None, deciding, foreign, ready)
    # A request for another pool has no capability at all.
    deciding.decision = SimpleNamespace(
        intent=intent.model_copy(update={"target": other}), observations=(seen,)
    )
    with pytest.raises(ValueError, match="intent has no retained Uniswap capability"):
        execution.validate_operation(None, deciding, foreign, ready)


def test_the_gateway_admits_the_swap_request_and_no_other_action(tmp_path):
    market, config, _, _ = admitted()
    desk = family.SwapDesk(
        tmp_path,
        config,
        lambda owned: observation(config, owned, market, entry_quote=quote(market)),
    )
    build = direct.binding(CONNECTOR).intent
    record = desk.runtime().submit(lambda state, observed: build(state.config, observed, request()))
    admitted_state = desk.store.load()
    assert (record.intent.kind, record.trigger.kind) == ("swap_exact_input", "operator")
    assert admitted_state.decision == record and admitted_state.initialized
    assert admitted_state.owned[0].inventory0.amount == AMOUNT


@pytest.mark.parametrize("refused", [RevokeExchange, ExitExchange])
def test_the_gateway_refuses_an_operator_request_that_is_not_the_swap(tmp_path, refused):
    market, config, _, _ = admitted()
    desk = family.SwapDesk(
        tmp_path,
        config,
        lambda owned: observation(config, owned, market, entry_quote=quote(market)),
    )
    other = refused[Target](target=config.target(ALLOCATION))
    with pytest.raises(ValueError, match="unsupported as a direct action"):
        desk.runtime().submit(other)
    assert desk.store.load().decision is None
