"""Offline walks of the authoring templates through entry and every unwind path.

A small simulated fund, lending market and pool apply each proposed intent the way the
gateway admits it: spending beyond owned inventory, a borrow or withdrawal below the
health-factor floor, a short full repay, and an exposure-increasing teardown intent all
fail the walk. The decision sequences here are the ones the fork lifecycle should show.
"""

import importlib.util
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from almanak.api import AssetType
from almanak.connectors.gmx_v2.api import LiquidationResult
from almanak.connectors.morpho_blue.liquidation import maximum_debt
from tests.reference_parameters import TypedHooks, authored_values

STRATEGIES = Path(__file__).parents[1] / "strategies"
WAD = 10**18
BPS = 10_000
WETH, USDC = "weth", "usdc"


def ns(**fields):
    return SimpleNamespace(**fields)


def load(name, filename="strategy.py"):
    spec = importlib.util.spec_from_file_location(f"loop_{name}", STRATEGIES / name / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return TypedHooks(module)


def authored_floor(name) -> str:
    raw = yaml.safe_load((STRATEGIES / name / "config.yaml").read_text())
    return raw["strategy"]["allocations"]["loan"]["minimum_health_factor"]


def configured(protocol, name=None, **overrides):
    """The config of template `name` (default: the protocol's), with its authored
    parameters and floor and any `overrides`."""
    name = name or protocol.name
    floor = int(Decimal(authored_floor(name)) * WAD)
    return protocol.config({**authored_values(name), **overrides}, floor)


class Config(SimpleNamespace):
    def allocation(self, name):
        return next(a for a in self.positions if a.name == name)


class Actions:
    def component(self, _name):
        return self

    def __getattr__(self, kind):
        return lambda name, *args, **options: ns(kind=kind, name=name, args=args, options=options)


class Market:
    """Collateral, debt and one pool exchanging the two, all in one fund; WETH and USDC
    on either side.

    `wallet` is the inventory the strategy owns; `outside` is loan asset sent to the fund
    that only a full repay may spend (and so absorb), as the gateway admits it.
    """

    def __init__(
        self,
        protocol,
        *,
        collateral=WETH,
        loan=USDC,
        capital=5 * 10**16,
        price=4000,
        floor="1.6",
        lt_bps=8300,
        slice_=1000 * 10**6,
    ):
        self.protocol = protocol
        self.collateral, self.loan = collateral, loan
        self.wallet = {collateral: capital, loan: 0}
        self.outside = {collateral: 0, loan: 0}
        self.supplied = 0
        self.debt = 0
        self.price = price  # USDC per WETH, oracle and pool alike before the pool spread
        self.floor = int(Decimal(floor) * WAD)
        self.lt_bps = lt_bps
        self.slice = slice_  # loan asset one entry swap sells: the trade's capital
        self.pool_bps = 9975  # what an exit swap returns against the oracle

    def value(self, asset, amount):
        """`amount` of `asset` in USDC base units (6 decimals)."""
        return amount * self.price * 10**6 // 10**18 if asset == WETH else amount

    def amount(self, asset, value):
        return value * 10**18 // (self.price * 10**6) if asset == WETH else value

    def collateral_value(self, supplied=None):
        return self.value(self.collateral, self.supplied if supplied is None else supplied)

    def health(self, supplied=None, debt=None):
        debt = self.value(self.loan, self.debt if debt is None else debt)
        if not debt:
            return 2**256 - 1
        return self.collateral_value(supplied) * self.lt_bps * WAD // (BPS * debt)

    def accrue(self):
        self.debt += self.debt // 1_000_000  # interest between decisions

    def sell(self, amount):
        """Loan asset the exit swap returns for `amount` of collateral: by default a 0.05%
        fee and a pool 0.2% under the oracle."""
        return self.amount(self.loan, self.value(self.collateral, amount)) * self.pool_bps // BPS

    def apply(self, intent, teardown):
        kind, args, options = intent.kind, intent.args, intent.options
        collateral, loan = self.collateral, self.loan
        exposure = kind in ("supply", "borrow") or (kind == "swap" and not options.get("exit"))
        assert not (teardown and exposure), f"teardown refused {kind}"
        if kind == "supply":
            (amount,) = args
            assert 0 < amount <= self.wallet[collateral], "inventory_unavailable"
            self.wallet[collateral] -= amount
            self.supplied += amount
        elif kind == "borrow":
            (amount,) = args
            assert self.health(debt=self.debt + amount) >= self.floor, "below the floor"
            self.debt += amount
            self.wallet[loan] += amount
        elif kind == "swap" and options.get("exit"):
            sold = self.wallet[collateral]
            self.wallet[collateral] = 0
            self.wallet[loan] += self.sell(sold)
        elif kind == "swap":
            sold = min(self.wallet[loan], self.slice)
            self.wallet[loan] -= sold
            bought = self.amount(collateral, self.value(loan, sold))
            self.wallet[collateral] += bought * 9975 // BPS
        elif kind == "repay":
            self.debt = full_repay(self.wallet, self.outside, loan, self.debt)
        elif kind == "repay_amount":
            (amount,) = args
            assert 0 < amount <= min(self.wallet[loan], self.debt), "inventory_unavailable"
            self.wallet[loan] -= amount
            self.debt -= amount
        elif kind == "withdraw_amount":
            (amount,) = args
            assert 0 < amount <= self.supplied
            assert self.health(supplied=self.supplied - amount) >= self.floor, "below the floor"
            self.supplied -= amount
            self.wallet[collateral] += amount
        elif kind == "withdraw":
            assert not self.debt, "full withdrawal with open debt"
            self.wallet[collateral] += self.supplied
            self.supplied = 0
        else:
            raise AssertionError(kind)

    def observations(self):
        return self.protocol.observations(self)


def full_repay(wallet, outside, asset, debt):
    """Spend owned inventory first, then loan asset sent to the fund; the debt left (0)."""
    assert wallet[asset] + outside[asset] >= debt, "repayment shortfall"
    absorbed = max(0, debt - wallet[asset])
    wallet[asset] -= debt - absorbed
    outside[asset] -= absorbed
    return 0


def trade_observation(market):
    collateral, loan = market.collateral, market.loan
    return ns(
        owned=ns(
            inventory0=ns(asset=WETH, amount=market.wallet[WETH]),
            inventory1=ns(asset=USDC, amount=market.wallet[USDC]),
        ),
        entry_quote=ns() if market.wallet[loan] else None,
        exit_quote=quote(market.sell(market.wallet[collateral]))
        if market.wallet[collateral]
        else None,
        needs_cleanup=False,
    )


def quote(amount):
    return ns(amount_out=ns(amount=amount))


class Aave:
    name = "arbitrum_aave_leverage_loop"
    lt_bps = 8300  # Arbitrum WETH liquidation threshold

    @staticmethod
    def config(parameters, floor):
        return Config(
            parameters=parameters,
            positions=[
                ns(
                    name="collateral",
                    account="fund",
                    chain="arbitrum",
                    reserve=ns(pool="pool", asset=WETH),
                    collateral=True,
                    borrow=False,
                    capital=5 * 10**16,
                    minimum_health_factor=floor,
                ),
                ns(
                    name="loan",
                    account="fund",
                    chain="arbitrum",
                    reserve=ns(pool="pool", asset=USDC),
                    collateral=False,
                    borrow=True,
                    capital=0,
                    minimum_health_factor=floor,
                ),
                trade_allocation(),
            ],
        )

    @staticmethod
    def observations(market):
        risk = ns(
            total_collateral_base=market.collateral_value() * 100,
            total_debt_base=market.value(market.loan, market.debt) * 100,
            liquidation_threshold_bps=market.lt_bps,
            health_factor_wad=market.health(),
        )

        def position(asset, **fields):
            decimals, price = (18, market.price * 10**8) if asset == WETH else (6, 10**8)
            reserve = ns(asset=ns(decimals=decimals), oracle=ns(price=price))
            return ns(reserve=reserve, risk=risk, **fields)

        collateral = ns(
            position=position(
                market.collateral,
                supplied_assets=market.supplied,
                supplied_scaled=market.supplied,
                collateral_enabled=bool(market.supplied),
            ),
            allowance=0,
        )
        loan = ns(
            position=position(market.loan, debt_assets=market.debt, debt_scaled=market.debt),
            wallet_balance=market.wallet[market.loan] + market.outside[market.loan],
            allowance=0,
        )
        return {"collateral": collateral, "loan": loan, "trade": trade_observation(market)}


def trade_allocation(chain="arbitrum"):
    return ns(
        name="trade",
        account="fund",
        chain=chain,
        pool=ns(currency0=WETH, currency1=USDC),
        capital=ns(asset=USDC, amount=1000 * 10**6),
        range=None,
        band=None,
    )


class Morpho:
    name = "base_morpho_leverage_loop"
    lt_bps = 8600  # the configured WETH/USDC market's LLTV

    @staticmethod
    def config(parameters, floor):
        def reserve(asset, is_loan):
            return ns(market="market", asset=asset, is_loan=is_loan)

        return Config(
            parameters=parameters,
            positions=[
                ns(
                    name="collateral",
                    account="fund",
                    chain="base",
                    reserve=reserve(WETH, False),
                    borrow=False,
                    capital=5 * 10**16,
                    minimum_health_factor=floor,
                ),
                ns(
                    name="loan",
                    account="fund",
                    chain="base",
                    reserve=reserve(USDC, True),
                    borrow=True,
                    capital=0,
                    minimum_health_factor=floor,
                ),
                trade_allocation("base"),
            ],
        )

    @staticmethod
    def prepare(module):
        # Debt shares map one to one onto assets in this simulation.
        module.borrowed_assets = lambda account: account.position.borrow_shares
        module.health_factor = morpho_health

    @staticmethod
    def observations(market):
        position = ns(collateral=market.supplied, borrow_shares=market.debt)
        # Loan base units per collateral base unit, scaled by 1e36.
        price = ns(price=market.price * 10**6 * 10**36 // 10**18)
        account = ns(
            position=position,
            market=ns(price=price, source=ns(lltv=market.lt_bps * 10**14)),
        )
        return {
            "collateral": ns(account=account, allowance=0),
            "loan": ns(
                account=account,
                wallet_balance=market.wallet[USDC] + market.outside[USDC],
                allowance=0,
            ),
            "trade": trade_observation(market),
        }


def morpho_health(account):
    """The SDK's Morpho health factor, with debt shares one to one onto assets."""
    debt = account.position.borrow_shares
    if not debt:
        return None
    market = account.market
    maximum = maximum_debt(account.position.collateral, market.price.price, market.source.lltv)
    return maximum * WAD // debt


PROTOCOLS = (Aave, Morpho)


def walk(module, market, config, memory, *, teardown=False, limit=40, step=None):
    kinds: list[str] = []
    for _ in range(limit):
        observations = market.observations()
        context = ns(
            config=config,
            memory=memory,
            teardown=teardown,
            actions=Actions(),
            observation=observations.get,
            decision=lambda intent=None, *, memory=None, reason=None: ns(
                intent=intent, memory=memory, reason=reason
            ),
        )
        decision = module.decide(context)
        memory = decision.memory or memory
        if decision.intent is None:
            return kinds, memory
        market.apply(decision.intent, teardown)
        kind = decision.intent.kind
        exit_swap = kind == "swap" and decision.intent.options.get("exit")
        kinds.append("swap(exit)" if exit_swap else kind)
        result = ns(status="completed", intent=ns(kind=kind))
        memory = module.on_outcome(ns(config=config, memory=memory, result=result))
        market.accrue()
        if step is not None:
            step(kind)
    raise AssertionError(f"no hold within {limit} decisions: {kinds}")


def dashboard_text(protocol):
    """`rounds_text` from the dashboard: dashboard.py when the reference has one."""
    split = (STRATEGIES / protocol.name / "dashboard.py").exists()
    return load(protocol.name, "dashboard.py" if split else "strategy.py").rounds_text


def setup(protocol, **parameters):
    module = load(protocol.name)
    getattr(protocol, "prepare", lambda _: None)(module)
    config = configured(protocol, **parameters)
    module.validate_config(config)
    market = Market(protocol, floor=authored_floor(protocol.name), lt_bps=protocol.lt_bps)
    return module, market, config, module.State()


ROUND = ["borrow", "swap", "supply"]


@pytest.mark.parametrize("protocol", PROTOCOLS, ids=lambda p: p.name)
@pytest.mark.parametrize("loops", (1, 2))
def test_entry_rounds_then_teardown_unwind(protocol, loops):
    module, market, config, memory = setup(protocol, loops=loops)
    entry, memory = walk(module, market, config, memory)
    assert entry == ["supply", *ROUND * loops]
    assert memory.rounds == loops and market.wallet == {WETH: 0, USDC: 0}
    assert market.health() >= int(Decimal("1.8") * WAD)
    teardown, memory = walk(module, market, config, memory, teardown=True)
    # The floor sits far enough below the entry health factor for one cycle.
    assert teardown == ["withdraw_amount", "swap(exit)", "repay", "withdraw"]
    # Leftover loan asset stays: teardown never buys collateral back.
    assert (market.debt, market.supplied) == (0, 0) and market.wallet[USDC] > 0


@pytest.mark.parametrize("protocol", PROTOCOLS, ids=lambda p: p.name)
def test_a_finished_loop_states_its_health_while_it_holds(protocol):
    module, market, config, memory = setup(protocol, loops=1)
    _, memory = walk(module, market, config, memory)
    observations = market.observations()
    held = module.decide(
        ns(
            config=config,
            memory=memory,
            teardown=False,
            actions=Actions(),
            observation=observations.get,
            decision=lambda intent=None, *, memory=None, reason=None: ns(
                intent=intent, memory=memory, reason=reason
            ),
        )
    )
    assert held.intent is None
    assert held.reason.startswith("All loop rounds done. Health ")
    assert held.reason.endswith("above the 1.8 unwind level")


@pytest.mark.parametrize("protocol", PROTOCOLS, ids=lambda p: p.name)
def test_fork_test_values_run_one_round_and_the_full_unwind(protocol):
    from almanak.configuration.unresolved import read_inputs

    tests = read_inputs(STRATEGIES / protocol.name / "config.yaml").test_parameters
    assert tests == {"loops": 1}
    module, market, config, memory = setup(protocol, **tests)
    entry, memory = walk(module, market, config, memory)
    assert entry == ["supply", *ROUND]
    teardown, memory = walk(module, market, config, memory, teardown=True)
    assert teardown == ["withdraw_amount", "swap(exit)", "repay", "withdraw"]


@pytest.mark.parametrize("protocol", PROTOCOLS, ids=lambda p: p.name)
def test_round_below_the_trigger_is_skipped(protocol):
    module, market, config, memory = setup(protocol, loops=5)
    entry, memory = walk(module, market, config, memory)
    # 0.25 a round: the third borrow would end near 1.70, under the 1.8 trigger.
    assert entry == ["supply", *ROUND * 2] and memory.rounds == 2
    # The cap is recorded, not silent, and the dashboard text carries it.
    assert memory.capped_reason.startswith("Stopped after 2 of 5 rounds")
    assert "below the 1.8 unwind level" in memory.capped_reason
    values = {"rounds": 2, "capped_reason": memory.capped_reason}
    assert dashboard_text(protocol)(values) == memory.capped_reason


@pytest.mark.parametrize("protocol", PROTOCOLS, ids=lambda p: p.name)
@pytest.mark.parametrize("fraction,rounds", (("0.27", 1), ("0.25", 2)))
def test_round_two_needs_borrow_fraction_within_the_threshold_bound(protocol, fraction, rounds):
    # Round two needs borrow_fraction x (2 + f) / (1 + f) x 1.8 <= the liquidation
    # threshold: f up to about 0.257 at Aave's 0.83 and 0.267 at Morpho's 0.86.
    module, market, config, memory = setup(protocol, loops=2, borrow_fraction=fraction)
    entry, memory = walk(module, market, config, memory)
    assert entry == ["supply", *ROUND * rounds] and memory.rounds == rounds
    assert dashboard_text(protocol)({"rounds": rounds, **memory.model_dump()}).startswith(
        "Completed" if rounds == 2 else "Stopped after 1 of 2 rounds"
    )


@pytest.mark.parametrize("protocol", PROTOCOLS, ids=lambda p: p.name)
def test_health_factor_breach_unwinds_and_converts_leftover(protocol):
    module, market, config, memory = setup(protocol, loops=2)
    _, memory = walk(module, market, config, memory)
    market.price = market.price * 75 // 100
    kinds, memory = walk(module, market, config, memory)
    assert memory.unwinding and kinds[-2:] == ["withdraw", "swap"]
    assert (market.debt, market.supplied, market.wallet[USDC]) == (0, 0, 0)
    assert market.wallet[WETH] > 0
    # Latched: a recovered health factor never levers up again.
    market.price = market.price * 2
    assert walk(module, market, config, memory)[0] == []


@pytest.mark.parametrize("protocol", PROTOCOLS, ids=lambda p: p.name)
def test_a_price_gap_through_the_trigger_still_unwinds_in_cycles(protocol):
    module, market, config, memory = setup(protocol, loops=2)
    _, memory = walk(module, market, config, memory)
    # WETH 4000 -> 2600 in one step: the health factor gaps from about 2.3 to about 1.5,
    # through the 1.8 trigger but above the 1.15 floor, so collateral can still be
    # withdrawn; the floor caps each withdrawal, so it repays over several cycles.
    market.price = 2600
    kinds, memory = walk(module, market, config, memory)
    partial = kinds.index("repay") // 3
    cycle = ["withdraw_amount", "swap(exit)"]
    assert partial and kinds[: 3 * partial] == [*cycle, "repay_amount"] * partial
    assert kinds[3 * partial : 3 * partial + 4] == [*cycle, "repay", "withdraw"]
    assert (market.debt, market.supplied) == (0, 0) and memory.shortfall == ""


@pytest.mark.parametrize("protocol", PROTOCOLS, ids=lambda p: p.name)
def test_below_the_floor_the_unwind_names_the_shortfall_until_a_top_up(protocol):
    module, market, config, memory = setup(protocol, loops=2)
    _, memory = walk(module, market, config, memory)
    # A gap below the 1.15 floor: every withdrawal is refused and the fund owns no USDC.
    market.price = 1800
    kinds, memory = walk(module, market, config, memory)
    assert kinds == [] and memory.unwinding and market.debt
    assert "frees no collateral" in memory.shortfall
    assert "to the fund to finish repaying" in memory.shortfall
    # The sent USDC is no inventory the strategy owns, yet the full repay may spend it.
    market.outside[USDC] += market.debt * 2
    kinds, memory = walk(module, market, config, memory)
    assert kinds == ["repay", "withdraw"] and memory.shortfall == ""
    assert (market.debt, market.supplied, market.wallet[USDC]) == (0, 0, 0)


@pytest.mark.parametrize("protocol", PROTOCOLS, ids=lambda p: p.name)
def test_failure_and_event_start_the_unwind(protocol):
    module, _, config, memory = setup(protocol)
    failed = ns(status="failed", intent=ns(kind="swap"))
    assert module.on_outcome(ns(config=config, memory=memory, result=failed)).unwinding
    assert module.on_event(ns(config=config, memory=memory)).unwinding


class LiquidityMarket(Market):
    """Adds one banded position funded in USDC; `loss_bps` is what it loses by close."""

    def __init__(self, protocol, *, loss_bps=0, **options):
        super().__init__(protocol, **options)
        self.position = None
        self.loss_bps = loss_bps

    def apply(self, intent, teardown):
        kind, options = intent.kind, intent.options
        if kind == "swap" and options.get("rebalance"):
            assert not teardown, "teardown refused the rebalance swap"
            sold = self.wallet[USDC] // 2
            self.wallet[USDC] -= sold
            self.wallet[WETH] += sold * 10**18 // (self.price * 10**6) * 9975 // BPS
        elif kind == "open_liquidity":
            assert not teardown and self.position is None
            self.position, self.wallet = dict(self.wallet), {WETH: 0, USDC: 0}
        elif kind == "close_liquidity":
            assert self.position is not None
            for asset, amount in self.position.items():
                self.wallet[asset] += amount * (BPS - self.loss_bps) // BPS
            self.position = None
        else:
            super().apply(intent, teardown)


class AaveLiquidity(Aave):
    name = "arbitrum_aave_borrow_lp"

    @staticmethod
    def config(parameters, floor):
        config = Aave.config(parameters, floor)
        liquidity = ns(**{**vars(trade_allocation()), "name": "liquidity", "band": ns()})
        config.positions[2] = liquidity
        return config

    @staticmethod
    def observations(market):
        values = Aave.observations(market)
        liquidity = values.pop("trade")
        liquidity.owned.position = market.position
        liquidity.rebalance_quote = ns() if market.position is None else None
        values["liquidity"] = liquidity
        return values


def liquidity_setup(*, reserve=5 * 10**6, parameters=None, **options):
    """The authored template: cover_shortfall off and a 5 USDC loan-asset reserve."""
    module = load(AaveLiquidity.name)
    config = configured(AaveLiquidity, **(parameters or {}))
    module.validate_config(config)
    market = LiquidityMarket(AaveLiquidity, floor=authored_floor(AaveLiquidity.name), **options)
    market.wallet[USDC] = reserve
    return module, market, config, module.State()


ENTRY = ["supply", "borrow", "swap", "open_liquidity"]
COVER = {"cover_shortfall": True}


def test_borrow_lp_entry_and_teardown_repay_from_the_position_and_reserve():
    module, market, config, memory = liquidity_setup()
    entry, memory = walk(module, market, config, memory)
    assert entry == ENTRY and market.position[USDC] and market.position[WETH]
    teardown, memory = walk(module, market, config, memory, teardown=True)
    # The reserve joined the position, so its proceeds cover fees and interest.
    assert teardown == ["close_liquidity", "swap(exit)", "repay", "withdraw"]
    assert (market.debt, market.supplied, market.position) == (0, 0, None)
    assert memory.shortfall == ""


def test_borrow_lp_without_cover_repays_what_it_owns_then_names_the_shortfall():
    module, market, config, memory = liquidity_setup(reserve=0, loss_bps=1000)
    _, memory = walk(module, market, config, memory)
    teardown, memory = walk(module, market, config, memory, teardown=True)
    assert teardown == ["close_liquidity", "swap(exit)", "repay_amount"]
    assert market.debt and market.supplied and "Send that much to the fund" in memory.shortfall
    assert "set cover_shortfall: true" in memory.shortfall
    # Loan asset sent to the fund is no inventory the strategy owns, yet the full repay
    # may spend it, so it resolves the shortfall without selling collateral.
    sent = market.debt * 2
    market.outside[USDC] += sent
    teardown, memory = walk(module, market, config, memory, teardown=True)
    assert teardown == ["repay", "withdraw"] and memory.shortfall == ""
    assert market.debt == 0 and 0 < market.outside[USDC] < sent


def test_borrow_lp_shortfall_with_cover_omits_the_cover_advice():
    module = load(AaveLiquidity.name)
    context = ns(config=configured(AaveLiquidity, **COVER))
    loan = ns(position=ns(reserve=ns(asset=ns(decimals=6))))
    shortfall = module.stalled(context, loan, 5_000_000)
    assert "5 of the borrowed asset is still owed" in shortfall
    assert "cover_shortfall" not in shortfall


def test_borrow_lp_cover_shortfall_sells_collateral_for_fees():
    module, market, config, memory = liquidity_setup(reserve=0, parameters=COVER)
    _, memory = walk(module, market, config, memory)
    teardown, _ = walk(module, market, config, memory, teardown=True)
    # Fees leave the position a little short of the debt; collateral covering the rest is
    # withdrawn before the sale, so one exit swap sells the position's WETH and it together.
    assert teardown == [
        *("close_liquidity", "withdraw_amount", "swap(exit)"),
        *("repay", "withdraw"),
    ]
    assert (market.debt, market.supplied, market.position) == (0, 0, None)


def test_borrow_lp_cover_shortfall_withdraws_again_when_the_pool_trails_the_oracle():
    # A pool 3% under the oracle, beyond the 1% headroom, and a position closing 20% down:
    # the oracle-sized withdrawal sells for less than planned, so the check after it
    # withdraws once more before the one swap.
    module, market, config, memory = liquidity_setup(reserve=0, loss_bps=2000, parameters=COVER)
    _, memory = walk(module, market, config, memory)
    market.pool_bps = 9700
    teardown, _ = walk(module, market, config, memory, teardown=True)
    assert teardown == [
        *("close_liquidity", "withdraw_amount", "withdraw_amount", "swap(exit)"),
        *("repay", "withdraw"),
    ]
    assert (market.debt, market.supplied) == (0, 0)


def test_borrow_lp_covers_a_losing_position_from_collateral():
    module, market, config, memory = liquidity_setup(reserve=0, loss_bps=500, parameters=COVER)
    _, memory = walk(module, market, config, memory)
    teardown, _ = walk(module, market, config, memory, teardown=True)
    assert teardown == [
        *("close_liquidity", "withdraw_amount", "swap(exit)"),
        *("repay", "withdraw"),
    ]
    assert market.debt == 0


def test_borrow_lp_trigger_unwind_converts_leftover_and_never_reopens():
    module, market, config, memory = liquidity_setup(reserve=0, parameters=COVER)
    _, memory = walk(module, market, config, memory)
    market.price = market.price * 60 // 100
    kinds, memory = walk(module, market, config, memory)
    assert kinds[0] == "close_liquidity" and memory.unwinding
    assert market.debt == 0 and market.position is None and market.wallet[USDC] == 0
    market.price = market.price * 2
    assert walk(module, market, config, memory)[0] == []


WSTETH = "wsteth"


class ForeignCollateralMarket(LiquidityMarket):
    """wstETH collateral (priced like WETH here) beside a WETH/USDC pool."""

    def __init__(self, protocol, **options):
        super().__init__(protocol, **options)
        self.wallet = {WSTETH: self.wallet.pop(WETH), WETH: 2 * 10**16, USDC: 0}

    def apply(self, intent, teardown):
        if intent.kind == "supply":
            assert not teardown and 0 < intent.args[0] <= self.wallet[WSTETH]
            self.wallet[WSTETH] -= intent.args[0]
            self.supplied += intent.args[0]
        elif intent.kind == "withdraw":
            assert not self.debt, "full withdrawal with open debt"
            self.wallet[WSTETH] += self.supplied
            self.supplied = 0
        elif intent.kind == "open_liquidity":
            assert not teardown and self.position is None
            self.position = {WETH: self.wallet[WETH], USDC: self.wallet[USDC]}
            self.wallet[WETH] = self.wallet[USDC] = 0
        else:
            assert intent.kind != "withdraw_amount", "no pool sells wstETH"
            super().apply(intent, teardown)


def foreign_config(**parameters):
    config = configured(AaveLiquidity, **parameters)
    config.positions[0].reserve.asset = WSTETH
    return config


def test_borrow_lp_takes_collateral_outside_the_pool_with_user_funded_weth():
    module = load(AaveLiquidity.name)
    config = foreign_config()
    module.validate_config(config)
    market = ForeignCollateralMarket(AaveLiquidity)
    entry, memory = walk(module, market, config, module.State())
    # The user's WETH stays for the position; no rebalance swap is needed.
    assert entry == ["supply", "borrow", "open_liquidity"] and market.supplied == 5 * 10**16
    teardown, memory = walk(module, market, config, memory, teardown=True)
    assert teardown == ["close_liquidity", "swap(exit)", "repay", "withdraw"]
    assert (market.debt, market.supplied, market.wallet[WSTETH]) == (0, 0, 5 * 10**16)


def test_borrow_lp_cover_shortfall_needs_collateral_in_the_pool():
    with pytest.raises(ValueError, match="pool must pair the collateral and loan"):
        load(AaveLiquidity.name).validate_config(foreign_config(**COVER))


MANAGED = "aave_v3_arbitrum_managed_borrow"


def managed_config(**parameters):
    config = configured(Aave, MANAGED, **parameters)
    config.positions = config.positions[:2]
    return config


def managed_observations(market):
    values = Aave.observations(market)
    del values["trade"]
    return values


def test_managed_borrow_steers_to_the_target_band():
    module = load(MANAGED)
    config = managed_config()
    module.validate_config(config)
    market = Market(Aave, floor=authored_floor(MANAGED), lt_bps=Aave.lt_bps)
    market.observations = lambda: managed_observations(market)
    memory = module.State()

    kinds, memory = walk(module, market, config, memory)
    assert kinds == ["supply", "borrow"]
    target = int(Decimal("2.2") * WAD)
    assert abs(market.health() - target) < target // 1000
    market.price = market.price * 85 // 100  # below the 1.9 repay trigger
    kinds, memory = walk(module, market, config, memory)
    assert kinds == ["repay_amount"] and abs(market.health() - target) < target // 1000
    market.price = market.price * 130 // 100  # above the 2.6 borrow trigger
    kinds, memory = walk(module, market, config, memory)
    assert kinds == ["borrow"]
    market.wallet[USDC] += 10**6  # the operator's interest buffer
    kinds, _ = walk(module, market, config, memory, teardown=True)
    assert kinds == ["repay", "withdraw"] and (market.debt, market.supplied) == (0, 0)


def test_managed_borrow_repays_what_it_holds_then_names_the_shortfall():
    module = load(MANAGED)
    config = managed_config()
    market = Market(Aave, floor=authored_floor(MANAGED), lt_bps=Aave.lt_bps)
    market.observations = lambda: managed_observations(market)
    _, memory = walk(module, market, config, module.State())
    # Interest accrued since the borrow leaves the borrowed USDC short of the debt.
    kinds, memory = walk(module, market, config, memory, teardown=True)
    assert kinds == ["repay_amount"] and market.debt and market.supplied
    assert "of the borrowed asset is still owed" in memory.shortfall
    assert "base units" not in memory.shortfall
    market.outside[USDC] += 10**6
    kinds, memory = walk(module, market, config, memory, teardown=True)
    assert kinds == ["repay", "withdraw"] and (market.debt, market.supplied) == (0, 0)
    assert memory.shortfall == ""


@pytest.mark.parametrize(
    "parameters",
    (
        {"repay_health_factor": "1.6"},
        {"repay_health_factor": "2.2"},
        {"borrow_health_factor": "2.2"},
    ),
)
def test_managed_borrow_refuses_an_unordered_band(parameters):
    with pytest.raises(ValueError, match="health_factor"):
        load(MANAGED).validate_config(managed_config(**parameters))


GMX = "gmx_v2_arbitrum_price_stop"
GMX_WETH = AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1")


def gmx_context(module, *, position, close, side="long", memory=None, teardown=False):
    allocation = ns(
        name="position",
        side=side,
        market=ns(index_asset=GMX_WETH),
        entry_collateral=ns(amount=10**17),
        entry_size_usd_e30=1000 * 10**30,
    )
    config = Config(
        allocations=[allocation],
        positions=[allocation],
        parameters=authored_values(GMX),
        asset_metadata={GMX_WETH: ns(decimals=18)},
    )
    observed = ns(owned=ns(position=position))
    return ns(
        config=config,
        memory=memory or module.State(),
        teardown=teardown,
        actions=Actions(),
        require_observation=lambda _name: observed,
        price=lambda _asset: ns(complete=close is not None, values=(close,)),
        decision=lambda intent=None, *, memory=None, reason=None: ns(
            intent=intent, memory=memory, reason=reason
        ),
    )


def gmx_position(entry):
    # 1000 USD of WETH opened at `entry` USD per WETH.
    return ns(
        size_usd_e30=1000 * 10**30,
        index_amount=ns(asset=GMX_WETH, amount=int(Decimal(1000) / Decimal(entry) * 10**18)),
    )


@pytest.mark.parametrize(
    "side,close,exits",
    (
        ("long", 4000, False),
        ("long", 3790, True),  # -5.25%: the 500 bps stop
        ("long", 4410, True),  # +10.25%: the 1000 bps take-profit
        ("long", 4300, False),
        ("short", 4210, True),  # price up 5.25% is a short's stop
        ("short", 3590, True),
        ("short", 3900, False),
        ("long", None, False),  # a stale series decides nothing
    ),
)
def test_gmx_price_stop_and_take_profit(side, close, exits):
    module = load(GMX)
    memory = module.State(opened=True)
    context = gmx_context(
        module, position=gmx_position(4000), close=close, side=side, memory=memory
    )
    decision = module.decide(context)
    assert (decision.intent is not None) == exits
    if exits:
        assert decision.intent.kind == "exit" and decision.memory.unwinding
        assert decision.reason.startswith(("Stop: price ", "Target: price "))
    elif close is None:
        assert decision.reason == "Position open; waiting for a fresh WETH price"
    else:
        assert decision.reason.startswith("Price ") and "; stop at " in decision.reason


@pytest.mark.parametrize(
    "side,increase,decrease,refused",
    (
        ("long", 10**18, 0, None),
        ("short", 0, 10**18, None),
        ("short", 10**18, 0, "side short"),
        ("long", 0, 10**18, "side long"),
        ("long", 10**18, 0, "index WETH"),
    ),
)
def test_gmx_refuses_acceptable_prices_that_do_not_suit_the_side(side, increase, decrease, refused):
    module = load(GMX)
    config = gmx_context(module, position=None, close=None, side=side).config
    allocation = config.allocation("position")
    allocation.increase_acceptable_price = increase
    allocation.decrease_acceptable_price = decrease
    if refused == "index WETH":
        allocation.market = ns(index_asset=AssetType("eip155:42161/erc20:0x" + "1" * 40))
    if refused is None:
        module.validate_config(config)
        return
    with pytest.raises(ValueError, match=refused):
        module.validate_config(config)
    with pytest.raises(ValueError, match="one position allocation"):
        module.validate_config(Config(allocations=[], positions=[]))


def test_gmx_opens_once_and_teardown_exits():
    module = load(GMX)
    opening = module.decide(gmx_context(module, position=None, close=4000))
    assert opening.intent.kind == "increase"
    assert opening.intent.options["size_delta_usd_e30"] == 1000 * 10**30
    closed = gmx_context(module, position=None, close=4000, memory=module.State(opened=True))
    assert module.decide(closed).intent is None
    teardown = gmx_context(
        module, position=gmx_position(4000), close=None, memory=module.State(opened=True)
    )
    teardown.teardown = True
    assert module.decide(teardown).intent.kind == "exit"


def test_gmx_outcomes_open_once_and_close_after_a_failure_or_liquidation():
    module = load(GMX)

    def after(result, memory=None):
        return module.on_outcome(ns(result=result, memory=memory or module.State()))

    def request(kind, *, complete=True, success=True):
        return ns(complete=complete, success=success, intent=ns(kind=kind))

    assert after(request("increase_position")).opened
    assert after(request("increase_position", complete=False)) == module.State()
    failed = after(request("increase_position", success=False), module.State(opened=True))
    assert failed.opened and failed.unwinding
    assert after(request("decrease_position")) == module.State()
    liquidated = after(LiquidationResult.model_construct())
    assert liquidated.opened and liquidated.unwinding


SHORT = "arbitrum_aave_lending_short"


def short_config(**parameters):
    """USDC collateral, WETH debt and the same pool, for the lending short."""
    config = configured(Aave, SHORT, **parameters)
    collateral, loan, trade = config.positions
    collateral.reserve.asset, loan.reserve.asset = USDC, WETH
    trade.capital = ns(asset=WETH, amount=10**18)
    return config


def short_setup():
    module = load(SHORT)
    config = short_config()
    module.validate_config(config)
    market = Market(
        Aave,
        collateral=USDC,
        loan=WETH,
        capital=200 * 10**6,
        floor=authored_floor(SHORT),
        lt_bps=7800,
        slice_=2**256,
    )
    return module, market, config, module.State()


SHORT_ENTRY = ["supply", "borrow", "swap"]


@pytest.mark.parametrize(
    "move,unwind",
    (
        # The stop: WETH up 6%; the proceeds buy back less than the debt, so the collateral
        # covering the rest is withdrawn first and one buy-back spends both.
        (106, ["withdraw_amount", "swap(exit)", "repay", "withdraw", "swap"]),
        # The take-profit: WETH down 11%; the proceeds buy back more than the debt.
        (89, ["swap(exit)", "repay", "withdraw", "swap"]),
    ),
)
def test_short_stop_and_take_profit(move, unwind):
    module, market, config, memory = short_setup()
    entry, memory = walk(module, market, config, memory)
    assert entry == SHORT_ENTRY and memory.entry_price == 4000 * 10**8
    market.price = market.price * move // 100
    kinds, memory = walk(module, market, config, memory)
    assert kinds == unwind and memory.unwinding
    assert (market.debt, market.supplied, market.wallet[WETH]) == (0, 0, 0)


def test_short_sold_in_slices_stops_against_the_first_sale():
    module, market, config, memory = short_setup()
    market.slice = 5 * 10**15  # a third of the borrowed WETH per entry swap
    sales = []

    def rise(kind):
        # Each slice sells 2% above the last: under the 5% stop, but 6% over three.
        if kind == "swap" and len(sales) < 3:
            sales.append(market.price)
            market.price = market.price * 102 // 100

    kinds, memory = walk(module, market, config, memory, step=rise)
    assert kinds[:5] == [*SHORT_ENTRY, "swap", "swap"] and kinds[5] != "swap"
    assert memory.entry_price == 4000 * 10**8 and memory.unwinding
    assert (market.debt, market.supplied) == (0, 0)


def test_short_full_repay_counts_weth_sent_to_the_fund():
    module, market, config, memory = short_setup()
    _, memory = walk(module, market, config, memory)
    # The sent WETH is no inventory the strategy owns, yet the full repay may spend it.
    market.outside[WETH] += market.debt * 2
    kinds, _ = walk(module, market, config, memory, teardown=True)
    assert kinds == ["repay", "withdraw"]
    assert (market.debt, market.supplied, market.wallet[WETH]) == (0, 0, 0)


def test_short_holds_inside_the_band_and_teardown_leaves_bought_back_weth():
    module, market, config, memory = short_setup()
    _, memory = walk(module, market, config, memory)
    market.price = market.price * 103 // 100
    assert walk(module, market, config, memory)[0] == []
    market.price = 4000 * 92 // 100
    kinds, _ = walk(module, market, config, memory, teardown=True)
    assert kinds == ["swap(exit)", "repay", "withdraw"]
    assert market.debt == 0 and market.wallet[WETH] > 0  # teardown never sells WETH again


@pytest.mark.parametrize(
    "move,unwind",
    (
        # The fork default: an unchanged price; fees leave the buy-back short, so the
        # collateral covering the rest is withdrawn first and one buy-back follows.
        (100, ["withdraw_amount", "swap(exit)", "repay", "withdraw"]),
        # WETH up 60%: the floor caps the withdrawal, so the unwind repays in two cycles.
        (
            160,
            [
                *("withdraw_amount", "swap(exit)", "repay_amount"),
                *("withdraw_amount", "swap(exit)", "repay", "withdraw"),
            ],
        ),
    ),
)
def test_short_teardown_withdraws_before_the_single_buy_back(move, unwind):
    module, market, config, memory = short_setup()
    _, memory = walk(module, market, config, memory)
    market.price = market.price * move // 100
    kinds, _ = walk(module, market, config, memory, teardown=True)
    assert kinds == unwind
    assert (market.debt, market.supplied) == (0, 0)


def test_short_refuses_an_aave_v4_market():
    from almanak.connectors.aave_v4.models import Position as AaveV4Position

    module = load(SHORT)
    config = short_config()
    config.positions[0] = AaveV4Position.model_construct(name="collateral")
    with pytest.raises(ValueError, match="needs Aave V3's partial withdraw and repay"):
        module.validate_config(config)


BUILDERS = {
    Aave.name: lambda **parameters: configured(Aave, **parameters),
    Morpho.name: lambda **parameters: configured(Morpho, **parameters),
    AaveLiquidity.name: lambda **parameters: configured(AaveLiquidity, **parameters),
    SHORT: short_config,
}


@pytest.mark.parametrize("name", BUILDERS)
@pytest.mark.parametrize("below", ("0", "0.05"))
def test_unwind_health_factor_must_sit_above_the_floor(name, below):
    trigger = Decimal(authored_floor(name)) - Decimal(below)
    config = BUILDERS[name](unwind_health_factor=str(trigger))
    with pytest.raises(ValueError, match="above minimum_health_factor"):
        load(name).validate_config(config)


def mutated(config, name, **fields):
    """`config` with allocation `name` (or one of its nested fields) changed."""
    allocation = config.allocation(name)
    for path, value in fields.items():
        target = allocation
        *parents, leaf = path.split("__")
        for parent in parents:
            target = getattr(target, parent)
        setattr(target, leaf, value)
    return config


def shapes(third):
    """Misshapen configs every collateral, loan and `third` allocation template refuses."""
    return (
        (lambda c: Config(**{**vars(c), "positions": c.positions[:2]}), "requires"),
        (lambda c: mutated(c, third, account="other"), "same named fund"),
        (lambda c: mutated(c, "loan", capital=1), "starts empty"),
    )


AAVE_SHAPES = (
    (lambda c: mutated(c, "loan", chain="base"), "same chain and Pool"),
    (lambda c: mutated(c, "collateral", collateral=False), "only the loan allocation"),
)
TRADE_SHAPES = (
    (lambda c: mutated(c, "trade", range=object()), "without opening liquidity"),
    (lambda c: mutated(c, "trade", band=object()), "without opening liquidity"),
    (lambda c: mutated(c, "trade", pool__currency1="dai"), "trade pool must exchange"),
    (
        lambda c: mutated(c, "trade", capital__asset=c.allocation("collateral").reserve.asset),
        "trade capital",
    ),
)
MISSHAPEN = {
    Aave.name: (*shapes("trade"), *AAVE_SHAPES, *TRADE_SHAPES),
    Morpho.name: (
        *shapes("trade"),
        *TRADE_SHAPES,
        (lambda c: mutated(c, "loan", reserve__market="other"), "same explicit Morpho market"),
        (lambda c: mutated(c, "collateral", reserve__is_loan=True), "collateral leg"),
    ),
    AaveLiquidity.name: (
        *shapes("liquidity"),
        *AAVE_SHAPES,
        (lambda c: mutated(c, "loan", borrow=False), "only the loan allocation"),
        (lambda c: mutated(c, "liquidity", band=None), "band on the lending chain"),
        (lambda c: mutated(c, "loan", reserve__asset="dai"), "include the loan asset"),
        (lambda c: mutated(c, "liquidity", capital__asset=WETH), "loan asset"),
    ),
    SHORT: (*shapes("trade"), *AAVE_SHAPES, *TRADE_SHAPES),
}


@pytest.mark.parametrize("name", MISSHAPEN)
def test_validate_config_refuses_a_misshapen_template(name):
    module = load(name)
    for change, message in MISSHAPEN[name]:
        with pytest.raises(ValueError, match=message):
            module.validate_config(change(BUILDERS[name]()))
