"""Entry slices repeat while funding inventory remains; enter-once references still enter once.

Blueprint 27 section 7: an exchange allocation is offered an entry quote whenever it holds
funding inventory, sized `min(funding inventory, capital x entry_swap_bps / 10000)`.
`entry_complete` keeps its meaning for accounting and reports only.
"""

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import AssetType
from almanak.connectors import v3_economic_model, v3_pool_observations
from almanak.connectors.aerodrome import economic_model as aerodrome_economic
from almanak.connectors.aerodrome import observations as aerodrome_observations
from almanak.connectors.uniswap_v3.positions import Ownership
from almanak.connectors.uniswap_v3.state import DecisionInput
from almanak.models import AssetAmount
from almanak.runtime import strategy_decision
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import DecisionSchedule
from tests.uniswap_v3_support import parameters

ROOT = Path(__file__).parents[1]
FUNDING = AssetType("eip155:1/erc20:0x" + "aa" * 20)
COUNTER = AssetType("eip155:1/erc20:0x" + "bb" * 20)
CAPITAL = 1_000_000


def owned(funding, counter=0, *, entered=True):
    target = SimpleNamespace(
        funding_currency=FUNDING, counter_currency=COUNTER, assets=(FUNDING, COUNTER)
    )
    return SimpleNamespace(
        target=target,
        inventory0=AssetAmount(asset=FUNDING, amount=funding),
        inventory1=AssetAmount(asset=COUNTER, amount=counter),
        entry_complete=entered,
        position=None,
    )


def allocation(bps):
    return SimpleNamespace(capital=AssetAmount(asset=FUNDING, amount=CAPITAL), entry_swap_bps=bps)


ENTRY_RULES = (v3_pool_observations.entry_amount, aerodrome_observations.entry_amount)


@pytest.mark.parametrize("entry_amount", ENTRY_RULES)
@pytest.mark.parametrize(
    ("bps", "funding", "expected"),
    (
        # One slice of capital, repeated after first exposure.
        (2500, CAPITAL, 250_000),
        (2500, 3 * CAPITAL, 250_000),
        # The slice is capped by the funding inventory still held.
        (2500, 100_000, 100_000),
        (10000, 400_000, 400_000),
        (10000, CAPITAL, CAPITAL),
    ),
)
def test_entry_slices_repeat_and_are_capped_by_funding_inventory(
    entry_amount, bps, funding, expected
):
    for entered in (False, True):
        amount = entry_amount(allocation(bps), owned(funding, 7, entered=entered))
        assert amount == AssetAmount(asset=FUNDING, amount=expected)


@pytest.mark.parametrize("entry_amount", ENTRY_RULES)
def test_no_entry_quote_without_funding_inventory_or_slice(entry_amount):
    assert entry_amount(allocation(2500), owned(0, 10**18)) is None
    tiny = SimpleNamespace(capital=AssetAmount(asset=FUNDING, amount=3), entry_swap_bps=1)
    assert entry_amount(tiny, owned(CAPITAL)) is None


def test_v3_quotes_offer_the_entry_slice_beside_the_exit_after_first_exposure():
    config = parameters(0)
    allocation = config.allocations[0]
    target = config.target(allocation.name)
    counter = next(asset for asset in target.assets if asset != target.funding_currency)
    held = {target.funding_currency: allocation.capital.amount // 3, counter: 10**15}
    exited = Ownership(
        target=target,
        inventory0=AssetAmount(asset=target.assets[0], amount=held[target.assets[0]]),
        inventory1=AssetAmount(asset=target.assets[1], amount=held[target.assets[1]]),
        entry_complete=True,
    )
    requested = []

    def quote(amount):
        requested.append(amount)
        return SimpleNamespace(amount_out=AssetAmount(asset=counter, amount=1))

    offered = v3_pool_observations.quotes_for(config, exited, quote)
    assert set(offered) == {"entry_quote", "exit_quote"}
    slice_ = allocation.capital.amount * allocation.entry_swap_bps // 10000
    assert requested[0] == AssetAmount(
        asset=target.funding_currency, amount=min(slice_, allocation.capital.amount // 3)
    )


def test_backtests_offer_entries_by_the_same_rule_as_live_observations():
    assert v3_economic_model.quotes_for is v3_pool_observations.quotes_for
    assert aerodrome_economic.quotes_for is aerodrome_observations.quotes_for


REFERENCES = sorted(
    path
    for prefix in ("uniswap_v3_", "pancakeswap_v3_", "aerodrome_")
    for path in (ROOT / "strategies").glob(prefix + "*/strategy.py")
)


@pytest.mark.parametrize("path", REFERENCES, ids=lambda path: path.parent.name)
def test_every_entry_swap_in_a_single_pool_reference_is_gated_on_first_exposure(path):
    """A reference that follows the entry quote checks `entry_complete` itself."""
    source = path.read_text()
    entry_swaps = re.findall(r"actions\.swap\(allocation\.name\)", source)
    guarded = re.findall(
        r"if not observed\.owned\.entry_complete:\n\s+return context\.actions\.swap\(", source
    )
    assert len(entry_swaps) == len(guarded)


def decide(style, memory, owned_update):
    from tests.test_uniswap_v3_validation import state_for

    directory = ROOT / f"strategies/uniswap_v3_ethereum_{style}"
    definition = StrategyDefinition(
        filename="strategy.py", source=(directory / "strategy.py").read_text()
    )
    state, observed, _ = state_for(chain=0)
    allocation = state.config.allocations[0]
    if style == "trading":
        allocation = allocation.model_copy(update={"range": None})
    config = state.config.model_copy(update={"allocations": (allocation,)})
    target = config.target(allocation.name)
    funding = allocation.capital
    inventory = {
        "inventory0" if target.assets[0] == funding.asset else "inventory1": funding,
    }
    held = observed.owned.model_copy(
        update={"target": target, "position": None, **inventory, **owned_update}
    )
    quote = observed.entry_quote.model_copy(update={"source": target})
    observed = observed.model_copy(
        update={"target": target, "owned": held, "position": None, "entry_quote": quote}
    )
    assert observed.entry_quote is not None
    context = DecisionInput(
        config=config,
        memory=memory,
        owned=(held,),
        observations=(observed,),
        trigger=DecisionSchedule().event(observed.effective_at),
    )
    return strategy_decision(definition, context), observed


def test_trading_reference_enters_once_although_the_entry_quote_repeats():
    first, _ = decide("trading", Memory(), {"entry_complete": False})
    assert first.intent.kind == "swap_exact_input"
    # After first exposure the entry slice is still quoted, and the reference declines it.
    again, observed = decide("trading", Memory(), {"entry_complete": True})
    assert observed.entry_quote is not None and again.intent is None


def test_lp_reference_does_not_swap_again_after_its_first_entry():
    first, _ = decide("lp", Memory(), {"entry_complete": False})
    assert first.intent.kind == "swap_exact_input"
    entered, _ = decide("lp", Memory(), {"entry_complete": True})
    assert entered.intent.kind == "open_liquidity"
