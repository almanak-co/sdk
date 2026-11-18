"""Author checks that an operator would otherwise find on a fork run or in production.

Every finding is a `warnings` entry with `code`, `field` and `message`. Gas limit,
gas amount and gas lower-bound findings read the author file and the shipped gas
references only. Gas price findings need the current market, so only an online
check reports them. A Uniswap V4 liquidity allocation whose entry quote is sized from
less than its capital is flagged from the author file too. Every finding is advisory.
"""

import json
from decimal import Decimal
from functools import cache
from importlib.resources import files

from almanak.chains import POLYGON
from almanak.configuration.input_values import resolve_quantity
from almanak.connectors.gas_lifecycle import (
    LIQUIDITY_ENTRY,
    MINIMUM_LIFECYCLE,
    TRADE_ENTRY,
)
from almanak.gateway.evm import native_metadata

# The gas references are measured through Safe/Roles execution.
PROFILE = "safe_roles"
# A per-transaction fee or budget above this many whole native units is implausible
# and usually means base units were written as a decimal.
MAX_NATIVE_UNITS = 10
# A cheap native coin makes a real budget many whole units (10 POL is a few dollars); the
# warning still catches base units written as whole units, which are orders larger.
CHAIN_MAX_NATIVE_UNITS = {POLYGON.chain: 10_000}
GAS_AMOUNTS = ("max_transaction_fee", "total_budget", "exit_reserve")
BPS = 10_000
# EIP-3529 caps a refund at a fifth of the gas a transaction consumed, so the estimate
# execution buffers can exceed the receipt's gasUsed by up to a quarter. A native mint on
# Robinhood Chain recorded 1,100,547 gasUsed but estimated about 1,273,866 on a later fork.
REFUND_ESTIMATE_BPS = 12_500
# A live coordinator starts a fresh sandboxed interpreter for every evaluation, and its
# trusted startup alone took 8 to 11 seconds on a CPU-limited container. A deployment
# that decided every 5 seconds kept its container near one core and was stopped by worker
# timeouts. Live runs flag intervals shorter than this.
MIN_LIVE_DECISION_INTERVAL_SECONDS = 30
# An allocation whose entry_swap_bps splits capital into more slices than this puts that
# many fee ceilings (each slice is its own swap and approvals) into its payer's entry
# minimum; ten already multiplies a single-swap trade's entry funding tenfold.
MAX_ENTRY_SLICES = 10
# A max_transaction_fee more than this many times what max_gas_limit costs at the
# highest bid of the recent window is not headroom. The payer's entry minimum is
# entry_transactions x max_transaction_fee + exit_reserve, so the excess multiplies into
# the funding requirement. AI-authored configs sized fees for 10 to 1000 gwei on chains
# quoting 0.001 to 0.2 gwei, which put the funding requirement 20 to 1000x above cost.
OVERSIZED_PRICE_FACTOR = 20
# The oversized check reads the policy's bid over this many blocks ending at the checked
# block, so one quiet block does not make an ordinary fee look oversized.
WINDOW_BLOCKS = 100
# The suggested max_transaction_fee pays for max_gas_limit and the L1 fee at this many
# times the window's highest bid; a larger spike makes transactions wait, losing nothing.
SUGGESTED_SPIKE_HEADROOM = 5
# Calldata length the OP Stack L1 data fee bound is read for when no transaction exists:
# above the Safe/Roles calldata the SDK sends.
L1_DATA_BYTES = 2_000


def warning(code: str, field: str, message: str) -> dict:
    return {"code": code, "field": field, "message": message}


@cache
def gas_references() -> tuple[dict, ...]:
    path = files("almanak.connectors").joinpath("gas_references.json")
    return tuple(json.loads(path.read_text())["references"])


def offline_warnings(inputs) -> list[dict]:
    result = []
    for name, policy in inputs.strategy.gas.items():
        result.extend(gas_limit_warnings(inputs, name, policy))
        result.extend(gas_amount_warnings(name, policy))
        result.extend(gas_bound_warnings(inputs, name, policy))
        result.extend(entry_slice_warnings(inputs, name, policy))
    result.extend(entry_budget_warnings(inputs))
    return result


def entry_budget_warnings(inputs) -> list[dict]:
    """A Uniswap V4 liquidity entry quote is sized from `entry_swap_bps` of capital and is
    offered only while no position is held (uniswap_v4/observations.py), so after the usual
    single entry swap the mint pairs only that share. A banded allocation is skipped: it
    recentres through `rebalance_quote`, sized against all inventory, and mints all of it.
    The V3 pool family sizes only the swap, and trading entries repeat. Zero offers no
    entry quote, so it states that the allocation deposits without an entry swap."""
    result = []
    for name, selection in inputs.strategy.allocations.items():
        if getattr(selection, "connector", None) != "uniswap_v4":
            continue
        bps = selection.entry_swap_bps
        if selection.kind != "liquidity" or selection.band is not None or not 0 < bps < BPS:
            continue
        result.append(
            warning(
                "entry_budget_partial",
                f"strategy.allocations.{name}.entry_swap_bps",
                f"entry_swap_bps {bps} sizes the Uniswap V4 liquidity entry quote from "
                f"{bps / 100:g}% of capital, and the quote is offered only while no position "
                "is held. With the usual single entry swap then open_liquidity, "
                f"{idle_capital(selection.capital, bps)} of capital is not swapped into the "
                "pair and stays idle in the funding asset. A strategy that swaps repeatedly "
                "before minting is not limited this way. Set entry_swap_bps to 10000 (the "
                "default) to size the entry from all of capital.",
            )
        )
    return result


def idle_capital(capital, bps: int) -> str:
    """Capital outside the entry budget, which execution sizes as `capital * bps // 10000`
    base units. Quoted decimals cannot be scaled offline, so their figure is approximate."""
    quantity = capital.quantity
    if isinstance(quantity, str):
        idle = Decimal(quantity) * (BPS - bps) / BPS
        return f"about {idle.normalize():f} {capital.asset}"
    units = quantity.base_units
    return f"{units - units * bps // BPS} base units of {capital.asset}"


def decision_interval_warnings(inputs) -> list[dict]:
    """A production or mode-agnostic check flags decisions too close for a live worker."""
    interval = inputs.strategy.decision_interval
    if interval >= MIN_LIVE_DECISION_INTERVAL_SECONDS:
        return []
    return [
        warning(
            "decision_interval",
            "strategy.decision_interval",
            f"decision_interval is {interval} seconds. Every decision starts a fresh sandboxed "
            "worker whose startup alone can take 8 to 11 seconds on a CPU-limited "
            "container, so a shorter interval can starve the deployment of CPU until its "
            "evaluations time out. Use at least "
            f"{MIN_LIVE_DECISION_INTERVAL_SECONDS} seconds for a live deployment.",
        )
    ]


def policy_connectors(inputs, accounts) -> set[str]:
    connectors = set()
    for group in (inputs.strategy.allocations, inputs.strategy.debts):
        for selection in group.values():
            market = inputs.markets.get(getattr(selection, "market", None))
            if market is not None and getattr(selection, "account", None) in accounts:
                connectors.add(str(market.connector))
    return connectors


def policy_variants(inputs, accounts) -> set[tuple[str, str]]:
    """(connector, `native`) for each connector whose liquidity market of these accounts
    names the chain's native coin; a native swap stays within the plain measurements."""
    variants = set()
    for group in (inputs.strategy.allocations, inputs.strategy.debts):
        for selection in group.values():
            market = inputs.markets.get(getattr(selection, "market", None))
            if market is None or getattr(selection, "account", None) not in accounts:
                continue
            if getattr(selection, "kind", None) != "liquidity":
                continue
            currencies = (getattr(market, "currency0", None), getattr(market, "currency1", None))
            if any(getattr(c, "namespace", None) == "slip44" for c in currencies):
                variants.add((str(market.connector), "native"))
    return variants


def largest_reference(connectors, chain, variants=frozenset()) -> dict | None:
    """The largest measured action of these connectors, on the policy chain when measured.

    A variant row (native-coin pools) bounds only policies whose markets of that connector
    use that variant.
    """
    groups: dict[tuple, list[dict]] = {}
    for r in gas_references():
        if r["connector"] in connectors and r["profile"] == PROFILE:
            if r.get("variant") is None or (r["connector"], r["variant"]) in variants:
                groups.setdefault((r["connector"], r.get("variant")), []).append(r)
    # Each connector and variant falls back to other chains only where it has no local
    # measurement, so one connector's local rows never hide another's larger action.
    chosen = [
        max([r for r in rows if r["chain"] == str(chain)] or rows, key=executable_gas)
        for rows in groups.values()
    ]
    return max(chosen, key=executable_gas, default=None)


def measured_gas(reference) -> int:
    """Gas an execution must estimate for the action: the call's estimate where it was
    re-measured, which exceeds the receipt's `gasUsed` when the call earns a refund."""
    return max(reference["gas_used"], reference.get("gas_estimate", 0))


def executable_gas(reference) -> int:
    """Gas execution may estimate for the action before its buffer.

    A re-measured estimate stands. A native-coin pool action unwraps and wraps the
    chain's wrapped coin and earns a storage refund, so without one its gasUsed is
    raised by the largest refund EIP-3529 allows.
    """
    if "gas_estimate" in reference or reference.get("variant") != "native":
        return measured_gas(reference)
    return -(-reference["gas_used"] * REFUND_ESTIMATE_BPS // BPS)


def gas_limit_warnings(inputs, name, policy) -> list[dict]:
    reference = largest_reference(
        policy_connectors(inputs, policy.accounts),
        policy.chain,
        policy_variants(inputs, policy.accounts),
    )
    if reference is None:
        return []
    buffer = policy.gas_limit_buffer_bps
    measured = executable_gas(reference)
    required = -(-measured * (BPS + buffer) // BPS)
    if policy.max_gas_limit >= required:
        return []
    source = reference["source"]
    chain = reference["chain"]
    if chain != str(policy.chain):
        chain += f", measured on another chain than {policy.chain}"
    return [
        warning(
            "gas_limit",
            f"strategy.gas.{name}.max_gas_limit",
            f"max_gas_limit {policy.max_gas_limit} is below {required}: {reference['connector']} "
            f"{reference['action']} needs {measured} gas through Safe/Roles on "
            f"{chain} (fork receipt {source['transaction']}{refund_note(reference, measured)}),"
            " and the SDK adds the "
            f"{buffer} bps gas_limit_buffer_bps to every estimate. Raise max_gas_limit to at "
            f"least {required}.",
        )
    ]


def refund_note(reference, measured) -> str:
    if measured == measured_gas(reference):
        return ""
    return f" used {reference['gas_used']}; its estimate includes a storage refund"


def gas_amount_warnings(name, policy) -> list[dict]:
    try:
        asset, native = native_metadata(policy.chain)
    except ValueError:
        return []
    result = []
    for field in GAS_AMOUNTS:
        if getattr(policy, field) is None:
            continue
        try:
            amount = resolve_quantity(getattr(policy, field), native.decimals)
        except ValueError:
            continue
        units = Decimal(amount).scaleb(-native.decimals)
        limit = CHAIN_MAX_NATIVE_UNITS.get(policy.chain, MAX_NATIVE_UNITS)
        if units > limit:
            result.append(
                warning(
                    "gas_amount_units",
                    f"strategy.gas.{name}.{field}",
                    f"{field} is {units.normalize():f} whole native units ({asset}); a gas "
                    f"amount above {limit} is implausible. Quoted decimals are "
                    "whole units; write base units as {base_units: N}.",
                )
            )
    return result


def lifecycle_transactions(inputs, accounts) -> tuple[int, int]:
    """The fewest entry and the exit transactions of the allocations these accounts pay for.

    Entries are lower bounds; exits are each allocation's `gas_lifecycle` exit, the
    count `almanak support` reports, and one where its connector counts none. A
    debt unwound in partial cycles adds `exit_per_unwind_cycle` per cycle, which the
    configuration cannot state, so these exits are the single-cycle floor.
    """
    from almanak.connectors.v3_pool_request import POOL_CONNECTORS

    entries = exits = 0
    for group in ("allocations", "debts"):
        for selection in getattr(inputs.strategy, group).values():
            if getattr(selection, "account", None) not in accounts:
                continue
            market = inputs.markets.get(getattr(selection, "market", None))
            entry = MINIMUM_LIFECYCLE[0]
            kinds = connector_kinds(market)
            if {"trade", "liquidity"} <= set(kinds) and str(market.connector) in POOL_CONNECTORS:
                entry = LIQUIDITY_ENTRY if is_liquidity(selection) else TRADE_ENTRY
            entries, exits = entries + entry, exits + selection_exit(group, selection, kinds)
    return entries, exits


def selection_exit(group, selection, kinds) -> int:
    """One allocation's or debt's exit from its connector's `gas_lifecycle` kinds."""
    if {"trade", "liquidity"} <= set(kinds):
        return kinds["liquidity" if is_liquidity(selection) else "trade"].exit
    if group == "debts" and "debt" in kinds:
        collateral = getattr(selection, "collateral", None) or ()
        return kinds["debt"].exits(items={"collateral": len(collateral)})
    if "lending" in kinds:
        lending = kinds["lending"]
        granted = [n for n in lending.exit_when_granted if getattr(selection, n, False)]
        return lending.exits(granted=granted)
    # A connector-prefixed kind such as gmx_position names its table kind.
    kind = str(getattr(selection, "kind", None)).rsplit("_", 1)[-1]
    if kind not in kinds and len(kinds) == 1:
        # A connector with one allocation kind (HyperCore perps) names none.
        (kind,) = kinds
    return kinds[kind].exits() if kind in kinds else MINIMUM_LIFECYCLE[1]


def is_liquidity(selection) -> bool:
    return bool(
        getattr(selection, "range", None)
        or getattr(selection, "band", None)
        or getattr(selection, "liquidity_enabled", False)
        or getattr(selection, "kind", None) == "liquidity"
    )


def connector_kinds(market) -> dict:
    """The `gas_lifecycle` kinds of a market's connector, else none."""
    if market is None:
        return {}
    from almanak.connectors.composition import installed_connectors

    bound = {str(name): b for name, b in installed_connectors()}.get(str(market.connector))
    kinds = getattr(getattr(bound, "execution", None), "gas_lifecycle", None)
    return {} if kinds is None else kinds.kinds


def unwind_cycle_note(inputs, accounts) -> str:
    """How a borrow these accounts pay for adds exits when it unwinds in cycles."""
    for selection in inputs.strategy.allocations.values():
        if getattr(selection, "account", None) not in accounts:
            continue
        lending = connector_kinds(inputs.markets.get(getattr(selection, "market", None)))
        cycle = lending.get("lending") and lending["lending"].exit_per_unwind_cycle
        if cycle and getattr(selection, "borrow", False):
            return (
                " These count one repayment. A strategy that resupplies what it bought (a "
                "leverage loop) withdraws collateral before its swap, 1 more exit, and each "
                f"further withdraw, swap, repay cycle of its unwind adds {cycle} plus the "
                "swapping allocation's exit (almanak support: exit_per_unwind_cycle)."
            )
    return ""


def gas_bound_warnings(inputs, name, policy) -> list[dict]:
    """Gas amounts that cannot pay for the lifecycle at the policy's own fee ceiling.

    Execution admits a transaction that adds exposure only while the spent budget,
    its fee and `exit_reserve` fit `total_budget`; exits draw on what is left. At the
    fee ceiling the budget therefore needs one fee per entry transaction on top of
    the reserve, and the reserve one fee per exit transaction. A fee ceiling below
    `max_gas_limit` x `max_gas_price_wei` refuses a transaction that both other
    ceilings admit. Without `total_budget` there is no lifetime cap to check.
    """
    try:
        fee, budget, reserve = (
            None
            if getattr(policy, field) is None
            else resolve_quantity(getattr(policy, field), native_metadata(policy.chain)[1].decimals)
            for field in GAS_AMOUNTS
        )
    except ValueError:
        return []
    if fee is None or reserve is None:
        return []
    prefix = f"strategy.gas.{name}"
    entries, exits = lifecycle_transactions(inputs, policy.accounts)
    result = []
    ceiling = policy.max_gas_limit * policy.max_gas_price_wei
    if fee < ceiling:
        result.append(
            warning(
                "gas_fee_ceiling",
                f"{prefix}.max_transaction_fee",
                f"max_transaction_fee {wei(fee)} is below max_gas_limit {policy.max_gas_limit} x "
                f"max_gas_price_wei {policy.max_gas_price_wei} = {wei(ceiling)}: a transaction "
                "both ceilings admit is refused when its gas times the quoted price exceeds the "
                f"fee ceiling, so at max_gas_limit the price can be at most "
                f"{fee // policy.max_gas_limit} wei. Raise max_transaction_fee to at least "
                f"{wei(ceiling)} or lower the gas ceilings to what you mean to pay.",
            )
        )
    required = entries * fee + reserve
    if budget is not None and budget < required:
        result.append(
            warning(
                "gas_budget",
                f"{prefix}.total_budget",
                f"total_budget {wei(budget)} is below {entries} entry transaction(s) x "
                f"max_transaction_fee {wei(fee)} + exit_reserve {wei(reserve)} = {wei(required)}. "
                f"Entering the allocations paid by {', '.join(policy.accounts)} takes at least "
                f"{entries} transactions, and each one must leave exit_reserve unspent, so at the "
                "fee ceiling the deployment stops partway through entry. Raise total_budget to at "
                f"least {wei(required)}.",
            )
        )
    if reserve < exits * fee:
        result.append(
            warning(
                "gas_exit_reserve",
                f"{prefix}.exit_reserve",
                f"exit_reserve {wei(reserve)} is below {exits} exit transaction(s) x "
                f"max_transaction_fee {wei(fee)} = {wei(exits * fee)}: once entries have spent "
                "the rest of total_budget, the reserve alone must pay for teardown, which can "
                f"take {exits} transactions (almanak support gas_lifecycle exits, each revocation "
                "they can need included). Raise exit_reserve to at least "
                f"{wei(exits * fee)} (and total_budget with it)."
                + unwind_cycle_note(inputs, policy.accounts),
            )
        )
    return result


def entry_slice_warnings(inputs, name, policy) -> list[dict]:
    """Allocations whose entry slices make the payer's entry minimum large.

    The funding statement counts one swap, with its approvals, per `entry_swap_bps`
    slice of capital (connectors/funding.entry_slices), at `max_transaction_fee` each.
    Offline the slice count is the lower bound ceil(10000 / entry_swap_bps); flooring
    the slice in base units can only add slices.
    """
    try:
        fee = resolve_quantity(
            policy.max_transaction_fee, native_metadata(policy.chain)[1].decimals
        )
    except ValueError:
        return []
    result = []
    for allocation, selection in inputs.strategy.allocations.items():
        bps = getattr(selection, "entry_swap_bps", None)
        if getattr(selection, "account", None) not in policy.accounts or not bps:
            continue
        slices = -(-BPS // bps)
        if slices <= MAX_ENTRY_SLICES:
            continue
        result.append(
            warning(
                "gas_entry_slices",
                f"strategy.allocations.{allocation}.entry_swap_bps",
                f"entry_swap_bps {bps} enters in at least {slices} slices of capital, each its "
                f"own swap with its approvals, so the entry minimum of the payer of "
                f"strategy.gas.{name} includes at least {slices} x max_transaction_fee "
                f"{wei(fee)} = {wei(slices * fee)} for this allocation's swaps alone. Raise "
                "entry_swap_bps to enter in fewer, larger swaps, or fund the payer for them.",
            )
        )
    return result


def wei(amount: int) -> str:
    return f"{amount} wei ({Decimal(amount).scaleb(-18).normalize():f} native)"


def market_errors() -> tuple[type[Exception], ...]:
    """Failures of one market read: a finding or a fallback, never a refusal."""
    from almanak.gateway.ethereum import DataError

    return (ValueError, RuntimeError, OSError, DataError)


def market_gas_warnings(policies, read_price, read_window=None, read_l1=None) -> list[dict]:
    """Gas ceilings against the market each policy's own settings quote now.

    The readers are injected so the provider stays with the online check.
    `read_price(policy)` returns the gas price execution would quote at the checked block;
    a failed read is a finding for its policy, never a refusal. `read_window(policy)`
    returns the highest bid over the WINDOW_BLOCKS blocks ending there and `read_l1(policy)`
    the SDK's OP Stack L1 data and operator fee bound for one max_gas_limit transaction;
    only the oversized finding uses them. Without a window it falls back to the single
    block and says so; without an L1 bound on an OP Stack chain it is skipped.
    """
    result = []
    for policy in policies:
        prefix = f"strategy.gas.{policy.name or ','.join(policy.accounts)}"
        try:
            price = read_price(policy)
        except market_errors() as error:
            result.append(
                warning(
                    "gas_price",
                    f"{prefix}.max_gas_price_wei",
                    f"the gas price ceiling was not checked: the fee quote failed: {error}",
                )
            )
            continue
        too_low = price_warnings(prefix, policy, price)
        result.extend(too_low or oversized_warnings(prefix, policy, price, read_window, read_l1))
    return result


def quote_basis(policy, price: int) -> str:
    return (
        f"the {policy.asset.chain} market quote with this policy's reward_percentile "
        f"{policy.reward_percentile} and base_fee_multiplier_bps "
        f"{policy.base_fee_multiplier_bps} is {price} wei"
    )


def price_warnings(prefix, policy, price: int) -> list[dict]:
    """Ceilings below the single-block quote, which make transactions wait now."""
    basis = quote_basis(policy, price)
    if policy.max_gas_price_wei < price:
        return [
            warning(
                "gas_price",
                f"{prefix}.max_gas_price_wei",
                f"max_gas_price_wei {policy.max_gas_price_wei} is below the current gas price: "
                f"{basis}, so every transaction is refused as gas_price until the market "
                f"falls. Raise max_gas_price_wei above {price} with headroom for spikes, "
                "with max_transaction_fee still at least max_gas_limit x max_gas_price_wei. "
                "An omitted max_gas_price_wei is max_transaction_fee / max_gas_limit: raise "
                "max_transaction_fee or lower max_gas_limit instead.",
            )
        ]
    fee = policy.max_transaction_fee.amount
    if fee < policy.max_gas_limit * price:
        return [
            warning(
                "gas_fee_quote",
                f"{prefix}.max_transaction_fee",
                f"max_transaction_fee {wei(fee)} pays for at most {fee // price} gas now: "
                f"{basis}, so a transaction whose buffered estimate is above that waits as "
                f"gas_fee_quote until gas falls, though max_gas_limit is {policy.max_gas_limit}.",
            )
        ]
    return []


def window_bid(policy, price: int, read_window) -> tuple[int, str]:
    """The highest bid over the window and how it was read, or the single-block quote."""
    reason = "no fee history window was read"
    if read_window is not None:
        try:
            bid = read_window(policy)
        except market_errors() as error:
            reason = f"the fee history window read failed: {error}"
        else:
            return bid, (
                f"the highest bid over the last {WINDOW_BLOCKS} {policy.asset.chain} blocks "
                f"with this policy's reward_percentile {policy.reward_percentile} and "
                f"base_fee_multiplier_bps {policy.base_fee_multiplier_bps} is {bid} wei"
            )
    return price, (
        f"{reason}, so this uses the single-block quote instead of the highest bid over the "
        f"last {WINDOW_BLOCKS} blocks: {quote_basis(policy, price)}"
    )


def l1_fee(policy, read_l1) -> int | None:
    """The OP Stack L1 data and operator fee bound for one transaction, 0 off OP Stack.

    None when it is needed and cannot be read: without it a fee that pays for L1 data
    would look oversized.
    """
    from almanak.chains import evm_network

    try:
        optimism = evm_network(policy.asset.chain).family == "optimism"
    except ValueError:
        return 0
    if not optimism:
        return 0
    if read_l1 is None:
        return None
    try:
        return read_l1(policy)
    except market_errors():
        return None


def oversized_warnings(prefix, policy, price: int, read_window, read_l1) -> list[dict]:
    """A max_transaction_fee so far above an executable fee that it only inflates funding.

    The executable fee is max_gas_limit x W + L1, with W the highest bid over the window
    and L1 the OP Stack L1 data and operator fee bound (0 elsewhere). The payer's entry
    minimum is entry_transactions x max_transaction_fee plus exit_reserve, so a fee many
    times the executable fee multiplies into the funding requirement. Only reached once
    no ceiling is below the single-block quote.
    """
    fee = policy.max_transaction_fee.amount
    limit = policy.max_gas_limit
    bid, basis = window_bid(policy, price, read_window)
    # L1 only lowers the factor, so a fee within it at the execution cost alone needs no read.
    if bid <= 0 or fee <= OVERSIZED_PRICE_FACTOR * limit * bid:
        return []
    l1 = l1_fee(policy, read_l1)
    if l1 is None:
        return []
    executable = limit * bid + l1
    suggested = significant_ceiling(SUGGESTED_SPIKE_HEADROOM * executable)
    if fee <= OVERSIZED_PRICE_FACTOR * executable or suggested >= fee:
        return []
    l1_part = f" + {l1} wei OP Stack L1 data and operator fee bound" if l1 else ""
    l1_spike = f" + {SUGGESTED_SPIKE_HEADROOM} x {l1} wei L1" if l1 else ""
    return [
        warning(
            "gas_ceiling_oversized",
            f"{prefix}.max_transaction_fee",
            f"max_transaction_fee {wei(fee)} is {fee // executable}x the executable fee "
            f"{wei(executable)} (max_gas_limit {limit} x {bid} wei{l1_part}): {basis}. The "
            "payer's entry minimum is entry_transactions x max_transaction_fee + exit_reserve, "
            "so the oversized fee inflates the funding requirement by the same factor. "
            f"Consider max_transaction_fee {wei(suggested)}, {limit} x {bid} wei x "
            f"{SUGGESTED_SPIKE_HEADROOM}{l1_spike} rounded up to two significant digits: a "
            f"spike beyond {SUGGESTED_SPIKE_HEADROOM}x makes transactions, exits included, wait "
            "until gas falls, and nothing is lost. Resize exit_reserve to match: exit "
            "transactions x the new fee. An omitted max_gas_price_wei follows the fee "
            "(max_transaction_fee / max_gas_limit); a stated one must come down with it.",
        )
    ]


def significant_ceiling(amount: int, digits: int = 2) -> int:
    """`amount` rounded up to `digits` significant digits, so a suggestion reads plainly."""
    scale = 10 ** max(len(str(amount)) - digits, 0)
    return -(-amount // scale) * scale
