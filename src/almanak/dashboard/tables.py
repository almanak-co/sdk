"""Table conventions every connector's dashboard shares: tokens by symbol, a fund account
named only when there are several, and one row per committed transaction."""

from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from almanak.chains import evm_network
from almanak.dashboard.bound import MAX_TABLE_ROWS
from almanak.dashboard.models import Column, Dataset, Ref, Scope, Value

# SLIP-44 coin types of the supported chains' native coins.
NATIVE_SYMBOLS = {"60": "ETH", "9005": "AVAX", "714": "BNB", "966": "POL", "2457": "HYPE"}


def symbol_of(state, asset):
    metadata = state.config.asset_metadata.get(asset)
    if metadata and metadata.symbol:
        return metadata.symbol
    if getattr(asset, "namespace", None) == "slip44":
        return NATIVE_SYMBOLS.get(asset.reference)
    return wrapped_native_symbol(asset)


# Every supported chain's native coin and wrapped native token (WETH, WAVAX, WBNB, WPOL, WHYPE)
# has 18 decimals.
WRAPPED_NATIVE_DECIMALS = 18


def decimals_of(state, asset):
    """An amount's decimals: the declared metadata's, else 18 for the chain's native coin or
    wrapped native token, so a symbol is never shown beside an amount still in base units."""
    metadata = state.config.asset_metadata.get(asset)
    if metadata is not None:
        return metadata.decimals
    native = getattr(asset, "namespace", None) == "slip44" and asset.reference in NATIVE_SYMBOLS
    if native or wrapped_native_symbol(asset):
        return WRAPPED_NATIVE_DECIMALS
    return None


def wrapped_native_symbol(asset):
    """WETH for Arbitrum's wrapped ether and the like: a protocol can pay in the chain's
    wrapped native token (GMX execution fees) without the strategy declaring it."""
    chain = getattr(asset, "chain", None)
    if chain is None:
        return None
    try:
        network = evm_network(chain)
    except ValueError:
        return None
    if asset != network.wrapped_native:
        return None
    native = NATIVE_SYMBOLS.get(network.native_asset.reference)
    return "W" + native if native else None


def several_accounts(state) -> bool:
    """Whether the strategy is configured with more than one fund account."""
    accounts = getattr(state.config, "accounts", None)
    return len(getattr(accounts, "bindings", ())) > 1


def named_accounts(columns, rows, key="account", *, keep=False):
    """Drop the `key` column when the strategy has one fund account (`keep` is False) and the
    rows name at most one: a lone account column reads as a choice between wallets the
    strategy does not have. A strategy with several keeps it even when one holds everything."""
    if keep or len({row[key] for row in rows}) > 1:
        return tuple(columns), tuple(rows)
    return (
        tuple(c for c in columns if c.key != key),
        tuple({k: v for k, v in row.items() if k != key} for row in rows),
    )


def accounts_dataset(*, columns, rows, coverage, truncated=False, key="account", keep=False):
    """A Dataset whose fund account column appears only when its rows name several."""
    columns, rows = named_accounts(columns, rows, key, keep=keep)
    return Dataset(columns=columns, rows=rows, coverage=coverage, truncated=truncated)


def token_dataset(rows, coverage, *, position=True, keep=False):
    """Position / (Fund account) / Token / Amount rows; the account column only when they
    name more than one. `position=False` for balances held by the fund, not a position."""
    columns = (
        *((Column(key="allocation", label="Position"),) if position else ()),
        Column(key="account", label="Fund account"),
        Column(key="token", label="Token"),
        Column(key="amount", label="Amount", kind="value"),
    )
    columns, rows = named_accounts(columns, rows, keep=keep)
    return Dataset(columns=columns, rows=rows, coverage=coverage)


def utc(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d %H:%M UTC")


def payer_of(state, outcome, account):
    execution = getattr(outcome, "execution", None)
    if execution is not None:
        return execution.submitter
    binding = next((b for b in state.config.accounts.bindings if b.name == account), None)
    return binding.profile.submitter if binding else None


def transactions_dataset(
    state,
    values,
    describe: Callable[[Any, Any], str],
    *,
    extra: Iterable[tuple[Column, Callable[[Any, dict], str | Ref]]] = (),
):
    """One row per committed transaction the strategy submitted, newest MAX_TABLE_ROWS: what
    it did (`describe(state, outcome)`), the native gas its receipt paid and when its block
    was mined. Outcomes without a receipt of ours (a keeper's execution) are not rows. Each
    `extra` (column, cell(outcome, values)) adds a connector-specific column."""
    extra = tuple(extra)
    submitted = [o for o in state.outcomes if getattr(o, "gas_paid", None) is not None]
    kept = submitted[-MAX_TABLE_ROWS:]
    # Several positions name theirs even when only one traded, or the cut kept only one.
    traded = {o.operation.target.allocation for o in kept}
    several = len({owned.target.allocation for owned in state.owned} | traded) > 1
    rows: list[dict[str, str | Ref]] = []
    for outcome in kept:
        key = "tx-" + outcome.tx_hash.removeprefix("0x")
        asset = outcome.gas_paid.asset
        target = outcome.operation.target
        values[key] = Value(
            id=key,
            amount=str(outcome.gas_paid.amount),
            unit="base_units",
            asset=asset,
            decimals=decimals_of(state, asset),
            symbol=symbol_of(state, asset),
            scope=Scope(
                account=target.account,
                fund=target.fund,
                payer=payer_of(state, outcome, target.account),
            ),
            evidence="recorded",
            block=outcome.after.block,
            reason=f"Native gas from the receipt of {outcome.tx_hash}.",
        )
        label = describe(state, outcome)
        row: dict[str, str | Ref] = {
            "transaction": label if outcome.success else f"{label} (reverted)",
            **{column.key: cell(outcome, values) for column, cell in extra},
            "gas": Ref(ref=key),
            "time": utc(outcome.after.block.timestamp),
        }
        rows.append({"allocation": target.allocation, **row} if several else row)
    columns = (
        Column(key="transaction", label="Transaction"),
        *(column for column, _ in extra),
        Column(key="gas", label="Receipt gas", kind="value"),
        Column(key="time", label="Time"),
    )
    return Dataset(
        columns=(Column(key="allocation", label="Position"), *columns) if several else columns,
        rows=tuple(rows),
        truncated=len(submitted) > MAX_TABLE_ROWS,
        coverage="Committed transactions the strategy submitted, newest last; payer totals count "
        "every receipt. Fiat conversion excluded.",
    )
