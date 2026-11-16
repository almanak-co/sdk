"""Connector projections share immutable capture identity and a browser protocol."""

import hashlib
import time

from almanak.connectors.composition import connector_for
from almanak.dashboard.models import Snapshot


def project(state, *, captured_at=None):
    if any(item.fault and item.fault.invalidated for item in state.work):
        raise ValueError("invalidated chain evidence cannot produce a financial dashboard")
    context, snapshot = connector_for(state.config).projection.dashboard(
        state, captured_at=captured_at
    )
    if context is not None and (tables := asset_names(state, context.datasets)):
        context = context.model_copy(update={"datasets": {**context.datasets, **tables}})
    labels = symbols(state, snapshot.values)
    if not labels:
        return context, snapshot
    snapshot = snapshot.model_copy(update={"values": {**snapshot.values, **labels}})
    if context is not None:
        context = context.model_copy(
            update={
                "values": {
                    **context.values,
                    **{k: v for k, v in labels.items() if k in context.values},
                }
            }
        )
    return context, snapshot


def symbols(state, values) -> dict:
    """Token amounts a family projected without a symbol, named from the strategy's asset
    metadata (or the chain's native coin), so no amount reads as bare "asset units"."""
    from almanak.dashboard.tables import decimals_of, symbol_of

    labelled = {}
    for key, value in values.items():
        if value.unit != "base_units" or value.asset is None:
            continue
        update: dict = {}
        if not value.symbol and (symbol := symbol_of(state, value.asset)):
            update["symbol"] = symbol
        if value.decimals is None and (decimals := decimals_of(state, value.asset)) is not None:
            update["decimals"] = decimals
        if update:
            labelled[key] = value.model_copy(update=update)
    return labelled


def asset_names(state, datasets) -> dict:
    """Tables whose text cells hold a bare asset identity, read as its symbol and a short form
    of the identity (`WETH · 0x82af…bab1`); the full identity stays on the row's values."""
    from almanak.caip import AssetType
    from almanak.connectors.dex_dashboard import symbol_of

    named = {}
    for key, dataset in datasets.items():
        rows, changed = [], False
        for row in dataset.rows:
            new = dict(row)
            for column, cell in row.items():
                if isinstance(cell, str) and cell.startswith("eip155:") and "/" in cell:
                    try:
                        symbol = symbol_of(state, AssetType(cell))
                    except ValueError:
                        symbol = None
                    if symbol:
                        new[column], changed = f"{symbol} · {short_asset(cell)}", True
            rows.append(new)
        if changed:
            named[key] = dataset.model_copy(update={"rows": tuple(rows)})
    return named


def digest(model) -> str:
    return hashlib.sha256(model.model_dump_json().encode()).hexdigest()


def record_digest(state) -> str:
    """The checkpoint's identity for display: the multi-chain scheduler cursor advances on
    every tick and changes nothing a dashboard shows, so it is left out."""
    return hashlib.sha256(state.model_dump_json(exclude={"scheduler_cursor"}).encode()).hexdigest()


def snapshot(state, values, captured_at):
    times = [v.block.timestamp for v in values.values() if v.block]
    notices = (
        "Retained snapshot, not a live feed. Recorded mode does not establish process health.",
        "Balances are sampled around transactions; readings at each decision are kept for "
        "charts. Fiat PnL is unavailable.",
        "Strategy explanations and holding times are author-owned; "
        "execution and ownership use retained facts.",
    )
    return Snapshot(
        deployment_id=state.deployment_id,
        strategy_hash=state.strategy.sha256 if state.strategy else None,
        config_hash=digest(state.config),
        record_hash=record_digest(state),
        captured_at=int(time.time()) if captured_at is None else captured_at,
        mode=state.mode,
        initialized=all(w.initialized for w in state.work) if state.work else state.initialized,
        chains=tuple(dict.fromkeys(b.profile.fund.chain for b in state.config.accounts.bindings)),
        fork_origin=getattr(state.config, "fork_origin", None),
        # A venue action signed by an agent has no transaction hash (blueprint 30, 13.3).
        pending_hash=getattr(state.pending, "tx_hash", None),
        reserved_attempt=state.plan.attempt_id if state.plan else None,
        undelivered_outcomes=len(state.outcomes) - state.delivered,
        latest_observation=max(times) if times else None,
        notices=notices,
        values=values,
    )


def short_asset(identity: str) -> str:
    """`0x82af…bab1` for a token, `native` for a chain's coin."""
    namespace, _, reference = identity.rpartition("/")[2].partition(":")
    if namespace == "slip44":
        return "native"
    return f"{reference[:6]}…{reference[-4:]}" if len(reference) > 12 else reference
