"""Read-only projections for terminal, JSON and portable HTML reports."""

import json
import sqlite3
from pathlib import Path
from urllib.parse import quote

from almanak.connectors.composition import checkpoint_from_json
from almanak.presentation import emit, render_text
from almanak.report_values import value_html as value_html

# The last production report and the checkpoint body it was built from. A supervised
# deployment is asked for status every few seconds; the report is a function of the body alone,
# so an unchanged checkpoint is not validated and projected again.
_last_report: tuple[str, str] | None = None


def checkpoint(root):
    body = checkpoint_body(root)
    return None if body is None else json.loads(body)


def checkpoint_body(root) -> str | None:
    database = root / "state.sqlite"
    if not database.exists():
        return None
    with sqlite3.connect(f"file:{quote(str(database.resolve()))}?mode=ro", uri=True) as db:
        try:
            row = db.execute("SELECT body FROM checkpoint WHERE id=1").fetchone()
        except sqlite3.OperationalError:
            # The file is created before its schema is committed, so a reader can
            # observe an empty database during coordinator startup. That is a
            # not-yet-started run, not an error.
            return None
    return row[0] if row else None


def retained(root):
    global _last_report
    body = checkpoint_body(root)
    if body is None:
        return {"mode": "not_started", "closed": False}
    if _last_report is not None and _last_report[0] == body:
        return json.loads(_last_report[1])
    wire = json.loads(body)
    if "config" in wire:
        # Production runtime checkpoints carry validated config and take the
        # connector-owned structured report. Backtest checkpoints have no
        # config key and fall through to the activity projection below.
        from almanak.reporting import report

        result = report(checkpoint_from_json(body))
        _last_report = (body, json.dumps(result))
        return result
    result = {
        "mode": wire.get("mode"),
        "run_id": wire.get("run_id"),
        "closed": wire.get("mode") == "closed",
        "pending": wire.get("pending"),
        "simulation_time": wire.get("time"),
        "portfolio": wire.get("portfolio"),
        "memory": wire.get("memory"),
        "measurement": "retained simulated quantities",
    }
    if (root / "report.json").exists():
        last = json.loads((root / "report.json").read_text())
        if wire.get("mode") == "finished":
            # The finished run's own report is the projection; it is not repeated beside it.
            result = {**last, **result, "closed": last.get("closed", False)}
        else:
            result["last_completed_report"] = last
    manifest = None
    if (root / "manifest.json").exists():
        manifest = json.loads((root / "manifest.json").read_text())
        result["assumptions"] = manifest.get("settings")
        result["cadence"] = manifest.get("interval")
    result["activity"] = simulation_activity(wire)
    result["series"] = simulation_series(wire, manifest)
    return result


def string_amount(delta):
    """Base-unit amounts serialize as decimal strings; JSON numbers cannot hold them exactly."""
    if isinstance(delta, dict) and isinstance(delta.get("amount"), int):
        return {**delta, "amount": str(delta["amount"])}
    return delta


def simulation_activity(wire):
    """Project completed intents and liquidation events into one chronological list.

    Rows sort by effective simulation time, ascending: ``eligible_at`` for completed
    intents and the recorded event time for liquidations. Completed intents precede
    events recorded at equal times (stable sort over intents-first input).
    """
    timed = []
    for entry in wire.get("completed", []):
        pending = entry["pending"]
        timed.append(
            (
                pending["eligible_at"],
                {
                    "action": pending["intent"]["kind"],
                    "scope": pending["intent"].get("target"),
                    "decision_time": pending.get("decision_time"),
                    "eligible_at": pending["eligible_at"],
                    "complete": entry["transition"].get("complete", True),
                    "asset_changes": [
                        string_amount(delta)
                        for delta in entry["transition"].get("effects", {}).get("assets", [])
                    ],
                },
            )
        )
    for event in wire.get("events", []):
        for effect in liquidation_effects(event):
            timed.append((event["time"], liquidation_row(event, effect)))
    timed.sort(key=lambda pair: pair[0])
    return [row for _, row in timed]


def liquidation_effects(event):
    """Liquidation facts of a recorded event; input-only events carry none."""
    result = event.get("transition", {}).get("result", {})
    if not isinstance(result, dict) or result.get("kind") != "liquidation":
        return ()
    return tuple(result.get("liquidations", ()))


def liquidation_row(event, effect):
    # Liquidations change position state, not wallets, so the row carries no
    # asset_changes. The debt target names the liquidated position.
    row = {
        "action": "liquidation",
        "eligible_at": event["time"],
        "complete": True,
        "liquidation": {
            "collateral_asset": effect["collateral_removed"]["asset"],
            "collateral_removed": str(effect["collateral_removed"]["amount"]),
            "debt_asset": effect["debt_restored"]["asset"],
            "debt_restored": str(effect["debt_restored"]["amount"]),
            "fee_shares": str(effect["fee_shares"]),
        },
    }
    if effect.get("debt") is not None:
        row["scope"] = effect["debt"]
    return row


def simulation_series(wire, manifest=None):
    """Replay retained effects into a per-step portfolio series in token units.

    The initial point carries the manifest's frozen initial portfolio at the
    window start. Each completed intent adds a step point after its effects
    apply; connector event effects (for example liquidations) fold into the
    running state without their own points, and events precede intents recorded
    at equal times, matching engine application order. The final point carries
    the checkpoint portfolio at the checkpoint time. Replay self-checks against
    that final portfolio: schema drift or unknown shapes mark the series
    ``complete: false`` instead of failing. Zero-amount wallet entries are
    kept, since they mark exited assets. Wallet amounts serialize as decimal
    strings; JSON numbers cannot hold base units exactly. Position payloads
    pass through verbatim; no revaluation happens here.
    """
    initial = initial_portfolio(manifest)
    schedule = wire.get("schedule")
    start = schedule.get("start") if isinstance(schedule, dict) else None
    complete = initial is not None and start is not None
    points = []
    wallets: dict[str, dict] = {}
    positions: dict[str, dict] = {}
    if complete:
        seed_state(wallets, positions, initial)
        points.append(series_point(start, "initial", wallets, positions))
        for time, pending, effects in replay_steps(wire):
            if apply_effects(wallets, positions, effects):
                complete = False
            if pending is not None:
                points.append(series_point(time, "step", wallets, positions, pending))
    portfolio = wire.get("portfolio")
    time = wire.get("time")
    if isinstance(portfolio, dict) and time is not None:
        if complete and portfolio_differs(wallets, positions, portfolio):
            complete = False
        points.append(final_point(time, portfolio))
    return {"complete": complete, "points": points}


def initial_portfolio(manifest):
    """Initial portfolio from the manifest's frozen initial state.

    Falls back to the funds inventory (the same mapping ``Fund.inventory``
    applies) when the initial state does not carry a portfolio; positions then
    start empty. Returns None when neither source is retained.
    """
    if not isinstance(manifest, dict):
        return None
    initial = manifest.get("initial")
    if isinstance(initial, dict) and isinstance(initial.get("portfolio"), dict):
        return initial["portfolio"]
    settings = manifest.get("settings")
    funds = settings.get("funds") if isinstance(settings, dict) else None
    if not isinstance(funds, list):
        return None
    balances = []
    for fund in funds:
        if not isinstance(fund, dict):
            return None
        account = fund.get("fund")
        if not isinstance(account, dict):
            return None
        for amount in fund.get("balances", []):
            balances.append(
                {
                    "key": {
                        "account": fund.get("name"),
                        "chain": account.get("chain"),
                        "asset": amount.get("asset"),
                    },
                    "amount": amount.get("amount"),
                }
            )
    return {"balances": balances, "positions": []}


def series_identity(key):
    """Stable identity for a wallet asset or position key: its full sorted JSON."""
    return json.dumps(key, sort_keys=True, separators=(",", ":"))


def seed_state(wallets, positions, initial):
    for balance in initial.get("balances", []):
        if isinstance(balance, dict) and "key" in balance:
            wallets[series_identity(balance["key"])] = {
                "key": balance["key"],
                "amount": balance.get("amount", 0),
            }
    for position in initial.get("positions", []):
        if isinstance(position, dict) and "key" in position:
            positions[series_identity(position["key"])] = position


def replay_steps(wire):
    """(time, pending, effects) steps in engine application order.

    Events rank before completed intents at equal times because the engine
    applies every event up to an intent's eligibility before executing it.
    """
    steps = [
        (event["time"], 0, None, (event.get("transition") or {}).get("effects"))
        for event in wire.get("events", [])
        if isinstance(event.get("time"), int)
    ]
    steps += [
        (
            entry["pending"]["eligible_at"],
            1,
            entry["pending"],
            (entry.get("transition") or {}).get("effects"),
        )
        for entry in wire.get("completed", [])
        if isinstance(entry.get("pending"), dict)
        and isinstance(entry["pending"].get("eligible_at"), int)
    ]
    steps.sort(key=lambda step: (step[0], step[1]))
    return [(time, pending, effects) for time, _, pending, effects in steps]


def apply_effects(wallets, positions, effects):
    """Apply one transition's effects; True when the retained shape drifts."""
    if not isinstance(effects, dict):
        return True
    drift = apply_asset_deltas(wallets, effects.get("assets", []))
    return apply_position_changes(positions, effects.get("positions", [])) or drift


def apply_asset_deltas(wallets, deltas):
    drift = False
    for delta in deltas:
        if (
            not isinstance(delta, dict)
            or "key" not in delta
            or not isinstance(delta.get("amount"), int)
        ):
            drift = True
            continue
        identity = series_identity(delta["key"])
        if identity not in wallets:
            # Engine rules bind deltas to initialized wallet assets; an unknown key is drift.
            drift = True
            wallets[identity] = {"key": delta["key"], "amount": 0}
        wallets[identity]["amount"] += delta["amount"]
        if wallets[identity]["amount"] < 0:
            drift = True
    return drift


def apply_position_changes(positions, changes):
    drift = False
    for change in changes:
        if not isinstance(change, dict):
            drift = True
            continue
        before, after = change.get("before"), change.get("after")
        current = after if after is not None else before
        if not isinstance(current, dict) or "key" not in current:
            drift = True
            continue
        identity = series_identity(current["key"])
        if positions.get(identity) != before:
            drift = True
        if after is None:
            positions.pop(identity, None)
        else:
            positions[identity] = after
    return drift


def portfolio_differs(wallets, positions, portfolio):
    """Self-check: replayed state against the checkpoint's final portfolio."""
    expected_wallets = {
        series_identity(balance["key"]): balance.get("amount")
        for balance in portfolio.get("balances", [])
        if isinstance(balance, dict) and "key" in balance
    }
    if {identity: entry["amount"] for identity, entry in wallets.items()} != expected_wallets:
        return True
    expected_positions = {
        series_identity(position["key"]): position.get("payload")
        for position in portfolio.get("positions", [])
        if isinstance(position, dict) and "key" in position
    }
    return {
        identity: position.get("payload") for identity, position in positions.items()
    } != expected_positions


def series_point(time, source, wallets, positions, pending=None):
    point = {"time": time, "source": source}
    if pending is not None:
        intent = pending.get("intent")
        point["action"] = intent.get("kind") if isinstance(intent, dict) else None
        point["decision_time"] = pending.get("decision_time")
    if wallets:
        point["wallets"] = [
            {"key": wallets[identity]["key"], "amount": str(wallets[identity]["amount"])}
            for identity in sorted(wallets)
        ]
    if positions:
        point["positions"] = [
            {"target": positions[identity]["key"], "payload": positions[identity].get("payload")}
            for identity in sorted(positions)
        ]
    return point


def final_point(time, portfolio):
    point = {"time": time, "source": "final"}
    balances = [
        string_amount(b)
        for b in portfolio.get("balances", [])
        if isinstance(b, dict) and "key" in b
    ]
    if balances:
        point["wallets"] = sorted(balances, key=lambda balance: series_identity(balance["key"]))
    positions = [
        {"target": position["key"], "payload": position.get("payload")}
        for position in portfolio.get("positions", [])
        if isinstance(position, dict) and "key" in position
    ]
    if positions:
        point["positions"] = sorted(positions, key=lambda entry: series_identity(entry["target"]))
    return point


def support_filters(connector=None, chain=None):
    """Validate the --connector and --chain filters as canonical identities."""
    from almanak.caip import ChainId
    from almanak.configuration.selection import require_connector
    from almanak.configuration.yaml import ConfigurationError

    selected = (
        require_connector(connector, field="--connector", file="<command line>")
        if connector is not None
        else None
    )
    try:
        selected_chain = ChainId(chain) if chain is not None else None
    except ValueError as error:
        raise ConfigurationError(str(error), file="<command line>", field="--chain") from error
    return selected, selected_chain


def gas_lifecycle(binding) -> dict:
    """The connector's per-kind entry and exit transaction counts, from its funding table."""
    return binding.execution.gas_lifecycle.model_dump(mode="json")["kinds"]


def support(connector=None, chain=None):
    from almanak.connectors.composition import installed_connectors

    selected, selected_chain = support_filters(connector, chain)
    entries = {
        str(name): [
            s.model_dump(mode="json") | {"gas_lifecycle": gas_lifecycle(binding)}
            for s in binding.chain_support
            if selected_chain is None or s.chain == selected_chain
        ]
        for name, binding in installed_connectors()
        if selected is None or name == selected
    }
    return {name: rows for name, rows in entries.items() if rows}


def project(root):
    facts = retained(root)
    wire = checkpoint(root) or {}
    metadata = wire.get("config", {}).get("asset_metadata", {})
    if not metadata and (root / "manifest.json").exists():
        metadata = (
            json.loads((root / "manifest.json").read_text())
            .get("config", {})
            .get("asset_metadata", {})
        )
    return {
        "title": "Strategy report",
        "source": str(root),
        "evidence": "Recorded evidence only; no fresh chain audit",
        **facts,
        "asset_metadata": metadata,
        "next_commands": [f"almanak status --run {root}"]
        if (root / "workspace.json").exists()
        else [],
    }


def custom_dashboard(root):
    wire = checkpoint(root)
    if wire is None or "config" not in wire:
        return (
            None,
            "Custom dashboard is unavailable for this snapshot; shared report remains available.",
        )
    from almanak.dashboard.evaluate import evaluate, evaluate_input

    try:
        state = checkpoint_from_json(json.dumps(wire))
        # The retained input carries the history the runtime kept for charts; the checkpoint alone
        # has no readings between transactions.
        retained = root / "dashboard" / "input.json"
        result = (
            evaluate_input(state, json.loads(retained.read_text()))
            if retained.is_file()
            else evaluate(state)
        )
        return result, result.diagnostic
    except (ValueError, TypeError, AttributeError):
        return None, "Custom dashboard could not be evaluated; shared report remains available."


def run_dashboard(root, workspace=None):
    """The strategy's own dashboard for a run, as its deployment would draw it. A fork test ends
    by winding everything down, so it is drawn from the last input taken while the strategy ran
    (`dashboard/running.json`) when the run kept one, else from the checkpoint.

    With `workspace`, the workspace's current strategy and dashboard.py draw over the run's
    data instead of the source the run captured: an edited dashboard is seen without a new run."""
    from almanak.dashboard.evaluate import evaluate, evaluate_input
    from almanak.dashboard.export import RUNNING_INPUT
    from almanak.strategy import StrategyDefinition

    wire = checkpoint(root)
    if wire is None or "config" not in wire:
        raise ValueError("this run has no checkpoint to draw a dashboard from")
    state = checkpoint_from_json(json.dumps(wire))
    # The workspace changes only which dashboard draws; the snapshot still names the run.
    current = None
    if workspace is not None:
        current = StrategyDefinition.read(Path(workspace) / "strategy.py")
    running = root / "dashboard" / RUNNING_INPUT
    if running.is_file():
        return evaluate_input(state, json.loads(running.read_text()), current)
    return evaluate(state, strategy=current)


def render_html(report, *, custom=None, diagnostic=None):
    from almanak.report_layout import render

    return render(report, custom=custom, diagnostic=diagnostic)


def references(connector=None, chain=None):
    from almanak.references import catalog

    selected, selected_chain = support_filters(connector, chain)
    return catalog(
        None if selected is None else str(selected),
        None if selected_chain is None else str(selected_chain),
    )


def support_command(args):
    connector, chain = getattr(args, "connector", None), getattr(args, "chain", None)
    schema = getattr(args, "schema", None)
    if schema is not None:
        if connector is not None or chain is not None:
            raise ValueError("--schema cannot be combined with --connector or --chain")
        from almanak.schemas import bundle

        emit(bundle(schema), args)
    elif getattr(args, "references", False):
        emit(references(connector, chain), args)
    else:
        emit(support(connector, chain), args)
    return 0


def command(args):
    if args.product == "support":
        return support_command(args)
    if getattr(args, "dashboard", False):
        if args.format == "html":
            raise ValueError(
                "--dashboard writes JSON or text; the HTML report embeds its dashboard"
            )
        workspace = args.workspace.resolve() if args.workspace else None
        response = run_dashboard(args.run.resolve(), workspace).model_dump(mode="json")
        if args.output is not None:
            text = (
                render_text(response) if args.format == "text" else json.dumps(response, indent=2)
            )
            args.output.write_text(text + "\n", encoding="utf-8")
        else:
            emit(response, args)
        return 0
    report = project(args.run.resolve())
    if args.format == "html":
        if args.output is None:
            raise ValueError("HTML reports require --output")
        custom, diagnostic = custom_dashboard(args.run.resolve())
        body = render_html(report, custom=custom, diagnostic=diagnostic)
    else:
        if args.output is None:
            emit(report, args)
            return 0
        body = render_text(report) if args.format == "text" else json.dumps(report, indent=2)
    args.output.write_text(body)
    emit({"report": str(args.output.resolve()), "evidence": report["evidence"]})
    return 0
